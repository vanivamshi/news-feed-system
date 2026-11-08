#!/usr/bin/env python3
"""
News Feed system (a Twitter/X-style home timeline) in pure Python.

Implements the design from geonews/learn1.md:

  * Snowflake IDs                 time-sortable, so "newest first" == "highest id first"
  * Posts / users / follows       SQL (sqlite, in memory) = the source of truth
  * Feed cache                    one sorted set per user: feed:{user_id} -> {post_id, ...}
  * Post cache                    post:{id} -> post, used to "hydrate" ids into posts
  * Queue + fan-out workers       POST /posts returns before fan-out finishes
  * Hybrid fan-out                push for normal users, pull for celebrities, merged on read
  * Cursor pagination, idempotent pushes, inactive-user skipping, graceful degradation

Redis and Kafka are replaced by small in-memory stand-ins (FakeRedis, EventQueue)
that expose the same ideas, so everything runs with only the standard library.

Usage:
    python3 newsfeed.py demo            # scripted walkthrough of every concept
    python3 newsfeed.py bench           # push vs pull vs hybrid on a synthetic graph
    python3 newsfeed.py serve [--port]  # HTTP API (see README.md)
"""

from __future__ import annotations

import argparse
import heapq
import json
import queue
import random
import sqlite3
import statistics
import threading
import time
from collections import Counter, OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DAY = 86_400
MAX_POST_LEN = 280


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

@dataclass
class Config:
    strategy: str = "hybrid"          # "push" | "pull" | "hybrid"
    celebrity_threshold: int = 10_000  # hybrid only: >= this many followers -> no fan-out
    feed_max_len: int = 800            # post ids kept per user in the feed cache
    inactive_days: int = 30            # skip fan-out to users idle this long
    fanout_workers: int = 4
    fanout_batch: int = 1_000          # followers fetched per DB page during fan-out
    local_cache_size: int = 10_000     # per-server in-process post cache (hot keys)
    local_cache_ttl: float = 5.0
    machine_id: int = 1                # Snowflake machine bits (0..1023)


# --------------------------------------------------------------------------- #
# Snowflake IDs:  [41 bits ms since epoch][10 bits machine][12 bits sequence]
# --------------------------------------------------------------------------- #

class Snowflake:
    EPOCH_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z

    def __init__(self, machine_id: int):
        if not 0 <= machine_id < 1024:
            raise ValueError("machine_id must be in 0..1023")
        self.machine_id = machine_id
        self._last_ms = -1
        self._seq = 0
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            now = int(time.time() * 1000)
            if now < self._last_ms:          # clock moved backwards: never reuse the past
                now = self._last_ms
            if now == self._last_ms:
                self._seq = (self._seq + 1) & 0xFFF
                if self._seq == 0:           # 4096 ids in this ms already: wait for the next ms
                    while now <= self._last_ms:
                        now = int(time.time() * 1000)
            else:
                self._seq = 0
            self._last_ms = now
            return ((now - self.EPOCH_MS) << 22) | (self.machine_id << 12) | self._seq

    @classmethod
    def decode(cls, sid: int) -> dict:
        return {
            "timestamp_ms": (sid >> 22) + cls.EPOCH_MS,
            "machine_id": (sid >> 12) & 0x3FF,
            "sequence": sid & 0xFFF,
        }


# --------------------------------------------------------------------------- #
# Small infrastructure pieces
# --------------------------------------------------------------------------- #

class Stats:
    def __init__(self):
        self._c: Counter = Counter()
        self._lock = threading.Lock()

    def incr(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._c[name] += n

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._c)


class FakeRedis:
    """In-memory stand-in for one Redis node, with only the commands we need.

    Feeds are sorted sets whose score is the post id itself (Snowflake ids are
    time-sortable), so a plain Python set plus heapq is enough here. Real Redis
    would use ZADD / ZREVRANGEBYSCORE / ZREMRANGEBYRANK on a skiplist.
    """

    def __init__(self):
        self._zsets: dict[str, set[int]] = {}
        self._kv: dict[str, object] = {}
        self._lock = threading.Lock()
        self.up = True
        self.ops = 0

    def _op(self) -> None:
        if not self.up:
            raise ConnectionError("redis: connection refused")
        self.ops += 1

    # --- sorted sets ---
    def zadd(self, key: str, *members: int) -> int:
        """Add members; returns how many were new. Re-adding is a no-op (idempotent)."""
        with self._lock:
            self._op()
            s = self._zsets.setdefault(key, set())
            before = len(s)
            s.update(members)
            return len(s) - before

    def zcard(self, key: str) -> int:
        with self._lock:
            self._op()
            return len(self._zsets.get(key, ()))

    def zrevrange_below(self, key: str, before: int | None, limit: int) -> list[int]:
        """Up to `limit` members, highest first, strictly below `before` (the cursor)."""
        with self._lock:
            self._op()
            s = self._zsets.get(key, ())
            candidates = s if before is None else (m for m in s if m < before)
            return heapq.nlargest(limit, candidates)

    def ztrim_newest(self, key: str, keep: int) -> None:
        with self._lock:
            self._op()
            s = self._zsets.get(key)
            if s and len(s) > keep:
                self._zsets[key] = set(heapq.nlargest(keep, s))

    def zmembers(self, key: str) -> set[int]:
        with self._lock:
            self._op()
            return set(self._zsets.get(key, ()))

    # --- plain keys ---
    def set(self, key: str, value) -> None:
        with self._lock:
            self._op()
            self._kv[key] = value

    def mget(self, keys: list[str]) -> list:
        with self._lock:
            self._op()
            return [self._kv.get(k) for k in keys]

    def exists(self, key: str) -> bool:
        with self._lock:
            self._op()
            return key in self._kv or key in self._zsets

    def delete(self, *keys: str) -> None:
        with self._lock:
            self._op()
            for k in keys:
                self._kv.pop(k, None)
                self._zsets.pop(k, None)

    def flushall(self) -> None:
        """Simulates the node being replaced: all cached data is gone."""
        with self._lock:
            self._zsets.clear()
            self._kv.clear()


class LocalCache:
    """Per-server LRU with a short TTL. Absorbs hot keys (a celebrity's post read by everyone)."""

    def __init__(self, capacity: int, ttl: float):
        self.capacity = capacity
        self.ttl = ttl
        self._d: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            item = self._d.get(key)
            if item is None:
                return None
            value, expires = item
            if expires < time.monotonic():
                del self._d[key]
                return None
            self._d.move_to_end(key)
            return value

    def put(self, key, value) -> None:
        if self.capacity <= 0 or self.ttl <= 0:
            return
        with self._lock:
            self._d[key] = (value, time.monotonic() + self.ttl)
            self._d.move_to_end(key)
            while len(self._d) > self.capacity:
                self._d.popitem(last=False)

    def pop(self, key) -> None:
        with self._lock:
            self._d.pop(key, None)

    def __len__(self) -> int:
        return len(self._d)


class EventQueue:
    """Stand-in for Kafka with at-least-once delivery.

    A consumer must ack() after processing; nack() puts the event back so it is
    delivered again (which is why fan-out pushes must be idempotent).
    """

    def __init__(self):
        self._q: queue.Queue = queue.Queue()
        self.published = 0
        self.redelivered = 0

    def publish(self, event: dict) -> None:
        self.published += 1
        self._q.put(event)

    def consume(self, timeout: float) -> dict:
        return self._q.get(timeout=timeout)

    def ack(self) -> None:
        self._q.task_done()

    def nack(self, event: dict) -> None:
        self.redelivered += 1
        self._q.put(event)
        self._q.task_done()

    def lag(self) -> int:
        """Events not yet fully processed (waiting + in flight)."""
        return self._q.unfinished_tasks


# --------------------------------------------------------------------------- #
# Database (source of truth)
# --------------------------------------------------------------------------- #

class Database:
    SCHEMA = """
    CREATE TABLE users (
        id              INTEGER PRIMARY KEY,
        name            TEXT UNIQUE NOT NULL,
        follower_count  INTEGER NOT NULL DEFAULT 0,
        last_active_at  REAL NOT NULL
    );
    CREATE TABLE posts (
        id          INTEGER PRIMARY KEY,          -- Snowflake id
        author_id   INTEGER NOT NULL,
        text        TEXT NOT NULL,
        media_url   TEXT,
        created_at  REAL NOT NULL,
        deleted     INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX posts_by_author ON posts(author_id, id DESC);
    CREATE TABLE follows (
        follower_id INTEGER NOT NULL,
        followee_id INTEGER NOT NULL,
        created_at  REAL NOT NULL,
        PRIMARY KEY (follower_id, followee_id)      -- "who do I follow?"
    );
    CREATE INDEX follows_by_followee ON follows(followee_id, follower_id);  -- "who follows the author?"
    """

    def __init__(self):
        self._conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(self.SCHEMA)
        self._lock = threading.RLock()
        self.queries = 0

    def _exec(self, sql: str, params=()) -> sqlite3.Cursor:
        with self._lock:
            self.queries += 1
            return self._conn.execute(sql, params)

    def _all(self, sql: str, params=()) -> list:
        with self._lock:
            return self._exec(sql, params).fetchall()

    @contextmanager
    def _tx(self):
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                yield
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # --- users ---
    def create_user(self, name: str) -> int:
        try:
            return self._exec(
                "INSERT INTO users(name, follower_count, last_active_at) VALUES (?, 0, ?)",
                (name, time.time()),
            ).lastrowid
        except sqlite3.IntegrityError:
            raise ValueError(f"user {name!r} already exists") from None

    def get_user(self, user_id: int) -> dict | None:
        rows = self._all("SELECT * FROM users WHERE id = ?", (user_id,))
        return dict(rows[0]) if rows else None

    def set_last_active(self, user_id: int, ts: float) -> None:
        self._exec("UPDATE users SET last_active_at = ? WHERE id = ?", (ts, user_id))

    # --- follows ---
    def follow(self, follower_id: int, followee_id: int) -> bool:
        with self._tx():
            cur = self._exec("INSERT OR IGNORE INTO follows VALUES (?, ?, ?)",
                             (follower_id, followee_id, time.time()))
            if cur.rowcount == 1:
                self._exec("UPDATE users SET follower_count = follower_count + 1 WHERE id = ?",
                           (followee_id,))
                return True
            return False

    def unfollow(self, follower_id: int, followee_id: int) -> bool:
        with self._tx():
            cur = self._exec("DELETE FROM follows WHERE follower_id = ? AND followee_id = ?",
                             (follower_id, followee_id))
            if cur.rowcount == 1:
                self._exec("UPDATE users SET follower_count = follower_count - 1 WHERE id = ?",
                           (followee_id,))
                return True
            return False

    def followees_with_counts(self, user_id: int) -> list[tuple[int, int]]:
        rows = self._all(
            "SELECT f.followee_id, u.follower_count FROM follows f "
            "JOIN users u ON u.id = f.followee_id WHERE f.follower_id = ?",
            (user_id,),
        )
        return [(r[0], r[1]) for r in rows]

    def active_follower_batches(self, followee_id: int, active_since: float, batch: int):
        """Yield pages of follower ids (keyset pagination, so 10M followers never load at once)."""
        last = 0
        while True:
            rows = self._all(
                "SELECT f.follower_id FROM follows f JOIN users u ON u.id = f.follower_id "
                "WHERE f.followee_id = ? AND f.follower_id > ? AND u.last_active_at >= ? "
                "ORDER BY f.follower_id LIMIT ?",
                (followee_id, last, active_since, batch),
            )
            if not rows:
                return
            ids = [r[0] for r in rows]
            yield ids
            last = ids[-1]

    # --- posts ---
    def insert_post(self, post_id: int, author_id: int, text: str,
                    media_url: str | None, created_at: float) -> None:
        self._exec("INSERT INTO posts(id, author_id, text, media_url, created_at) VALUES (?,?,?,?,?)",
                   (post_id, author_id, text, media_url, created_at))

    def recent_post_ids(self, author_ids: list[int], before: int | None, limit: int) -> list[int]:
        """The "pull" query:  posts by these authors, newest first, below the cursor."""
        if not author_ids or limit <= 0:
            return []
        marks = ",".join("?" * len(author_ids))
        sql = f"SELECT id FROM posts WHERE author_id IN ({marks}) AND deleted = 0"
        params: list = list(author_ids)
        if before is not None:
            sql += " AND id < ?"
            params.append(before)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return [r[0] for r in self._all(sql, params)]

    def get_posts(self, ids: list[int]) -> dict[int, dict]:
        if not ids:
            return {}
        marks = ",".join("?" * len(ids))
        rows = self._all(
            "SELECT p.id, p.author_id, u.name AS author_name, p.text, p.media_url, "
            f"p.created_at, p.deleted FROM posts p JOIN users u ON u.id = p.author_id "
            f"WHERE p.id IN ({marks})",
            ids,
        )
        return {r["id"]: {**dict(r), "deleted": bool(r["deleted"])} for r in rows}

    def delete_post(self, post_id: int, author_id: int) -> bool:
        cur = self._exec("UPDATE posts SET deleted = 1 WHERE id = ? AND author_id = ? AND deleted = 0",
                         (post_id, author_id))
        return cur.rowcount == 1

    # --- bulk loading (bench only) ---
    def bulk_load(self, users: list[str], follows: list[tuple[int, int]],
                  posts: list[tuple[int, int, str, float]]) -> None:
        now = time.time()
        with self._tx():
            self._conn.executemany(
                "INSERT INTO users(id, name, follower_count, last_active_at) VALUES (?, ?, 0, ?)",
                [(i + 1, n, now) for i, n in enumerate(users)])
            self._conn.executemany("INSERT OR IGNORE INTO follows VALUES (?, ?, ?)",
                                   [(a, b, now) for a, b in follows])
            self._conn.executemany(
                "INSERT INTO posts(id, author_id, text, created_at) VALUES (?, ?, ?, ?)", posts)
            self._conn.execute(
                "UPDATE users SET follower_count = "
                "(SELECT COUNT(*) FROM follows WHERE followee_id = users.id)")


# --------------------------------------------------------------------------- #
# Fan-out workers
# --------------------------------------------------------------------------- #

class SimulatedCrash(Exception):
    pass


def feed_key(user_id: int) -> str:
    return f"feed:{user_id}"


def ready_key(user_id: int) -> str:
    """Marks a feed as built. Fan-out only pushes into built feeds; cold ones are rebuilt on read."""
    return f"feed_ready:{user_id}"


def post_key(post_id: int) -> str:
    return f"post:{post_id}"


class FanoutWorkers:
    def __init__(self, app: "NewsFeedApp"):
        self.app = app
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._running = threading.Event()
        self._running.set()
        self._crash_lock = threading.Lock()
        self._crash_after: int | None = None

    # --- lifecycle ---
    def start(self) -> None:
        for i in range(self.app.cfg.fanout_workers):
            t = threading.Thread(target=self._loop, name=f"fanout-{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)

    def pause(self) -> None:
        self._running.clear()

    def resume(self) -> None:
        self._running.set()

    def crash_after(self, pushes: int) -> None:
        """Make the next fan-out die after `pushes` successful pushes (once)."""
        with self._crash_lock:
            self._crash_after = pushes

    # --- work ---
    def _loop(self) -> None:
        events = self.app.events
        while not self._stop.is_set():
            if not self._running.wait(0.05):
                continue
            try:
                event = events.consume(timeout=0.05)
            except queue.Empty:
                continue
            try:
                self.handle(event)
                events.ack()
            except Exception:
                self.app.stats.incr("fanout_failures")
                time.sleep(0.05)          # back off, then let the queue redeliver
                events.nack(event)

    def handle(self, event: dict) -> None:
        if event.get("type") != "post_created":
            return
        app = self.app
        author = app.db.get_user(event["author_id"])
        if author is None:
            return
        post_id = event["post_id"]

        self._push(author["id"], post_id)         # the author's own timeline

        if app.is_celebrity(author):
            # Hybrid: celebrity posts stay in the Posts DB and are pulled at read time.
            app.stats.incr("fanout_skipped_celebrity")
            return

        active_since = time.time() - app.cfg.inactive_days * DAY
        delivered = 0
        for batch in app.db.active_follower_batches(author["id"], active_since, app.cfg.fanout_batch):
            for follower_id in batch:
                self._push(follower_id, post_id)
                delivered += 1
            app.stats.incr("fanout_follower_pushes", len(batch))
        skipped = author["follower_count"] - delivered
        if skipped > 0:
            app.stats.incr("fanout_skipped_inactive", skipped)

    def _push(self, user_id: int, post_id: int) -> None:
        app, r = self.app, self.app.redis
        if not r.exists(ready_key(user_id)):
            app.stats.incr("fanout_skipped_cold")     # never built (or evicted): rebuilt on read
            return
        with self._crash_lock:
            if self._crash_after is not None:
                if self._crash_after == 0:
                    self._crash_after = None
                    raise SimulatedCrash("worker died mid fan-out")
                self._crash_after -= 1
        if r.zadd(feed_key(user_id), post_id):
            app.stats.incr("fanout_inserts")
        else:
            app.stats.incr("fanout_duplicates")       # redelivery: zadd made it a no-op
        r.ztrim_newest(feed_key(user_id), app.cfg.feed_max_len)


# --------------------------------------------------------------------------- #
# The application: post service + follow service + feed service
# --------------------------------------------------------------------------- #

class NewsFeedApp:
    def __init__(self, cfg: Config | None = None, start_workers: bool = True):
        self.cfg = cfg or Config()
        if self.cfg.strategy not in ("push", "pull", "hybrid"):
            raise ValueError("strategy must be push, pull or hybrid")
        self.db = Database()
        self.redis = FakeRedis()
        self.events = EventQueue()
        self.stats = Stats()
        self.ids = Snowflake(self.cfg.machine_id)
        self.local_cache = LocalCache(self.cfg.local_cache_size, self.cfg.local_cache_ttl)
        self.workers = FanoutWorkers(self)
        if start_workers:
            self.workers.start()

    def close(self) -> None:
        self.workers.stop()
        with self.db._lock:
            self.db._conn.close()

    def wait_for_fanout(self, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while self.events.lag() > 0:
            if time.monotonic() > deadline:
                return False
            time.sleep(0.005)
        return True

    def is_celebrity(self, user: dict) -> bool:
        return (self.cfg.strategy == "hybrid"
                and user["follower_count"] >= self.cfg.celebrity_threshold)

    # ---------------- users & follows ----------------
    def create_user(self, name: str) -> dict:
        name = (name or "").strip()
        if not name:
            raise ValueError("name is required")
        uid = self.db.create_user(name)
        return self._require_user(uid)

    def _require_user(self, user_id: int) -> dict:
        user = self.db.get_user(user_id)
        if user is None:
            raise LookupError(f"user {user_id} not found")
        return user

    def follow(self, follower_id: int, followee_id: int) -> bool:
        if follower_id == followee_id:
            raise ValueError("cannot follow yourself")
        self._require_user(follower_id)
        self._require_user(followee_id)
        changed = self.db.follow(follower_id, followee_id)
        if changed:
            self._invalidate_feed(follower_id)
        return changed

    def unfollow(self, follower_id: int, followee_id: int) -> bool:
        changed = self.db.unfollow(follower_id, followee_id)
        if changed:
            self._invalidate_feed(follower_id)
        return changed

    def _invalidate_feed(self, user_id: int) -> None:
        # Simplest correct option: drop the cached feed and rebuild it with one pull
        # on the next read. (Backfilling/removing individual ids is an optimisation.)
        try:
            self.redis.delete(ready_key(user_id), feed_key(user_id))
        except ConnectionError:
            pass

    # ---------------- write path ----------------
    def create_post(self, author_id: int, text: str, media_url: str | None = None) -> dict:
        text = (text or "").strip()
        if not text:
            raise ValueError("text is required")
        if len(text) > MAX_POST_LEN:
            raise ValueError(f"text is longer than {MAX_POST_LEN} characters")
        author = self._require_user(author_id)

        post_id = self.ids.next_id()
        now = time.time()
        self.db.insert_post(post_id, author_id, text, media_url, now)   # 1. source of truth
        self.db.set_last_active(author_id, now)
        post = {"id": post_id, "author_id": author_id, "author_name": author["name"],
                "text": text, "media_url": media_url, "created_at": now, "deleted": False}

        if self.cfg.strategy != "pull":
            try:
                self.redis.set(post_key(post_id), post)                    # 2. warm the post cache
                if self.redis.exists(ready_key(author_id)):                # 3. read-your-own-writes
                    self.redis.zadd(feed_key(author_id), post_id)
            except ConnectionError:
                pass                                                       # caches are optional
            self.events.publish({"type": "post_created",                   # 4. fan-out happens later
                                 "post_id": post_id, "author_id": author_id})
        self.stats.incr("posts_created")
        return _public(post)

    def delete_post(self, author_id: int, post_id: int) -> None:
        if not self.db.delete_post(post_id, author_id):
            raise LookupError("post not found")
        # Don't chase the id through millions of feeds: hydration drops deleted posts.
        # In a real fleet the local caches on other servers expire within the TTL.
        self.local_cache.pop(post_id)
        try:
            self.redis.delete(post_key(post_id))
        except ConnectionError:
            pass

    # ---------------- read path ----------------
    def get_feed(self, user_id: int, cursor: int | None = None, limit: int = 20) -> dict:
        limit = max(1, min(int(limit), 100))
        user = self._require_user(user_id)
        self.stats.incr("feed_reads")

        now = time.time()
        was_inactive = user["last_active_at"] < now - self.cfg.inactive_days * DAY
        self.db.set_last_active(user_id, now)

        normal, celebs = self._split_followees(user_id)
        source = self._candidate_source(user_id, normal, celebs, rebuild=was_inactive)

        items: list[dict] = []
        before = cursor
        while len(items) < limit:
            want = limit - len(items)
            ids = source(before, want)
            if not ids:
                break
            posts = self._hydrate(ids)
            items.extend(posts[i] for i in ids if i in posts)   # deleted posts are dropped here
            before = ids[-1]
            if len(ids) < want:
                break
        next_cursor = items[-1]["id"] if len(items) == limit else None
        return {"items": items, "next_cursor": next_cursor}

    def _split_followees(self, user_id: int) -> tuple[list[int], list[int]]:
        rows = self.db.followees_with_counts(user_id)
        if self.cfg.strategy != "hybrid":
            return [uid for uid, _ in rows], []
        t = self.cfg.celebrity_threshold
        return [u for u, c in rows if c < t], [u for u, c in rows if c >= t]

    def _candidate_source(self, user_id, normal, celebs, rebuild):
        """Returns f(before, n) -> up to n post ids, newest first, merged from all sources."""
        everyone = normal + celebs + [user_id]
        if self.cfg.strategy == "pull":
            return lambda before, n: self.db.recent_post_ids(everyone, before, n)

        degraded = False
        try:
            if rebuild or not self.redis.exists(ready_key(user_id)):
                self._rebuild_feed(user_id, normal + [user_id])
        except ConnectionError:
            degraded = True

        def source(before, n):
            nonlocal degraded
            pushed: list[int] = []
            if not degraded:
                try:
                    pushed = self._pushed_ids(user_id, normal, before, n)
                except ConnectionError:
                    degraded = True
            if degraded:
                # Redis is down: a slow feed beats an error page. Fall back to pure pull.
                self.stats.incr("degraded_reads")
                pushed = self.db.recent_post_ids(normal + [user_id], before, n)
            pulled = self.db.recent_post_ids(celebs, before, n) if celebs else []
            # Merge by id == merge by time, thanks to Snowflake ids. The set dedupes.
            return heapq.nlargest(n, set(pushed) | set(pulled))

        return source

    def _pushed_ids(self, user_id: int, normal: list[int], before: int | None, n: int) -> list[int]:
        key = feed_key(user_id)
        ids = self.redis.zrevrange_below(key, before, n)
        if len(ids) < n and self.redis.zcard(key) >= self.cfg.feed_max_len:
            # The cache only holds the newest feed_max_len ids; deeper pages come from the DB.
            self.stats.incr("deep_page_db_reads")
            oldest = ids[-1] if ids else before
            ids += self.db.recent_post_ids(normal + [user_id], oldest, n - len(ids))
        return ids

    def _rebuild_feed(self, user_id: int, authors: list[int]) -> None:
        self.stats.incr("feed_rebuilds")
        r = self.redis
        r.delete(feed_key(user_id))
        # Mark ready FIRST, so a fan-out racing with this rebuild lands in the new feed
        # instead of being skipped as "cold". zadd is idempotent, so overlap is harmless.
        r.set(ready_key(user_id), True)
        ids = self.db.recent_post_ids(authors, None, self.cfg.feed_max_len)
        if ids:
            r.zadd(feed_key(user_id), *ids)
            r.ztrim_newest(feed_key(user_id), self.cfg.feed_max_len)

    def _hydrate(self, ids: list[int]) -> dict[int, dict]:
        """post ids -> posts. Local cache, then Redis (one multi-get), then the DB."""
        found: dict[int, dict] = {}
        missing: list[int] = []
        for pid in ids:
            p = self.local_cache.get(pid)
            if p is not None:
                found[pid] = p
                self.stats.incr("local_cache_hits")
            else:
                missing.append(pid)

        use_redis = self.cfg.strategy != "pull"
        if missing and use_redis:
            try:
                values = self.redis.mget([post_key(p) for p in missing])
            except ConnectionError:
                values = [None] * len(missing)
            still = []
            for pid, v in zip(missing, values):
                if v is None:
                    still.append(pid)
                else:
                    found[pid] = v
                    self.local_cache.put(pid, v)
                    self.stats.incr("redis_cache_hits")
            missing = still

        if missing:
            rows = self.db.get_posts(missing)
            self.stats.incr("db_hydrations", len(rows))
            for pid, p in rows.items():
                found[pid] = p
                self.local_cache.put(pid, p)
            if use_redis:
                try:
                    for pid, p in rows.items():
                        self.redis.set(post_key(pid), p)
                except ConnectionError:
                    pass

        return {pid: _public(p) for pid, p in found.items() if not p.get("deleted")}

    # ---------------- introspection ----------------
    def snapshot(self) -> dict:
        return {
            "strategy": self.cfg.strategy,
            "celebrity_threshold": self.cfg.celebrity_threshold,
            "queue_lag": self.events.lag(),
            "events_published": self.events.published,
            "events_redelivered": self.events.redelivered,
            "db_queries": self.db.queries,
            "redis_ops": self.redis.ops,
            "redis_up": self.redis.up,
            "local_cache_entries": len(self.local_cache),
            **self.stats.snapshot(),
        }


def _public(post: dict) -> dict:
    return {k: v for k, v in post.items() if k != "deleted"}


# --------------------------------------------------------------------------- #
# HTTP API
# --------------------------------------------------------------------------- #

def make_handler(app: NewsFeedApp, verbose: bool = False):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NewsFeed/1.0"

        def log_message(self, fmt, *args):
            if verbose:
                super().log_message(fmt, *args)

        def _send(self, code: int, body) -> None:
            data = json.dumps(body, indent=2).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length") or 0)
            if not n:
                return {}
            try:
                body = json.loads(self.rfile.read(n))
            except json.JSONDecodeError:
                raise ValueError("body must be JSON") from None
            if not isinstance(body, dict):
                raise ValueError("body must be a JSON object")
            return body

        def _me(self) -> int:
            raw = self.headers.get("X-User-Id")
            if not raw or not raw.isdigit():
                raise PermissionError("X-User-Id header is required")
            return int(raw)

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def do_DELETE(self):
            self._dispatch("DELETE")

        def _dispatch(self, method: str) -> None:
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            qs = parse_qs(url.query)
            try:
                if method == "POST" and parts == ["users"]:
                    return self._send(201, app.create_user(self._body().get("name")))
                if method == "GET" and len(parts) == 2 and parts[0] == "users":
                    return self._send(200, app._require_user(_int(parts[1])))
                if method == "POST" and parts == ["posts"]:
                    b = self._body()
                    return self._send(201, app.create_post(self._me(), b.get("text"), b.get("media_url")))
                if method == "DELETE" and len(parts) == 2 and parts[0] == "posts":
                    app.delete_post(self._me(), _int(parts[1]))
                    return self._send(200, {"deleted": True})
                if len(parts) == 3 and parts[0] == "users" and parts[2] == "follow":
                    target = _int(parts[1])
                    if method == "POST":
                        return self._send(200, {"following": True, "changed": app.follow(self._me(), target)})
                    if method == "DELETE":
                        return self._send(200, {"following": False, "changed": app.unfollow(self._me(), target)})
                if method == "GET" and parts == ["feed"]:
                    cursor = qs.get("cursor", [None])[0]
                    limit = qs.get("limit", ["20"])[0]
                    return self._send(200, app.get_feed(self._me(),
                                                        _int(cursor) if cursor else None, _int(limit)))
                if method == "GET" and parts == ["stats"]:
                    return self._send(200, app.snapshot())
                if method == "POST" and parts[:2] == ["admin", "redis"] and len(parts) == 3:
                    action = parts[2]
                    if action == "down":
                        app.redis.up = False
                    elif action == "up":
                        app.redis.up = True
                    elif action == "flush":
                        app.redis.flushall()
                    else:
                        raise LookupError("unknown admin action")
                    return self._send(200, {"redis_up": app.redis.up, "action": action})
                self._send(404, {"error": "not found"})
            except PermissionError as e:
                self._send(401, {"error": str(e)})
            except LookupError as e:
                self._send(404, {"error": str(e)})
            except ValueError as e:
                self._send(400, {"error": str(e)})

    return Handler


def _int(s) -> int:
    try:
        return int(s)
    except (TypeError, ValueError):
        raise ValueError(f"expected an integer, got {s!r}") from None


def serve(args) -> None:
    cfg = Config(strategy=args.strategy, celebrity_threshold=args.celebrity_threshold)
    app = NewsFeedApp(cfg)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(app, verbose=True))
    print(f"News feed API on http://{args.host}:{args.port}  "
          f"(strategy={cfg.strategy}, celebrity_threshold={cfg.celebrity_threshold})")
    print("Ctrl+C to stop. See README.md for curl examples.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        app.close()


# --------------------------------------------------------------------------- #
# Demo: a guided walk through learn1.md
# --------------------------------------------------------------------------- #

def demo(_args) -> None:
    app = NewsFeedApp(Config(celebrity_threshold=5, local_cache_ttl=0))
    names: dict[int, str] = {}

    def title(n, text):
        print(f"\n{'=' * 78}\n{n}. {text}\n{'=' * 78}")

    def user(name):
        u = app.create_user(name)
        names[u["id"]] = name
        return u["id"]

    def show(uid, label=None, **kw):
        page = app.get_feed(uid, **kw)
        print(f"  {label or names[uid] + ' feed'}:")
        if not page["items"]:
            print("    (empty)")
        for p in page["items"]:
            print(f"    - [{p['author_name']:>6}] {p['text']:<40} id={p['id']}")
        return page

    def stat(*keys):
        s = app.stats.snapshot()
        print("  stats: " + ", ".join(f"{k}={s.get(k, 0)}" for k in keys))

    # 1 ------------------------------------------------------------------
    title(1, "Snowflake IDs: time-sortable, so 'newest first' == 'highest id first'")
    for _ in range(3):
        sid = app.ids.next_id()
        d = Snowflake.decode(sid)
        print(f"  {sid}  -> ms={d['timestamp_ms']} machine={d['machine_id']} seq={d['sequence']}")

    # 2 ------------------------------------------------------------------
    title(2, "Users and follows (celebrity threshold lowered to 5 for the demo)")
    alice, bob, carol, dave, erin = (user(n) for n in ("alice", "bob", "carol", "dave", "erin"))
    taylor = user("taylor")
    for a, b in [(alice, bob), (alice, carol), (bob, alice), (carol, alice), (carol, bob),
                 (dave, bob), (erin, bob),
                 (alice, taylor), (bob, taylor), (carol, taylor), (dave, taylor), (erin, taylor)]:
        app.follow(a, b)
    for uid in names:
        u = app.db.get_user(uid)
        tag = "  <- celebrity: no fan-out" if app.is_celebrity(u) else ""
        print(f"  {names[uid]:>6}: {u['follower_count']} followers{tag}")
    for uid in names:            # everyone opens the app once, which builds their cached feed
        app.get_feed(uid)
    stat("feed_rebuilds")

    # 3 ------------------------------------------------------------------
    title(3, "Write path + eventual consistency (fan-out workers paused)")
    app.workers.pause()
    t0 = time.perf_counter()
    post = app.create_post(bob, "hello from bob")
    print(f"  POST /posts returned in {(time.perf_counter() - t0) * 1000:.2f} ms "
          f"(queue lag = {app.events.lag()})")
    show(alice, "alice's feed BEFORE fan-out")
    show(bob, "bob's own feed (read-your-own-writes)")
    app.workers.resume()
    app.wait_for_fanout()
    show(alice, "alice's feed AFTER fan-out")
    stat("fanout_inserts")

    # 4 ------------------------------------------------------------------
    title(4, "Hybrid fan-out: the celebrity's post is NOT pushed, it's pulled at read time")
    before = app.stats.snapshot().get("fanout_follower_pushes", 0)
    tpost = app.create_post(taylor, "new album out now!")
    app.wait_for_fanout()
    after = app.stats.snapshot().get("fanout_follower_pushes", 0)
    print(f"  pushes into followers' feeds for taylor's post: {after - before}")
    print(f"  taylor's post id in alice's Redis feed? {tpost['id'] in app.redis.zmembers(feed_key(alice))}")
    show(alice, "alice's feed (Redis ids + celebrity pull, merged by id)")
    stat("fanout_skipped_celebrity")

    # 5 ------------------------------------------------------------------
    title(5, "Cursor pagination: new posts arriving between pages cause no duplicates")
    for i in range(1, 6):
        app.create_post(bob, f"bob post #{i}")
    app.wait_for_fanout()
    p1 = show(alice, "page 1 (limit=3)", limit=3)
    app.create_post(carol, "carol posts while alice is scrolling")
    app.wait_for_fanout()
    p2 = show(alice, f"page 2 (cursor={p1['next_cursor']})", cursor=p1["next_cursor"], limit=3)
    dupes = {p["id"] for p in p1["items"]} & {p["id"] for p in p2["items"]}
    print(f"  duplicates between pages: {len(dupes)}   (page=2 offsets would have shown 1)")

    # 6 ------------------------------------------------------------------
    title(6, "A worker crashes mid fan-out: the queue redelivers, pushes are idempotent")
    s0, r0 = app.stats.snapshot(), app.events.redelivered
    app.workers.crash_after(2)
    crashed = app.create_post(bob, "post that survives a worker crash")
    app.wait_for_fanout()
    s1 = app.stats.snapshot()
    print(f"  worker crashes: {s1.get('fanout_failures', 0) - s0.get('fanout_failures', 0)}, "
          f"events redelivered: {app.events.redelivered - r0}")
    print(f"  pushes that were already there (deduped by zadd): "
          f"{s1.get('fanout_duplicates', 0) - s0.get('fanout_duplicates', 0)}")
    ids = [p["id"] for p in app.get_feed(carol, limit=50)["items"]]
    print(f"  times the post appears in carol's feed: {ids.count(crashed['id'])}")

    # 7 ------------------------------------------------------------------
    title(7, "Inactive users are skipped during fan-out and rebuilt when they return")
    app.db.set_last_active(erin, time.time() - 40 * DAY)
    app.create_post(bob, "erin is away for this one")
    app.wait_for_fanout()
    stat("fanout_skipped_inactive")
    rebuilds = app.stats.snapshot()["feed_rebuilds"]
    show(erin, "erin comes back after 40 days", limit=3)
    print(f"  feed rebuilt via pull on return: {app.stats.snapshot()['feed_rebuilds'] - rebuilds == 1}")

    # 8 ------------------------------------------------------------------
    title(8, "Deleting a post: don't chase the id, drop it during hydration")
    app.delete_post(bob, post["id"])
    print(f"  id still in alice's Redis feed: {post['id'] in app.redis.zmembers(feed_key(alice))}")
    ids = [p["id"] for p in app.get_feed(alice, limit=50)["items"]]
    print(f"  shown in alice's feed:          {post['id'] in ids}")

    # 9 ------------------------------------------------------------------
    title(9, "The Redis node dies: degrade to pure pull, then rebuild when it's back")
    app.redis.up = False
    show(alice, "alice's feed with Redis DOWN (served from the DB)", limit=3)
    app.create_post(carol, "posted while Redis is down")
    time.sleep(0.3)
    print(f"  fan-out keeps failing and retrying; queue lag = {app.events.lag()}")
    app.redis.flushall()           # the replacement node starts empty
    app.redis.up = True
    app.wait_for_fanout()
    show(alice, "alice's feed after Redis comes back (rebuilt)", limit=3)
    stat("degraded_reads", "fanout_failures", "fanout_skipped_cold", "feed_rebuilds")

    # --------------------------------------------------------------------
    title("*", "Final counters")
    for k, v in sorted(app.snapshot().items()):
        print(f"  {k:<26} {v}")
    app.close()


# --------------------------------------------------------------------------- #
# Bench: push vs pull vs hybrid
# --------------------------------------------------------------------------- #

def bench(args) -> None:
    rng = random.Random(42)
    n_users, per_user, posts_each = args.users, args.follows, args.posts
    print(f"Graph: {n_users} users, each follows {per_user} random users + 1 celebrity "
          f"(followed by everyone). {posts_each} posts per user.\n")

    names = [f"user{i}" for i in range(1, n_users + 1)]
    celeb = 1
    follows = []
    for u in range(2, n_users + 1):
        follows.append((u, celeb))
        for v in rng.sample(range(2, n_users + 1), per_user):
            if v != u:
                follows.append((u, v))
    sf = Snowflake(machine_id=9)
    base = time.time()
    posts = [(sf.next_id(), a, f"post {k} by user{a}", base)
             for k in range(posts_each) for a in range(1, n_users + 1)]

    readers = rng.sample(range(2, n_users + 1), min(args.reads, n_users - 1))
    rows = []
    for strategy in ("push", "pull", "hybrid"):
        app = NewsFeedApp(Config(strategy=strategy, celebrity_threshold=max(2, n_users // 4),
                                 fanout_workers=4))
        app.db.bulk_load(names, follows, posts)
        for u in range(1, n_users + 1):             # warm every feed, as if all users are active
            app.get_feed(u, limit=1)
        app.wait_for_fanout()

        s0 = app.stats.snapshot()
        t0 = time.perf_counter()
        app.create_post(celeb, "celebrity post")
        app.wait_for_fanout(60)
        celeb_ms = (time.perf_counter() - t0) * 1000
        celeb_inserts = (app.stats.snapshot().get("fanout_follower_pushes", 0)
                         - s0.get("fanout_follower_pushes", 0))

        lat = []
        q0, r0 = app.db.queries, app.redis.ops
        h0 = app.stats.snapshot().get("local_cache_hits", 0)
        for u in readers:
            t = time.perf_counter()
            page = app.get_feed(u, limit=20)
            lat.append((time.perf_counter() - t) * 1000)
            assert page["items"] and page["items"][0]["author_id"] == celeb
        n = len(readers)
        lat.sort()
        rows.append((strategy, celeb_ms, celeb_inserts, statistics.median(lat),
                     lat[int(n * 0.99) - 1], (app.db.queries - q0) / n, (app.redis.ops - r0) / n,
                     app.stats.snapshot().get("local_cache_hits", 0) - h0))
        app.close()

    hdr = ("strategy", "celeb post→done", "follower pushes", "read p50", "read p99",
           "DB q/read", "Redis ops/read", "local hits")
    print("  ".join(f"{h:>15}" for h in hdr))
    for r in rows:
        print(f"{r[0]:>15}  {r[1]:>12.1f} ms  {r[2]:>15}  {r[3]:>12.2f} ms  {r[4]:>12.2f} ms  "
              f"{r[5]:>15.1f}  {r[6]:>15.1f}  {r[7]:>15}")
    print("\nHow to read it:")
    print("  push   - reads touch Redis only, but ONE celebrity post = one insert per follower.")
    print("  pull   - posting is free, but every read is a big IN (...) ORDER BY query on the DB.")
    print("  hybrid - near-push reads, and the celebrity post costs no follower pushes.")
    print("  local hits - the celebrity post is a hot key; the per-server cache absorbs it.")
    print("  In production each DB query is a network round trip, so the gaps get much wider.")


# --------------------------------------------------------------------------- #

def main() -> None:
    p = argparse.ArgumentParser(description="News feed system (learn1.md)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("demo", help="scripted walkthrough").set_defaults(func=demo)

    b = sub.add_parser("bench", help="compare push / pull / hybrid")
    b.add_argument("--users", type=int, default=2000)
    b.add_argument("--follows", type=int, default=50)
    b.add_argument("--posts", type=int, default=5)
    b.add_argument("--reads", type=int, default=500)
    b.set_defaults(func=bench)

    s = sub.add_parser("serve", help="run the HTTP API")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080)
    s.add_argument("--strategy", choices=("push", "pull", "hybrid"), default="hybrid")
    s.add_argument("--celebrity-threshold", type=int, default=10_000)
    s.set_defaults(func=serve)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
