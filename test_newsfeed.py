"""Tests for newsfeed.py.  Run:  python3 -m unittest -v"""

import json
import random
import threading
import time
import unittest
import urllib.request
from http.server import ThreadingHTTPServer

from newsfeed import DAY, Config, NewsFeedApp, Snowflake, feed_key, make_handler


def ids_of(page):
    return [p["id"] for p in page["items"]]


class Base(unittest.TestCase):
    cfg = dict(celebrity_threshold=3, local_cache_ttl=0)

    def setUp(self):
        self.app = NewsFeedApp(Config(**self.cfg))
        self.u = {n: self.app.create_user(n)["id"] for n in ("a", "b", "c", "d", "e")}

    def tearDown(self):
        self.app.close()

    def feed(self, name, **kw):
        return self.app.get_feed(self.u[name], **kw)

    def post(self, name, text="hi"):
        p = self.app.create_post(self.u[name], text)
        self.app.wait_for_fanout()
        return p


class TestSnowflake(unittest.TestCase):
    def test_unique_and_increasing(self):
        sf = Snowflake(7)
        ids = [sf.next_id() for _ in range(20_000)]
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(ids, sorted(ids))

    def test_decode(self):
        sid = Snowflake(7).next_id()
        d = Snowflake.decode(sid)
        self.assertEqual(d["machine_id"], 7)
        self.assertLess(abs(d["timestamp_ms"] - time.time() * 1000), 2000)

    def test_threads_never_collide(self):
        sf, out = Snowflake(1), []
        lock = threading.Lock()

        def work():
            local = [sf.next_id() for _ in range(5000)]
            with lock:
                out.extend(local)
        ts = [threading.Thread(target=work) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(len(set(out)), 20_000)


class TestFeed(Base):
    def test_fanout_delivers_to_followers_newest_first(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        p1, p2 = self.post("b", "one"), self.post("b", "two")
        self.assertEqual(ids_of(self.feed("a")), [p2["id"], p1["id"]])
        self.assertEqual(ids_of(self.feed("c")), [])  # c doesn't follow b

    def test_eventual_consistency_and_read_your_own_writes(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a"), self.feed("b")
        self.app.workers.pause()
        p = self.app.create_post(self.u["b"], "x")
        self.assertNotIn(p["id"], ids_of(self.feed("a")))   # not fanned out yet
        self.assertIn(p["id"], ids_of(self.feed("b")))      # but the author sees it
        self.app.workers.resume()
        self.app.wait_for_fanout()
        self.assertIn(p["id"], ids_of(self.feed("a")))

    def test_celebrity_is_pulled_not_pushed(self):
        for n in ("a", "c", "d"):
            self.app.follow(self.u[n], self.u["b"])         # b reaches the threshold (3)
        self.feed("a")
        p = self.post("b")
        self.assertEqual(self.app.stats.snapshot().get("fanout_follower_pushes", 0), 0)
        self.assertNotIn(p["id"], self.app.redis.zmembers(feed_key(self.u["a"])))
        self.assertEqual(ids_of(self.feed("a")), [p["id"]])

    def test_cursor_pagination_has_no_duplicates_or_gaps(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        posted = [self.post("b", f"p{i}")["id"] for i in range(7)]
        page1 = self.feed("a", limit=3)
        self.post("b", "arrives mid-scroll")
        seen, cursor = ids_of(page1), page1["next_cursor"]
        while cursor:
            page = self.feed("a", cursor=cursor, limit=3)
            seen += ids_of(page)
            cursor = page["next_cursor"]
        self.assertEqual(seen, sorted(posted, reverse=True))

    def test_redelivery_after_crash_is_idempotent(self):
        for n in ("a", "c"):
            self.app.follow(self.u[n], self.u["b"])
            self.feed(n)
        self.app.workers.crash_after(1)
        p = self.post("b")
        self.assertGreaterEqual(self.app.events.redelivered, 1)
        for n in ("a", "c"):
            self.assertEqual(ids_of(self.feed(n)).count(p["id"]), 1)

    def test_deleted_posts_are_dropped_at_hydration(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        keep, gone = self.post("b", "keep"), self.post("b", "gone")
        self.app.delete_post(self.u["b"], gone["id"])
        self.assertIn(gone["id"], self.app.redis.zmembers(feed_key(self.u["a"])))
        self.assertEqual(ids_of(self.feed("a")), [keep["id"]])
        with self.assertRaises(LookupError):
            self.app.delete_post(self.u["c"], keep["id"])  # not the author

    def test_deleted_posts_do_not_shorten_a_page(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        ps = [self.post("b", f"p{i}")["id"] for i in range(6)]
        for pid in ps[3:]:
            self.app.delete_post(self.u["b"], pid)
        self.assertEqual(ids_of(self.feed("a", limit=3)), ps[2::-1])

    def test_inactive_users_skipped_then_rebuilt(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        self.app.db.set_last_active(self.u["a"], time.time() - 40 * DAY)
        p = self.post("b")
        self.assertEqual(self.app.stats.snapshot().get("fanout_skipped_inactive"), 1)
        self.assertNotIn(p["id"], self.app.redis.zmembers(feed_key(self.u["a"])))
        self.assertEqual(ids_of(self.feed("a")), [p["id"]])

    def test_unfollow_removes_posts(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        self.post("b")
        self.app.unfollow(self.u["a"], self.u["b"])
        self.assertEqual(ids_of(self.feed("a")), [])

    def test_follow_backfills_existing_posts(self):
        p = self.post("b")
        self.feed("a")
        self.app.follow(self.u["a"], self.u["b"])
        self.assertEqual(ids_of(self.feed("a")), [p["id"]])

    def test_redis_down_degrades_to_pull(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        p = self.post("b")
        self.app.redis.up = False
        self.assertEqual(ids_of(self.feed("a")), [p["id"]])
        p2 = self.app.create_post(self.u["b"], "while down")    # posting still works
        self.assertEqual(ids_of(self.feed("a")), [p2["id"], p["id"]])
        self.app.redis.flushall()
        self.app.redis.up = True
        self.assertTrue(self.app.wait_for_fanout())
        self.assertEqual(ids_of(self.feed("a")), [p2["id"], p["id"]])
        self.assertGreaterEqual(self.app.stats.snapshot()["degraded_reads"], 1)

    def test_validation(self):
        with self.assertRaises(ValueError):
            self.app.create_post(self.u["a"], "")
        with self.assertRaises(ValueError):
            self.app.create_post(self.u["a"], "x" * 281)
        with self.assertRaises(ValueError):
            self.app.follow(self.u["a"], self.u["a"])
        with self.assertRaises(LookupError):
            self.app.get_feed(999)
        with self.assertRaises(ValueError):
            self.app.create_user("a")


class TestFeedTrim(Base):
    cfg = dict(celebrity_threshold=100, feed_max_len=5, local_cache_ttl=0)

    def test_deep_pages_fall_back_to_db(self):
        self.app.follow(self.u["a"], self.u["b"])
        self.feed("a")
        posted = [self.post("b", f"p{i}")["id"] for i in range(12)]
        self.assertEqual(self.app.redis.zcard(feed_key(self.u["a"])), 5)
        seen, cursor = [], None
        while True:
            page = self.feed("a", cursor=cursor, limit=4)
            seen += ids_of(page)
            cursor = page["next_cursor"]
            if not cursor:
                break
        self.assertEqual(seen, sorted(posted, reverse=True))


class TestStrategiesAgree(unittest.TestCase):
    """push, pull and hybrid are different plumbing for the SAME answer."""

    def test_same_feeds(self):
        rng = random.Random(7)
        n = 30
        follows = {(a, b) for a in range(n) for b in rng.sample(range(n), 8) if a != b}
        follows |= {(a, 0) for a in range(1, n)}          # user 0 is the celebrity
        script = [rng.randrange(n) for _ in range(150)]

        results = {}
        for strategy in ("push", "pull", "hybrid"):
            app = NewsFeedApp(Config(strategy=strategy, celebrity_threshold=15, local_cache_ttl=0))
            uids = [app.create_user(f"u{i}")["id"] for i in range(n)]
            for a, b in sorted(follows):
                app.follow(uids[a], uids[b])
            for u in uids:
                app.get_feed(u)
            texts = {}
            for i, author in enumerate(script):
                texts[app.create_post(uids[author], f"post {i}")["id"]] = f"post {i}"
            app.wait_for_fanout()
            results[strategy] = [[p["text"] for p in app.get_feed(u, limit=100)["items"]]
                                 for u in uids]
            app.close()
        self.assertEqual(results["push"], results["pull"])
        self.assertEqual(results["hybrid"], results["pull"])


class TestHTTP(unittest.TestCase):
    def setUp(self):
        self.app = NewsFeedApp(Config(local_cache_ttl=0))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.app))
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.app.close()

    def call(self, method, path, body=None, user=None):
        req = urllib.request.Request(self.base + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None)
        req.add_header("Content-Type", "application/json")
        if user:
            req.add_header("X-User-Id", str(user))
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            with e:
                return e.code, json.loads(e.read())

    def test_end_to_end(self):
        _, alice = self.call("POST", "/users", {"name": "alice"})
        _, bob = self.call("POST", "/users", {"name": "bob"})
        self.assertEqual(self.call("POST", f"/users/{bob['id']}/follow", user=alice["id"])[0], 200)
        status, post = self.call("POST", "/posts", {"text": "hi"}, user=bob["id"])
        self.assertEqual(status, 201)
        self.app.wait_for_fanout()
        status, feed = self.call("GET", "/feed?limit=5", user=alice["id"])
        self.assertEqual((status, [p["id"] for p in feed["items"]]), (200, [post["id"]]))
        self.assertEqual(self.call("GET", "/feed")[0], 401)
        self.assertEqual(self.call("POST", "/posts", {"text": ""}, user=bob["id"])[0], 400)
        self.assertEqual(self.call("GET", "/nope")[0], 404)
        self.assertEqual(self.call("GET", "/stats")[0], 200)


if __name__ == "__main__":
    unittest.main()
