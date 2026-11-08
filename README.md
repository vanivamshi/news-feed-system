# News Feed system: pure Python

A working version of a Twitter/X-style home timeline, built from a
system design write-up (requirements, estimates, fan-out on write vs read,
failures, scale evolution). It uses only the Python standard
library (3.10+), with no installs. Redis and Kafka are replaced by small
in-memory stand-ins that behave the same way where it matters, so every idea
in the design runs on your laptop.

```bash
python3 newsfeed.py demo     # guided walkthrough: one section per concept
python3 newsfeed.py bench    # push vs pull vs hybrid on a 2,000-user graph
python3 newsfeed.py serve    # HTTP API on http://127.0.0.1:8080
python3 -m unittest -v       # 18 tests
```

## How the design maps to the code

| learn1.md | Code ([`newsfeed.py`](newsfeed.py)) |
|---|---|
| Posts DB, Follows DB (source of truth) | `Database`: sqlite in memory, with `follows` indexed in **both** directions |
| Snowflake IDs | `Snowflake`: 41 bits ms · 10 bits machine · 12 bits sequence |
| Feed cache `feed:{user_id}` | `FakeRedis` sorted set per user, trimmed to `feed_max_len` (800) |
| Post cache `post:{id}` | `FakeRedis` key/value, read with one `mget` during hydration |
| Queue (Kafka) | `EventQueue`: at-least-once, with `ack` / `nack` and redelivery |
| Fan-out workers | `FanoutWorkers`: threads that page through followers in batches of 1,000 |
| Post service (write path) | `NewsFeedApp.create_post` |
| Feed service (read path) | `NewsFeedApp.get_feed`, `_candidate_source`, `_hydrate` |
| Local cache for hot posts | `LocalCache`: per-server LRU with a 5 s TTL |

### Write path (`create_post`)
1. Save to the Posts DB with a new Snowflake ID.
2. Warm the post cache, and push the ID into the **author's own** feed
   (read-your-own-writes).
3. Publish `post_created` to the queue and **return**. Fan-out happens later.
4. A worker reads the event. If the author is a celebrity (hybrid), it stops
   there. Otherwise it pushes the ID into every **active** follower's feed.

### Read path (`get_feed`)
1. If the user's cached feed doesn't exist (new user, Redis node lost, or back
   after 30+ days away), rebuild it once with a pull query.
2. Candidates = Redis feed IDs below the cursor **plus** recent posts from
   followed celebrities (pull), merged by ID. With Snowflake IDs, merging by
   ID is the same as merging by time.
3. Hydrate the IDs: local cache, then Redis `mget`, then the DB. Deleted posts
   are dropped here.
4. Return `{items, next_cursor}`. The cursor is the last ID returned.

## Things worth reading in the code

- **Idempotent pushes.** `zadd` of an ID that's already present does nothing,
  so the queue can redeliver after a worker crash without creating duplicates
  (`demo` §6).
- **The `feed_ready` marker.** Fan-out only pushes into feeds that were already
  built. Otherwise one pushed ID would create a feed that *looks* complete but
  is missing older posts. A rebuild sets the marker **before** it runs the
  pull, so a fan-out that races with the rebuild isn't lost.
- **Follow/unfollow invalidate the feed** instead of patching it. That's simple
  and correct, and costs one pull on the next read.
- **Deep pages.** Redis only keeps the newest 800 IDs, so `_pushed_ids` gets
  older pages from the DB.
- **Graceful degradation.** If Redis is down, reads fall back to pure pull
  (slower, but no error page), posting still works, and fan-out retries until
  Redis is back.
- **Strategies are interchangeable.** `Config(strategy="push"|"pull"|"hybrid")`
  only changes how the feed is built. `TestStrategiesAgree` checks that all
  three return identical feeds for the same random graph.

## Benchmark (`bench`)

Example output (2,000 users, 50 follows each, plus one celebrity followed by
everyone):

```
strategy  celeb post→done  follower pushes  read p50   DB q/read  Redis ops/read
push            31.3 ms             1999     0.28 ms        3.4             4.0
pull             0.1 ms                0     1.07 ms        4.4             0.0
hybrid           5.3 ms                0     0.33 ms        4.4             4.0
```

Push gives fast reads, but one celebrity post becomes one insert per follower.
Pull costs nothing to write, but every read runs a big query. Hybrid gets the
best of both. Here sqlite runs in-process, so a DB query costs microseconds. In
production each query is a network round trip, which makes the gaps much wider.

## HTTP API (`serve`)

The caller is identified with an `X-User-Id` header, which keeps auth out of
scope.

```bash
python3 newsfeed.py serve --celebrity-threshold 2

curl -s -XPOST localhost:8080/users -d '{"name":"alice"}'          # -> id 1
curl -s -XPOST localhost:8080/users -d '{"name":"bob"}'            # -> id 2
curl -s -XPOST localhost:8080/users/2/follow -H 'X-User-Id: 1'     # alice follows bob
curl -s -XPOST localhost:8080/posts -H 'X-User-Id: 2' -d '{"text":"hello"}'
curl -s 'localhost:8080/feed?limit=20' -H 'X-User-Id: 1'
curl -s 'localhost:8080/feed?limit=20&cursor=<next_cursor>' -H 'X-User-Id: 1'
curl -s -XDELETE localhost:8080/posts/<id> -H 'X-User-Id: 2'
curl -s -XDELETE localhost:8080/users/2/follow -H 'X-User-Id: 1'   # unfollow
curl -s localhost:8080/stats

# failure drills
curl -s -XPOST localhost:8080/admin/redis/down
curl -s -XPOST localhost:8080/admin/redis/flush
curl -s -XPOST localhost:8080/admin/redis/up
```

## Known simplifications

- One process plays every server. In a real fleet the local cache lives on each
  Feed server, so a deleted post can linger there for up to the TTL.
- A user who drops back **below** the celebrity threshold: posts made while
  they were a celebrity were never pushed, and they're no longer pulled either.
  Real systems fix this with hysteresis (different thresholds for entering and
  leaving) or a backfill job.
- The DB write and the queue publish aren't atomic. Production systems use the
  *transactional outbox* pattern so an event can never be lost.
- Fan-out pages through followers with keyset pagination. At 10M followers
  you'd also split one post into many queue messages so many workers can share
  the fan-out.
