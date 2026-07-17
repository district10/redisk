# redisk

Redis (or [Kvrocks](https://github.com/apache/kvrocks)) backed cache with
local disk offload for large values.

`redisk` is derived from
[python-diskcache](http://www.grantjenks.com/docs/diskcache/). The design is
the same — small values live in the key/value store, large values are
offloaded to files on local disk — but the SQLite key/value store is replaced
by Redis:

* Only simple Redis commands are used: `GET`, `SET` (with `PX`/`NX`), `DEL`,
  `EXISTS`, `PTTL`, `SCAN`, plus hash (`HINCRBY`/`HGET`/`HSET`) and sorted-set
  (`ZADD`/`ZRANGE`/`ZREM`) commands for bookkeeping.
* **No Lua scripts.**
* **Reads never write**: `get` is a single `GET`. No TTL refresh, no access
  time/count updates on lookup.
* Expiry uses native server-side TTLs (`SET ... PX`), which Kvrocks supports
  natively.

## Installation

```console
$ pip install redisk
```

For development and testing:

```console
$ pip install -e '.[test]'
```

## Quickstart

```python
from redisk import Cache

cache = Cache(
    redis_conn_url='redis://localhost:6666/0',  # your Redis/Kvrocks URL
    offload_folder='/var/cache/myapp',          # where large values go
    size_limit=2**30,                           # 1 GiB disk budget (default)
    default_ttl=86400,                          # default 1 day instead of 7
)

cache['key'] = 'small value'        # stored inside Redis
cache['blob'] = b'x' * 2**20        # offloaded to a file (>= 32 KiB)
print(cache['key'])

cache.set('session', {'user': 42}, expire=3600)  # native Redis TTL
cache.close()
```

**Every entry has a TTL.** When `expire` is not given, entries use the cache's
`default_ttl` (constructor argument, default `redisk.MAX_TTL_SECS` = 7 days),
and larger `expire` values are clamped to `MAX_TTL_SECS`. Nothing is stored
permanently — neither in Redis nor on disk.

The constructor takes these main parameters:

| Parameter          | Meaning                                                        |
|--------------------|----------------------------------------------------------------|
| `redis_conn_url`   | Redis/Kvrocks connection URL, **or** an existing client object |
| `offload_folder`   | Local directory for offloaded value files                      |
| `size_limit`       | Capacity limit in bytes for the offload folder (default 1 GiB) |
| `prefix`           | Redis key namespace (default `'redisk'`)                       |
| `cache_key_prefix` | Explicit prefix for cache entry keys (default `{prefix}:cache:`)|
| `default_ttl`      | Default TTL in seconds when `expire` is not given (default 7 d)|

Additional keyword arguments mirror diskcache's settings: `statistics`,
`eviction_policy`, `cull_limit`, `disk_min_file_size` (default 32 KiB), and
`disk_pickle_protocol`.

Passing an existing client (anything with the `redis.Redis` interface, e.g.
`fakeredis.FakeStrictRedis()`) instead of a URL is supported; the client must
use `decode_responses=False` (the default) and is left open on `close()`.

## How it works

### Storage layout

Each cache entry is a single Redis key under the namespace
`{prefix}:cache:{typed-key}` holding a msgpack-encoded record:

```
(store_time, expire_time, tag, size, mode, filename, value)
```

* Small values (`< disk_min_file_size`) are stored inline in the record.
* Large values are written to randomly named files below `offload_folder`
  (two levels of sub-directories, like diskcache) and the record stores the
  relative `filename` and byte `size`.

Two bookkeeping keys complete the picture:

* `{prefix}:meta` — a hash with `count`, `size`, `hits`, `misses` counters.
* `{prefix}:index` — a sorted set of entry keys scored by `store_time`,
  used for least-recently-stored eviction.

### Serialization

**Everything written to Redis is bytes, and serialization is always
[msgpack](https://msgpack.org/) — never pickle.** Keys, values, and tags
must be msgpack-native types: `None`, `bool`, `int` (64-bit), `float`,
`str`, `bytes`, `list`, `dict`, and `tuple` (preserved via a msgpack
extension type, so tuples round-trip as tuples). Anything else — custom
objects, integers outside the 64-bit range — raises `TypeError`; encode
such data to `bytes` yourself before storing.

`str`/`bytes`/`int`/`float` keys are used as-is; other keys are
msgpack-encoded. `str` and `bytes` values are stored raw (text/binary);
other supported values are msgpack-encoded. `memoize(typed=True)` encodes
argument types as their qualified name strings, so typed keys keep working
(but iterating such keys yields the type *name*, not the type object).

Serialization is pluggable via the `disk` parameter: `Disk` (default,
msgpack) or `JSONDisk` (JSON + zlib), or your own subclass — same protocol
as diskcache.

### Expiry

`set(key, value, expire=seconds)` maps to `SET key record PX ms`. Expired
entries disappear automatically on the server side; `get` stays a pure `GET`.

Every entry carries a TTL: `expire=None` falls back to the cache's
`default_ttl` (default `MAX_TTL_SECS` = 7 days), and `expire` values larger
than `MAX_TTL_SECS` are clamped to it. There is no permanent storage.

### Eviction and the disk limit

`size_limit` bounds the total size of **offloaded files** (inline values live
in Redis and are not counted — Redis/Kvrocks manages its own capacity). After
every write, if `volume()` exceeds `size_limit`, up to `cull_limit` oldest
entries are evicted (`eviction_policy='least-recently-stored'`, the default;
`'none'` disables eviction). `cache.cull()` evicts until under the limit.

### When is the disk cleaned?

Offloaded value files are removed **immediately** when their entry is:

* overwritten by `set` (the old file is deleted after the new record is set),
* deleted via `delete` / `del cache[key]` / `pop`,
* removed by `evict(tag)`, `clear()`, or eviction (`cull()` / automatic cull
  on writes when over `size_limit`),
* removed by `expire()` for records whose expire time has passed but which
  still exist (e.g. clock skew).

The one exception is **TTL expiry**: when Redis expires a key on the server
side, there is no hook to delete its file, so the file stays behind as an
orphan. Orphan files are reclaimed by `check(fix=True)`, which walks
`offload_folder` and deletes any file not referenced by a live record (it
also corrects the `count`/`size` counters and prunes stale index members).
Because every entry has a TTL of at most `MAX_TTL_SECS`, orphans are bounded
— run `check(fix=True)` periodically (e.g. daily from a cron job) to reclaim
them. `expire()` and `cull()` also purge stale index members left by TTL
expiry, but only `check(fix=True)` removes the orphan files themselves.

Empty sub-directories are removed together with their last file (the
`offload_folder` root itself is always kept).

### Consistency caveats

Redis has no multi-key transactions without Lua, so `redisk` trades
diskcache's strong atomicity for simplicity:

* `incr`/`decr` and `pop` are **not atomic** (GET + SET/DEL). `add` **is**
  atomic (`SET ... NX`).
* When an entry expires via TTL, its offloaded file, index member, and the
  `count`/`size` counters are not updated immediately. They are reconciled
  lazily: `expire()` and `cull()` purge stale index members, and
  `check(fix=True)` removes orphan files and corrects the counters. Run
  `check(fix=True)` periodically (e.g. from a cron job) if you rely on
  `len(cache)` / `volume()` being exact.
* `len(cache)` counts items including possibly-expired ones (same semantics
  as diskcache).

## API summary

Mapping-style: `cache[key]`, `cache[key] = value`, `del cache[key]`,
`key in cache`, `len(cache)`, `iter(cache)`, `reversed(cache)`.

Methods (see docstrings for details):

* `set(key, value, expire=None, read=False, tag=None)`
* `get(key, default=None, read=False, expire_time=False, tag=False)`
* `add(key, value, expire=None, read=False, tag=None)` — atomic set-if-absent
* `delete(key)`, `pop(key, ...)`, `touch(key, expire=None)`
* `incr(key, delta=1, default=0)`, `decr(...)` — not atomic
* `read(key)` — file handle for file-backed values
* `evict(tag)`, `expire()`, `cull()`, `clear()`
* `stats(enable=True, reset=False)`, `volume()`
* `check(fix=False)` — consistency check and repair
* `memoize(name=None, typed=False, expire=None, tag=None, ignore=())`
* `iterkeys()`, `close()`, context-manager support

## Differences from diskcache

| diskcache                              | redisk                                        |
|----------------------------------------|-----------------------------------------------|
| SQLite k/v store                       | Redis/Kvrocks k/v store                       |
| `Cache(directory)`                     | `Cache(redis_conn_url, offload_folder, ...)`  |
| pickle serialization                   | msgpack only; keys/values/tags must be msgpack-native (tuple supported via ext type) |
| Client-side expiry checks              | Native Redis TTLs; every entry has one (`MAX_TTL_SECS` default & cap) |
| Transactions, `Timeout`, `retry` args  | Dropped (no multi-key atomicity without Lua)  |
| LRU/LFU eviction (writes on read)      | Dropped by design; `none` / `least-recently-stored` only |
| `push`/`pull`/`peek`/`peekitem` queues | Dropped                                       |
| `FanoutCache`, `Deque`, `Index`, `DjangoCache`, recipes | Dropped              |
| `volume()` = SQLite pages + files      | `volume()` = offloaded file bytes only        |
| `incr`/`pop` atomic                    | Not atomic (documented)                       |
| Settings persisted in SQLite           | Settings are constructor-only                 |
| `iterkeys()` sorted                    | `iterkeys()` arbitrary order (SCAN)           |

## Testing

Tests run against an in-process [fakeredis](https://github.com/cunla/fakeredis-py)
by default — no server required:

```console
$ pip install -e '.[test]'
$ pytest
```

To run against a real Redis or Kvrocks server:

```console
$ REDISK_TEST_REDIS_URL=redis://localhost:6666/0 pytest
```

## License

Apache 2.0. Derived from
[python-diskcache](https://github.com/grantjenks/python-diskcache),
Copyright 2016-2023 Grant Jenks. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
