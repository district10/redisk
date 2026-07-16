"""Test redisk.core.Cache.

Migrated from python-diskcache's tests/test_core.py and adapted to the
Redis-backed implementation:

* SQLite-specific tests (pragmas, timeouts, integrity check, copy/rsync)
  are dropped.
* Queue methods (push/pull/peek/peekitem) and LRU/LFU eviction policies
  are unsupported and their tests are dropped.
* Iteration order is arbitrary (Redis SCAN), so ordering assertions are
  replaced with set comparisons.
"""

import errno
import hashlib
import io
import os
import os.path as op
import pathlib
import pickle
import time
import warnings
from unittest import mock

import pytest

import redisk as dc

pytestmark = pytest.mark.filterwarnings('ignore', category=dc.EmptyDirWarning)


def test_init(cache):
    for key, value in dc.DEFAULT_SETTINGS.items():
        assert getattr(cache, key) == value
    cache.check()
    cache.close()
    cache.close()


def test_init_path(cache):
    path = pathlib.Path(cache.directory)
    other = dc.Cache(cache._redis, path, prefix=cache.prefix + '-other')
    other.close()
    assert cache.directory == other.directory


def test_init_disk(redis_client, tmp_path):
    with dc.Cache(
        redis_client,
        str(tmp_path / 'offload'),
        prefix='redisk-test-init-disk',
        disk_pickle_protocol=1,
        disk_min_file_size=2**20,
    ) as cache:
        key = (None, 0, 'abc')
        cache[key] = 0
        cache.check()
        assert cache.disk_min_file_size == 2**20
        assert cache.disk_pickle_protocol == 1
        assert cache._disk.min_file_size == 2**20
        assert cache._disk.pickle_protocol == 1


def test_disk_valueerror():
    with pytest.raises(ValueError):
        with dc.Cache(disk=dc.Disk('test')):
            pass


def test_unknown_setting(cache):
    with pytest.raises(ValueError):
        dc.Cache(cache._redis, cache.directory, prefix='x', sqlite_cache_size=100)


def test_eviction_policy_valueerror(cache):
    with pytest.raises(ValueError):
        dc.Cache(
            cache._redis,
            cache.directory,
            prefix='x',
            eviction_policy='least-recently-used',
        )


def test_custom_disk(redis_client, tmp_path):
    with dc.Cache(
        redis_client,
        str(tmp_path / 'offload'),
        prefix='redisk-test-custom-disk',
        disk=dc.JSONDisk,
        disk_compress_level=6,
    ) as cache:
        values = [None, True, 0, 1.23, {}, [None] * 10000]

        for value in values:
            cache[value] = value

        for value in values:
            assert cache[value] == value

        # Iteration order is arbitrary; compare via repr.
        assert sorted(map(repr, cache)) == sorted(map(repr, values))

        test_memoize_iter(cache)


class SHA256FilenameDisk(dc.Disk):
    def filename(self, key=dc.UNKNOWN, value=dc.UNKNOWN):
        filename = hashlib.sha256(key).hexdigest()[:32]
        full_path = op.join(self._directory, filename)
        return filename, full_path


def test_custom_filename_disk(redis_client, tmp_path):
    directory = str(tmp_path / 'offload')

    with dc.Cache(
        redis_client,
        directory,
        prefix='redisk-test-sha256-disk',
        disk=SHA256FilenameDisk,
    ) as cache:
        for count in range(100, 200):
            key = str(count).encode('ascii')
            cache[key] = str(count) * int(1e5)

    for count in range(100, 200):
        key = str(count).encode('ascii')
        filename = hashlib.sha256(key).hexdigest()[:32]
        full_path = op.join(directory, filename)

        with open(full_path) as reader:
            content = reader.read()
            assert content == str(count) * int(1e5)


def test_init_makedirs(tmp_path):
    cache_dir = str(tmp_path / 'does-not-exist')
    makedirs = mock.Mock(side_effect=OSError(errno.EACCES))

    with pytest.raises(EnvironmentError):
        with mock.patch('os.makedirs', makedirs):
            dc.Cache('redis://localhost:6379/0', cache_dir)


def test_getsetdel(cache):
    values = [
        (None, False),
        ((None,) * 2**20, False),
        (1234, False),
        (2**512, False),
        (56.78, False),
        ('hello', False),
        ('hello' * 2**20, False),
        (b'world', False),
        (b'world' * 2**20, False),
        (io.BytesIO(b'world' * 2**20), True),
    ]

    for key, (value, file_like) in enumerate(values):
        assert cache.set(key, value, read=file_like)

    assert len(cache) == len(values)

    for key, (value, file_like) in enumerate(values):
        if file_like:
            assert cache[key] == value.getvalue()
        else:
            assert cache[key] == value

    for key, _ in enumerate(values):
        del cache[key]

    assert len(cache) == 0

    for value, (key, _) in enumerate(values):
        cache[key] = value

    assert len(cache) == len(values)

    for value, (key, _) in enumerate(values):
        assert cache[key] == value

    for _, (key, _) in enumerate(values):
        del cache[key]

    assert len(cache) == 0

    cache.check()


def test_get_keyerror1(cache):
    with pytest.raises(KeyError):
        cache[0]


def test_get_keyerror4(cache):
    func = mock.Mock(side_effect=IOError(errno.ENOENT, ''))

    cache.stats(enable=True)
    cache[0] = b'abcd' * 2**20

    with mock.patch('redisk.core.open', func):
        with pytest.raises((IOError, KeyError, OSError)):
            cache[0]


def test_read(cache):
    cache.set(0, b'abcd' * 2**20)
    with cache.read(0) as reader:
        assert reader is not None


def test_read_keyerror(cache):
    with pytest.raises(KeyError):
        with cache.read(0):
            pass


def test_set_twice(cache):
    large_value = b'abcd' * 2**20

    cache[0] = 0
    cache[0] = 1

    assert cache[0] == 1

    cache[0] = large_value

    assert cache[0] == large_value
    with cache.get(0, read=True) as reader:
        assert reader.name is not None

    cache[0] = 2

    assert cache[0] == 2
    assert cache.get(0, read=True) == 2

    cache.check()


def test_raw(cache):
    assert cache.set(0, io.BytesIO(b'abcd'), read=True)
    assert cache[0] == b'abcd'


def test_get(cache):
    assert cache.get(0) is None
    assert cache.get(1, 'dne') == 'dne'
    assert cache.get(2, {}) == {}
    assert cache.get(0, expire_time=True, tag=True) == (None, None, None)

    assert cache.set(0, 0, expire=None, tag='number')

    assert cache.get(0, expire_time=True) == (0, None)
    assert cache.get(0, tag=True) == (0, 'number')
    assert cache.get(0, expire_time=True, tag=True) == (0, None, 'number')


def test_get_expired(cache):
    assert cache.set(0, 0, expire=0.001)
    time.sleep(0.01)
    assert cache.get(0) is None


def test_get_ioerror(cache):
    assert cache.set(0, 0)

    disk = mock.Mock()
    put = mock.Mock()
    fetch = mock.Mock()

    disk.put = put
    put.side_effect = [(0, True)]
    disk.fetch = fetch
    io_error = IOError()
    io_error.errno = errno.ENOENT
    fetch.side_effect = io_error

    with mock.patch.object(cache, '_disk', disk):
        assert cache.get(0) is None


def test_pop(cache):
    assert cache.incr('alpha') == 1
    assert cache.pop('alpha') == 1
    assert cache.get('alpha') is None
    assert cache.check() == []

    assert cache.set('alpha', 123, expire=1, tag='blue')
    assert cache.pop('alpha', tag=True) == (123, 'blue')

    assert cache.set('beta', 456, expire=1e-9, tag='green')
    time.sleep(0.01)
    assert cache.pop('beta', 'dne') == 'dne'

    assert cache.set('gamma', 789, tag='red')
    assert cache.pop('gamma', expire_time=True, tag=True) == (789, None, 'red')

    assert cache.pop('dne') is None

    assert cache.set('delta', 210)
    assert cache.pop('delta', expire_time=True) == (210, None)

    assert cache.set('epsilon', '0' * 2**20)
    assert cache.pop('epsilon') == '0' * 2**20


def test_pop_ioerror(cache):
    assert cache.set(0, 0)

    disk = mock.Mock()
    put = mock.Mock()
    fetch = mock.Mock()

    disk.put = put
    put.side_effect = [(0, True)]
    disk.fetch = fetch
    io_error = IOError()
    io_error.errno = errno.ENOENT
    fetch.side_effect = io_error

    with mock.patch.object(cache, '_disk', disk):
        assert cache.pop(0) is None


def test_delete(cache):
    cache[0] = 0
    assert cache.delete(0)
    assert len(cache) == 0
    assert not cache.delete(0)
    assert len(cache.check()) == 0


def test_del(cache):
    with pytest.raises(KeyError):
        del cache[0]


def test_del_expired(cache):
    cache.set(0, 0, expire=0.001)
    time.sleep(0.01)
    with pytest.raises(KeyError):
        del cache[0]


def test_stats(cache):
    cache[0] = 0

    assert cache.stats(enable=True) == (0, 0)

    for _ in range(100):
        cache[0]

    for _ in range(10):
        cache.get(1)

    assert cache.stats(reset=True) == (100, 10)
    assert cache.stats(enable=False) == (0, 0)

    for _ in range(100):
        cache[0]

    for _ in range(10):
        cache.get(1)

    assert cache.stats() == (0, 0)
    assert len(cache.check()) == 0


def test_path(cache):
    cache[0] = 'abc'
    large_value = b'abc' * 2**20
    cache[1] = large_value

    assert cache.get(0, read=True) == 'abc'

    with cache.get(1, read=True) as reader:
        assert reader.name is not None
        path = reader.name

    with open(path, 'rb') as reader:
        value = reader.read()

    assert value == large_value

    assert len(cache.check()) == 0


def test_least_recently_stored(cache):
    cache.eviction_policy = 'least-recently-stored'
    cache.size_limit = int(5.5e6)
    cache.cull_limit = 1

    million = b'x' * int(1e6)

    for value in range(5):
        cache[value] = million

    assert len(cache) == 5

    cache[5] = million  # 6e6 > 5.5e6, evicts oldest (key 0).

    assert len(cache) == 5
    assert 0 not in cache
    assert cache[5] == million

    # Re-storing refreshes the store time and protects from eviction.

    cache[2] = million
    cache[6] = million  # Over limit again, evicts oldest (key 1).

    assert 1 not in cache
    assert 2 in cache
    assert len(cache) == 5

    assert len(cache.check()) == 0


def test_check(cache):
    blob = b'a' * 2**20
    keys = (0, 1, 1234, 56.78, 'hello', b'world', None)

    for key in keys:
        cache[key] = blob

    # Cause mayhem.

    with cache.get(0, read=True) as reader:
        full_path = reader.name
    os.rename(full_path, full_path + '_moved')

    with cache.get(1, read=True) as reader:
        full_path = reader.name
    os.remove(full_path)

    cache._redis.hset(cache._meta_key, 'count', 0)
    cache._redis.hset(cache._meta_key, 'size', 0)

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        cache.check()
        cache.check(fix=True)

    assert len(cache.check()) == 0  # Should display no warnings.


def test_expire(cache):
    for value in range(10):
        assert cache.set(value, value, expire=0.01)

    for value in range(10, 15):
        assert cache.set(value, value)

    assert len(cache) == 15

    time.sleep(0.05)

    # Server-side TTLs removed the keys; expire() reconciles the index
    # and counters.
    assert cache.expire() == 10

    assert len(cache) == 5
    assert len(cache.check()) == 0


def test_evict(cache):
    colors = ('red', 'blue', 'yellow')

    for value in range(90):
        assert cache.set(value, value, tag=colors[value % len(colors)])

    assert len(cache) == 90
    assert cache.evict('red') == 30
    assert len(cache) == 60
    assert len(cache.check()) == 0


def test_clear(cache):
    for value in range(100):
        cache[value] = value
    assert len(cache) == 100
    assert cache.clear() == 100
    assert len(cache) == 0
    assert len(cache.check()) == 0


def test_tag(cache):
    assert cache.set(0, None, tag='zero')
    assert cache.set(1, None, tag=1234)
    assert cache.set(2, None, tag=5.67)
    assert cache.set(3, None, tag=b'three')

    assert cache.get(0, tag=True) == (None, 'zero')
    assert cache.get(1, tag=True) == (None, 1234)
    assert cache.get(2, tag=True) == (None, 5.67)
    assert cache.get(3, tag=True) == (None, b'three')


def test_with(cache):
    with dc.Cache(cache._redis, cache.directory, prefix=cache.prefix) as tmp:
        tmp['a'] = 0
        tmp['b'] = 1

    assert cache['a'] == 0
    assert cache['b'] == 1


def test_contains(cache):
    assert 0 not in cache
    cache[0] = 0
    assert 0 in cache


def test_touch(cache):
    assert cache.set(0, None, expire=60)
    assert cache.touch(0, expire=None)
    assert cache.touch(0, expire=0)
    assert not cache.touch(0)


def test_add(cache):
    assert cache.add(1, 1)
    assert cache.get(1) == 1
    assert not cache.add(1, 2)
    assert cache.get(1) == 1
    assert cache.delete(1)
    assert cache.add(1, 1, expire=0.001)
    time.sleep(0.01)
    assert cache.add(1, 1)
    cache.check()


def test_add_large_value(cache):
    value = b'abcd' * 2**20
    assert cache.add(b'test-key', value)
    assert cache.get(b'test-key') == value
    assert not cache.add(b'test-key', value * 2)
    assert cache.get(b'test-key') == value
    cache.check()


def test_incr(cache):
    assert cache.incr('key', default=5) == 6
    assert cache.incr('key', 2) == 8
    assert cache.get('key', expire_time=True, tag=True) == (8, None, None)
    assert cache.delete('key')
    assert cache.set('key', 100, expire=0.100)
    assert cache.get('key') == 100
    time.sleep(0.120)
    assert cache.incr('key') == 1


def test_incr_insert_keyerror(cache):
    with pytest.raises(KeyError):
        cache.incr('key', default=None)


def test_incr_update_keyerror(cache):
    assert cache.set('key', 100, expire=0.100)
    assert cache.get('key') == 100
    time.sleep(0.120)
    with pytest.raises(KeyError):
        cache.incr('key', default=None)


def test_decr(cache):
    assert cache.decr('key', default=5) == 4
    assert cache.decr('key', 2) == 2
    assert cache.get('key', expire_time=True, tag=True) == (2, None, None)
    assert cache.delete('key')
    assert cache.set('key', 100, expire=0.100)
    assert cache.get('key') == 100
    time.sleep(0.120)
    assert cache.decr('key') == -1


def test_iter(cache):
    sequence = list('abcdef') + [('g',)]

    for index, value in enumerate(sequence):
        cache[value] = index

    # Iteration order is arbitrary.
    assert set(iter(cache)) == set(sequence)


def test_iter_expire(cache):
    for num in range(10):
        cache.set(num, num, expire=0.01)
    for num in range(10, 20):
        cache.set(num, num)

    time.sleep(0.05)

    assert set(iter(cache)) == set(range(10, 20))


def test_iter_error(cache):
    with pytest.raises(StopIteration):
        next(iter(cache))


def test_reversed(cache):
    sequence = 'abcdef'

    for index, value in enumerate(sequence):
        cache[value] = index

    assert set(reversed(cache)) == set(sequence)


def test_reversed_error(cache):
    with pytest.raises(StopIteration):
        next(reversed(cache))


def test_iterkeys(cache):
    assert list(cache.iterkeys()) == []

    for key in [4, 1, 3, 0, 2]:
        cache[key] = key

    assert set(cache.iterkeys()) == {0, 1, 2, 3, 4}


def test_pickle(tmp_path):
    cache = dc.Cache(
        'redis://localhost:6379/0',
        str(tmp_path / 'offload'),
        prefix='redisk-test-pickle',
    )

    data = pickle.dumps(cache)
    other = pickle.loads(data)

    assert other.directory == cache.directory
    assert other.prefix == cache.prefix

    other.close()
    cache.close()


def test_pickle_client_error(cache):
    with pytest.raises(TypeError):
        pickle.dumps(cache)


def test_size_limit_with_files(cache):
    cache.cull_limit = 0
    size_limit = 30 * cache.disk_min_file_size
    cache.size_limit = size_limit
    value = b'foo' * cache.disk_min_file_size

    for key in range(40):
        cache.set(key, value)

    assert cache.volume() > size_limit
    cache.cull()
    assert cache.volume() <= size_limit


def test_size_limit_with_inline_values(cache):
    cache.cull_limit = 0
    cache.size_limit = 2 * cache.disk_min_file_size
    value = b'0123456789' * 10

    for key in range(100):
        cache.set(key, value)

    # Small values live in Redis and do not count towards the disk limit.
    assert cache.volume() == 0
    assert len(cache) == 100
    cache.cull()
    assert len(cache) == 100


def test_cull_eviction_policy_none(cache):
    cache.eviction_policy = 'none'
    cache.size_limit = 2 * cache.disk_min_file_size
    value = b'foo' * cache.disk_min_file_size

    for key in range(10):
        cache.set(key, value)

    assert cache.volume() > cache.size_limit
    cache.cull()
    assert cache.volume() > cache.size_limit


def test_cull_size_limit_0(cache):
    cache.cull_limit = 0
    cache.size_limit = 0
    value = b'foo' * cache.disk_min_file_size

    for key in range(10):
        cache.set(key, value)

    assert cache.volume() > 0
    cache.cull()
    assert cache.volume() <= 0
    assert len(cache) == 0


def test_key_roundtrip(cache):
    key_part_0 = 'part0'
    key_part_1 = 'part1'
    to_test = [
        (key_part_0, key_part_1),
        [key_part_0, key_part_1],
    ]

    for key in to_test:
        cache.clear()
        cache[key] = {'example0': ['value0']}
        keys = list(cache)
        assert len(keys) == 1
        cache_key = keys[0]
        assert cache[key] == {'example0': ['value0']}
        assert cache[cache_key] == {'example0': ['value0']}


def test_constant():
    import redisk.core

    assert repr(redisk.core.ENOVAL) == 'ENOVAL'


def test_memoize(cache):
    count = 1000

    def fibiter(num):
        alpha, beta = 0, 1

        for _ in range(num):
            alpha, beta = beta, alpha + beta

        return alpha

    @cache.memoize()
    def fibrec(num):
        if num == 0:
            return 0
        elif num == 1:
            return 1
        else:
            return fibrec(num - 1) + fibrec(num - 2)

    cache.stats(enable=True)

    for value in range(count):
        assert fibrec(value) == fibiter(value)

    hits1, misses1 = cache.stats()

    for value in range(count):
        assert fibrec(value) == fibiter(value)

    hits2, misses2 = cache.stats()

    assert hits2 == (hits1 + count)
    assert misses2 == misses1


def test_memoize_kwargs(cache):
    @cache.memoize(typed=True)
    def foo(*args, **kwargs):
        return args, kwargs

    assert foo(1, 2, 3, a=4, b=5) == ((1, 2, 3), {'a': 4, 'b': 5})


def test_memoize_ignore(cache):
    @cache.memoize(ignore={1, 'arg1'})
    def test(*args, **kwargs):
        return args, kwargs

    cache.stats(enable=True)
    assert test('a', 'b', 'c', arg0='d', arg1='e', arg2='f')
    assert test('a', 'w', 'c', arg0='d', arg1='x', arg2='f')
    assert test('a', 'y', 'c', arg0='d', arg1='z', arg2='f')
    assert cache.stats() == (2, 1)


def test_memoize_iter(cache):
    @cache.memoize()
    def test(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    cache.clear()
    assert test(1, 2, 3)
    assert test(a=1, b=2, c=3)
    assert test(-1, 0, 1, a=1, b=2, c=3)
    assert len(cache) == 3
    for key in cache:
        assert cache[key] == 6


def test_cleanup_dirs(cache):
    value = b'\0' * 2**20
    start_count = len(os.listdir(cache.directory))
    for i in range(10):
        cache[i] = value
    set_count = len(os.listdir(cache.directory))
    assert set_count > start_count
    for i in range(10):
        del cache[i]
    del_count = len(os.listdir(cache.directory))
    assert start_count == del_count


def test_disk_write_os_error(cache):
    func = mock.Mock(side_effect=[OSError] * 10)
    with mock.patch('redisk.core.open', func):
        with pytest.raises(OSError):
            cache[0] = '\0' * 2**20
