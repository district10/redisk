"""Core Redis and file backed cache API.

Derived from python-diskcache (diskcache/core.py) by Grant Jenks, with the
SQLite key/value store replaced by Redis (or Kvrocks). Only simple Redis
commands are used: GET, SET (with PX/NX), DEL, EXISTS, PTTL, SCAN, and
hash/sorted-set commands for bookkeeping. No Lua scripts, and reads never
update TTLs or access metadata.

Large values are offloaded to files in a local directory, exactly like
diskcache's ``Disk`` serialization.
"""

import codecs
import contextlib as cl
import errno
import functools as ft
import io
import json
import os
import os.path as op
import pickle
import pickletools
import tempfile
import time
import warnings
import zlib
from collections import namedtuple

import redis


def full_name(func):
    """Return full name of `func` by adding the module and function name."""
    return func.__module__ + '.' + func.__qualname__


class Constant(tuple):
    """Pretty display of immutable constant."""

    def __new__(cls, name):
        return tuple.__new__(cls, (name,))

    def __repr__(self):
        return '%s' % self[0]


ENOVAL = Constant('ENOVAL')
UNKNOWN = Constant('UNKNOWN')

MODE_NONE = 0
MODE_RAW = 1
MODE_BINARY = 2
MODE_TEXT = 3
MODE_PICKLE = 4

DEFAULT_SETTINGS = {
    'statistics': 0,  # False
    'eviction_policy': 'least-recently-stored',
    'size_limit': 2**30,  # 1gb
    'cull_limit': 10,
    'disk_min_file_size': 2**15,  # 32kb
    'disk_pickle_protocol': pickle.HIGHEST_PROTOCOL,
}

# Eviction policies that do not require writes during reads. The
# 'least-recently-used' and 'least-frequently-used' policies from diskcache
# are intentionally unsupported: they update access metadata on every get,
# which this package avoids by design.
EVICTION_POLICY = (
    'none',
    'least-recently-stored',
)

# Record stored (pickled) as the Redis value for each cache entry.
_Record = namedtuple(
    '_Record',
    'store_time expire_time tag size mode filename value',
)


def _encode_key(db_key, raw):
    """Encode a database key (from Disk.put) as bytes with a type tag."""
    if not raw:
        return b'p' + db_key
    if isinstance(db_key, bytes):
        return b'b' + db_key
    if isinstance(db_key, str):
        return b's' + db_key.encode('utf-8')
    if isinstance(db_key, int):
        return b'i' + str(db_key).encode('ascii')
    assert isinstance(db_key, float)
    return b'f' + repr(db_key).encode('ascii')


def _decode_key(encoded):
    """Decode bytes from `_encode_key` back to a (database key, raw) pair."""
    tag, payload = encoded[:1], encoded[1:]
    if tag == b'b':
        return payload, True
    if tag == b's':
        return payload.decode('utf-8'), True
    if tag == b'i':
        return int(payload), True
    if tag == b'f':
        return float(payload), True
    assert tag == b'p'
    return payload, False


class Disk:
    """Cache key and value serialization for Redis values and files."""

    def __init__(self, directory, min_file_size=0, pickle_protocol=0):
        """Initialize disk instance.

        :param str directory: directory path
        :param int min_file_size: minimum size for file use
        :param int pickle_protocol: pickle protocol for serialization

        """
        self._directory = directory
        self.min_file_size = min_file_size
        self.pickle_protocol = pickle_protocol

    def put(self, key):
        """Convert `key` to database key and raw pair.

        :param key: key to convert
        :return: (database key, raw boolean) pair

        """
        # pylint: disable=unidiomatic-typecheck
        type_key = type(key)

        if type_key is bytes:
            return key, True
        elif (
            (type_key is str)
            or (
                type_key is int
                and -9223372036854775808 <= key <= 9223372036854775807
            )
            or (type_key is float)
        ):
            return key, True
        else:
            data = pickle.dumps(key, protocol=self.pickle_protocol)
            result = pickletools.optimize(data)
            return result, False

    def get(self, key, raw):
        """Convert database key and raw pair back to the original key.

        :param key: database key to convert
        :param bool raw: flag indicating raw storage
        :return: corresponding Python key

        """
        if raw:
            return key
        else:
            return pickle.load(io.BytesIO(key))

    def store(self, value, read, key=UNKNOWN):
        """Convert `value` to fields size, mode, filename, and value.

        Values smaller than `min_file_size` are returned inline (to be stored
        inside the Redis record); larger values are written to files.

        :param value: value to convert
        :param bool read: True when value is file-like object
        :param key: key for item (default UNKNOWN)
        :return: (size, mode, filename, value) tuple

        """
        # pylint: disable=unidiomatic-typecheck
        type_value = type(value)
        min_file_size = self.min_file_size

        if (
            (type_value is str and len(value) < min_file_size)
            or (
                type_value is int
                and -9223372036854775808 <= value <= 9223372036854775807
            )
            or (type_value is float)
        ):
            return 0, MODE_RAW, None, value
        elif type_value is bytes:
            if len(value) < min_file_size:
                return 0, MODE_RAW, None, value
            else:
                filename, full_path = self.filename(key, value)
                self._write(full_path, io.BytesIO(value), 'xb')
                return len(value), MODE_BINARY, filename, None
        elif type_value is str:
            filename, full_path = self.filename(key, value)
            self._write(full_path, io.StringIO(value), 'x', 'UTF-8')
            size = op.getsize(full_path)
            return size, MODE_TEXT, filename, None
        elif read:
            reader = ft.partial(value.read, 2**22)
            filename, full_path = self.filename(key, value)
            iterator = iter(reader, b'')
            size = self._write(full_path, iterator, 'xb')
            return size, MODE_BINARY, filename, None
        else:
            result = pickle.dumps(value, protocol=self.pickle_protocol)

            if len(result) < min_file_size:
                return 0, MODE_PICKLE, None, result
            else:
                filename, full_path = self.filename(key, value)
                self._write(full_path, io.BytesIO(result), 'xb')
                return len(result), MODE_PICKLE, filename, None

    def _write(self, full_path, iterator, mode, encoding=None):
        full_dir, _ = op.split(full_path)

        for count in range(1, 11):
            with cl.suppress(OSError):
                os.makedirs(full_dir)

            try:
                # Another cache may have deleted the directory before
                # the file could be opened.
                writer = open(full_path, mode, encoding=encoding)
            except OSError:
                if count == 10:
                    # Give up after 10 tries to open the file.
                    raise
                continue

            with writer:
                size = 0
                for chunk in iterator:
                    size += len(chunk)
                    writer.write(chunk)
                return size

    def fetch(self, mode, filename, value, read):
        """Convert fields `mode`, `filename`, and `value` back to a value.

        :param int mode: value mode raw, binary, text, or pickle
        :param str filename: filename of corresponding value
        :param value: inline value from the Redis record
        :param bool read: when True, return an open file handle
        :return: corresponding Python value
        :raises: IOError if the value cannot be read

        """
        # pylint: disable=consider-using-with
        if mode == MODE_RAW:
            return value
        elif mode == MODE_BINARY:
            if read:
                return open(op.join(self._directory, filename), 'rb')
            else:
                with open(op.join(self._directory, filename), 'rb') as reader:
                    return reader.read()
        elif mode == MODE_TEXT:
            full_path = op.join(self._directory, filename)
            with open(full_path, 'r', encoding='UTF-8') as reader:
                return reader.read()
        elif mode == MODE_PICKLE:
            if value is None:
                with open(op.join(self._directory, filename), 'rb') as reader:
                    return pickle.load(reader)
            else:
                return pickle.load(io.BytesIO(value))

    def filename(self, key=UNKNOWN, value=UNKNOWN):
        """Return filename and full-path tuple for file storage.

        Filename will be a randomly generated 28 character hexadecimal string
        with ".val" suffixed. Two levels of sub-directories will be used to
        reduce the size of directories. On older filesystems, lookups in
        directories with many files may be slow.

        The default implementation ignores the `key` and `value` parameters.

        :param key: key for item (default UNKNOWN)
        :param value: value for item (default UNKNOWN)

        """
        # pylint: disable=unused-argument
        hex_name = codecs.encode(os.urandom(16), 'hex').decode('utf-8')
        sub_dir = op.join(hex_name[:2], hex_name[2:4])
        name = hex_name[4:] + '.val'
        filename = op.join(sub_dir, name)
        full_path = op.join(self._directory, filename)
        return filename, full_path

    def remove(self, file_path):
        """Remove a file given by `file_path`.

        This method is cross-thread and cross-process safe. If an OSError
        occurs, it is suppressed.

        :param str file_path: relative path to file

        """
        full_path = op.join(self._directory, file_path)

        # Suppress OSError that may occur if two caches attempt to delete the
        # same file or directory at the same time.

        with cl.suppress(OSError):
            os.remove(full_path)

        # Remove empty sub-directories, but never the root directory itself
        # (unlike os.removedirs, which would remove it too).

        root = op.abspath(self._directory)
        dirpath = op.abspath(op.dirname(full_path))

        while dirpath.startswith(root + os.sep):
            try:
                os.rmdir(dirpath)
            except OSError:
                break
            dirpath = op.dirname(dirpath)


class JSONDisk(Disk):
    """Cache key and value using JSON serialization with zlib compression."""

    def __init__(self, directory, compress_level=1, **kwargs):
        """Initialize JSON disk instance.

        Keys and values are compressed using the zlib library. The
        `compress_level` is an integer from 0 to 9 controlling the level of
        compression; 1 is fastest and produces the least compression, 9 is
        slowest and produces the most compression, and 0 is no compression.

        :param str directory: directory path
        :param int compress_level: zlib compression level (default 1)
        :param kwargs: super class arguments

        """
        self.compress_level = compress_level
        super().__init__(directory, **kwargs)

    def put(self, key):
        json_bytes = json.dumps(key).encode('utf-8')
        data = zlib.compress(json_bytes, self.compress_level)
        return super().put(data)

    def get(self, key, raw):
        data = super().get(key, raw)
        return json.loads(zlib.decompress(data).decode('utf-8'))

    def store(self, value, read, key=UNKNOWN):
        if not read:
            json_bytes = json.dumps(value).encode('utf-8')
            value = zlib.compress(json_bytes, self.compress_level)
        return super().store(value, read, key=key)

    def fetch(self, mode, filename, value, read):
        data = super().fetch(mode, filename, value, read)
        if not read:
            data = json.loads(zlib.decompress(data).decode('utf-8'))
        return data


class UnknownFileWarning(UserWarning):
    """Warning used by Cache.check for unknown files."""


class EmptyDirWarning(UserWarning):
    """Warning used by Cache.check for empty directories."""


def args_to_key(base, args, kwargs, typed, ignore):
    """Create cache key out of function arguments.

    :param tuple base: base of key
    :param tuple args: function arguments
    :param dict kwargs: function keyword arguments
    :param bool typed: include types in cache key
    :param set ignore: positional or keyword args to ignore
    :return: cache key tuple

    """
    args = tuple(arg for index, arg in enumerate(args) if index not in ignore)
    key = base + args + (None,)

    if kwargs:
        kwargs = {key: val for key, val in kwargs.items() if key not in ignore}
        sorted_items = sorted(kwargs.items())

        for item in sorted_items:
            key += item

    if typed:
        key += tuple(type(arg) for arg in args)

        if kwargs:
            key += tuple(type(value) for _, value in sorted_items)

    return key


class Cache:
    """Redis and file backed cache.

    Each entry is a single Redis key holding a pickled record with the entry
    metadata and (for small values) the value itself. Values larger than
    ``disk_min_file_size`` are written to files below ``offload_folder`` and
    the record holds the filename instead.

    Expiry uses native Redis TTLs (``SET ... PX``). Reads are a single GET:
    they never update TTLs or access metadata.
    """

    def __init__(
        self,
        redis_conn_url='redis://localhost:6379/0',
        offload_folder=None,
        prefix='redisk',
        disk=Disk,
        **settings,
    ):
        """Initialize cache instance.

        :param redis_conn_url: Redis connection URL (for example
            ``redis://localhost:6379/0`` or ``rediss://`` for TLS) or an
            already-constructed client object with the ``redis.Redis``
            interface (useful for testing, e.g. fakeredis). Clients must
            use ``decode_responses=False`` (the default).
        :param str offload_folder: directory for offloaded value files
            (default None, a temporary directory is created)
        :param str prefix: namespace for all Redis keys used by this cache
            (default ``'redisk'``); use distinct prefixes to share one
            Redis/Kvrocks instance between caches
        :param disk: Disk type or subclass for serialization
        :param settings: any of DEFAULT_SETTINGS (``statistics``,
            ``eviction_policy``, ``size_limit``, ``cull_limit``) plus
            ``disk_``-prefixed arguments for the Disk instance

        """
        try:
            assert issubclass(disk, Disk)
        except (TypeError, AssertionError):
            raise ValueError('disk must subclass redisk.Disk') from None

        if offload_folder is None:
            offload_folder = tempfile.mkdtemp(prefix='redisk-')
        directory = str(offload_folder)
        directory = op.expanduser(directory)
        directory = op.expandvars(directory)

        self._directory = directory

        if not op.isdir(directory):
            try:
                os.makedirs(directory, 0o755)
            except OSError as error:
                if error.errno != errno.EEXIST:
                    raise EnvironmentError(
                        error.errno,
                        'Cache directory "%s" does not exist'
                        ' and could not be created' % self._directory,
                    ) from None

        # Setup Redis client. When a URL string is given, the client is
        # created (and owned) by the cache; otherwise the given client-like
        # object is used as-is and left open on close().

        if isinstance(redis_conn_url, str):
            self._redis_url = redis_conn_url
            self._redis = redis.Redis.from_url(redis_conn_url)
            self._owns_redis = True
        else:
            self._redis_url = None
            self._redis = redis_conn_url
            self._owns_redis = False

        self._prefix = prefix
        self._key_prefix = prefix.encode('utf-8') + b':cache:'
        self._meta_key = prefix + ':meta'
        self._index_key = prefix + ':index'

        # Setup settings.

        sets = DEFAULT_SETTINGS.copy()
        sets.update(settings)

        unknown = set(sets) - set(DEFAULT_SETTINGS)
        unknown = {key for key in unknown if not key.startswith('disk_')}

        if unknown:
            raise ValueError('unknown settings: %s' % sorted(unknown))

        if sets['eviction_policy'] not in EVICTION_POLICY:
            raise ValueError(
                'eviction_policy must be one of %r' % (EVICTION_POLICY,)
            )

        self._settings = sets
        self.statistics = sets['statistics']
        self.eviction_policy = sets['eviction_policy']
        self.size_limit = sets['size_limit']
        self.cull_limit = sets['cull_limit']

        # Setup Disk object.

        kwargs = {
            key[5:]: value
            for key, value in sets.items()
            if key.startswith('disk_')
        }
        self._disk = disk(directory, **kwargs)

    @property
    def directory(self):
        """Cache directory for offloaded files."""
        return self._directory

    @property
    def offload_folder(self):
        """Alias for :attr:`directory`."""
        return self._directory

    @property
    def prefix(self):
        """Namespace prefix for Redis keys."""
        return self._prefix

    @property
    def disk(self):
        """Disk used for serialization."""
        return self._disk

    @property
    def disk_min_file_size(self):
        """Minimum size in bytes for value file offload."""
        return self._disk.min_file_size

    @property
    def disk_pickle_protocol(self):
        """Pickle protocol used for serialization."""
        return self._disk.pickle_protocol

    def _rkey(self, key):
        """Compute the Redis key for a Python cache key."""
        db_key, raw = self._disk.put(key)
        return self._key_prefix + _encode_key(db_key, raw)

    @staticmethod
    def _pack(record):
        return pickle.dumps(record, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def _unpack(data):
        return pickle.loads(data)

    def _meta_incr(self, field, delta=1):
        self._redis.hincrby(self._meta_key, field, delta)

    def _meta_get(self, field):
        value = self._redis.hget(self._meta_key, field)
        return 0 if value is None else int(value)

    @staticmethod
    def _set_kwargs(expire):
        """Redis SET keyword arguments for the given expiry in seconds."""
        if expire is None:
            return {}
        return {'px': max(1, int(expire * 1000))}

    def _delete_record(self, rkey, record):
        """Delete Redis key, index member, counters, and value file."""
        self._redis.delete(rkey)
        self._redis.zrem(self._index_key, rkey)
        self._meta_incr('count', -1)
        self._meta_incr('size', -record.size)
        if record.filename is not None:
            self._disk.remove(record.filename)

    def set(self, key, value, expire=None, read=False, tag=None):
        """Set `key` and `value` item in cache.

        When `read` is `True`, `value` should be a file-like object opened
        for reading in binary mode.

        :param key: key for item
        :param value: value for item
        :param float expire: seconds until item expires
            (default None, no expiry)
        :param bool read: read value as bytes from file (default False)
        :param tag: value to associate with key (default None)
        :return: True if item was set

        """
        now = time.time()
        rkey = self._rkey(key)
        expire_time = None if expire is None else now + expire
        size, mode, filename, db_value = self._disk.store(value, read, key=key)
        record = self._pack(
            _Record(now, expire_time, tag, size, mode, filename, db_value)
        )

        try:
            old_data = self._redis.get(rkey)
            self._redis.set(rkey, record, **self._set_kwargs(expire))
            self._redis.zadd(self._index_key, {rkey: now})
        except BaseException:
            # Do not leak the freshly written value file on failure.
            if filename is not None:
                self._disk.remove(filename)
            raise

        if old_data is None:
            self._meta_incr('count', 1)
            self._meta_incr('size', size)
        else:
            old = self._unpack(old_data)
            self._meta_incr('size', size - old.size)
            if old.filename is not None:
                self._disk.remove(old.filename)

        self._cull(now)

        return True

    def __setitem__(self, key, value):
        """Set corresponding `value` for `key` in cache.

        :param key: key for item
        :param value: value for item

        """
        self.set(key, value)

    def _cull(self, now):
        """Evict oldest items when over the size limit (called on writes)."""
        cull_limit = self.cull_limit

        if cull_limit == 0 or self.eviction_policy == 'none':
            return

        if self.volume() <= self.size_limit:
            return

        self._evict_oldest(cull_limit)

    def _evict_oldest(self, limit):
        """Evict up to `limit` items, oldest stored first.

        Stale index members (whose Redis key has expired) are removed from
        the index but do not count towards `limit`.

        :return: count of items evicted

        """
        count = 0

        while count < limit:
            members = self._redis.zrange(self._index_key, 0, limit - count - 1)

            if not members:
                break

            for rkey in members:
                self._redis.zrem(self._index_key, rkey)
                data = self._redis.get(rkey)

                if data is None:
                    # Key expired via Redis TTL; clean the stale index
                    # member and reconcile the count counter.
                    self._meta_incr('count', -1)
                    continue

                self._delete_record(rkey, self._unpack(data))
                count += 1

        return count

    def touch(self, key, expire=None):
        """Touch `key` in cache and update `expire` time.

        :param key: key for item
        :param float expire: seconds until item expires
            (default None, no expiry)
        :return: True if key was touched

        """
        now = time.time()
        rkey = self._rkey(key)
        data = self._redis.get(rkey)

        if data is None:
            return False

        record = self._unpack(data)

        if expire is not None and expire <= 0:
            # Consistent with diskcache semantics, an immediate expiry.
            self._delete_record(rkey, record)
            return True

        expire_time = None if expire is None else now + expire
        record = record._replace(expire_time=expire_time)
        self._redis.set(rkey, self._pack(record), **self._set_kwargs(expire))

        return True

    def add(self, key, value, expire=None, read=False, tag=None):
        """Add `key` and `value` item to cache.

        Similar to `set`, but only add to cache if key not present.

        Operation is atomic (uses ``SET ... NX``). Only one concurrent add
        operation for a given key will succeed.

        When `read` is `True`, `value` should be a file-like object opened
        for reading in binary mode.

        :param key: key for item
        :param value: value for item
        :param float expire: seconds until the key expires
            (default None, no expiry)
        :param bool read: read value as bytes from file (default False)
        :param tag: value to associate with key (default None)
        :return: True if item was added

        """
        now = time.time()
        rkey = self._rkey(key)
        expire_time = None if expire is None else now + expire
        size, mode, filename, db_value = self._disk.store(value, read, key=key)
        record = self._pack(
            _Record(now, expire_time, tag, size, mode, filename, db_value)
        )

        kwargs = self._set_kwargs(expire)
        kwargs['nx'] = True

        try:
            added = self._redis.set(rkey, record, **kwargs)
        except BaseException:
            if filename is not None:
                self._disk.remove(filename)
            raise

        if not added:
            # Key already present; discard the freshly written value file.
            if filename is not None:
                self._disk.remove(filename)
            return False

        self._redis.zadd(self._index_key, {rkey: now})
        self._meta_incr('count', 1)
        self._meta_incr('size', size)

        self._cull(now)

        return True

    def incr(self, key, delta=1, default=0):
        """Increment value by delta for item with key.

        If key is missing and default is None then raise KeyError. Else if
        key is missing and default is not None then use default for value.

        Unlike diskcache, this operation is NOT atomic: it is implemented as
        a GET followed by a SET (Lua scripts are intentionally not used).
        Concurrent increments of the same key may lose updates.

        :param key: key for item
        :param int delta: amount to increment (default 1)
        :param int default: value if key is missing (default 0)
        :return: new value for item
        :raises KeyError: if key is not found and default is None

        """
        now = time.time()
        rkey = self._rkey(key)
        data = self._redis.get(rkey)

        if data is None:
            if default is None:
                raise KeyError(key)

            value = default + delta
            size, mode, filename, db_value = self._disk.store(
                value, False, key=key
            )
            record = self._pack(
                _Record(now, None, None, size, mode, filename, db_value)
            )
            self._redis.set(rkey, record)
            self._redis.zadd(self._index_key, {rkey: now})
            self._meta_incr('count', 1)
            self._meta_incr('size', size)
            self._cull(now)
            return value

        old = self._unpack(data)
        pttl = self._redis.pttl(rkey)

        value = self._disk.fetch(old.mode, old.filename, old.value, False)
        value += delta

        size, mode, filename, db_value = self._disk.store(value, False, key=key)
        expire_time = None if pttl <= 0 else now + pttl / 1000.0
        record = _Record(now, expire_time, old.tag, size, mode, filename, db_value)

        kwargs = {'px': pttl} if pttl > 0 else {}
        self._redis.set(rkey, self._pack(record), **kwargs)
        self._redis.zadd(self._index_key, {rkey: now})
        self._meta_incr('size', size - old.size)

        if old.filename is not None:
            self._disk.remove(old.filename)

        return value

    def decr(self, key, delta=1, default=0):
        """Decrement value by delta for item with key.

        If key is missing and default is None then raise KeyError. Else if
        key is missing and default is not None then use default for value.

        Unlike diskcache, this operation is NOT atomic (see :meth:`incr`).

        Unlike Memcached, negative values are supported. Value may be
        decremented below zero.

        :param key: key for item
        :param int delta: amount to decrement (default 1)
        :param int default: value if key is missing (default 0)
        :return: new value for item
        :raises KeyError: if key is not found and default is None

        """
        return self.incr(key, -delta, default)

    def get(self, key, default=None, read=False, expire_time=False, tag=False):
        """Retrieve value from cache. If `key` is missing, return `default`.

        Reads are a single Redis GET. Expiry is enforced by the server-side
        TTL; no TTL or access metadata is updated during reads.

        :param key: key for item
        :param default: value to return if key is missing (default None)
        :param bool read: if True, return file handle to value
            (default False)
        :param bool expire_time: if True, return expire_time in tuple
            (default False)
        :param bool tag: if True, return tag in tuple (default False)
        :return: value for item or default if key not found

        """
        rkey = self._rkey(key)

        if expire_time and tag:
            default = (default, None, None)
        elif expire_time or tag:
            default = (default, None)

        data = self._redis.get(rkey)

        if data is None:
            if self.statistics:
                self._meta_incr('misses')
            return default

        record = self._unpack(data)

        try:
            value = self._disk.fetch(
                record.mode, record.filename, record.value, read
            )
        except IOError:
            # Value file was deleted before we could retrieve result.
            if self.statistics:
                self._meta_incr('misses')
            return default

        if self.statistics:
            self._meta_incr('hits')

        if expire_time and tag:
            return (value, record.expire_time, record.tag)
        elif expire_time:
            return (value, record.expire_time)
        elif tag:
            return (value, record.tag)
        else:
            return value

    def __getitem__(self, key):
        """Return corresponding value for `key` from cache.

        :param key: key matching item
        :return: corresponding value
        :raises KeyError: if key is not found

        """
        value = self.get(key, default=ENOVAL)
        if value is ENOVAL:
            raise KeyError(key)
        return value

    def read(self, key):
        """Return file handle value corresponding to `key` from cache.

        :param key: key matching item
        :return: file open for reading in binary mode
        :raises KeyError: if key is not found

        """
        handle = self.get(key, default=ENOVAL, read=True)
        if handle is ENOVAL:
            raise KeyError(key)
        return handle

    def __contains__(self, key):
        """Return `True` if `key` matching item is found in cache.

        :param key: key matching item
        :return: True if key matching item

        """
        return bool(self._redis.exists(self._rkey(key)))

    def pop(self, key, default=None, expire_time=False, tag=False):
        """Remove corresponding item for `key` from cache and return value.

        If `key` is missing, return `default`.

        Unlike diskcache, this operation is NOT atomic: it is implemented as
        a GET followed by a DEL (Lua scripts are intentionally not used).

        :param key: key for item
        :param default: value to return if key is missing (default None)
        :param bool expire_time: if True, return expire_time in tuple
            (default False)
        :param bool tag: if True, return tag in tuple (default False)
        :return: value for item or default if key not found

        """
        rkey = self._rkey(key)

        if expire_time and tag:
            default = default, None, None
        elif expire_time or tag:
            default = default, None

        data = self._redis.get(rkey)

        if data is None:
            return default

        record = self._unpack(data)

        self._redis.delete(rkey)
        self._redis.zrem(self._index_key, rkey)
        self._meta_incr('count', -1)
        self._meta_incr('size', -record.size)

        try:
            value = self._disk.fetch(
                record.mode, record.filename, record.value, False
            )
        except IOError:
            # Value file was deleted before we could retrieve result.
            return default
        finally:
            if record.filename is not None:
                self._disk.remove(record.filename)

        if expire_time and tag:
            return value, record.expire_time, record.tag
        elif expire_time:
            return value, record.expire_time
        elif tag:
            return value, record.tag
        else:
            return value

    def __delitem__(self, key):
        """Delete corresponding item for `key` from cache.

        :param key: key matching item
        :raises KeyError: if key is not found

        """
        rkey = self._rkey(key)
        data = self._redis.get(rkey)

        if data is None:
            raise KeyError(key)

        self._delete_record(rkey, self._unpack(data))

        return True

    def delete(self, key):
        """Delete corresponding item for `key` from cache.

        Missing keys are ignored.

        :param key: key matching item
        :return: True if item was deleted

        """
        # pylint: disable=unnecessary-dunder-call
        try:
            return self.__delitem__(key)
        except KeyError:
            return False

    def memoize(self, name=None, typed=False, expire=None, tag=None, ignore=()):
        """Memoizing cache decorator.

        Decorator to wrap callable with memoizing function using cache.
        Repeated calls with the same arguments will lookup result in cache
        and avoid function evaluation.

        If name is set to None (default), the callable name will be
        determined automatically.

        When expire is set to zero, function results will not be set in the
        cache. Cache lookups still occur, however.

        If typed is set to True, function arguments of different types will
        be cached separately. For example, f(3) and f(3.0) will be treated
        as distinct calls with distinct results.

        The original underlying function is accessible through the
        __wrapped__ attribute. An additional `__cache_key__` attribute can
        be used to generate the cache key used for the given arguments.

        :param str name: name given for callable (default None, automatic)
        :param bool typed: cache different types separately (default False)
        :param float expire: seconds until arguments expire
            (default None, no expiry)
        :param str tag: text to associate with arguments (default None)
        :param set ignore: positional or keyword args to ignore (default ())
        :return: callable decorator

        """
        if callable(name):
            raise TypeError('name cannot be callable')

        def decorator(func):
            """Decorator created by memoize() for callable `func`."""
            base = (full_name(func),) if name is None else (name,)

            @ft.wraps(func)
            def wrapper(*args, **kwargs):
                """Wrapper for callable to cache arguments and values."""
                key = wrapper.__cache_key__(*args, **kwargs)
                result = self.get(key, default=ENOVAL)

                if result is ENOVAL:
                    result = func(*args, **kwargs)
                    if expire is None or expire > 0:
                        self.set(key, result, expire, tag=tag)

                return result

            def __cache_key__(*args, **kwargs):
                """Make key for cache given function arguments."""
                return args_to_key(base, args, kwargs, typed, ignore)

            wrapper.__cache_key__ = __cache_key__
            return wrapper

        return decorator

    def check(self, fix=False):
        """Check Redis records and file system consistency.

        Intended for use in testing and post-mortem error analysis. Also
        reconciles the bookkeeping counters (count and size) and the
        store-time index, which may drift when entries expire via Redis TTL
        or a write fails midway.

        :param bool fix: correct inconsistencies
        :return: list of warnings

        """
        # pylint: disable=too-many-branches
        with warnings.catch_warnings(record=True) as warns:
            filenames = set()
            count = 0
            size = 0

            for rkey in self._redis.scan_iter(
                match=self._key_prefix + b'*', count=100
            ):
                data = self._redis.get(rkey)

                if data is None:
                    continue

                record = self._unpack(data)

                if record.filename is not None:
                    full_path = op.join(self._directory, record.filename)

                    if not op.exists(full_path):
                        warnings.warn('file not found: %s' % full_path)

                        if fix:
                            self._redis.delete(rkey)
                            self._redis.zrem(self._index_key, rkey)
                            continue

                    else:
                        real_size = op.getsize(full_path)

                        if record.size != real_size:
                            message = 'wrong file size: %s, %d != %d'
                            args = full_path, real_size, record.size
                            warnings.warn(message % args)

                            if fix:
                                pttl = self._redis.pttl(rkey)
                                record = record._replace(size=real_size)
                                kwargs = {'px': pttl} if pttl > 0 else {}
                                self._redis.set(
                                    rkey, self._pack(record), **kwargs
                                )

                        filenames.add(full_path)

                count += 1
                size += record.size

            # Check file system against record filenames.

            for dirpath, _, files in os.walk(self._directory):
                paths = [op.join(dirpath, filename) for filename in files]
                error = set(paths) - filenames

                for full_path in error:
                    message = 'unknown file: %s' % full_path
                    warnings.warn(message, UnknownFileWarning)

                    if fix:
                        os.remove(full_path)

            # Check for empty directories (the root directory is exempt).

            for dirpath, dirs, files in os.walk(self._directory):
                if dirpath == self._directory:
                    continue

                if not (dirs or files):
                    message = 'empty directory: %s' % dirpath
                    warnings.warn(message, EmptyDirWarning)

                    if fix:
                        os.rmdir(dirpath)

            # Check counters against actual records.

            meta_count = self._meta_get('count')

            if meta_count != count:
                message = 'count != number of records; %d != %d'
                warnings.warn(message % (meta_count, count))

                if fix:
                    self._redis.hset(self._meta_key, 'count', count)

            meta_size = self._meta_get('size')

            if meta_size != size:
                message = 'size != sum of record sizes; %d != %d'
                warnings.warn(message % (meta_size, size))

                if fix:
                    self._redis.hset(self._meta_key, 'size', size)

            # Check the store-time index for stale members.

            for member in self._redis.zrange(self._index_key, 0, -1):
                if not self._redis.exists(member):
                    warnings.warn('stale index member: %r' % member)

                    if fix:
                        self._redis.zrem(self._index_key, member)

            return warns

    def evict(self, tag):
        """Remove items with matching `tag` from cache.

        Removing items is an iterative process: all entries are scanned and
        matching items are removed.

        :param str tag: tag identifying items
        :return: count of items removed

        """
        count = 0

        for rkey in self._redis.scan_iter(
            match=self._key_prefix + b'*', count=100
        ):
            data = self._redis.get(rkey)

            if data is None:
                continue

            record = self._unpack(data)

            if record.tag == tag:
                self._delete_record(rkey, record)
                count += 1

        return count

    def expire(self, now=None):
        """Remove expired items from cache and reconcile the index.

        Expiry is normally enforced by the server-side Redis TTL. This
        method removes records whose expire time has passed but which still
        exist (for example due to clock skew), and purges stale store-time
        index members left behind by TTL-expired keys.

        :param float now: current time (default None, ``time.time()`` used)
        :return: count of items removed

        """
        if now is None:
            now = time.time()

        count = 0

        for rkey in self._redis.scan_iter(
            match=self._key_prefix + b'*', count=100
        ):
            data = self._redis.get(rkey)

            if data is None:
                continue

            record = self._unpack(data)

            if record.expire_time is not None and record.expire_time < now:
                self._delete_record(rkey, record)
                count += 1

        # Purge index members whose key expired via Redis TTL.

        for member in self._redis.zrange(self._index_key, 0, -1):
            if not self._redis.exists(member):
                self._redis.zrem(self._index_key, member)
                self._meta_incr('count', -1)
                count += 1

        return count

    def cull(self):
        """Cull items from cache until volume is less than size limit.

        Expired items are removed first, then items are evicted oldest
        stored first (unless the eviction policy is 'none').

        :return: count of items removed

        """
        now = time.time()

        # Remove expired items.

        count = self.expire(now)

        # Remove items by policy.

        if self.eviction_policy == 'none':
            return count

        while self.volume() > self.size_limit:
            removed = self._evict_oldest(10)

            if removed == 0:
                break

            count += removed

        return count

    def clear(self):
        """Remove all items from cache.

        Removing items is an iterative process. In each iteration, a subset
        of items is removed. Concurrent writes may occur between iterations.

        :return: count of items removed

        """
        count = 0

        for rkey in self._redis.scan_iter(
            match=self._key_prefix + b'*', count=100
        ):
            data = self._redis.get(rkey)

            if data is not None:
                record = self._unpack(data)

                if record.filename is not None:
                    self._disk.remove(record.filename)

                count += 1

            self._redis.delete(rkey)
            self._redis.zrem(self._index_key, rkey)

        self._redis.hset(
            self._meta_key, mapping={'count': 0, 'size': 0}
        )

        return count

    def iterkeys(self):
        """Iterate cache keys.

        Unlike diskcache, keys are yielded in arbitrary (Redis SCAN) order,
        not sorted order.

        :return: iterator of cache keys

        """
        for rkey in self._redis.scan_iter(
            match=self._key_prefix + b'*', count=100
        ):
            encoded = rkey[len(self._key_prefix) :]
            db_key, raw = _decode_key(encoded)
            yield self._disk.get(db_key, raw)

    def __iter__(self):
        """Iterate keys in cache, in arbitrary order."""
        return self.iterkeys()

    def __reversed__(self):
        """Reverse iterate keys in cache, in arbitrary order."""
        return iter(list(self.iterkeys())[::-1])

    def stats(self, enable=True, reset=False):
        """Return cache statistics hits and misses.

        Statistics are collected only while enabled (default disabled), so
        that reads stay a single GET.

        :param bool enable: enable collecting statistics (default True)
        :param bool reset: reset hits and misses to 0 (default False)
        :return: (hits, misses)

        """
        hits = self._meta_get('hits')
        misses = self._meta_get('misses')

        if reset:
            self._redis.hset(
                self._meta_key, mapping={'hits': 0, 'misses': 0}
            )

        self.statistics = 1 if enable else 0

        return (hits, misses)

    def volume(self):
        """Return estimated total size of offloaded value files on disk.

        Unlike diskcache, this only counts offloaded value files; small
        values live in Redis and are not counted.

        :return: size in bytes

        """
        return self._meta_get('size')

    def close(self):
        """Close the Redis connection.

        Only closes the client when it was created by the cache from a
        connection URL; a client passed in by the caller is left open.

        """
        if self._owns_redis:
            self._redis.close()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.close()

    def __len__(self):
        """Count of items in cache, including possibly-expired items.

        The count is a bookkeeping counter; entries that expire via Redis
        TTL decrement it only when purged by :meth:`expire`, :meth:`cull`,
        or :meth:`check` with ``fix=True``.

        """
        return self._meta_get('count')

    def __getstate__(self):
        if self._redis_url is None:
            raise TypeError(
                'cannot pickle a cache created from a client object;'
                ' construct it with a Redis connection URL instead'
            )
        return (
            self._redis_url,
            self._directory,
            self._prefix,
            type(self._disk),
            self._settings,
        )

    def __setstate__(self, state):
        redis_url, directory, prefix, disk, settings = state
        self.__init__(
            redis_url,
            directory,
            prefix=prefix,
            disk=disk,
            **settings,
        )
