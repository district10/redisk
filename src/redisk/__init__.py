"""
Redisk API Reference
====================

Redis (or Kvrocks) backed cache with local disk offload for large values.

Derived from `python-diskcache <https://github.com/grantjenks/python-diskcache>`_
with the SQLite key/value store replaced by Redis.
"""

from .core import (
    DEFAULT_SETTINGS,
    ENOVAL,
    EVICTION_POLICY,
    MAX_TTL_SECS,
    UNKNOWN,
    Cache,
    Disk,
    EmptyDirWarning,
    JSONDisk,
    UnknownFileWarning,
)

__all__ = [
    'Cache',
    'DEFAULT_SETTINGS',
    'Disk',
    'ENOVAL',
    'EVICTION_POLICY',
    'EmptyDirWarning',
    'JSONDisk',
    'MAX_TTL_SECS',
    'UNKNOWN',
    'UnknownFileWarning',
]

__title__ = 'redisk'
__version__ = '0.1.0'
__build__ = 0x000100
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2026 TANG ZHIXIONG; derived from python-diskcache, Copyright 2016-2023 Grant Jenks'
