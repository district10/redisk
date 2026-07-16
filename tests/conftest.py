"""Test fixtures for redisk.

By default tests run against an in-process fake Redis (fakeredis), so no
server is needed. To run against a real Redis or Kvrocks server, set the
environment variable REDISK_TEST_REDIS_URL, for example:

    REDISK_TEST_REDIS_URL=redis://localhost:6666/0 pytest
"""

import os
import uuid

import pytest

import redisk


def make_redis_client():
    url = os.environ.get('REDISK_TEST_REDIS_URL')

    if url:
        import redis

        return redis.Redis.from_url(url)

    fakeredis = pytest.importorskip('fakeredis')
    return fakeredis.FakeStrictRedis()


@pytest.fixture
def redis_client():
    client = make_redis_client()
    yield client
    client.close()


@pytest.fixture
def cache(redis_client, tmp_path):
    prefix = 'redisk-test-%s' % uuid.uuid4().hex[:12]
    cache = redisk.Cache(
        redis_client,
        str(tmp_path / 'offload'),
        prefix=prefix,
    )

    with cache:
        yield cache

    # Clean up Redis keys when running against a real server.
    keys = list(
        redis_client.scan_iter(match=prefix.encode('utf-8') + b':*', count=1000)
    )
    if keys:
        redis_client.delete(*keys)
