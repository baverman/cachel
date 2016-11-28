from __future__ import absolute_import

from redis import StrictRedis


class RedisCache(object):
    def __init__(self, url=None, client=None):
        if url:  # pragma: no cover
            self.client = StrictRedis.from_url(url)
        elif client:  # pragma: no cover
            self.client = client
        else:
            self.client = StrictRedis()

    def get(self, key):
        return self.client.get(key)

    def mget(self, keys):
        return self.client.mget(keys)

    def delete(self, key):
        self.client.delete(key)

    def mdelete(self, keys):
        self.client.delete(*keys)

    def set(self, key, value, ttl):
        self.client.set(key, value, ttl)

    def mset(self, items, ttl):
        p = self.client.pipeline(transaction=False)
        for key, value in items:
            p.set(key, value, ttl)
        p.execute()
