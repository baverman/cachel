from __future__ import absolute_import
from uwsgi import cache_set, cache_get, cache_del
from . import BaseCache


class UWSGICache(BaseCache):
    def __init__(self, name):
        self.name = name

    def get(self, key):
        return cache_get(key, self.name)

    def mget(self, keys):
        name = self.name
        return [cache_get(key, name) for key in keys]

    def set(self, key, value, ttl):
        cache_set(key, value, ttl, self.name)

    def delete(self, key):
        cache_del(key, self.name)
