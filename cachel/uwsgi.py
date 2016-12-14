from __future__ import absolute_import, print_function
from uwsgi import cache_update, cache_get, cache_del, mule_msg, mule_get_msg

from . import offload
from .base import BaseCache, SERIALIZERS

dumps, loads = SERIALIZERS['pickle']


class UWSGICache(BaseCache):
    def __init__(self, name):
        self.name = name

    def get(self, key):
        return cache_get(key, self.name)

    def mget(self, keys):
        name = self.name
        return [cache_get(key, name) for key in keys]

    def set(self, key, value, ttl):
        cache_update(key, value, ttl, self.name)

    def delete(self, key):
        cache_del(key, self.name)


def offloader(mule=None):
    @offload.offloader
    def do_offload(params):
        mule_msg(dumps(params), mule)
    return do_offload


def offload_worker(offload_cache):
    def worker():
        print('Offload worker started')
        while True:
            payload = mule_get_msg()
            params = loads(payload)
            offload_cache.offload_helper(params)

    return worker
