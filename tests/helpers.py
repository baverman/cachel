from cachel.base import BaseCache, SERIALIZERS
from cachel import compat


class Cache(BaseCache):
    def __init__(self):
        self.cache = {}

    def set(self, key, value, expire):
        self.cache[key] = value, expire

    def get(self, key):
        try:
            return self.cache[key][0]
        except KeyError:
            pass

    def delete(self, key):
        self.cache.pop(key, None)


if compat.ASYNC_AWAIT:
    from ._async_helpers import AsyncCache
