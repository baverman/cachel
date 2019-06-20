from cachel.base import AsyncBaseCache


class AsyncCache(AsyncBaseCache):
    def __init__(self):
        self.cache = {}

    async def set(self, key, value, expire):
        self.cache[key] = value, expire

    async def get(self, key):
        try:
            return self.cache[key][0]
        except KeyError:
            pass

    async def delete(self, key):
        self.cache.pop(key, None)
