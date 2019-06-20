from cachel.compat import iteritems
from cachel.base import _Expire
from cachel.wrappers import BaseCacheWrapper, agg_expire


class CacheWrapper(BaseCacheWrapper):
    async def __call__(self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = await self.cache.get(k)
        if result is None:
            result = self.func(*args, **kwargs)
            if type(result) is _Expire:
                result, ttl = result
            else:
                ttl = self.ttl
            await self.cache.set(k, self.dumps(result), ttl)
            return result
        else:
            return self.loads(result)

    async def get(self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = await self.cache.get(k)
        if result:
            return self.loads(result)

    async def set(self, value, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        await self.cache.set(k, self.dumps(value), self.ttl)

    async def invalidate(self, *args, **kwargs):
        key = self.keyfunc(*args, **kwargs)
        await self.cache.delete(key)


class ObjectsCacheWrapper(CacheWrapper):
    async def __call__(self, ids, *args, **kwargs):
        if not isinstance(ids, (list, tuple)):
            ids = list(ids)

        dumps = self.dumps
        loads = self.loads

        keys = self.keyfunc(ids, *args, **kwargs)
        cresult = {}
        if keys:
            for oid, value in zip(ids, await self.cache.mget(keys)):
                if value is not None:
                    cresult[oid] = loads(value)

        ids_to_fetch = set(ids) - set(cresult)
        if ids_to_fetch:
            fresult = self.func(ids_to_fetch, *args, **kwargs)
            if fresult:
                agg_result = iteritems(agg_expire(fresult, self.ttl))
                for ttl, result in agg_result:
                    to_cache_ids, to_cache_values = zip(*iteritems(result))
                    keys = self.keyfunc(to_cache_ids, *args, **kwargs)
                    values = [dumps(it) for it in to_cache_values]
                    await self.cache.mset(zip(keys, values), ttl)
                    cresult.update(result)

        return cresult

    async def invalidate(self, ids, *args, **kwargs):
        keys = self.keyfunc(ids, *args, **kwargs)
        await self.cache.mdelete(keys)

    async def one(self, id, *args, **kwargs):
        default = kwargs.pop('_default', None)
        return (await self([id], *args, **kwargs)).get(id, default)
