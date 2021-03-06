from cachel.compat import iteritems
from cachel.base import _Expire
from cachel.wrappers import BaseCacheWrapper, agg_expire

__await__cache = __await__fn = __await__call = __async__call = __async__cache = None


class CacheWrapper(BaseCacheWrapper):
    def __call__(__async__call, self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = __await__cache(self.cache.get(k))
        if result is None:
            result = __await__fn(self.func(*args, **kwargs))
            if type(result) is _Expire:
                result, ttl = result
            else:
                ttl = self.ttl
            __await__cache(self.cache.set(k, self.dumps(result), ttl))
            return result
        else:
            return self.loads(result)

    def get(__async__cache, self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = __await__cache(self.cache.get(k))
        if result:
            return self.loads(result)

    def set(__async__cache, self, value, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        __await__cache(self.cache.set(k, self.dumps(value), self.ttl))

    def invalidate(__async__cache, self, *args, **kwargs):
        key = self.keyfunc(*args, **kwargs)
        __await__cache(self.cache.delete(key))


class ObjectsCacheWrapper(CacheWrapper):
    def __call__(__async__call, self, ids, *args, **kwargs):
        if not isinstance(ids, (list, tuple)):
            ids = list(ids)

        dumps = self.dumps
        loads = self.loads

        keys = self.keyfunc(ids, *args, **kwargs)
        cresult = {}
        if keys:
            for oid, value in zip(ids, __await__cache(self.cache.mget(keys))):
                if value is not None:
                    cresult[oid] = loads(value)

        ids_to_fetch = set(ids) - set(cresult)
        if ids_to_fetch:
            fresult = __await__fn(self.func(ids_to_fetch, *args, **kwargs))
            if fresult:
                agg_result = iteritems(agg_expire(fresult, self.ttl))
                for ttl, result in agg_result:
                    to_cache_ids, to_cache_values = zip(*iteritems(result))
                    keys = self.keyfunc(to_cache_ids, *args, **kwargs)
                    values = [dumps(it) for it in to_cache_values]
                    __await__cache(self.cache.mset(zip(keys, values), ttl))
                    cresult.update(result)

        return cresult

    def invalidate(__async__cache, self, ids, *args, **kwargs):
        keys = self.keyfunc(ids, *args, **kwargs)
        __await__cache(self.cache.mdelete(keys))

    def one(__async__call, self, id, *args, **kwargs):
        default = kwargs.pop('_default', None)
        return __await__call(self([id], *args, **kwargs)).get(id, default)
