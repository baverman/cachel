from functools import wraps

from .base import make_key_func, get_serializer, get_expire
from .compat import listitems


class CacheWrapper(object):
    def __init__(self, func, cache, keyfunc, serializer, ttl):
        self.func = func
        self.cache = cache
        self.keyfunc = keyfunc
        self.dumps, self.loads = serializer
        self.ttl = ttl

    def __call__(self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = self.cache.get(k)
        if result is None:
            result = self.func(*args, **kwargs)
            self.cache.set(k, self.dumps(result), self.ttl)
            return result
        else:
            return self.loads(result)

    def get(self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = self.cache.get(k)
        if result:
            return self.loads(result)

    def set(self, value, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        self.cache.set(k, self.dumps(value), self.ttl)

    def invalidate(self, *args, **kwargs):
        key = self.keyfunc(*args, **kwargs)
        self.cache.delete(key)


class ObjectsCacheWrapper(CacheWrapper):
    def __call__(self, ids, *args, **kwargs):
        if not isinstance(ids, (list, tuple)):
            ids = list(ids)

        dumps = self.dumps
        loads = self.loads

        keys = self.keyfunc(ids, *args, **kwargs)
        cresult = {}
        if keys:
            for oid, value in zip(ids, self.cache.mget(keys)):
                if value is not None:
                    cresult[oid] = loads(value)

        ids_to_fetch = set(ids) - set(cresult)
        if ids_to_fetch:
            fresult = self.func(ids_to_fetch, *args, **kwargs)
            if fresult:
                to_cache_pairs = listitems(fresult)
                to_cache_ids, to_cache_values = zip(*to_cache_pairs)
                self.cache.mset(zip(self.keyfunc(to_cache_ids, *args, **kwargs),
                                    [dumps(r) for r in to_cache_values]), self.ttl)
            cresult.update(fresult)

        return cresult

    def invalidate(self, ids, *args, **kwargs):
        keys = self.keyfunc(ids, *args, **kwargs)
        self.cache.mdelete(keys)

    def one(self, id, *args, **kwargs):
        default = kwargs.pop('_default', None)
        return self([id], *args, **kwargs).get(id, default)


class make_cache(object):
    def __init__(self, cache, ttl=600, fmt='msgpack', fuzzy_ttl=True):
        self.cache = cache
        self.ttl = ttl
        self.fmt = fmt
        self.fuzzy_ttl = fuzzy_ttl

    def _wrapper(self, cls, tpl, ttl, fmt, fuzzy_ttl, multi=False):
        def decorator(func):
            return wraps(func)(cls(
                func, self.cache,
                make_key_func(tpl, func, multi),
                get_serializer(fmt or self.fmt),
                get_expire(ttl or self.ttl, fuzzy_ttl or self.fuzzy_ttl)))
        return decorator

    def __call__(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(CacheWrapper, tpl, ttl, fmt, fuzzy_ttl)

    def objects(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(ObjectsCacheWrapper, tpl, ttl, fmt, fuzzy_ttl, multi=True)
