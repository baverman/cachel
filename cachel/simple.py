from functools import wraps

from .base import make_key_func, get_serializer, get_expire, _Expire
from .compat import listitems, iteritems


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
            if type(result) is _Expire:
                result, ttl = result
            else:
                ttl = self.ttl
            self.cache.set(k, self.dumps(result), ttl)
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


def agg_expire(result, default_ttl):
    dttl = {k: v for k, v in iteritems(result) if type(v) is not _Expire}
    if len(dttl) == len(result):  # fast path, there are no values with custom ttl
        return {default_ttl: result}

    ttls = {}
    for k, v in iteritems(result):
        if type(v) is _Expire:
            ttls.setdefault(v[1], {})[k] = v[0]

    if dttl:
        ttls[default_ttl] = dttl

    return ttls


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
                agg_result = iteritems(agg_expire(fresult, self.ttl))
                for ttl, result in agg_result:
                    to_cache_ids, to_cache_values = zip(*iteritems(result))
                    keys = self.keyfunc(to_cache_ids, *args, **kwargs)
                    values = [dumps(it) for it in to_cache_values]
                    self.cache.mset(zip(keys, values), ttl)
                    cresult.update(result)

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
            fttl = self.fuzzy_ttl if fuzzy_ttl is None else fuzzy_ttl
            return wraps(func)(cls(
                func, self.cache,
                make_key_func(tpl, func, multi),
                get_serializer(fmt or self.fmt),
                get_expire(ttl or self.ttl, fttl)))
        return decorator

    def __call__(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(CacheWrapper, tpl, ttl, fmt, fuzzy_ttl)

    def objects(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(ObjectsCacheWrapper, tpl, ttl, fmt, fuzzy_ttl, multi=True)
