import logging
from functools import wraps
from time import time

from .base import make_key_func, get_serializer, get_expire
from .compat import PY2, listitems

log = logging.getLogger('cachel')


class OffloadCacheWrapper(object):
    def __init__(self, func, keyfunc, serializer, cache1,
                 cache2, ttl1, ttl2, offload):
        self.id = '{}.{}'.format(func.__module__, func.__name__)
        self.func = func
        self.cache1 = cache1
        self.cache2 = cache2
        self.keyfunc = keyfunc
        self.dumps, self.loads = serializer
        self.ttl1 = ttl1
        self.ttl2 = ttl2
        self.offload = offload

    def __call__(self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = self.cache1.get(k)
        if result is None:
            result = self.cache2.get(k)
            if result is None:
                result = self.func(*args, **kwargs)
                sdata = self.dumps(result)
                self.cache1.set(k, sdata, self.ttl1)
                self.cache2.set(k, self.dumps2(sdata), self.ttl2)
            else:
                expire, result = self.loads2(result)
                self.cache1.set(k, result, self.ttl1)
                if time() > expire:
                    self.offload(self, k, args, kwargs)
                return self.loads(result)
            return result
        else:
            return self.loads(result)

    def loads2(self, data):
        expire, _, data = data.partition(b':')
        return int(expire), data

    def dumps2(self, data, now=None):  # pragma: nocover
        expire = int((now or time()) + self.ttl1)
        if PY2:
            return '{}:{}'.format(expire, data)
        else:
            return str(expire).encode() + b':' + data

    def get(self, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        result = self.cache2.get(k)
        if result:
            return self.loads(self.loads2(result)[1])

    def set(self, value, *args, **kwargs):
        k = self.keyfunc(*args, **kwargs)
        self.cache2.set(k, self.dumps2(self.dumps(value)), self.ttl2)

    def invalidate(self, *args, **kwargs):
        key = self.keyfunc(*args, **kwargs)
        self.cache2.delete(key)


class OffloadObjectsCacheWrapper(OffloadCacheWrapper):
    def __call__(self, ids, *args, **kwargs):
        if not isinstance(ids, (list, tuple)):
            ids = list(ids)

        loads = self.loads
        loads2 = self.loads2
        now = time()

        keys = self.keyfunc(ids, *args, **kwargs)
        cresult = {}
        if keys:
            for oid, value in zip(ids, self.cache1.mget(keys)):
                if value is not None:
                    cresult[oid] = loads(value)

        c2_ids_to_fetch = list(set(ids) - set(cresult))
        c2_keys = self.keyfunc(c2_ids_to_fetch, *args, **kwargs)
        c2_result = {}
        offload_ids = []
        update_data = []
        if c2_ids_to_fetch:
            for key, oid, value in zip(c2_keys, c2_ids_to_fetch, self.cache2.mget(c2_keys)):
                if value is not None:
                    expire, data = loads2(value)
                    update_data.append((key, data))
                    if now > expire:
                        offload_ids.append(oid)
                    c2_result[oid] = loads(data)
            cresult.update(c2_result)

        if update_data:
            self.cache1.mset(update_data, self.ttl1)

        if offload_ids:
            self.offload(self, offload_ids, args, kwargs, multi=True)

        ids_to_fetch = set(c2_ids_to_fetch) - set(c2_result)
        if ids_to_fetch:
            fresult = self._get_func_result(ids_to_fetch, args, kwargs, now)
            cresult.update(fresult)

        return cresult

    def _get_func_result(self, ids, args, kwargs, now=None):
        now = now or time()
        dumps = self.dumps
        dumps2 = self.dumps2
        fresult = self.func(ids, *args, **kwargs)
        if fresult:
            to_cache_pairs = listitems(fresult)
            to_cache_ids, to_cache_values = zip(*to_cache_pairs)
            keys = self.keyfunc(to_cache_ids, *args, **kwargs)
            values = [dumps(r) for r in to_cache_values]
            evalues = [dumps2(r, now) for r in values]
            self.cache1.mset(zip(keys, values), self.ttl1)
            self.cache2.mset(zip(keys, evalues), self.ttl2)
        return fresult

    def one(self, id, *args, **kwargs):
        default = kwargs.pop('_default', None)
        return self([id], *args, **kwargs).get(id, default)


def default_offload(cache, key, args, kwargs, multi=False):
    try:
        if multi:
            cache._get_func_result(key, args, kwargs)
        else:
            result = cache.func(*args, **kwargs)
            sdata = cache.dumps(result)
            cache.cache1.set(key, sdata, cache.ttl1)
            cache.cache2.set(key, cache.dumps2(sdata), cache.ttl2)
    except Exception:
        log.exception('Error refreshing offload cache key')


def offloader(func):
    @wraps(func)
    def inner(cache, key, args, kwargs, multi=False):
        params = {'cache_id': cache.id, 'key': key, 'args': args,
                  'kwargs': kwargs, 'multi': multi}
        func(params)
    return inner


class make_offload_cache(object):
    def __init__(self, cache1, cache2, ttl1=600, ttl2=None, fmt='msgpack',
                 fuzzy_ttl=True, offload=None):
        self.caches = {}
        self.cache1 = cache1
        self.cache2 = cache2
        self.ttl1 = ttl1
        self.ttl2 = ttl2
        self.fmt = fmt
        self.fuzzy_ttl = fuzzy_ttl
        self.offload = offload

    def _wrapper(self, cls, tpl, ttl1, ttl2, fmt, fuzzy_ttl, multi=False):
        def decorator(func):
            ttl = ttl1 or self.ttl1
            cache = cls(
                func,
                make_key_func(tpl, func, multi),
                get_serializer(fmt or self.fmt),
                self.cache1,
                self.cache2,
                get_expire(ttl, fuzzy_ttl or self.fuzzy_ttl),
                ttl2 or self.ttl2 or ttl * 2,
                self.offload or default_offload
            )
            self.caches[cache.id] = cache
            return wraps(func)(cache)
        return decorator

    def __call__(self, tpl, ttl1=None, ttl2=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(OffloadCacheWrapper, tpl, ttl1, ttl2, fmt, fuzzy_ttl)

    def objects(self, tpl, ttl1=None, ttl2=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(OffloadObjectsCacheWrapper, tpl, ttl1, ttl2, fmt, fuzzy_ttl, multi=True)

    def offload_helper(self, params):
        cache_id = params.pop('cache_id')
        cache = self.caches.get(cache_id)
        if not cache:  # pragma: no cover
            log.error('Cache not found: %s', cache_id)
            return

        default_offload(cache, **params)
