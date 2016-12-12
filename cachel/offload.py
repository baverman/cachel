import logging
from functools import wraps
from time import time

from .base import make_key_func, get_serializer, get_expire
from .compat import PY2

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
                cache1_updated = False
                if time() > expire:
                    cache1_updated = self.offload(self, k, args, kwargs)
                if not cache1_updated:
                    self.cache1.set(k, result, self.ttl1)
                return self.loads(result)
            return result
        else:
            return self.loads(result)

    def loads2(self, data):
        expire, _, data = data.partition(b':')
        return int(expire), data

    def dumps2(self, data):  # pragma: nocover
        expire = int(time() + self.ttl1)
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


def default_offload(cache, key, args, kwargs, update_cache1=False):
    try:
        result = cache.func(*args, **kwargs)
    except Exception:
        log.exception('Error reshing local cache key')
        if update_cache1:
            result = cache.cache2.get(key)
            if result:
                _, result = cache.loads2(result)
                cache.cache1.set(key, result, cache.ttl1)
        return update_cache1
    else:
        sdata = cache.dumps(result)
        cache.cache1.set(key, sdata, cache.ttl1)
        cache.cache2.set(key, cache.dumps2(sdata), cache.ttl2)
    return True


def offloader(func):
    @wraps(func)
    def inner(cache, key, args, kwargs):
        params = {'cache_id': cache.id,
                  'key': key,
                  'args': args,
                  'kwargs': kwargs}
        func(params)
        return False
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

    def offload_params(self, params):
        cache_id = params.pop('cache_id')
        cache = self.caches.get(cache_id)
        if not cache:  # pragma: no cover
            log.error('Cache not found: %s', cache_id)
            return

        default_offload(cache, update_cache1=True, **params)

    # def objects(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
    #     return self._wrapper(ObjectsCacheWrapper, tpl, ttl, fmt, fuzzy_ttl, multi=True)
