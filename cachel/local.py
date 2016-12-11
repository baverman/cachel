import logging
from functools import wraps
from time import time

from .base import make_key_func, get_serializer, get_expire
from .compat import PY2

log = logging.getLogger('cachel')


class LocalCacheWrapper(object):
    def __init__(self, func, keyfunc, serializer, cache1,
                 cache2, ttl1, ttl2, refresh=None):
        self.func = func
        self.cache1 = cache1
        self.cache2 = cache2
        self.keyfunc = keyfunc
        self.dumps, self.loads = serializer
        self.ttl1 = ttl1
        self.ttl2 = ttl2
        self.refresh = refresh

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
                    if self.refresh:
                        cache1_updated = self.refresh(self, k, *args, **kwargs)
                    else:
                        cache1_updated = self.default_refresh(k, *args, **kwargs)
                if not cache1_updated:
                    self.cache1.set(k, result, self.ttl1)
                return self.loads(result)
            return result
        else:
            return self.loads(result)

    def default_refresh(self, key, *args, **kwargs):
        try:
            result = self.func(*args, **kwargs)
        except Exception:
            log.exception('Error refreshing local cache key')
            return False

        sdata = self.dumps(result)
        self.cache1.set(key, sdata, self.ttl1)
        self.cache2.set(key, self.dumps2(sdata), self.ttl2)
        return True

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


class make_local_cache(object):
    def __init__(self, cache1, cache2, ttl1=600, ttl2=None, fmt='msgpack', fuzzy_ttl=True, refresh=None):
        self.cache1 = cache1
        self.cache2 = cache2
        self.ttl1 = ttl1
        self.ttl2 = ttl2
        self.fmt = fmt
        self.fuzzy_ttl = fuzzy_ttl
        self.refresh = refresh

    def _wrapper(self, cls, tpl, ttl1, ttl2, fmt, fuzzy_ttl, multi=False):
        def decorator(func):
            ttl = ttl1 or self.ttl1
            return wraps(func)(cls(
                func,
                make_key_func(tpl, func, multi),
                get_serializer(fmt or self.fmt),
                self.cache1,
                self.cache2,
                get_expire(ttl, fuzzy_ttl or self.fuzzy_ttl),
                ttl2 or self.ttl2 or ttl * 2,
                self.refresh))
        return decorator

    def __call__(self, tpl, ttl1=None, ttl2=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(LocalCacheWrapper, tpl, ttl1, ttl2, fmt, fuzzy_ttl)

    # def objects(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
    #     return self._wrapper(ObjectsCacheWrapper, tpl, ttl, fmt, fuzzy_ttl, multi=True)
