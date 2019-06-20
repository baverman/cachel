from functools import wraps

from . import compat
from .base import make_key_func, get_serializer, get_expire
from .wrappers import load_wrappers


if compat.ASYNC:  # pragma: no cover
    import asyncio
    def is_async_func(fn):
        return asyncio.iscoroutinefunction(fn)
else:  # pragma: no cover
    def is_async_func(fn):
        return False


class make_cache(object):
    def __init__(self, cache, ttl=600, fmt='msgpack', fuzzy_ttl=True):
        self.cache = cache
        self.ttl = ttl
        self.fmt = fmt
        self.fuzzy_ttl = fuzzy_ttl

    def _wrapper(self, tpl, ttl, fmt, fuzzy_ttl, multi=False):
        def decorator(func):
            m = load_wrappers(
                is_async_func(func), getattr(self.cache, 'is_async', False))
            cls = m.ObjectsCacheWrapper if multi else m.CacheWrapper
            fttl = self.fuzzy_ttl if fuzzy_ttl is None else fuzzy_ttl
            return wraps(func)(cls(
                func, self.cache,
                make_key_func(tpl, func, multi),
                get_serializer(fmt or self.fmt),
                get_expire(ttl or self.ttl, fttl)))
        return decorator

    def __call__(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(tpl, ttl, fmt, fuzzy_ttl)

    def objects(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(tpl, ttl, fmt, fuzzy_ttl, multi=True)
