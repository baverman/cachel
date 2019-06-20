import sys
from cachel.compat import iteritems
from cachel.base import _Expire

WRAPPER_NAMES = {
    (False, False): 'cachel.wrappers.full_sync',
    (True, False): 'cachel.wrappers.async_fn',
    (False, True): 'cachel.wrappers.async_cache',
    (True, True): 'cachel.wrappers.full_async',
}


class BaseCacheWrapper(object):
    def __init__(self, func, cache, keyfunc, serializer, ttl):
        self.func = func
        self.cache = cache
        self.keyfunc = keyfunc
        self.dumps, self.loads = serializer
        self.ttl = ttl


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


def load_wrappers(async_fn, async_cache):
    mname = WRAPPER_NAMES[(async_fn, async_cache)]
    __import__(mname)
    return sys.modules[mname]
