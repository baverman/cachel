import json
import logging
from functools import partial, wraps
from string import Formatter
from random import randint
from inspect import CO_VARARGS, CO_VARKEYWORDS

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover py2
    import pickle

import msgpack

from .compat import iteritems, listitems

__all__ = ['SERIALIZERS', 'make_key_func', 'make_cache', 'NullCache', 'BaseCache',
           'wrap_in', 'wrap_dict_value_in', 'delayed_cache']

log = logging.getLogger('cachel')

SERIALIZERS = {
    'json': (partial(json.dumps, ensure_ascii=False), json.loads),
    'pickle': (partial(pickle.dumps, protocol=2), pickle.loads),
    'msgpack': (partial(msgpack.dumps, use_bin_type=True),
                partial(msgpack.loads, encoding='utf-8')),
}

try: # pragma: no cover
    import ujson
    SERIALIZERS['ujson'] = partial(ujson.dumps, ensure_ascii=False), ujson.loads
    SERIALIZERS['slow_json'] = SERIALIZERS['json']
    SERIALIZERS['json'] = SERIALIZERS['ujson']
except ImportError:  # pragma: no cover
    pass

formatter = Formatter()


def gen_expire(expire, spread=10):
    a = expire - expire // spread
    b = expire + expire // spread
    return randint(a, b)


def get_serializer(fmt):
    try:
        return SERIALIZERS[fmt]
    except KeyError:
        raise Exception('Unknown serializer: {}'.format(fmt))


def get_expire(ttl, fuzzy_ttl):
    if fuzzy_ttl:  # pragma: no cover
        ttl = gen_expire(ttl)
    return ttl


def make_key_func(tpl, func, multi=False):
    if callable(tpl):
        return tpl

    fields = list(formatter.parse(tpl))
    if len(fields) == 1 and fields[0][1] is None:
        return lambda *args, **kwargs: tpl

    c = func.__code__
    assert (not c.co_flags & (CO_VARKEYWORDS | CO_VARARGS)), \
        'functions with varargs are not supported'

    sargs = args = c.co_varnames[:c.co_argcount]
    if multi:
        args = ['id'] + list(args[1:])
    for f in fields:
        assert not f[1] or f[1] in args, \
            'unknown param: "{}", valid fields are {}'.format(f[1], args)

    fd = func.__defaults__ or ()
    defaults = {arg: value for arg, value in zip(args[-len(fd):], fd)}

    signature = []
    for arg in sargs:
        if arg in defaults:
            signature.append('{}={}'.format(arg, repr(defaults[arg])))
        else:
            signature.append(arg)

    targs = []
    template = []
    idx = 0
    for sep, name, fmt, _ in fields:
        if sep:
            template.append(sep)

        if name is not None:
            if name:
                i = args.index(name)
                targs.append(name)
            else:
                i = idx
                idx += 1
                targs.append(args[i])
            template.append('{{{}:{}}}'.format(i, fmt))

    signature = ', '.join(signature)
    ftpl = ''.join(template)
    if multi:
        context = {'tpl': ftpl}
        return eval('lambda {}: [tpl.format({}) for id in {}]'.format(
            signature, ', '.join(targs), sargs[0]), context)
    else:
        return eval('lambda {}: {}.format({})'.format(
            signature, repr(ftpl), ', '.join(targs)))


class CacheWrapper(object):
    def __init__(self, func, cache, keyfunc, serializer, ttl):
        self.func = func
        self.__name__ = self.func.__name__
        self.__doc__ = self.func.__doc__
        self.__module__ = self.func.__module__
        self._cachel_original_func = getattr(func, '_cachel_original_func', func)
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


class MultiCacheWrapper(CacheWrapper):
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


class make_cache(object):
    def __init__(self, cache, ttl=600, fmt='msgpack', fuzzy_ttl=True):
        self.cache = cache
        self.ttl = ttl
        self.fmt = fmt
        self.fuzzy_ttl = fuzzy_ttl

    def _wrapper(self, cls, tpl, ttl, fmt, fuzzy_ttl, multi=False):
        def decorator(func):
            original_func = getattr(func, '_cachel_original_func', func)
            return cls(
                func, self.cache,
                make_key_func(tpl, original_func, multi),
                get_serializer(fmt or self.fmt),
                get_expire(ttl or self.ttl, fuzzy_ttl or self.fuzzy_ttl))
        return decorator

    def __call__(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(CacheWrapper, tpl, ttl, fmt, fuzzy_ttl)

    def objects(self, tpl, ttl=None, fmt=None, fuzzy_ttl=None):
        return self._wrapper(MultiCacheWrapper, tpl, ttl, fmt, fuzzy_ttl, multi=True)


class BaseCache(object):
    def set(self, key, value, ttl):  # pragma: no cover
        raise NotImplementedError()

    def get(self, key):  # pragma: no cover
        raise NotImplementedError()

    def delete(self, key):  # pragma: no cover
        raise NotImplementedError()

    def mget(self, keys):
        return [self.get(k) for k in keys]

    def mset(self, items, expire):
        for k, v in items:
            self.set(k, v, expire)

    def mdelete(self, keys):
        for k in keys:
            self.delete(k)


class NullCache(BaseCache):
    def set(self, key, value, ttl): pass

    def get(self, key): pass

    def delete(self, key): pass


def wrap_in(wrapper):
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            return wrapper(func(*args, **kwargs))
        return inner
    return decorator


def wrap_dict_value_in(wrapper):
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            result = func(*args, **kwargs)
            return {k: wrapper(v) for k, v in iteritems(result)}
        return inner
    return decorator


def delayed_cache(default=None):
    def decorator(func):
        l1 = func
        l2 = l1.func
        func = l2.func

        @wraps(func)
        def inner(*args, **kwargs):
            result = l1.get(*args, **kwargs)
            if result is None:
                try:
                    result = func(*args, **kwargs)
                except Exception:
                    log.exception('Cache error')
                    result = l2.get(*args, **kwargs)
                else:
                    l2.set(result, *args, **kwargs)

            if result is None:
                result = default
            else:
                l1.set(result, *args, **kwargs)

            return result
        return inner
    return decorator
