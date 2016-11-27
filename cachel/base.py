import json
from functools import partial, wraps
from string import Formatter
from random import randint
from inspect import CO_VARARGS, CO_VARKEYWORDS

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover py2
    import pickle

import msgpack

__all__ = ['SERIALIZERS', 'make_key_func', 'make_cache', 'NullCache', 'BaseCache',
           'wrap_in', 'wrap_dict_value_in']

SERIALIZERS = {
    'json': (partial(json.dumps, ensure_ascii=False), json.loads),
    'pickle': (partial(pickle.dumps, protocol=2), pickle.loads),
    'msgpack': (partial(msgpack.dumps, use_bin_type=True),
                partial(msgpack.loads, encoding='utf-8')),
}

try:
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


def make_cache(cache, ttl=600, fmt='msgpack', fuzzy_ttl=True):
    class Cacher(object):
        _cache = cache

        def __call__(self, tpl, ttl=ttl, fmt=fmt, fuzzy_ttl=fuzzy_ttl):
            def decorator(func):
                keyfunc = make_key_func(tpl, func)
                dumps, loads = get_serializer(fmt)
                expire = get_expire(ttl, fuzzy_ttl)

                class Cache(object):
                    def __call__(self, *args, **kwargs):
                        k = keyfunc(*args, **kwargs)
                        result = cache.get(k)
                        if result is None:
                            result = func(*args, **kwargs)
                            cache.set(k, dumps(result), expire)
                            return result
                        else:
                            return loads(result)

                    def get(self, *args, **kwargs):
                        k = keyfunc(*args, **kwargs)
                        result = cache.get(k)
                        if result:
                            return loads(result)

                    def invalidate(self, *args, **kwargs):
                        key = keyfunc(*args, **kwargs)
                        cache.delete(key)

                return wraps(func)(Cache())
            return decorator

        def objects(self, tpl, ttl=ttl, fmt=fmt, fuzzy_ttl=fuzzy_ttl):
            def decorator(func):
                keyfunc = make_key_func(tpl, func, True)
                dumps, loads = get_serializer(fmt)
                expire = get_expire(ttl, fuzzy_ttl)

                class ObjectsCache(object):
                    def __call__(self, ids, *args, **kwargs):
                        if not isinstance(ids, (list, tuple)):
                            ids = list(ids)
                        keys = keyfunc(ids, *args, **kwargs)
                        cresult = {}
                        if keys:
                            for oid, value in zip(ids, cache.mget(keys)):
                                if value is not None:
                                    cresult[oid] = loads(value)

                        ids_to_fetch = set(ids) - set(cresult)
                        if ids_to_fetch:
                            fresult = func(ids_to_fetch, *args, **kwargs)
                            if fresult:
                                to_cache_pairs = list(fresult.items())
                                to_cache_ids, to_cache_values = zip(*to_cache_pairs)
                                cache.mset(zip(keyfunc(to_cache_ids, *args, **kwargs),
                                               [dumps(r) for r in to_cache_values]), expire)
                            cresult.update(fresult)

                        return cresult

                    def invalidate(self, ids, *args, **kwargs):
                        keys = keyfunc(ids, *args, **kwargs)
                        cache.mdelete(keys)

                return wraps(func)(ObjectsCache())
            return decorator

    return Cacher()


class BaseCache(object):
    def mget(self, keys):
        return [self.get(k) for k in keys]

    def mset(self, items, expire):
        for k, v in items:
            self.set(k, v, expire)

    def mdelete(self, keys):
        for k in keys:
            self.delete(k)


class NullCache(object):
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
            return {k: wrapper(v) for k, v in result.iteritems()}
        return inner
    return decorator
