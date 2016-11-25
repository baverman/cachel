from functools import partial, wraps
from string import Formatter
from random import randint
from inspect import CO_VARARGS, CO_VARKEYWORDS

import json
import msgpack
try:
    import cPickle as pickle
except ImportError:
    import pickle

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
except ImportError:
    pass

formatter = Formatter()


def gen_expire(expire, spread=10):
    a = expire - expire / spread
    b = expire + expire / spread
    return randint(a, b)


def make_key_func(tpl, func, *head):
    if callable(tpl):
        return tpl

    fields = list(formatter.parse(tpl))
    if len(fields) == 1 and fields[0][1] is None:
        return lambda *args, **kwargs: tpl

    c = func.__code__
    assert (not c.co_flags & (CO_VARKEYWORDS | CO_VARARGS)), \
        'functions with varargs are not supported'

    args = c.co_varnames[:c.co_argcount]
    if head:
        args = head + args[len(head):]
    for f in fields:
        assert not f[1] or f[1] in args, \
            'unknown param: "{}", valid fields are {}'.format(f[1], args)

    fd = func.__defaults__ or ()
    defaults = {arg: value for arg, value in zip(args[-len(fd):], fd)}

    signature = []
    for arg in args:
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
    return eval('lambda {}: {}.format({})'.format(
        signature, repr(''.join(template)), ', '.join(targs)))


def make_cache(cache, default_ttl=600, default_fmt='msgpack'):
    class Cacher(object):
        def __call__(tpl, ttl=default_ttl, fmt=default_fmt):
            def decorator(func):
                keyfunc = make_key_func(tpl, func)
                loads, dumps = SERIALIZERS[fmt]
                expire = gen_expire(ttl)

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

        def objects(key, ttl=default_ttl, fmt=default_fmt):
            pass

    return Cacher()


class NullCache(object):
    def set(self, key, value, ttl): pass
    def get(self, key): pass
    def delete(self, key): pass
