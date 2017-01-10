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

from .compat import iteritems

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
    aidx = 0
    for sep, name, fmt, _ in fields:
        if sep:
            template.append(sep)

        if name is not None:
            if name:
                targs.append(name)
            else:
                targs.append(args[aidx])
                aidx += 1
            template.append('{{{}:{}}}'.format(idx, fmt))
            idx += 1

    signature = ', '.join(signature)
    ftpl = ''.join(template)
    if multi:
        context = {'tpl': ftpl}
        return eval('lambda {}: [tpl.format({}) for id in {}]'.format(
            signature, ', '.join(targs), sargs[0]), context)
    else:
        return eval('lambda {}: {}.format({})'.format(
            signature, repr(ftpl), ', '.join(targs)))


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
