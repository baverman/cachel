from cachel.base import BaseCache, SERIALIZERS


class Cache(BaseCache):
    def __init__(self):
        self.cache = {}

    def set(self, key, value, expire):
        self.cache[key] = value, expire

    def get(self, key):
        try:
            return self.cache[key][0]
        except KeyError:
            pass

    def delete(self, key):
        self.cache.pop(key, None)


def dumps(x):
    if isinstance(x, type(u'')):
        x = x.encode('utf-8')
    return x


def loads(x):
    return x.decode('utf-8')


SERIALIZERS['test'] = dumps, loads
