import pytest

from cachel import (make_key_func, make_cache, BaseCache,
                    wrap_in, wrap_dict_value_in)
from cachel.base import gen_expire


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


def test_make_key_func():
    f = make_key_func(lambda foo: foo, lambda foo: None)
    assert f(10) == 10

    f = make_key_func('key', lambda foo: None)
    assert f(10) == 'key'

    f = make_key_func('{}', lambda foo: None)
    assert f('foo') == 'foo'
    assert f(10) == '10'
    assert f(foo=10) == '10'

    f = make_key_func('{foo}', lambda foo: None)
    assert f('foo') == 'foo'
    assert f(10) == '10'
    assert f(foo=10) == '10'

    f = make_key_func('{foo}', lambda foo=20: None)
    assert f() == '20'

    f = make_key_func('{foo}-{boo}', lambda foo, boo: None)
    assert f(10, 20) == '10-20'

    f = make_key_func('{id}', lambda foo: None, True)
    assert f([20, 30]) == ['20', '30']


def test_gen_expire():
    assert 9 <= gen_expire(10) <= 11


def test_make_cache():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False)
    called = [0]

    @cache('user:{}')
    def get_user(user_id):
        called[0] += 1
        return u'user-{}'.format(user_id)

    result = get_user(10)
    assert result == 'user-10'
    assert called == [1]
    assert cache._cache.cache['user:10'] == (b'\xa7user-10', 42)

    result = get_user(10)
    assert result == 'user-10'
    assert called == [1]

    assert get_user.get(10) == 'user-10'
    assert called == [1]

    get_user.invalidate(10)
    assert get_user.get(10) is None
    assert called == [1]

    result = get_user(10)
    assert result == 'user-10'
    assert called == [2]


def test_make_cache_for_unknown_serializer():
    c = make_cache(None, fmt='boo')
    with pytest.raises(Exception) as ei:
        c('boo')(lambda boo: None)

    assert 'Unknown serializer' in str(ei.value)


def test_make_cache_objects():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False)
    called = [0]

    @cache.objects('user:{}')
    def get_users(ids):
        called[0] += 1
        return {r: u'user-{}'.format(r) for r in ids}

    result = get_users([1, 2])
    assert result == {1: 'user-1', 2: 'user-2'}
    assert called == [1]
    assert cache._cache.cache == {'user:1': (b'\xa6user-1', 42),
                                  'user:2': (b'\xa6user-2', 42)}

    result = get_users([1, 2])
    assert result == {1: 'user-1', 2: 'user-2'}
    assert called == [1]

    get_users.invalidate([1])
    assert cache._cache.cache == {'user:2': (b'\xa6user-2', 42)}

    result = get_users(set((1, 2)))
    assert result == {1: 'user-1', 2: 'user-2'}
    assert called == [2]


def test_wrap_in():
    @wrap_in(int)
    def boo():
        return '10'

    assert boo() == 10


def test_wrap_dict_value_in():
    @wrap_dict_value_in(int)
    def boo():
        return {'key': '10'}

    assert boo() == {'key': 10}
