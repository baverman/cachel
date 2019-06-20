import pytest
from cachel import expire
from cachel.simple import make_cache
from .helpers import Cache


def test_make_cache():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False, fmt='test')
    called = [0]

    @cache('user:{}')
    def get_user(user_id):
        called[0] += 1
        return u'user-{}'.format(user_id)

    result = get_user(10)
    assert result == 'user-10'
    assert called == [1]
    assert get_user.cache.cache['user:10'] == (b'user-10', 42)

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

    get_user.set('user-20', 10)
    result = get_user(10)
    assert result == 'user-20'
    assert called == [2]


def test_make_cache_custom_expire():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False, fmt='test')

    @cache('user:{}')
    def get_user(user_id):
        return expire(u'user-{}'.format(user_id), 100)

    result = get_user(10)
    assert result == 'user-10'
    assert get_user.cache.cache['user:10'] == (b'user-10', 100)


def test_make_cache_for_unknown_serializer():
    c = make_cache(None, fmt='boo')
    with pytest.raises(Exception) as ei:
        c('boo')(lambda boo: None)

    assert 'Unknown serializer' in str(ei.value)


def test_make_cache_objects():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False, fmt='test')
    called = [0]

    @cache.objects('user:{}')
    def get_users(ids):
        called[0] += 1
        return {r: u'user-{}'.format(r) for r in ids if r != 3}

    result = get_users([1, 2])
    assert result == {1: 'user-1', 2: 'user-2'}
    assert called == [1]
    assert get_users.cache.cache == {'user:1': (b'user-1', 42),
                                  'user:2': (b'user-2', 42)}

    result = get_users([1, 2])
    assert result == {1: 'user-1', 2: 'user-2'}
    assert called == [1]

    get_users.invalidate([1])
    assert get_users.cache.cache == {'user:2': (b'user-2', 42)}

    result = get_users(set((1, 2)))
    assert result == {1: 'user-1', 2: 'user-2'}
    assert called == [2]

    assert get_users.one(1) == 'user-1'
    assert get_users.one(3) is None
    assert get_users.one(3, _default='None') == 'None'


def test_make_cache_objects_custom_expire():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False, fmt='test')

    @cache.objects('user:{}')
    def get_users(ids):
        return {1: 'boo', 2: expire('foo', 100)}

    result = get_users([1, 2])
    assert result == {1: 'boo', 2: 'foo'}
    assert get_users.cache.cache == {'user:1': (b'boo', 42),
                                     'user:2': (b'foo', 100)}
