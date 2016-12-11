import time
from cachel import local
from .helpers import Cache


def test_local_cache(monkeypatch):
    c1 = Cache()
    c2 = Cache()
    cache = local.make_local_cache(c1, c2, fmt='test')
    called = [0]

    @cache('user:{}', 5, 10, fuzzy_ttl=False)
    def foo(user_id):
        called[0] += 1
        return 'user-{}'.format(user_id)

    # first get
    monkeypatch.setattr(local, 'time', lambda: 20)
    result = foo(1)
    assert result == 'user-1'
    assert called == [1]
    assert c1.cache == {'user:1': (b'user-1', 5)}
    assert c2.cache == {'user:1': (b'25:user-1', 10)}

    # get with unexpired caches
    result = foo(1)
    assert result == 'user-1'
    assert called == [1]
    assert c1.cache == {'user:1': (b'user-1', 5)}
    assert c2.cache == {'user:1': (b'25:user-1', 10)}

    # get with expired c1 and unexpired value from c2
    c1.delete('user:1')
    result = foo(1)
    assert result == 'user-1'
    assert called == [1]
    assert c1.cache == {'user:1': (b'user-1', 5)}
    assert c2.cache == {'user:1': (b'25:user-1', 10)}

    # get with expired c1 and expired value from c2
    monkeypatch.setattr(local, 'time', lambda: 26)
    c1.delete('user:1')
    result = foo(1)
    assert result == 'user-1'
    assert called == [2]
    assert c1.cache == {'user:1': (b'user-1', 5)}
    assert c2.cache == {'user:1': (b'31:user-1', 10)}


def test_default_refresh_with_exc(monkeypatch):
    c1 = Cache()
    c2 = Cache()
    cache = local.make_local_cache(c1, c2, fmt='test')
    called = [0]

    @cache('user:{}', 5, 10, fuzzy_ttl=False)
    def foo(user_id, exc=False):
        called[0] += 1
        if exc:
            raise Exception('Boo')
        return 'user-{}'.format(user_id)

    monkeypatch.setattr(local, 'time', lambda: 20)
    foo(1)

    monkeypatch.setattr(local, 'time', lambda: 26)
    c1.delete('user:1')
    result = foo(1, True)
    assert result == 'user-1'
    assert called == [2]
    assert c1.cache == {'user:1': (b'user-1', 5)}


def test_refresh(monkeypatch):
    def custom_refresh(cache, key, user_id):
        return True

    c1 = Cache()
    c2 = Cache()
    cache = local.make_local_cache(c1, c2, fmt='test', refresh=custom_refresh)
    called = [0]

    @cache('user:{}', 5, 10, fuzzy_ttl=False)
    def foo(user_id):
        called[0] += 1
        return 'user-{}'.format(user_id)

    monkeypatch.setattr(local, 'time', lambda: 20)
    foo(1)

    monkeypatch.setattr(local, 'time', lambda: 26)
    c1.delete('user:1')
    result = foo(1)
    assert result == 'user-1'
    assert called == [1]
    assert c1.cache == {}


def test_get_set_invalidate():
    c1 = Cache()
    c2 = Cache()
    cache = local.make_local_cache(c1, c2, fmt='test')

    @cache('user:{}', 5, 10, fuzzy_ttl=False)
    def foo(user_id):
        return 'user-{}'.format(user_id)

    foo.set('boo', 1)
    assert foo(1) == 'boo'
    assert foo.get(1) == 'boo'

    foo.invalidate(1)
    c1.delete('user:1')
    assert foo(1) == 'user-1'
