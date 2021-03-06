from cachel.base import make_key_func, wrap_in, wrap_dict_value_in
from cachel.base import gen_expire


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

    f = make_key_func('{boo}-{foo}', lambda foo, boo: None)
    assert f(10, 20) == '20-10'

    f = make_key_func('{id}', lambda foo: None, True)
    assert f([20, 30]) == ['20', '30']

    f = make_key_func('{foo}-{id}', lambda ids, foo=None: None, True)
    assert f([20, 30]) == ['None-20', 'None-30']
    assert f([20, 30], 'boo') == ['boo-20', 'boo-30']


def test_gen_expire():
    assert 9 <= gen_expire(10) <= 11


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


# def est_delayed_cache():
#     l1 = make_cache(Cache())
#     l2 = make_cache(Cache())
#
#     @delayed_cache('boo')
#     @l1('key:{}', 10)
#     @l2('key:{}', 20)
#     def foo(value, variant, error=False):
#         if error:
#             raise Exception(error)
#         return u'value:{}:{}'.format(value, variant)
#
#     result = foo('key', 'val')
#     assert result == 'value:key:val'
#     assert l1.cache.cache['key:key'][0] == b'\xadvalue:key:val'
#     assert l2.cache.cache['key:key'][0] == b'\xadvalue:key:val'
#
#     result = foo('key', 'val2')
#     assert result == 'value:key:val'
#     assert l1.cache.cache['key:key'][0] == b'\xadvalue:key:val'
#     assert l2.cache.cache['key:key'][0] == b'\xadvalue:key:val'
#
#     del l1.cache.cache['key:key']
#     result = foo('key', 'val2')
#     assert result == 'value:key:val2'
#     assert l1.cache.cache['key:key'][0] == b'\xaevalue:key:val2'
#     assert l2.cache.cache['key:key'][0] == b'\xaevalue:key:val2'
#
#     del l1.cache.cache['key:key']
#     result = foo('key', 'val3', True)
#     assert result == 'value:key:val2'
#     assert l1.cache.cache['key:key'][0] == b'\xaevalue:key:val2'
#     assert l2.cache.cache['key:key'][0] == b'\xaevalue:key:val2'
#
#     del l1.cache.cache['key:key']
#     del l2.cache.cache['key:key']
#     result = foo('key', 'val3', True)
#     assert result == 'boo'
#     assert not l1.cache.cache
#     assert not l2.cache.cache
