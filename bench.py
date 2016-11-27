from cachel.base import make_key_func

def foo(id, arg1, arg2='boo'):
    pass

f = make_key_func('{}-{arg2}-{}', foo)
print f(10, 'foo')

# for _ in xrange(1000000):
#     f(10, 'foo', 'boo')

def boo(ids, arg1, arg2='boo'):
    pass

b = make_key_func('{id}-{arg1}-{arg2}', boo, True)
keys = list(range(1000))
print b([10, 20], 'boo', 'foo')
