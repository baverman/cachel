from cachel.base import make_key_func

def foo(id, arg1, arg2):
    pass

f = make_key_func('{}-{arg2}-{}', foo)
print f(10, 'foo', 'boo')

for _ in xrange(1000000):
    f(10, 'foo', 'boo')
