from cachel.base import make_key_func

def foo(id, arg1, arg2):
    pass

f = make_key_func('{}-{arg1}-{arg2}', foo)

print f(10, 'foo', 'boo')
