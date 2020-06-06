import sys

PY2 = sys.version_info[0] == 2
ASYNC_AWAIT = sys.version_info[:2] >= (3, 5)
COROUTINE = sys.version_info[:2] >= (3, 4)
ASYNC_COMPREHENSIONS = sys.version_info[:2] >= (3, 6)

utype = type(u'')

if PY2:  # pragma: no cover
    import __builtin__ as builtins
    iterkeys = lambda d: d.iterkeys()
    itervalues = lambda d: d.itervalues()
    iteritems = lambda d: d.iteritems()
    listkeys = lambda d: d.keys()
    listvalues = lambda d: d.values()
    listitems = lambda d: d.items()

    def iscoroutinefunction(fun):
        return False

else:  # pragma: no cover
    import builtins
    from asyncio import iscoroutinefunction
    iterkeys = lambda d: d.keys()
    itervalues = lambda d: d.values()
    iteritems = lambda d: d.items()
    listkeys = lambda d: list(d.keys())
    listvalues = lambda d: list(d.values())
    listitems = lambda d: list(d.items())
