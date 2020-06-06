from cachel import compat

if compat.ASYNC_AWAIT:
    from ._test_simple_async import *
