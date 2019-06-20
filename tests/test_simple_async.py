from cachel import compat

if compat.ASYNC:
    from ._test_simple_async import *
