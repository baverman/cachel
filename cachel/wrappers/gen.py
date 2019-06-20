import os.path
import re
from cachel.wrappers import WRAPPER_NAMES


def gen_wrapper_module(tpl, is_fn_async=False, is_cache_async=False):
    fn_async = fn_await = c_async = c_await = any_await = ''
    if is_fn_async:
        fn_async = 'async '
        fn_await = 'await '
        any_await = 'await '
    if is_cache_async:
        fn_async = 'async '
        c_async = 'async '
        c_await = 'await '
        any_await = 'await '

    tpl = re.sub('async_fn,.+?None\n\n', '', tpl)
    tpl = re.sub('@async_fn\n    ', fn_async, tpl)
    tpl = re.sub('@async_cache\n    ', c_async, tpl)
    tpl = re.sub('await_fn\s+&\s+', fn_await, tpl)
    tpl = re.sub('await_cache\s+&\s+', c_await, tpl)
    tpl = re.sub('await_any\s+&\s+', any_await, tpl)
    return tpl


def main():
    root = os.path.dirname(__file__)
    tpl = open(os.path.join(root, 'template.py')).read()

    modules = [
        ('full_sync', False, False),
        ('async_fn', True, False),
        ('async_cache', False, True),
        ('full_async', True, True),
    ]

    for (is_fn_async, is_cache_async), name in WRAPPER_NAMES.items():
        name = name.rpartition('.')[2]
        with open(os.path.join(root, name + '.py'), 'w') as f:
            f.write(gen_wrapper_module(tpl, is_fn_async, is_cache_async))


if __name__ == '__main__':
    main()
