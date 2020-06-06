import cachel.wrappers
from cachel.ast_transformer import import_module


def test_simple():
    params = (('fn', False), ('cache', False), ('call', False))
    import_module('cachel.wrappers.wrappers_t', params)
    from cachel.wrappers import wrappers_t
    assert wrappers_t.params == params
    assert wrappers_t.CacheWrapper
    assert wrappers_t.ObjectsCacheWrapper
