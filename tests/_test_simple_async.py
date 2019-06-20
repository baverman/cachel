import pytest
from cachel import expire
from cachel.simple import make_cache
from .helpers import Cache, AsyncCache


@pytest.mark.asyncio
async def test_simple_async_func():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False, fmt='unicode')

    @cache('val:{}')
    async def get_val(key):
        if key == 'exp':
            return expire(key, 10)
        else:
            return key

    assert await get_val('boo') == 'boo'
    assert await get_val('exp') == 'exp'
    assert await get_val('exp') == 'exp'

    assert get_val.get('boo') == 'boo'
    get_val.set('BAR', 'boo')
    assert get_val.get('boo') == 'BAR'
    get_val.invalidate('boo')
    assert get_val.get('boo') == None


@pytest.mark.asyncio
async def test_objects_async_func():
    cache = make_cache(Cache(), ttl=42, fuzzy_ttl=False, fmt='unicode')

    @cache.objects('val:{}')
    async def get_val(ids):
        result = {1: 'boo', 2: expire('foo', 10)}
        return {it: result[it] for it in ids if it in result}

    await get_val([1, 2])
    await get_val([1])
    await get_val({1: None})
    get_val.invalidate([1])
    assert await get_val.one(3) == None


@pytest.mark.asyncio
async def test_simple_async_cache():
    cache = make_cache(AsyncCache(), ttl=42, fuzzy_ttl=False, fmt='unicode')

    @cache('val:{}')
    def get_val(key):
        if key == 'exp':
            return expire(key, 10)
        else:
            return key

    assert await get_val('boo') == 'boo'
    assert await get_val('exp') == 'exp'
    assert await get_val('exp') == 'exp'

    assert await get_val.get('boo') == 'boo'
    await get_val.set('BAR', 'boo')
    assert await get_val.get('boo') == 'BAR'
    await get_val.invalidate('boo')
    assert await get_val.get('boo') == None


@pytest.mark.asyncio
async def test_objects_async_cache():
    cache = make_cache(AsyncCache(), ttl=42, fuzzy_ttl=False, fmt='unicode')

    @cache.objects('val:{}')
    def get_val(ids):
        result = {1: 'boo', 2: expire('foo', 10)}
        return {it: result[it] for it in ids if it in result}

    await get_val([1, 2])
    await get_val([1])
    await get_val({1: None})
    await get_val.invalidate([1])
    assert await get_val.one(3) == None


@pytest.mark.asyncio
async def test_simple_async_full():
    cache = make_cache(AsyncCache(), ttl=42, fuzzy_ttl=False, fmt='unicode')

    @cache('val:{}')
    async def get_val(key):
        if key == 'exp':
            return expire(key, 10)
        else:
            return key

    assert await get_val('boo') == 'boo'
    assert await get_val('exp') == 'exp'
    assert await get_val('exp') == 'exp'

    assert await get_val.get('boo') == 'boo'
    await get_val.set('BAR', 'boo')
    assert await get_val.get('boo') == 'BAR'
    await get_val.invalidate('boo')
    assert await get_val.get('boo') == None


@pytest.mark.asyncio
async def test_objects_async_full():
    cache = make_cache(AsyncCache(), ttl=42, fuzzy_ttl=False, fmt='unicode')

    @cache.objects('val:{}')
    async def get_val(ids):
        result = {1: 'boo', 2: expire('foo', 10)}
        return {it: result[it] for it in ids if it in result}

    await get_val([1, 2])
    await get_val([1])
    await get_val({1: None})
    await get_val.invalidate([1])
    assert await get_val.one(3) == None
