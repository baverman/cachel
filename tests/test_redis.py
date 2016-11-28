from cachel.redis import RedisCache


def test_redis():
    c = RedisCache()
    c.mdelete(['key1', 'key2'])

    assert c.get('key1') is None

    c.set('key1', 'value', 10)
    assert c.get('key1') == 'value'
    assert c.client.ttl('key1') == 10

    c.mset({'key1': 'value1', 'key2': 'value2'}.items(), 20)
    assert c.get('key1') == 'value1'
    assert c.get('key2') == 'value2'
    assert c.mget(['key2', 'key1']) == ['value2', 'value1']
    assert c.client.ttl('key1') == 20
    assert c.client.ttl('key2') == 20

    c.delete('key1')
    assert c.get('key1') is None

    c.mdelete(['key2'])
    assert c.get('key2') is None
