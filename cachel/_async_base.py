from textwrap import dedent
from . import compat


class AsyncBaseCache(object):
    is_async = True

    async def set(self, key, value, ttl):  # pragma: no cover
        raise NotImplementedError()

    async def get(self, key):  # pragma: no cover
        raise NotImplementedError()

    async def delete(self, key):  # pragma: no cover
        raise NotImplementedError()

    if compat.ASYNC_COMPREHENSIONS:  # pragma: no cover
        exec(dedent('''\
            async def mget(self, keys):
                return [await self.get(k) for k in keys]
        '''))
    else:  # pragma: no cover
        async def mget(self, keys):
            result = []
            for k in keys:
                result.append(await self.get(k))
            return result

    async def mset(self, items, expire):
        for k, v in items:
            await self.set(k, v, expire)

    async def mdelete(self, keys):
        for k in keys:
            await self.delete(k)
