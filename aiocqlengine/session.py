from functools import partial
from types import MethodType

from aiocassandra import aiosession
from cassandra.cluster import ResultSet


def asyncio_result_set(self, async_fut, cassandra_fut, result):
    """
    Return ResultSet instead of return initial response
    """
    if async_fut.cancelled():
        return

    result_set = ResultSet(cassandra_fut, result)
    self._asyncio_loop.call_soon_threadsafe(
        async_fut.set_result, result_set)


async def execute_future(self, *args, **kwargs):
    _request = partial(self.execute_async, *args, **kwargs)
    cassandra_fut = await self._asyncio_loop.run_in_executor(
        self._asyncio_executor,
        _request
    )

    asyncio_fut = self._asyncio_fut_factory()

    cassandra_fut.add_callbacks(
        callback=partial(self._asyncio_result, asyncio_fut, cassandra_fut),
        errback=partial(self._asyncio_exception, asyncio_fut)
    )

    return await asyncio_fut


def aiosession_for_cqlengine(session, *, executor=None, loop=None):
    session = aiosession(session, executor=executor, loop=loop)
    session._asyncio_result = MethodType(asyncio_result_set, session)
    session.execute_future = MethodType(execute_future, session)
    return session

