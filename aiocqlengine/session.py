import asyncio
from functools import partial
from types import MethodType

from cassandra.cluster import ResultSet


def _asyncio_result(self, async_fut, cassandra_fut, result):
    """
    Return ResultSet instead of return initial response
    """
    if async_fut.cancelled():
        return

    result_set = ResultSet(cassandra_fut, result)
    self._asyncio_loop.call_soon_threadsafe(async_fut.set_result,
                                            result_set)


def _asyncio_exception(self, fut, exc):
    if fut.cancelled():
        return
    self._asyncio_loop.call_soon_threadsafe(fut.set_exception, exc)


async def execute_future(self, *args, **kwargs):
    cassandra_fut = self.execute_async(*args, **kwargs)
    future = asyncio.Future(loop=self._asyncio_loop)
    cassandra_fut.add_callbacks(
        callback=partial(self._asyncio_result, future, cassandra_fut),
        errback=partial(self._asyncio_exception, future)
    )

    return await future


def aiosession_for_cqlengine(session, *, loop=None):
    session._asyncio_loop = loop
    session._asyncio_exception = MethodType(_asyncio_exception, session)
    session._asyncio_result = MethodType(_asyncio_result, session)
    session.execute_future = MethodType(execute_future, session)
    return session
