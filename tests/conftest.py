import asyncio
import os

import pytest
from cassandra.cluster import Cluster
from cassandra.cqlengine import management
from cassandra.cqlengine import connection as cqlengine_connection

from aiocqlengine.session import aiosession_for_cqlengine

asyncio.set_event_loop(None)


@pytest.fixture
def cluster():
    cluster = Cluster()
    yield cluster
    cluster.shutdown()


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def cassandra(cluster, event_loop):
    session = cluster.connect()
    return aiosession_for_cqlengine(session, loop=event_loop)


@pytest.fixture
def cqlengine_management(cassandra):
    """Setup cqlengine management
    """
    # create test keyspace
    os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "true"
    test_keyspace = "test_async_cqlengine"
    cqlengine_connection.register_connection("cqlengine",
                                             session=cassandra,
                                             default=True)
    management.create_keyspace_simple(test_keyspace, replication_factor=1)

    # setup cqlengine session
    cassandra.set_keyspace(test_keyspace)
    cqlengine_connection.set_session(cassandra)
    yield management
    management.drop_keyspace(test_keyspace)
    cqlengine_connection.unregister_connection("cqlengine")
