import asyncio
import uuid
import os
from time import time

from cassandra.io.asyncioreactor import AsyncioConnection

from aiocqlengine.models import AioModel
from aiocqlengine.query import AioBatchQuery
from aiocqlengine.session import aiosession_for_cqlengine
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns, connection, management


class User(AioModel):
    user_id = columns.UUID(primary_key=True)
    username = columns.Text()


async def run_aiocqlengine_example():
    # Model.objects.create() and Model.create() in async way:
    user_id = uuid.uuid4()
    await User.objects.async_create(user_id=user_id, username='user1')
    await User.async_create(user_id=uuid.uuid4(), username='user2')

    # Model.objects.all() and Model.all() in async way:
    print(list(await User.async_all()))
    print(list(await User.objects.filter(user_id=user_id).async_all()))

    # Model.object.update() in async way:
    await User.objects(user_id=user_id).async_update(username='updated-user1')

    # Model.objects.get() and Model.get() in async way:
    user = await User.objects.async_get(user_id=user_id)
    await User.async_get(user_id=user_id)
    print(user, user.username)

    # Model.save() in async way:
    user.username = 'saved-user1'
    await user.async_save()

    # Model.delete() in async way:
    await user.async_delete()

    # Batch Query in async way:
    batch_query = AioBatchQuery()
    User.batch(batch_query).create(user_id=uuid.uuid4(), username="user-1")
    User.batch(batch_query).create(user_id=uuid.uuid4(), username="user-2")
    User.batch(batch_query).create(user_id=uuid.uuid4(), username="user-3")
    await batch_query.async_execute()

    # Async iterator
    async for users in User.async_iterate(fetch_size=100):
        pass

    # The original cqlengine functions were still there
    print(len(User.objects.all()))


def create_session():
    cluster = Cluster()  # connection_class=AsyncioConnection)
    session = cluster.connect()

    # Create keyspace, if already have keyspace your can skip this
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = 'true'
    connection.register_connection('cqlengine', session=session, default=True)
    management.create_keyspace_simple('example', replication_factor=1)
    management.sync_table(User, keyspaces=['example'])

    # Run the example function in asyncio loop
    loop = asyncio.get_event_loop()

    # Wrap cqlengine connection with aiosession
    aiosession_for_cqlengine(session, loop=loop)
    session.set_keyspace('example')
    connection.set_session(session)
    return session, loop


async def benchmark():
    await asyncio.gather(*[
        User.async_create(user_id=uuid.uuid4(),
                          username='') for _ in range(2000)
    ])


def main():
    # Setup connection for cqlengine
    session, loop = create_session()

    print('start')
    start = time()
    loop.run_until_complete(benchmark())
    end = time()
    print(f'Elapsed time: {end - start}')

    # Shutdown the connection and loop
    session.cluster.shutdown()
    loop.close()


if __name__ == '__main__':
    main()
