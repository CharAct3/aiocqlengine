# aiocqlengine
Async wrapper for cqlengine of cassandra python driver.

This project is built on [cassandra-python-driver](https://github.com/datastax/python-driver) and [aiocassandra](https://github.com/aio-libs/aiocassandra).

[![Actions Status](https://github.com/charact3/aiocqlengine/workflows/unittest/badge.svg)](https://github.com/charact3/aiocqlengine/actions)

## Installation
```sh
$ pip install aiocqlengine
```

## Get Started

```python
import asyncio
import uuid
import os

from aiocassandra import aiosession
from aiocqlengine.models import AioModel
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns, connection, management

cluster = Cluster()
session = cluster.connect()


class User(AioModel):
    user_id = columns.UUID(primary_key=True)
    username = columns.Text()


async def main():
    aiosession(session)

    # Set aiosession for cqlengine
    session.set_keyspace('example_keyspace')
    connection.set_session(session)

    # Model.objects.create() and Model.create() in async way:
    user_id = uuid.uuid4()
    await User.objects.async_create(user_id=user_id, username='user1')
    # also can use: await User.async_create(user_id=user_id, username='user1)

    # Model.objects.all() and Model.all() in async way:
    print(list(await User.async_all()))
    print(list(await User.objects.filter(user_id=user_id).async_all()))

    # Model.object.update() in async way:
    await User.objects(user_id=user_id).async_update(username='updated-user1')

    # Model.objects.get() and Model.get() in async way:
    user = await User.objects.async_get(user_id=user_id)
    assert user.user_id == (await User.async_get(user_id=user_id)).user_id
    print(user, user.username)

    # obj.save() in async way:
    user.username = 'saved-user1'
    await user.async_save()

    # obj.delete() in async way:
    await user.async_delete()

    # Didn't break original functions
    print('Left users: ', len(User.objects.all()))


def create_keyspace(keyspace):
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = 'true'
    connection.register_connection('cqlengine', session=session, default=True)
    management.create_keyspace_simple(keyspace, replication_factor=1)
    management.sync_table(User, keyspaces=[keyspace])


create_keyspace('example_keyspace')

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
cluster.shutdown()
loop.close()

```


## License
This project is under MIT license.
