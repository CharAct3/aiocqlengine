import uuid

import pytest
from cassandra.cqlengine import columns

from aiocqlengine.models import AioModel
from aiocqlengine.query import AioBatchQuery


class User(AioModel):
    user_id = columns.UUID(primary_key=True)
    username = columns.Text()


@pytest.mark.asyncio
async def test_queryset_async_functions(cqlengine_management):
    """test cqlengine Model async functions:
    Model.objects.async_get()
    Model.objects.async_first()
    Model.objects.async_all()
    Model.objects.async_create()
    Model.objects(id=obj_id).async_update()
    """
    cqlengine_management.sync_table(User)

    # test: Model.objects.async_create(), Model.objects.async_all(), Model.objects.async_get()
    username1 = "test-username-1"
    await User.objects.async_create(user_id=uuid.uuid4(), username=username1)
    users = await User.objects.async_all()
    user = users[0]
    _user = await User.objects.async_get(user_id=user.user_id)
    _user2 = await User.async_first()
    assert user.username == _user.username == _user2.username == username1

    # test DML query: Model.objects(id=obj_id).async_update()
    username2 = "test-username-2"
    await User.objects(user_id=user.user_id).async_update(username=username2)
    updated_user = await User.objects.async_get(user_id=user.user_id)
    assert updated_user.username == username2


@pytest.mark.asyncio
async def test_model_async_functions(cqlengine_management):
    """test cqlengine Model async functions:
    Model.async_get()
    Model.async_first()
    Model.async_all()
    Model.async_create()
    obj.async_update()
    obj.async_save()
    obj.async_delete()
    """
    cqlengine_management.sync_table(User)

    # test: Model.async_create(), Model.async_all(), Model.async_get(), Model.async_first()
    username1 = "test-username-1"
    await User.async_create(user_id=uuid.uuid4(), username=username1)
    users = await User.async_all()
    user = users[0]
    _user = await User.async_get(user_id=user.user_id)
    _user2 = await User.async_first()
    assert user.username == _user.username == _user2.username == username1

    # test: obj.async_save()
    username2 = "test-username-2"
    user.username = username2
    await user.async_save()
    _user = await User.async_get(user_id=user.user_id)
    assert user.username == _user.username == username2

    # test: obj.async_update()
    username3 = "test-username-3"
    await user.async_update(username=username3)
    _user = await User.async_get(user_id=user.user_id)
    assert user.username == _user.username == username3

    # test: obj.async_delete()
    await user.async_delete()
    assert len(await User.objects.async_all()) == 0


@pytest.mark.asyncio
async def test_batch_query_async_execute(cqlengine_management):
    cqlengine_management.sync_table(User)
    batch_query = AioBatchQuery()
    User.batch(batch_query).create(user_id=uuid.uuid4(), username="user-1")
    User.batch(batch_query).create(user_id=uuid.uuid4(), username="user-2")
    User.batch(batch_query).create(user_id=uuid.uuid4(), username="user-3")
    await batch_query.async_execute()

    users = await User.async_all()
    username_set = {user.username for user in users}
    assert username_set == {"user-1", "user-2", "user-3"}


@pytest.mark.asyncio
async def test_async_iterate(cqlengine_management):
    cqlengine_management.sync_table(User)
    batch_query = AioBatchQuery()
    for i in range(101):
        User.batch(batch_query).create(user_id=uuid.uuid4(), username=f'{i}')
    await batch_query.async_execute()

    # Try iterate all
    iter_count_1 = 0
    result_count_1 = 0
    async for users in User.async_iterate(fetch_size=10):
        iter_count_1 += 1
        result_count_1 += len(users)
    assert iter_count_1 == 11
    assert result_count_1 == 101

    # Try iterate with limit
    iter_count_2 = 0
    result_count_2 = 0
    async for users in User.async_iterate(fetch_size=10, limit=80):
        iter_count_2 += 1
        result_count_2 += len(users)
    assert iter_count_2 == 8
    assert result_count_2 == 80
