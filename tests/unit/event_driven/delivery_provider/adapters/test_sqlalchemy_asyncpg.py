import asyncio
from copy import copy

import orjson
import pytest

from wibbley.event_driven.message_broker.adapters.sqlalchemy_asyncpg import (
    SQLAlchemyAsyncpgAdapter,
)
from wibbley.event_driven.messages import Event
from wibbley.utilities.async_retry import AsyncRetry


class FakeRow:
    def __init__(self, id):
        self.id = id


class FakeResult:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self):
        self.exec_driver_sql_calls = []
        self.commit_calls = []
        self.close_calls = []

    async def exec_driver_sql(self, stmt):
        self.exec_driver_sql_calls.append(stmt)
        return FakeResult(FakeRow("123"))

    async def commit(self):
        self.commit_calls.append(None)

    async def close(self):
        self.close_calls.append(None)


class FakeEmptyConnection:
    def __init__(self):
        self.exec_driver_sql_calls = []
        self.commit_calls = []
        self.close_calls = []

    async def exec_driver_sql(self, stmt):
        self.exec_driver_sql_calls.append(stmt)
        return FakeResult(None)

    async def commit(self):
        self.commit_calls.append(None)

    async def close(self):
        self.close_calls.append(None)


class FakeEmptySession:
    def __init__(self):
        self.connection_var = None
        self.commit_calls = []

    async def connection(self):
        self.connection_var = FakeEmptyConnection()
        return self.connection_var

    async def commit(self):
        self.commit_calls.append(None)


class FakeConnectionFactory:
    def __init__(self):
        self.connections = []

    async def connect(self):
        self.connections.append(FakeConnection())
        return self.connections[-1]


class FakeEmptyConnectionFactory:
    def __init__(self):
        self.connections = []

    async def connect(self):
        self.connections.append(FakeEmptyConnection())
        return self.connections[-1]


class FakeSession:
    def __init__(self):
        self.connection_var = None
        self.commit_calls = []

    async def connection(self):
        self.connection_var = FakeConnection()
        return self.connection_var

    async def commit(self):
        self.commit_calls.append(None)


class FakePublishTask:
    def __init__(self):
        self.called = False

    async def publish(self, event, queue):
        self.called = True


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter__execute_async__calls_exec_driver_sql_and_commit_and_close_on_connection():
    # Arrange
    connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(connection_factory)
    stmt = "SELECT * FROM wibbley.outbox;"

    # Act
    await adapter.execute_async(stmt)

    # Assert
    assert connection_factory.connections[0].exec_driver_sql_calls == [stmt]
    assert connection_factory.connections[0].commit_calls == [None]
    assert connection_factory.connections[0].close_calls == [None]


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_enable_exactly_once_processing__calls_execute_async_with_expected_stmts():
    # Arrange
    connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(connection_factory)

    # Act
    await adapter.enable_exactly_once_processing()

    # Assert
    assert (
        connection_factory.connections[0].exec_driver_sql_calls[0]
        == "CREATE SCHEMA IF NOT EXISTS wibbley;"
    )
    assert (
        connection_factory.connections[1].exec_driver_sql_calls[0]
        == "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)"
    )
    assert (
        connection_factory.connections[2].exec_driver_sql_calls[0]
        == "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)"
    )


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_stage__calls_exec_driver_sql_on_session_connection():
    # Arrange
    connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(connection_factory)
    fake_session = FakeSession()
    fake_event = Event()

    # Act
    await adapter.stage(fake_event, fake_session)

    # Assert
    event_dict = copy(vars(fake_event))
    del event_dict["acknowledgement_queue"]
    expected_event_json = orjson.dumps(event_dict).decode("utf-8")
    assert (
        fake_session.connection_var.exec_driver_sql_calls[0]
        == f"INSERT INTO wibbley.outbox (id, created_at, event, delivered) VALUES ('{fake_event.id}', '{fake_event.created_at}', '{expected_event_json}', FALSE)"
    )


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_publish_task__calls_exec_driver_sql_and_commit_on_session_connection():
    # Arrange
    fake_connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    fake_event = Event()
    fake_event.acknowledgement_queue.put_nowait(True)

    # Act
    await adapter._publish_task(fake_event, queue=asyncio.Queue())

    # Assert
    assert (
        fake_connection_factory.connections[0].exec_driver_sql_calls[0]
        == f"SELECT * FROM wibbley.outbox WHERE id = '{fake_event.id}';"
    )
    assert (
        fake_connection_factory.connections[0].exec_driver_sql_calls[1]
        == f"UPDATE wibbley.outbox SET delivered = TRUE WHERE id = '123';"
    )
    assert fake_connection_factory.connections[0].commit_calls == [None]


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_publish_task__when_acknowledgement_queue_times_out__does_not_commit():
    # Arrange
    fake_connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    adapter.async_retry = AsyncRetry(max_attempts=1, base_delay=0)
    adapter.ack_timeout = 0
    fake_session = FakeSession()
    fake_event = Event()

    # Act
    await adapter._publish_task(fake_event, queue=asyncio.Queue())

    # Assert
    assert fake_session.commit_calls == []


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_is_duplicate__when_record_exists__calls_ack_and_returns_true():
    # Arrange
    fake_connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    fake_session = FakeSession()
    fake_event = Event()

    # Act
    result = await adapter.is_duplicate(fake_event, fake_session)

    # Assert
    assert result == True
    assert (
        fake_session.connection_var.exec_driver_sql_calls[0]
        == f"SELECT * FROM wibbley.inbox WHERE id = '{fake_event.id}';"
    )
    assert fake_session.commit_calls == []


@pytest.mark.asyncio
async def test__sqlalchemy_asyncgp_adapter_is_duplicate__when_record_does_not_exist__calls_execute_and_returns_false():
    # Arrange
    fake_connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    fake_session = FakeEmptySession()
    fake_event = Event()

    # Act
    result = await adapter.is_duplicate(fake_event, fake_session)

    # Assert
    assert result == False
    assert (
        fake_session.connection_var.exec_driver_sql_calls[0]
        == f"SELECT * FROM wibbley.inbox WHERE id = '{fake_event.id}';"
    )
    event_dict = copy(vars(fake_event))
    del event_dict["acknowledgement_queue"]
    expected_event_json = orjson.dumps(event_dict).decode("utf-8")
    assert (
        fake_session.connection_var.exec_driver_sql_calls[1]
        == f"INSERT INTO wibbley.inbox (id, created_at, event) VALUES ('{fake_event.id}', '{fake_event.created_at}', '{expected_event_json}')"
    )


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_ack__puts_true_on_acknowledgement_queue():
    # Arrange
    fake_connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    fake_event = Event()

    # Act
    adapter.ack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() == True


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_nack__puts_false_on_acknowledgement_queue():
    # Arrange
    fake_connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    fake_event = Event()

    # Act
    adapter.nack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() == False


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_publish_task__when_query_returns_none__returns_none():
    # Arrange
    fake_connection_factory = FakeEmptyConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    fake_session = FakeEmptySession()
    fake_event = Event()
    fake_event.acknowledgement_queue.put_nowait(True)

    # Act
    result = await adapter._publish_task(fake_event, fake_session)

    # Assert
    assert result == None


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_publish__calls_publish_test():
    # Arrange
    fake_event = Event()
    fake_connection_factory = FakeEmptyConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(fake_connection_factory)
    fake_publish_task = FakePublishTask()
    adapter._publish_task = fake_publish_task.publish

    # ACT
    try:
        result = await asyncio.wait_for(adapter.publish(fake_event), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # ASSERT
    assert fake_publish_task.called == True
