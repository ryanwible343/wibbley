import asyncio
from copy import copy

import orjson
import pytest

from wibbley.event_driven.message_client.adapters.sqlalchemy_asyncpg import (
    SQLAlchemyAsyncpgAdapter,
)
from wibbley.event_driven.messagebus.messages import Event
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
async def test__sqlalchemy_asyncpg_adapter_get_table_creation_statements__returns_statements():
    # ARRANGE
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = adapter.get_table_creation_statements()

    assert result == [
        "CREATE SCHEMA IF NOT EXISTS wibbley;",
        "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)",
        "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)",
    ]


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_get_connection__returns_connection():
    # ARRANGE
    connection_factory = FakeConnectionFactory()
    adapter = SQLAlchemyAsyncpgAdapter(connection_factory)

    # ACT
    result = await adapter.get_connection()

    assert isinstance(result, FakeConnection)


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_close_connection__closes_connection():
    # ARRANGE
    connection = FakeConnection()
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    await adapter.close_connection(connection)

    assert connection.close_calls == [None]


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_commit_connection__commits_connection():
    # ARRANGE
    connection = FakeConnection()
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    await adapter.commit_connection(connection)

    assert connection.commit_calls == [None]


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_get_outbox_insert_stmt__returns_insert_stmt():
    # ARRANGE
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = adapter.get_outbox_insert_stmt("123", "2021-01-01", '{"test": "test"}')

    assert (
        result
        == "INSERT INTO wibbley.outbox (id, created_at, event, delivered) VALUES ('123', '2021-01-01', '{\"test\": \"test\"}', FALSE)"
    )


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_get_outbox_select_stmt__returns_select_stmt():
    # ARRANGE
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = adapter.get_outbox_select_stmt("123")

    assert result == "SELECT * FROM wibbley.outbox WHERE id = '123';"


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_get_outbox_update_stmt__returns_update_stmt():
    # ARRANGE
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = adapter.get_outbox_update_stmt("123")

    assert result == "UPDATE wibbley.outbox SET delivered = TRUE WHERE id = '123';"


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_get_inbox_select_stmt__returns_select_stmt():
    # ARRANGE
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = adapter.get_inbox_select_stmt("123")

    assert result == "SELECT * FROM wibbley.inbox WHERE id = '123';"


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_get_inbox_insert_stmt__returns_insert_stmt():
    # ARRANGE
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = adapter.get_inbox_insert_stmt("123", "2021-01-01", '{"test": "test"}')

    assert (
        result
        == "INSERT INTO wibbley.inbox (id, created_at, event) VALUES ('123', '2021-01-01', '{\"test\": \"test\"}')"
    )


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_execute_stmt_on_connection__executes_stmt_on_connection():
    # ARRANGE
    connection = FakeConnection()
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = await adapter.execute_stmt_on_connection("test", connection)

    assert connection.exec_driver_sql_calls == ["test"]
    assert result.row.id == "123"


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_execute_stmt_on_transaction__executes_stmt_on_transaction():
    # ARRANGE
    session = FakeSession()
    adapter = SQLAlchemyAsyncpgAdapter(None)

    # ACT
    result = await adapter.execute_stmt_on_transaction("test", session)

    assert session.connection_var.exec_driver_sql_calls == ["test"]
    assert result.row.id == "123"


@pytest.mark.asyncio
async def test__sqlalchemy_asyncpg_adapter_get_first_row__returns_first_row():
    # ARRANGE
    adapter = SQLAlchemyAsyncpgAdapter(None)
    result = FakeResult(FakeRow("123"))

    # ACT
    result = adapter.get_first_row(result)

    assert result.id == "123"
