import asyncio
from abc import ABC, abstractmethod
from typing import Literal, Union

ALLOWED_DB_NAMES = [
    "postgres",
]


class AsyncConnection(ABC):
    @abstractmethod
    async def execute(self, stmt):
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def commit(self):
        pass


class AsyncConnectionFactory(ABC):
    @abstractmethod
    async def connect(self) -> AsyncConnection:
        pass


class Connection(ABC):
    @abstractmethod
    def execute(self, stmt):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def commit(self):
        pass


class ConnectionFactory(ABC):
    @abstractmethod
    def connect(self) -> Connection:
        pass


class DeliveryProvider:
    def __init__(self):
        pass

    async def execute_async(self, connection_factory, stmt):
        connection_factory_type = type(connection_factory).__module__
        connection = await connection_factory.connect()
        if "sqlalchemy" in connection_factory_type:
            await connection.exec_driver_sql(stmt)
        else:
            await connection.execute(stmt)
        await connection.commit()
        await connection.close()

    def execute_sync(self, connection_factory, stmt):
        connection_factory_type = type(connection_factory).__module__
        connection = connection_factory.connect()
        if "sqlalchemy" in connection_factory_type:
            connection.exec_driver_sql(stmt)
        else:
            connection.execute(stmt)
        connection.commit()
        connection.close()

    def enable_exactly_once_processing(
        self,
        db_name: Literal["postgres",],
        connection_factory: Union[AsyncConnectionFactory, ConnectionFactory],
        run_async: bool = False,
    ):
        if db_name not in ALLOWED_DB_NAMES:
            raise ValueError(f"Unknown database name: {db_name}")

        schema_stmt = ""
        outbox_stmt = ""
        inbox_stmt = ""
        if db_name == "postgres":
            schema_stmt = """
                CREATE SCHEMA IF NOT EXISTS wibbley;
            """
            outbox_stmt = "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)"
            inbox_stmt = "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)"
        if run_async:
            asyncio.gather(
                self.execute_async(connection_factory, schema_stmt),
                self.execute_async(connection_factory, inbox_stmt),
                self.execute_async(connection_factory, outbox_stmt),
            )
            return

        self.execute_sync(connection_factory, schema_stmt)
        self.execute_sync(connection_factory, inbox_stmt)
        self.execute_sync(connection_factory, outbox_stmt)
