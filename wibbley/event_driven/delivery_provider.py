import asyncio
import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Literal, Union

import orjson

from wibbley.event_driven.messages import Event
from wibbley.event_driven.queue import wibbley_queue

ALLOWED_DB_NAMES = [
    "postgres",
]

LOGGER = logging.getLogger("wibbley")


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


class AbstractAsyncSession(ABC):
    @abstractmethod
    async def execute(self, stmt):
        pass

    @abstractmethod
    async def commit(self):
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
            loop = asyncio.get_event_loop()
            loop.run_until_complete(
                asyncio.gather(
                    self.execute_async(connection_factory, schema_stmt),
                    self.execute_async(connection_factory, inbox_stmt),
                    self.execute_async(connection_factory, outbox_stmt),
                )
            )
            loop.close()
            return

        self.execute_sync(connection_factory, schema_stmt)
        self.execute_sync(connection_factory, inbox_stmt)
        self.execute_sync(connection_factory, outbox_stmt)


async def stage(event: Event, session: AbstractAsyncSession):
    event_dict = deepcopy(vars(event))
    del event_dict["acknowledgement_queue"]
    event_json = orjson.dumps(event_dict).decode("utf-8")
    stmt = f"INSERT INTO wibbley.outbox (id, created_at, event, delivered) VALUES ('{event.id}', '{event.created_at}', '{event_json}', FALSE)"
    session_type = type(session).__module__
    if "sqlalchemy" in session_type:
        session_connection = await session.connection()
        await session_connection.exec_driver_sql(stmt)
    else:
        await session.execute(stmt)


async def publish(event: Event, session: Union[AbstractAsyncSession, None] = None):
    if session is None:
        await wibbley_queue.put(event)
        return

    select_stmt = f"SELECT * FROM wibbley.outbox WHERE id = '{event.id}';"
    session_type = type(session).__module__
    if "sqlalchemy" in session_type:
        session_connection = await session.connection()
        result = await session_connection.exec_driver_sql(select_stmt)
    else:
        result = await session.execute(select_stmt)

    if result:
        event_id = result.fetchone().id
        update_stmt = (
            f"UPDATE wibbley.outbox SET delivered = TRUE WHERE id = '{event_id}';"
        )
        if "sqlalchemy" in session_type:
            session_connection = await session.connection()
            await session_connection.exec_driver_sql(update_stmt)
            await wibbley_queue.put(event)
            ack = await asyncio.wait_for(event.acknowledgement_queue.get(), timeout=2)
            if ack:
                LOGGER.info(f"Event {event.id} acknowledged")
                await session.commit()
        else:
            await session.execute(update_stmt)
            await wibbley_queue.put(event)
            await session.commit()


def ack(event: Event):
    event.acknowledgement_queue.put_nowait(True)
    return True


def nack(event: Event):
    event.acknowledgement_queue.put_nowait(False)
    return False
