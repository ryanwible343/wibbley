import asyncio
from copy import copy
from logging import getLogger

import orjson

from wibbley.event_driven.delivery_provider.adapters.abstract_adapter import (
    AbstractAdapter,
)
from wibbley.event_driven.queue import wibbley_queue

LOGGER = getLogger("wibbley")


class SQLAlchemyAsyncpgAdapter(AbstractAdapter):
    async def execute_async(self, connection_factory, stmt):
        connection = await connection_factory.connect()
        await connection.exec_driver_sql(stmt)
        await connection.commit()
        await connection.close()

    async def enable_exactly_once_processing(self, connection_factory):
        schema_stmt = """
            CREATE SCHEMA IF NOT EXISTS wibbley;
        """
        outbox_stmt = "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)"
        inbox_stmt = "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)"
        await self.execute_async(connection_factory, schema_stmt)
        await self.execute_async(connection_factory, inbox_stmt)
        await self.execute_async(connection_factory, outbox_stmt)
        return

    async def stage(self, event, session):
        event_dict = copy(vars(event))
        del event_dict["acknowledgement_queue"]
        event_json = orjson.dumps(event_dict).decode("utf-8")
        stmt = f"INSERT INTO wibbley.outbox (id, created_at, event, delivered) VALUES ('{event.id}', '{event.created_at}', '{event_json}', FALSE)"
        session_type = type(session).__module__
        if "sqlalchemy" in session_type:
            session_connection = await session.connection()
            await session_connection.exec_driver_sql(stmt)
        else:
            await session.execute(stmt)

    async def publish(self, event, session):
        select_stmt = f"SELECT * FROM wibbley.outbox WHERE id = '{event.id}';"
        session_connection = await session.connection()
        result = await session_connection.exec_driver_sql(select_stmt)
        record = result.fetchone()
        if record == None:
            return

        event_id = record.id
        update_stmt = (
            f"UPDATE wibbley.outbox SET delivered = TRUE WHERE id = '{event_id}';"
        )
        session_connection = await session.connection()
        await session_connection.exec_driver_sql(update_stmt)
        await wibbley_queue.put(event)
        try:
            ack = await asyncio.wait_for(event.acknowledgement_queue.get(), timeout=2)
        except asyncio.TimeoutError:
            # TODO: Add a loop configuration here to retry sending the event
            ack = False
        if ack:
            LOGGER.info(f"Event {event.id} acknowledged")
            await session.commit()

    async def is_duplicate(self, event, session) -> bool:
        select_stmt = f"SELECT * FROM wibbley.inbox WHERE id = '{event.id}';"
        session_type = type(session).__module__
        session_connection = await session.connection()
        result = await session_connection.exec_driver_sql(select_stmt)
        record = result.fetchone()
        if record:
            self.ack(event)
            return True

        event_dict = copy(vars(event))
        del event_dict["acknowledgement_queue"]
        event_json = orjson.dumps(event_dict).decode("utf-8")
        stmt = f"INSERT INTO wibbley.inbox (id, created_at, event) VALUES ('{event.id}', '{event.created_at}', '{event_json}')"
        if "sqlalchemy" in session_type:
            session_connection = await session.connection()
            await session_connection.exec_driver_sql(stmt)
        else:
            await session.execute(stmt)
        return False

    def ack(self, event):
        event.acknowledgement_queue.put_nowait(True)
        return True

    def nack(self, event):
        event.acknowledgement_queue.put_nowait(False)
        return False
