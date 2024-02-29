import asyncio
import inspect
from copy import copy
from dataclasses import dataclass
from logging import getLogger

import orjson

from wibbley.event_driven.message_client.adapters.abstract_adapter import (
    AbstractAdapter,
)
from wibbley.event_driven.queue import wibbley_queue
from wibbley.utilities.async_retry import AsyncRetry

LOGGER = getLogger("wibbley")


@dataclass
class PreFlightEvent:
    acknowledgement_queue: asyncio.Queue


class SQLAlchemyAsyncpgAdapter(AbstractAdapter):
    def __init__(self, connection_factory):
        self.async_retry = AsyncRetry()
        self.ack_timeout = 2
        self.connection_factory = connection_factory

    async def execute_async(self, stmt):
        connection = await self.connection_factory.connect()
        await connection.exec_driver_sql(stmt)
        await connection.commit()
        await connection.close()

    async def enable_exactly_once_processing(self):
        schema_stmt = "CREATE SCHEMA IF NOT EXISTS wibbley;"
        outbox_stmt = "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)"
        inbox_stmt = "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)"
        await self.execute_async(schema_stmt)
        await self.execute_async(inbox_stmt)
        await self.execute_async(outbox_stmt)
        return

    async def stage(self, event, session):
        event_dict = copy(vars(event))
        del event_dict["acknowledgement_queue"]
        event_json = orjson.dumps(event_dict).decode("utf-8")
        stmt = f"INSERT INTO wibbley.outbox (id, created_at, event, delivered) VALUES ('{event.id}', '{event.created_at}', '{event_json}', FALSE)"
        session_connection = await session.connection()
        await session_connection.exec_driver_sql(stmt)

    async def _publish_task(self, event, queue):
        @self.async_retry
        async def wait_for_ack(event):
            try:
                return await asyncio.wait_for(
                    event.acknowledgement_queue.get(), timeout=self.ack_timeout
                )
            except asyncio.TimeoutError:
                return False

        select_stmt = f"SELECT * FROM wibbley.outbox WHERE id = '{event.id}';"
        connection = await self.connection_factory.connect()
        result = await connection.exec_driver_sql(select_stmt)
        record = result.fetchone()
        if record is None:
            return

        event_id = record.id
        update_stmt = (
            f"UPDATE wibbley.outbox SET delivered = TRUE WHERE id = '{event_id}';"
        )
        await connection.exec_driver_sql(update_stmt)
        await queue.put(event)
        expected_ack_count = await wait_for_ack(event)
        acked_count = 0
        for _ in range(expected_ack_count):
            ack = await wait_for_ack(event)
            if ack:
                acked_count += 1
        if acked_count == expected_ack_count:
            LOGGER.info(f"Event {event.id} acknowledged")
            await connection.commit()
        await connection.close()

    async def publish(self, event, queue=wibbley_queue):
        asyncio.create_task(self._publish_task(event, queue))

    async def is_duplicate(self, event, session) -> bool:
        select_stmt = f"SELECT * FROM wibbley.inbox WHERE id = '{event.id}';"
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
        await session_connection.exec_driver_sql(stmt)
        return False

    def ack(self, event):
        event.acknowledgement_queue.put_nowait(True)
        return True

    def nack(self, event):
        event.acknowledgement_queue.put_nowait(False)
        return False
