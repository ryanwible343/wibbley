import asyncio
import inspect
from copy import copy
from dataclasses import dataclass
from logging import getLogger

import orjson

from wibbley.event_driven.message_broker.queue import wibbley_queue
from wibbley.event_driven.message_client.adapters.abstract_adapter import (
    AbstractAdapter,
)
from wibbley.utilities.async_retry import AsyncRetry

LOGGER = getLogger("wibbley")


@dataclass
class PreFlightEvent:
    acknowledgement_queue: asyncio.Queue


class SQLAlchemyAsyncpgAdapter:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory

    def get_table_creation_statements(self):
        return [
            "CREATE SCHEMA IF NOT EXISTS wibbley;",
            "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)",
            "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)",
        ]

    async def get_connection(self):
        return await self.connection_factory.connect()

    async def close_connection(self, connection):
        await connection.close()

    async def commit_connection(self, connection):
        await connection.commit()

    def get_outbox_insert_stmt(self, event_id, event_created_at, event_json):
        return f"INSERT INTO wibbley.outbox (id, created_at, event, delivered) VALUES ('{event_id}', '{event_created_at}', '{event_json}', FALSE)"

    def get_outbox_select_stmt(self, event_id):
        return f"SELECT * FROM wibbley.outbox WHERE id = '{event_id}';"

    def get_outbox_update_stmt(self, event_id):
        return f"UPDATE wibbley.outbox SET delivered = TRUE WHERE id = '{event_id}';"

    def get_inbox_select_stmt(self, event_id):
        return f"SELECT * FROM wibbley.inbox WHERE id = '{event_id}';"

    def get_inbox_insert_stmt(self, event_id, event_created_at, event_json):
        return f"INSERT INTO wibbley.inbox (id, created_at, event) VALUES ('{event_id}', '{event_created_at}', '{event_json}')"

    async def execute_stmt_on_connection(self, stmt, connection):
        return await connection.exec_driver_sql(stmt)

    async def execute_stmt_on_transaction(self, stmt, transaction_connection):
        connection = await transaction_connection.connection()
        return await self.execute_stmt_on_connection(stmt, connection)

    def get_first_row(self, result):
        return result.fetchone()
