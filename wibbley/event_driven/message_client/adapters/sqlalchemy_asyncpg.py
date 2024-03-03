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
            "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN, attempts INT);",
            "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID, fanout_key VARCHAR(200), created_at TIMESTAMPTZ, event JSONB, PRIMARY KEY (id, fanout_key));",
            "CREATE TABLE IF NOT EXISTS wibbley.fanout (id UUID, fanout_key VARCHAR(200), created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN, PRIMARY KEY (id, fanout_key));",
        ]

    async def get_connection(self):
        return await self.connection_factory.connect()

    async def close_connection(self, connection):
        await connection.close()

    async def commit_connection(self, connection):
        await connection.commit()

    async def get_transaction(self, connection):
        return connection

    async def start_transaction(self, transaction):
        pass

    async def rollback_transaction(self, transaction):
        await transaction.rollback()

    async def commit_transaction(self, transaction):
        await transaction.commit()

    def get_outbox_insert_stmt(self, event_id, event_created_at, event_json, attempts):
        return f"INSERT INTO wibbley.outbox (id, created_at, event, delivered, attempts) VALUES ('{event_id}', '{event_created_at}', '{event_json}', FALSE, {attempts})"

    def get_outbox_select_stmt(self, event_id):
        return f"SELECT * FROM wibbley.outbox WHERE id = '{event_id}';"

    def get_outbox_outstanding_select_stmt(self):
        return f"SELECT * FROM wibbley.outbox WHERE delivered = FALSE;"

    def get_outbox_update_stmt(self, event_id, attempts):
        return f"UPDATE wibbley.outbox SET delivered = TRUE, attempts = {attempts} WHERE id = '{event_id}';"

    def get_outbox_failed_delivery_update_stmt(self, event_id, attempts):
        return (
            f"UPDATE wibbley.outbox SET attempts = {attempts} WHERE id = '{event_id}';"
        )

    def get_inbox_select_stmt(self, event_id, fanout_key):
        return f"SELECT * FROM wibbley.inbox WHERE id = '{event_id}' AND fanout_key = '{fanout_key}';"

    def get_inbox_insert_stmt(self, event_id, event_created_at, fanout_key, event_json):
        return f"INSERT INTO wibbley.inbox (id, created_at, event, fanout_key) VALUES ('{event_id}', '{event_created_at}', '{event_json}', '{fanout_key}')"

    def get_fanout_insert_stmt(
        self, event_id, fanout_key, event_created_at, event_json
    ):
        return f"INSERT INTO wibbley.fanout (id, fanout_key, created_at, event, delivered) VALUES ('{event_id}', '{fanout_key}', '{event_created_at}', '{event_json}', FALSE)"

    def get_fanout_select_all_stmt(self, event_id):
        return f"SELECT * FROM wibbley.fanout WHERE id = '{event_id}';"

    def delete_fanout_stmt(self, event_id, fanout_key):
        return f"DELETE FROM wibbley.fanout WHERE id = '{event_id}' AND fanout_key = '{fanout_key}';"

    async def execute_stmt_on_connection(self, stmt, connection):
        return await connection.exec_driver_sql(stmt)

    async def execute_stmt_on_transaction(self, stmt, transaction_connection):
        connection = await transaction_connection.connection()
        return await self.execute_stmt_on_connection(stmt, connection)

    def get_first_row(self, result):
        return result.fetchone()

    def get_all_rows(self, result):
        return result.fetchall()
