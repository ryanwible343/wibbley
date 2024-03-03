import asyncio
import logging
from abc import ABC, abstractmethod
from copy import copy

import orjson

from wibbley.event_driven.message_broker.queue import wibbley_queue
from wibbley.event_driven.message_client.adapters import ADAPTERS
from wibbley.event_driven.message_client.adapters.abstract_adapter import (
    AbstractAdapter,
)
from wibbley.event_driven.message_client.adapters.sqlalchemy_asyncpg import (
    SQLAlchemyAsyncpgAdapter,
)
from wibbley.event_driven.messagebus.messages import Event
from wibbley.utilities.async_retry import AsyncRetry

LOGGER = logging.getLogger("wibbley")


class AsyncConnection(ABC):
    @abstractmethod
    async def execute(self, stmt):
        """Execute a text string sql statement on the connection.."""

    @abstractmethod
    async def close(self):
        """Close the connection."""

    @abstractmethod
    async def commit(self):
        """Commit the current transaction."""


class AsyncConnectionFactory(ABC):
    @abstractmethod
    async def connect(self) -> AsyncConnection:
        """Return a new AsyncConnection."""


class Connection(ABC):
    @abstractmethod
    def execute(self, stmt):
        """Execute a text string sql statement on the connection."""

    @abstractmethod
    def close(self):
        """Close the connection."""

    @abstractmethod
    def commit(self):
        """Commit the current transaction."""


class ConnectionFactory(ABC):
    @abstractmethod
    def connect(self) -> Connection:
        """Return a new Connection"""


class AbstractAsyncSession(ABC):
    @abstractmethod
    async def execute(self, stmt):
        """Execute a text string sql statement on the connection."""

    @abstractmethod
    async def commit(self):
        """Commit the current transaction."""


class MessageClient:
    def __init__(
        self,
        adapter_name: str,
        connection_factory: AsyncConnectionFactory,
        adapters=ADAPTERS,
    ):
        if adapter_name not in adapters:
            raise ValueError("Unavailable adapter selected")
        self.adapter = adapters[adapter_name](connection_factory)
        self.async_retry = AsyncRetry()
        self.ack_timeout = 5

    async def stage(
        self,
        event: Event,
        session: AbstractAsyncSession,
    ):
        event_dict = copy(vars(event))
        del event_dict["acknowledgement_queue"]
        event_json = orjson.dumps(event_dict).decode("utf-8")
        stmt = self.adapter.get_outbox_insert_stmt(
            event.id, event.created_at, event_json, 0
        )
        return await self.adapter.execute_stmt_on_transaction(stmt, session)

    async def _publish_task(self, event, queue):
        @self.async_retry
        async def wait_for_ack(event):
            try:
                return await asyncio.wait_for(
                    event.acknowledgement_queue.get(), timeout=self.ack_timeout
                )
            except asyncio.TimeoutError:
                return False

        select_stmt = self.adapter.get_outbox_select_stmt(event.id)
        connection = await self.adapter.get_connection()
        result = await self.adapter.execute_stmt_on_connection(select_stmt, connection)
        record = self.adapter.get_first_row(result)
        if record is None:
            return

        event_id = record.id
        attempts = record.attempts
        update_stmt = self.adapter.get_outbox_update_stmt(event_id, attempts + 1)
        await self.adapter.execute_stmt_on_connection(update_stmt, connection)
        await queue.put(event)
        ack = await wait_for_ack(event)
        if ack:
            await self.adapter.commit_connection(connection)
        await self.adapter.close_connection(connection)

    async def publish(self, event: Event, queue=wibbley_queue):
        asyncio.create_task(self._publish_task(event, queue))

    async def is_duplicate(
        self,
        event: Event,
        session: AbstractAsyncSession,
    ) -> bool:
        select_stmt = self.adapter.get_inbox_select_stmt(event.id, event.fanout_key)
        result = await self.adapter.execute_stmt_on_transaction(select_stmt, session)
        record = self.adapter.get_first_row(result)
        if record:
            self.ack(event)
            return True

        event_dict = copy(vars(event))
        del event_dict["acknowledgement_queue"]
        event_json = orjson.dumps(event_dict).decode("utf-8")
        stmt = self.adapter.get_inbox_insert_stmt(
            event.id, event.created_at, event.fanout_key, event_json
        )
        await self.adapter.execute_stmt_on_transaction(stmt, session)
        return False

    def ack(self, event: Event):
        event.acknowledgement_queue.put_nowait(True)

    def nack(self, event: Event):
        event.acknowledgement_queue.put_nowait(False)
