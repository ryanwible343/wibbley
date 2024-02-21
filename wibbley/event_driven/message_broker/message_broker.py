import logging
import sys
from abc import ABC, abstractmethod
from typing import Dict, Union

from wibbley.event_driven.message_broker.adapters.abstract_adapter import (
    AbstractAdapter,
)
from wibbley.event_driven.message_broker.adapters.sqlalchemy_asyncpg import (
    SQLAlchemyAsyncpgAdapter,
)
from wibbley.event_driven.messages import Event

LOGGER = logging.getLogger("wibbley")

ADAPTERS = {"sqlalchemy+asyncpg": SQLAlchemyAsyncpgAdapter}


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


class MessageBroker:
    def __init__(
        self,
        adapter_name: str,
        connection_factory: AsyncConnectionFactory,
        adapters=ADAPTERS,
    ):
        if adapter_name not in adapters:
            raise ValueError("Unavailable adapter selected")
        self.adapter = adapters[adapter_name](connection_factory)

    async def stage(
        self,
        event: Event,
        session: AbstractAsyncSession,
    ):
        return await self.adapter.stage(event, session)

    async def publish(self, event: Event):
        return await self.adapter.publish(event)

    async def is_duplicate(
        self,
        event: Event,
        session: AbstractAsyncSession,
    ) -> bool:
        return await self.adapter.is_duplicate(event, session)

    def ack(self, event: Event):
        event.acknowledgement_queue.put_nowait(True)

    def nack(self, event: Event):
        event.acknowledgement_queue.put_nowait(False)
