import logging
from abc import ABC, abstractmethod
from typing import Dict, Union

from wibbley.event_driven.delivery_provider.adapters.abstract_adapter import (
    AbstractAdapter,
)
from wibbley.event_driven.delivery_provider.adapters.sqlalchemy_asyncpg import (
    SQLAlchemyAsyncpgAdapter,
)
from wibbley.event_driven.delivery_provider.delivery_provider_adapter_global import (
    delivery_provider_adapter,
)
from wibbley.event_driven.messages import Event

LOGGER = logging.getLogger("wibbley")

ALLOWED_ADAPTERS = {
    "sqlalchemy+asyncpg": SQLAlchemyAsyncpgAdapter(),
}


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


async def enable_exactly_once_processing(
    connection_factory: Union[AsyncConnectionFactory, ConnectionFactory],
    adapters: Dict[str, AbstractAdapter] = ALLOWED_ADAPTERS,
    adapter_name: str = delivery_provider_adapter["name"],
):
    if adapter_name not in adapters:
        raise ValueError(f"Unknown adapter: {adapter_name}")

    adapter = adapters[adapter_name]
    return await adapter.enable_exactly_once_processing(connection_factory)


async def stage(
    event: Event,
    session: AbstractAsyncSession,
    adapters: Dict[str, AbstractAdapter] = ALLOWED_ADAPTERS,
    adapter_name: str = delivery_provider_adapter["name"],
):
    adapter = adapters[adapter_name]
    return await adapter.stage(event, session)


async def publish(
    event: Event,
    session: Union[AbstractAsyncSession, None] = None,
    adapters: Dict[str, AbstractAdapter] = ALLOWED_ADAPTERS,
    adapter_name: str = delivery_provider_adapter["name"],
):
    adapter = adapters[adapter_name]
    return await adapter.publish(event, session)


async def is_duplicate(
    event: Event,
    session: AbstractAsyncSession,
    adapters: Dict[str, AbstractAdapter] = ALLOWED_ADAPTERS,
    adapter_name: str = delivery_provider_adapter["name"],
) -> bool:
    adapter = adapters[adapter_name]
    return await adapter.is_duplicate(event, session)


def ack(event: Event):
    event.acknowledgement_queue.put_nowait(True)


def nack(event: Event):
    event.acknowledgement_queue.put_nowait(False)
