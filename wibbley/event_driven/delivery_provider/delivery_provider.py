import logging
from abc import ABC, abstractmethod
from typing import Union

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

ALLOWED_ADAPTERS = ["sqlalchemy+asyncpg"]


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


async def enable_exactly_once_processing(
    connection_factory: Union[AsyncConnectionFactory, ConnectionFactory],
    adapters: dict[str, AbstractAdapter] = {
        "sqlalchemy+asyncpg": SQLAlchemyAsyncpgAdapter()
    },
    adapter_name: str = delivery_provider_adapter["name"],
):
    if adapter_name not in ALLOWED_ADAPTERS:
        raise ValueError(f"Unknown adapter: {adapter_name}")

    adapter = adapters[adapter_name]
    return await adapter.enable_exactly_once_processing(connection_factory)


async def stage(
    event: Event,
    session: AbstractAsyncSession,
    adapters: dict[str, AbstractAdapter] = {
        "sqlalchemy+asyncpg": SQLAlchemyAsyncpgAdapter()
    },
    adapter_name: str = delivery_provider_adapter["name"],
):
    adapter = adapters[adapter_name]
    return await adapter.stage(event, session)


async def publish(
    event: Event,
    session: Union[AbstractAsyncSession, None] = None,
    adapters: dict[str, AbstractAdapter] = {
        "sqlalchemy+asyncpg": SQLAlchemyAsyncpgAdapter()
    },
    adapter_name: str = delivery_provider_adapter["name"],
):
    adapter = adapters[adapter_name]
    return await adapter.publish(event, session)


async def is_duplicate(
    event: Event,
    session: AbstractAsyncSession,
    adapters: dict[str, AbstractAdapter] = {
        "sqlalchemy+asyncpg": SQLAlchemyAsyncpgAdapter()
    },
    adapter_name: str = delivery_provider_adapter["name"],
) -> bool:
    adapter = adapters[adapter_name]
    return await adapter.is_duplicate(event, session)


def ack(event: Event):
    event.acknowledgement_queue.put_nowait(True)


def nack(event: Event):
    event.acknowledgement_queue.put_nowait(False)
