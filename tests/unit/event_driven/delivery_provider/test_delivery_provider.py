import pytest

from wibbley.event_driven.delivery_provider.delivery_provider import (
    ack,
    enable_exactly_once_processing,
    is_duplicate,
    nack,
    publish,
    stage,
)
from wibbley.event_driven.messages import Event


class FakeConnectionFactory:
    pass


class FakeAdapter:
    def __init__(self):
        self.enable_exactly_once_processing_calls = []
        self.ack_calls = []
        self.nack_calls = []
        self.publish_calls = []
        self.stage_calls = []
        self.is_duplicate_calls = []

    async def ack(self, event):
        self.ack_calls.append(event)

    async def nack(self, event):
        self.nack_calls.append(event)

    async def publish(self, event, session):
        self.publish_calls.append((event, session))

    async def stage(self, event, session):
        self.stage_calls.append((event, session))

    async def is_duplicate(self, event, session):
        self.is_duplicate_calls.append((event, session))

    async def enable_exactly_once_processing(self, event):
        self.enable_exactly_once_processing_calls.append(event)


@pytest.mark.asyncio
async def test__enable_exactly_once_processing__calls_adapter():
    # Arrange
    fake_adapter = FakeAdapter()
    fake_connection_factory = FakeConnectionFactory()

    # Act
    await enable_exactly_once_processing(
        fake_connection_factory,
        {"sqlalchemy+asyncpg": fake_adapter},
        "sqlalchemy+asyncpg",
    )

    # Assert
    assert fake_adapter.enable_exactly_once_processing_calls == [
        fake_connection_factory
    ]


@pytest.mark.asyncio
async def test__stage__calls_adapter():
    # Arrange
    fake_adapter = FakeAdapter()
    fake_event = "event"
    fake_session = "session"

    # Act
    await stage(
        fake_event,
        fake_session,
        {"sqlalchemy+asyncpg": fake_adapter},
        "sqlalchemy+asyncpg",
    )

    # Assert
    assert fake_adapter.stage_calls == [(fake_event, fake_session)]


@pytest.mark.asyncio
async def test__publish__calls_adapter():
    # Arrange
    fake_adapter = FakeAdapter()
    fake_event = "event"
    fake_session = "session"

    # Act
    await publish(
        fake_event,
        fake_session,
        {"sqlalchemy+asyncpg": fake_adapter},
        "sqlalchemy+asyncpg",
    )

    # Assert
    assert fake_adapter.publish_calls == [(fake_event, fake_session)]


@pytest.mark.asyncio
async def test__is_duplicate__calls_adapter():
    # Arrange
    fake_adapter = FakeAdapter()
    fake_event = "event"
    fake_session = "session"

    # Act
    await is_duplicate(
        fake_event,
        fake_session,
        {"sqlalchemy+asyncpg": fake_adapter},
        "sqlalchemy+asyncpg",
    )

    # Assert
    assert fake_adapter.is_duplicate_calls == [(fake_event, fake_session)]


@pytest.mark.asyncio
async def test__ack__adds_true_to_event_acknowledgement_queue():
    # Arrange
    fake_event = Event()

    # Act
    result = ack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() is True


@pytest.mark.asyncio
async def test__nack__adds_false_to_event_acknowledgement_queue():
    # Arrange
    fake_event = Event()

    # Act
    result = nack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() is False
