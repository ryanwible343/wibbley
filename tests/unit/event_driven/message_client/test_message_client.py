import pytest

from wibbley.event_driven.message_client.message_client import MessageClient
from wibbley.event_driven.messagebus.messages import Event


class FakeConnectionFactory:
    pass


class FakeAdapter:
    def __init__(self, connection_factory):
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

    async def publish(self, event):
        self.publish_calls.append(event)

    async def stage(self, event, session):
        self.stage_calls.append((event, session))

    async def is_duplicate(self, event, session):
        self.is_duplicate_calls.append((event, session))


@pytest.mark.asyncio
async def test__stage__calls_adapter():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = "event"
    fake_session = "session"
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # Act
    await message_client.stage(
        fake_event,
        fake_session,
    )

    # Assert
    assert message_client.adapter.stage_calls == [(fake_event, fake_session)]


@pytest.mark.asyncio
async def test__publish__calls_adapter():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = "event"
    fake_session = "session"
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # Act
    await message_client.publish(fake_event)

    # Assert
    assert message_client.adapter.publish_calls == [fake_event]


@pytest.mark.asyncio
async def test__is_duplicate__calls_adapter():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = "event"
    fake_session = "session"
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # Act
    await message_client.is_duplicate(
        fake_event,
        fake_session,
    )

    # Assert
    assert message_client.adapter.is_duplicate_calls == [(fake_event, fake_session)]


@pytest.mark.asyncio
async def test__ack__adds_true_to_event_acknowledgement_queue():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # Act
    result = message_client.ack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() is True


@pytest.mark.asyncio
async def test__nack__adds_false_to_event_acknowledgement_queue():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )
    # Act
    result = message_client.nack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() is False


@pytest.mark.asyncio
async def test__message_client_init__when_adapter_is_invalid__raises_value_error():  # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_connection_factory = FakeConnectionFactory()

    # Act/Assert
    with pytest.raises(ValueError):
        MessageClient(
            adapter_name="not_there",
            connection_factory=fake_connection_factory,
            adapters=allowed_adapters,
        )
