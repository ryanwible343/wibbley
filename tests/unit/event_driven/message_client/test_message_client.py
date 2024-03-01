import asyncio
from copy import copy

import orjson
import pytest

from wibbley.event_driven.message_client.message_client import MessageClient
from wibbley.event_driven.messagebus.messages import Event


class FakeConnectionFactory:
    pass


class FakeRecord:
    def __init__(self):
        self.id = "test"


class FakeEmptyAdapter:
    def __init__(self, connection_factory):
        self.get_table_creation_statements_calls = []
        self.get_connection_calls = []
        self.close_connection_calls = []
        self.commit_connection_calls = []
        self.get_outbox_insert_stmt_calls = []
        self.get_outbox_select_stmt_calls = []
        self.get_outbox_update_stmt_calls = []
        self.get_inbox_select_stmt_calls = []
        self.get_inbox_insert_stmt_calls = []
        self.execute_stmt_on_connection_calls = []
        self.execute_stmt_on_transaction_calls = []
        self.get_first_row_calls = []

    def get_table_creation_statements(self):
        self.get_table_creation_statements_calls.append(None)
        return []

    async def get_connection(self):
        self.get_connection_calls.append(None)

    async def close_connection(self, connection):
        self.close_connection_calls.append(connection)

    async def commit_connection(self, connection):
        self.commit_connection_calls.append(connection)

    def get_outbox_insert_stmt(self, event_id, event_created_at, event_json):
        self.get_outbox_insert_stmt_calls.append(
            (event_id, event_created_at, event_json)
        )
        return "fake_stmt"

    def get_outbox_select_stmt(self, event_id):
        self.get_outbox_select_stmt_calls.append(event_id)
        return "fake_stmt"

    def get_outbox_update_stmt(self, event_id):
        self.get_outbox_update_stmt_calls.append(event_id)
        return "fake_stmt"

    def get_inbox_select_stmt(self, event_id):
        self.get_inbox_select_stmt_calls.append(event_id)
        return "fake_stmt"

    def get_inbox_insert_stmt(self, event_id, event_created_at, event_json):
        self.get_inbox_insert_stmt_calls.append(
            (event_id, event_created_at, event_json)
        )
        return "fake_stmt"

    async def execute_stmt_on_connection(self, stmt, connection):
        self.execute_stmt_on_connection_calls.append((stmt, connection))
        return "test"

    async def execute_stmt_on_transaction(self, stmt, transaction_connection):
        self.execute_stmt_on_transaction_calls.append((stmt, transaction_connection))
        return "test"

    def get_first_row(self, result):
        self.get_first_row_calls.append(result)
        return None

    def get_table_creation_statements(self):
        return [
            "CREATE SCHEMA IF NOT EXISTS wibbley;",
            "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)",
            "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)",
        ]


class FakeAdapter:
    def __init__(self, connection_factory):
        self.get_table_creation_statements_calls = []
        self.get_connection_calls = []
        self.close_connection_calls = []
        self.commit_connection_calls = []
        self.get_outbox_insert_stmt_calls = []
        self.get_outbox_select_stmt_calls = []
        self.get_outbox_update_stmt_calls = []
        self.get_inbox_select_stmt_calls = []
        self.get_inbox_insert_stmt_calls = []
        self.execute_stmt_on_connection_calls = []
        self.execute_stmt_on_transaction_calls = []
        self.get_first_row_calls = []

    def get_table_creation_statements(self):
        self.get_table_creation_statements_calls.append(None)
        return []

    async def get_connection(self):
        self.get_connection_calls.append(None)

    async def close_connection(self, connection):
        self.close_connection_calls.append(connection)

    async def commit_connection(self, connection):
        self.commit_connection_calls.append(connection)

    def get_outbox_insert_stmt(self, event_id, event_created_at, event_json):
        self.get_outbox_insert_stmt_calls.append(
            (event_id, event_created_at, event_json)
        )
        return "fake_stmt"

    def get_outbox_select_stmt(self, event_id):
        self.get_outbox_select_stmt_calls.append(event_id)
        return "fake_stmt"

    def get_outbox_update_stmt(self, event_id):
        self.get_outbox_update_stmt_calls.append(event_id)
        return "fake_stmt"

    def get_inbox_select_stmt(self, event_id):
        self.get_inbox_select_stmt_calls.append(event_id)
        return "fake_stmt"

    def get_inbox_insert_stmt(self, event_id, event_created_at, event_json):
        self.get_inbox_insert_stmt_calls.append(
            (event_id, event_created_at, event_json)
        )
        return "fake_stmt"

    async def execute_stmt_on_connection(self, stmt, connection):
        self.execute_stmt_on_connection_calls.append((stmt, connection))
        return "test"

    async def execute_stmt_on_transaction(self, stmt, transaction_connection):
        self.execute_stmt_on_transaction_calls.append((stmt, transaction_connection))
        return "test"

    def get_first_row(self, result):
        self.get_first_row_calls.append(result)
        return FakeRecord()

    def get_table_creation_statements(self):
        return [
            "CREATE SCHEMA IF NOT EXISTS wibbley;",
            "CREATE TABLE IF NOT EXISTS wibbley.outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB, delivered BOOLEAN)",
            "CREATE TABLE IF NOT EXISTS wibbley.inbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ, event JSONB)",
        ]


class FakePublishTask:
    def __init__(self):
        self.called = False

    async def publish(self, event, queue):
        self.called = True


@pytest.mark.asyncio
async def test__stage__gets_outbox_stmt_and_executes():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_session = "session"
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    event_dict = copy(vars(fake_event))
    del event_dict["acknowledgement_queue"]
    fake_event_json = orjson.dumps(event_dict).decode("utf-8")

    # Act
    await message_client.stage(
        fake_event,
        fake_session,
    )

    # Assert
    assert message_client.adapter.get_outbox_insert_stmt_calls == [
        (fake_event.id, fake_event.created_at, fake_event_json)
    ]
    assert message_client.adapter.execute_stmt_on_transaction_calls == [
        ("fake_stmt", fake_session)
    ]


@pytest.mark.asyncio
async def test__publish_task__pulls_from_outbox_publishes_and_updates():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_event.acknowledgement_queue.put_nowait(1)
    fake_event.acknowledgement_queue.put_nowait(True)
    fake_queue = asyncio.Queue()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # ACT
    await message_client._publish_task(fake_event, fake_queue)

    # Assert
    assert message_client.adapter.get_outbox_select_stmt_calls == [fake_event.id]
    assert message_client.adapter.execute_stmt_on_connection_calls == [
        ("fake_stmt", None),
        ("fake_stmt", None),
    ]
    assert message_client.adapter.get_first_row_calls == ["test"]
    assert message_client.adapter.commit_connection_calls == [None]
    assert fake_queue.qsize() == 1


@pytest.mark.asyncio
async def test__publish_task__when_record_is_none__returns_early():
    # Arrange
    allowed_adapters = {"fake": FakeEmptyAdapter}
    fake_event = Event()
    fake_queue = asyncio.Queue()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # ACT
    await message_client._publish_task(fake_event, fake_queue)

    # Assert
    assert message_client.adapter.get_outbox_select_stmt_calls == [fake_event.id]
    assert message_client.adapter.execute_stmt_on_connection_calls == [
        ("fake_stmt", None)
    ]
    assert message_client.adapter.get_first_row_calls == ["test"]
    assert message_client.adapter.commit_connection_calls == []
    assert fake_queue.qsize() == 0


@pytest.mark.asyncio
async def test__publish_test__when_nacked__does_not_commit():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_event.acknowledgement_queue.put_nowait(1)
    fake_event.acknowledgement_queue.put_nowait(False)
    fake_queue = asyncio.Queue()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # ACT
    await message_client._publish_task(fake_event, fake_queue)

    # Assert
    assert message_client.adapter.get_outbox_select_stmt_calls == [fake_event.id]
    assert message_client.adapter.execute_stmt_on_connection_calls == [
        ("fake_stmt", None),
        ("fake_stmt", None),
    ]
    assert message_client.adapter.get_first_row_calls == ["test"]
    assert message_client.adapter.commit_connection_calls == []
    assert fake_queue.qsize() == 1


@pytest.mark.asyncio
async def test__publish__creates_asyncio_task():
    # ARRANGE
    allowed_adapters = {"fake": FakeAdapter}
    fake_publish_task = FakePublishTask()
    fake_event = Event()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )
    message_client._publish_task = fake_publish_task.publish

    # ACT
    try:
        await asyncio.wait_for(message_client.publish(fake_event), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # ASSERT
    assert fake_publish_task.called == True


@pytest.mark.asyncio
async def test__is_duplicate__when_is_duplicate__returns_true():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_session = "session"
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # ACT
    result = await message_client.is_duplicate(fake_event, fake_session)

    # Assert
    assert message_client.adapter.get_inbox_select_stmt_calls == [fake_event.id]
    assert message_client.adapter.execute_stmt_on_transaction_calls == [
        ("fake_stmt", fake_session)
    ]
    assert message_client.adapter.get_first_row_calls == ["test"]
    assert result == True


@pytest.mark.asyncio
async def test__is_duplicate__when_not_duplicate__returns_false():
    # Arrange
    allowed_adapters = {"fake": FakeEmptyAdapter}
    fake_event = Event()
    fake_session = "session"
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )
    event_dict = copy(vars(fake_event))
    del event_dict["acknowledgement_queue"]
    fake_event_json = orjson.dumps(event_dict).decode("utf-8")

    # ACT
    result = await message_client.is_duplicate(fake_event, fake_session)

    # Assert
    assert message_client.adapter.get_inbox_select_stmt_calls == [fake_event.id]
    assert message_client.adapter.get_inbox_insert_stmt_calls == [
        (fake_event.id, fake_event.created_at, fake_event_json)
    ]
    assert message_client.adapter.execute_stmt_on_transaction_calls == [
        ("fake_stmt", fake_session),
        ("fake_stmt", fake_session),
    ]
    assert message_client.adapter.get_first_row_calls == ["test"]
    assert result == False


@pytest.mark.asyncio
async def test__ack__puts_true_on_acknowledgement_queue():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # ACT
    message_client.ack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() == True


@pytest.mark.asyncio
async def test__nack__puts_false_on_acknowledgement_queue():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )

    # ACT
    message_client.nack(fake_event)

    # Assert
    assert fake_event.acknowledgement_queue.get_nowait() == False


@pytest.mark.asyncio
async def test__publish_task__when_ack_times_out_on_expected_ack_count__closes_connection_before_committing():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_queue = asyncio.Queue()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )
    message_client.ack_timeout = 0

    # ACT
    await message_client._publish_task(fake_event, fake_queue)

    # Assert
    assert message_client.adapter.get_connection_calls == [None]
    assert message_client.adapter.get_outbox_select_stmt_calls == [fake_event.id]
    assert message_client.adapter.get_outbox_update_stmt_calls == ["test"]
    assert message_client.adapter.execute_stmt_on_connection_calls == [
        ("fake_stmt", None),
        ("fake_stmt", None),
    ]
    assert message_client.adapter.commit_connection_calls == []
    assert message_client.adapter.close_connection_calls == [None]


@pytest.mark.asyncio
async def test__publish_task__when_ack_times_out_on_ack__closes_connection_before_committing():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_event = Event()
    fake_queue = asyncio.Queue()
    fake_connection_factory = FakeConnectionFactory()
    message_client = MessageClient(
        adapter_name="fake",
        connection_factory=fake_connection_factory,
        adapters=allowed_adapters,
    )
    message_client.ack_timeout = 0
    fake_event.acknowledgement_queue.put_nowait(1)

    # ACT
    await message_client._publish_task(fake_event, fake_queue)

    # Assert
    assert message_client.adapter.get_connection_calls == [None]
    assert message_client.adapter.get_outbox_select_stmt_calls == [fake_event.id]
    assert message_client.adapter.get_outbox_update_stmt_calls == ["test"]
    assert message_client.adapter.execute_stmt_on_connection_calls == [
        ("fake_stmt", None),
        ("fake_stmt", None),
    ]
    assert message_client.adapter.commit_connection_calls == []
    assert message_client.adapter.close_connection_calls == [None]


@pytest.mark.asyncio
async def test__message_client_init__when_adapter_does_not_exist__raises_value_error():
    # Arrange
    allowed_adapters = {"fake": FakeAdapter}
    fake_connection_factory = FakeConnectionFactory()

    # Act
    with pytest.raises(ValueError):
        MessageClient(
            adapter_name="non_existent",
            connection_factory=fake_connection_factory,
            adapters=allowed_adapters,
        )
