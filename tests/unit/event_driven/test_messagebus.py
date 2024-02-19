import asyncio

import pytest

from wibbley.event_driven.messagebus import Messagebus, send
from wibbley.event_driven.messages import Command, Event, Query
from wibbley.utilities.async_retry import AsyncRetry


class FakeClass:
    pass


def test__messagebus_is_function__when_is_function__returns_true():
    # Arrange
    messagebus = Messagebus()
    obj = lambda: None

    # Act
    result = messagebus.is_function(obj)

    # Assert
    assert result == True


def test__messagebus_is_function__when_is_not_function__returns_false():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = messagebus.is_function(FakeClass)

    # Assert
    assert result == False


def test__messagebus_is_function__when_is_str__returns_false():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = messagebus.is_function("test")

    # Assert
    assert result == False


def test__messagebus_is_class__when_is_class__returns_true():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = messagebus.is_class(FakeClass)

    # Assert
    assert result == True


def test__messagebus_is_class__when_is_not_class__returns_false():
    # Arrange
    messagebus = Messagebus()
    obj = lambda: None

    # Act
    result = messagebus.is_class(obj)

    # Assert
    assert result == False


def test__messagebus_is_class__when_is_str__returns_false():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = messagebus.is_class("test")

    # Assert
    assert result == False


def test__messagebus_listen__when_message_is_event_and_decorated_is_class__adds_event_handler():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Event)
    class FakeClass:
        def handle(self):
            pass

    # Assert
    assert type(messagebus.event_handlers[Event][0]) == type(FakeClass().handle)


def test__messagebus_listen__when_message_is_event_and_decorated_is_function__adds_event_handler():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Event)
    async def fake_function():
        pass

    # Assert
    assert type(messagebus.event_handlers[Event][0]) == type(fake_function)


def test__messagebus_listen__when_multiple_event_handlers_decorated__appends_to_handler_list():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Event)
    async def fake_function():
        pass

    @messagebus.listen(Event)
    async def another_fake_function():
        pass

    # Assert
    assert len(messagebus.event_handlers[Event]) == 2


def test__messagebus_listen__when_message_is_command_and_decorated_is_class__adds_command_handler():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Command)
    class FakeClass:
        def handle(self):
            pass

    # Assert
    assert type(messagebus.command_handlers[Command]) == type(FakeClass().handle)


def test__messagebus_listen__when_message_is_command_and_decorated_is_function__adds_command_handler():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Command)
    async def fake_function():
        pass

    # Assert
    assert type(messagebus.command_handlers[Command]) == type(fake_function)


def test__messagebus_listen__when_command_handler_already_registered__raises_value_error():
    # Arrange
    messagebus = Messagebus()

    # Act/Assert
    with pytest.raises(ValueError) as e:

        @messagebus.listen(Command)
        async def fake_function():
            pass

        @messagebus.listen(Command)
        async def fake_function():
            pass


def test__messagebus_listen__when_message_is_query_and_decorated_is_class__adds_query():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Query)
    class FakeClass:
        def handle(self):
            pass

    # Assert
    assert type(messagebus.query_handlers[Query]) == type(FakeClass().handle)


def test__messagebus_listen__when_message_is_query_and_decorated_is_function__adds_query():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Query)
    async def fake_function():
        pass

    # Assert
    assert type(messagebus.query_handlers[Query]) == type(fake_function)


def test__messagebus_listen__when_query_handler_already_registered__raises_value_error():
    # Arrange
    messagebus = Messagebus()

    # Act/Assert
    with pytest.raises(ValueError) as e:

        @messagebus.listen(Query)
        async def fake_function():
            pass

        @messagebus.listen(Query)
        async def fake_function():
            pass


@pytest.mark.asyncio
async def test__messagebus_handle__when_message_is_command__calls_command_handler():
    # Arrange
    messagebus = Messagebus()

    @messagebus.listen(Command)
    async def fake_function(message):
        return True

    # Act
    result = await messagebus.handle(Command())

    # Assert
    assert result == True


@pytest.mark.asyncio
async def test__messagebus_handle__when_no_command_handler_registered__returns_false():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = await messagebus.handle(Command())

    # Assert
    assert result == False


@pytest.mark.asyncio
async def test__messagebus_handle__when_command_handler_raises_exception__reraises_exception():
    # Arrange
    messagebus = Messagebus()

    @messagebus.listen(Command)
    async def fake_function(message):
        raise ValueError("Fake error")

    # Act/Assert
    with pytest.raises(ValueError) as e:
        await messagebus.handle(Command())

    assert str(e.value) == "Fake error"


@pytest.mark.asyncio
async def test__messagebus_handle__when_message_is_query__returns_query_result():
    # Arrange
    messagebus = Messagebus()

    @messagebus.listen(Query)
    async def fake_function(message):
        return True

    # Act
    result = await messagebus.handle(Query())

    # Assert
    assert result == True


@pytest.mark.asyncio
async def test__messagebus_handle__when_no_query_handler_registered__returns_none():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = await messagebus.handle(Query())

    # Assert
    assert result == None


@pytest.mark.asyncio
async def test__messagebus_handle__when_query_handler_raises_exception__reraises_exception():
    # Arrange
    messagebus = Messagebus()

    @messagebus.listen(Query)
    async def fake_function(message):
        raise ValueError("Fake error")

    # Act/Assert
    with pytest.raises(ValueError) as e:
        await messagebus.handle(Query())

    assert str(e.value) == "Fake error"


@pytest.mark.asyncio
async def test__messagebus_handle__when_message_is_event__calls_event_handlers():
    # Arrange
    messagebus = Messagebus()

    @messagebus.listen(Event)
    async def fake_function(message):
        return True

    @messagebus.listen(Event)
    async def another_fake_function(message):
        return False

    # Act
    result = await messagebus.handle(Event())

    # Assert
    assert result == True


@pytest.mark.asyncio
async def test__messagebus_handle__when_no_event_handler_registered__returns_false():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = await messagebus.handle(Event())

    # Assert
    assert result == False


@pytest.mark.asyncio
async def test__messagebus_handle__when_event_handler_raises_exception__continues_to_next_handler():
    # Arrange
    messagebus = Messagebus()
    messagebus.async_retry = AsyncRetry(max_attempts=1, base_delay=0)
    another_fake_function_attempts = 0

    @messagebus.listen(Event)
    async def fake_function(message):
        raise ValueError("Fake error")

    @messagebus.listen(Event)
    async def another_fake_function(message):
        nonlocal another_fake_function_attempts
        another_fake_function_attempts += 1
        return True

    # Act
    result = await messagebus.handle(Event())

    # Assert
    assert result == True
    assert another_fake_function_attempts == 1


@pytest.mark.asyncio
async def test__messagebus_handle__when_message_type_unknown__returns_false():
    # Arrange
    messagebus = Messagebus()

    # Act
    result = await messagebus.handle(FakeClass())

    # Assert
    assert result == False


@pytest.mark.asyncio
async def test__send__puts_message_on_queue():
    # ARRANGE
    queue = asyncio.Queue()

    # ACT
    await send(Command(), queue=queue)

    # ASSERT
    assert queue.empty() == False


def test__messagebus_listen__when_message_type_unknown__raises_value_error():
    # Arrange
    messagebus = Messagebus()

    # Act/Assert
    with pytest.raises(ValueError) as e:

        @messagebus.listen(FakeClass)
        async def fake_function():
            pass


def test__messagebus_listen__when_class_decorated_command_handler_already_registered__raises_value_error():
    # Arrange
    messagebus = Messagebus()

    # Act/Assert
    with pytest.raises(ValueError) as e:

        @messagebus.listen(Command)
        class FakeClass:
            def handle(self):
                pass

        @messagebus.listen(Command)
        class AnotherFakeClass:
            def handle(self):
                pass


def test__messagebus_listen__when_class_decorated_query_handler_already_registered__raises_value_error():
    # Arrange
    messagebus = Messagebus()

    # Act/Assert
    with pytest.raises(ValueError) as e:

        @messagebus.listen(Query)
        class FakeClass:
            def handle(self):
                pass

        @messagebus.listen(Query)
        class AnotherFakeClass:
            def handle(self):
                pass


def test__messagebus_listen__when_class_decorated_event_handler_already_registered__appends_listener():
    # Arrange
    messagebus = Messagebus()

    # Act
    @messagebus.listen(Event)
    class FakeClass:
        def handle(self):
            pass

    @messagebus.listen(Event)
    class AnotherFakeClass:
        def handle(self):
            pass

    # Assert
    assert len(messagebus.event_handlers[Event]) == 2


def test__messagebus_listen__when_decorated_class_is_not_event_command_or_query__raises_value_error():
    # Arrange
    messagebus = Messagebus()

    class FakeClass:
        pass

    # Act/Assert
    with pytest.raises(ValueError) as e:

        @messagebus.listen(FakeClass)
        class FakeListener:
            def handle(self):
                pass


def test__messagebus_add_durability__sets_durability():
    # Arrange
    messagebus = Messagebus()

    # Act
    messagebus.add_durability(adapter="test", connection_factory="test")

    # Assert
    assert messagebus.is_durable == True
    assert messagebus.connection_factory == "test"


@pytest.mark.asyncio
async def test__messagebus_enable_exactly_once_processing__calls_delivery_provider_enable_exactly_once_processing(
    mocker,
):
    # Arrange
    messagebus = Messagebus()
    messagebus.connection_factory = "test"
    delivery_provider_enable_exactly_once_processing = mocker.patch(
        "wibbley.event_driven.messagebus.enable_exactly_once_processing",
    )

    # Act
    await messagebus.enable_exactly_once_processing()

    # Assert
    assert delivery_provider_enable_exactly_once_processing.called == True
