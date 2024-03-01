import logging
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker
from src.database import engine
from src.model import Shape

from wibbley.event_driven import (
    Command,
    Event,
    MessageBroker,
    MessageBrokerSettings,
    Messagebus,
    MessageClient,
)

messagebus = Messagebus()
message_client = MessageClient("sqlalchemy+asyncpg", engine)
message_broker = MessageBroker(
    messagebus=messagebus,
    adapter_name="sqlalchemy+asyncpg",
    connection_factory=engine,
    message_broker_settings=MessageBrokerSettings(
        event_handler_count=1, outbox_poller_count=1
    ),
)


LOGGER = logging.getLogger(__name__)


class SquareCreatedEvent(Event):
    pass


class CreateSquareCommand(Command):
    pass


@messagebus.listen(CreateSquareCommand)
class Test:
    async def handle(self, command, message_client=message_client):
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        async with sessionmaker() as session:
            square = Shape(id=str(uuid4()), type="square", volume=4)
            session.add(square)
            event = SquareCreatedEvent()
            await message_client.stage(event, session)
            await session.commit()
            await message_client.publish(event)


@messagebus.listen(SquareCreatedEvent)
class MyEventListener:
    async def handle(self, event, message_client=message_client):
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        async with sessionmaker() as session:
            if await message_client.is_duplicate(event, session):
                LOGGER.info(f"Event already processed: {event.id}")
                return None
            LOGGER.info(f"Event received: {event.id}")
            message_client.ack(event)
            return None
