import logging
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker
from src.database import engine
from src.model import Shape

from wibbley.event_driven import Command, Event, MessageBroker, Messagebus

messagebus = Messagebus()
message_broker = MessageBroker("sqlalchemy+asyncpg", engine)


LOGGER = logging.getLogger(__name__)


class SquareCreatedEvent(Event):
    pass


class CreateSquareCommand(Command):
    pass


@messagebus.listen(CreateSquareCommand)
class Test:
    async def handle(self, command, message_broker=message_broker):
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        async with sessionmaker() as session:
            square = Shape(id=str(uuid4()), type="square", volume=4)
            session.add(square)
            event = SquareCreatedEvent()
            await message_broker.stage(event, session)
            await session.commit()
            await message_broker.publish(event)


@messagebus.listen(SquareCreatedEvent)
class MyEventListener:
    async def handle(self, event, message_broker=message_broker):
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        async with sessionmaker() as session:
            if await message_broker.is_duplicate(event, session):
                LOGGER.info(f"Event already processed: {event.id}")
                return None
            LOGGER.info(f"Event received: {event.id}")
            message_broker.ack(event)
            return None
