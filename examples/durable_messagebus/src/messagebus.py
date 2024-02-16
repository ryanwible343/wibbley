import logging
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker
from src.database import engine, sync_engine
from src.model import Shape

from wibbley.event_driven import (
    Command,
    Event,
    Messagebus,
    ack,
    is_duplicate,
    publish,
    stage,
)

messagebus = Messagebus()
messagebus.add_durability(
    adapter="sqlalchemy+asyncpg",
    connection_factory=engine,
)


LOGGER = logging.getLogger(__name__)


class SquareCreatedEvent(Event):
    pass


class CreateSquareCommand(Command):
    pass


@messagebus.listen(CreateSquareCommand)
async def my_command_listener(command):
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    async with sessionmaker() as session:
        square = Shape(id=str(uuid4()), type="square", volume=4)
        session.add(square)
        event = SquareCreatedEvent()
        await stage(event, session)
        await session.commit()
        await publish(event, session)


@messagebus.listen(SquareCreatedEvent)
class MyEventListener:
    async def handle(self, event):
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        async with sessionmaker() as session:
            if await is_duplicate(event, session):
                LOGGER.info(f"Event already processed: {event.id}")
                return None
            LOGGER.info(f"Event received: {event.id}")
            ack(event)
            return None
