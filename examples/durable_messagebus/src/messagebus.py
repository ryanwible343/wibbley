import logging
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker
from src.database import engine, sync_engine
from src.model import Shape

from wibbley.event_driven import Command, Event, Messagebus, ack, publish, stage

messagebus = Messagebus()
messagebus.enable_exactly_once_processing(
    db_name="postgres",
    connection_factory=sync_engine,
    run_async=False,
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
        LOGGER.info(f"Event received: {event.id}")
        ack(event)
        return None
