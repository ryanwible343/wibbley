import logging

from src.database import engine

from wibbley.event_driven import Command, Event, Messagebus, send

messagebus = Messagebus()
messagebus.enable_exactly_once_processing(
    db_name="postgres",
    connection_factory=engine,
    run_async=True,
)


LOGGER = logging.getLogger(__name__)


class MyEvent(Event):
    pass


class MyCommand(Command):
    pass


@messagebus.listen(MyCommand)
async def my_command_listener(command):
    LOGGER.info(f"Command received: {command}")
    await send(MyEvent())


@messagebus.listen(MyEvent)
class MyEventListener:
    async def handle(self, event):
        LOGGER.info(f"Event received: {event}")
        return None
