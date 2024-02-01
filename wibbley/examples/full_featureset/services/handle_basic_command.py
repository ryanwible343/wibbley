import logging

from wibbley.messagebus.listen import Listener
from wibbley.messages import Command, Event, Query

LOGGER = logging.getLogger(__name__)

listener = Listener()


@listener.listen(Command)
async def handle_command(command):
    LOGGER.info(f"Handling command: {command}")
    return Event()


@listener.listen(Event)
async def handle_event(event):
    LOGGER.info(f"Handling event: {event}")
    return None


@listener.listen(Query)
async def handle_query(query):
    LOGGER.info(f"Handling query: {query}")
    return {"message": "Hello World!"}
