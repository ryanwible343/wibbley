import logging

from wibbley.listen import Listener
from wibbley.messagebus import Command
from wibbley.messages import Event

LOGGER = logging.getLogger(__name__)

listener = Listener()


@listener.listen(Command)
async def handle_command(command):
    LOGGER.info(f"Handling command: {command}")
    return Event()
