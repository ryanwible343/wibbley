import asyncio
import logging
from abc import ABC, abstractmethod

from wibbley.event_driven.messages import Command, Event, Query

LOGGER = logging.getLogger(__name__)


class AbstractMessagebus(ABC):
    @abstractmethod
    async def handle(self, message):
        raise NotImplementedError


class Messagebus(AbstractMessagebus):
    def __init__(self):
        self.event_handlers = {}
        self.command_handlers = {}
        self.query_handlers = {}
        self.queue = asyncio.Queue()

    def listen(self, message):
        def decorator(func):
            if issubclass(message, Event):
                self.event_handlers[message] = func
            elif issubclass(message, Command):
                self.command_handlers[message] = func
            elif issubclass(message, Query):
                self.query_handlers[message] = func
            else:
                raise ValueError(f"Unknown message type: {type(message)}")
            return func

        return decorator

    async def handle(self, message):
        if isinstance(message, Command):
            command_handler = self.command_handlers[type(message)]
            event = await command_handler(message)
            if event is not None:
                await self.queue.put(event)
            return True
        elif isinstance(message, Event):
            event_handler = self.event_handlers[type(message)]
            event = await event_handler(message)
            if event is not None:
                await self.queue.put(event)
            return True
        elif isinstance(message, Query):
            query_handler = self.query_handlers[type(message)]
            result = await query_handler(message)
            return result
        else:
            raise ValueError(f"Unknown message type: {type(message)}")

    async def handle_queue(self):
        message = self.queue.get_nowait()
        await self.handle(message)
        self.queue.task_done()
