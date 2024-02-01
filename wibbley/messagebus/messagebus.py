import asyncio
import logging
from abc import ABC, abstractmethod

from wibbley.messagebus.listen import Listener
from wibbley.messages import Command, Event, Query

LOGGER = logging.getLogger(__name__)


class AbstractMessagebus(ABC):
    @abstractmethod
    async def handle(self, message):
        raise NotImplementedError


class Messagebus(AbstractMessagebus):
    def __init__(
        self,
        listeners: list[Listener],
        is_unplugged: bool = False,
    ):
        self.is_unplugged = is_unplugged
        self.listeners = listeners
        if not is_unplugged:
            self.queue = asyncio.Queue()

        self.event_handlers = {}
        self.command_handlers = {}
        self.query_handlers = {}
        for listener in self.listeners:
            self.event_handlers.update(listener.event_handlers)
            self.command_handlers.update(listener.command_handlers)
            self.query_handlers.update(listener.query_handlers)

    def listen_to_event(self, event):
        def decorator(func):
            self.event_handlers[event] = func
            return func

        return decorator

    def listen_to_command(self, command):
        def decorator(func):
            self.command_handlers[command] = func
            return func

        return decorator

    def listen_to_query(self, query):
        def decorator(func):
            self.query_handlers[query] = func
            return func

        return decorator

    async def _handle_command(self, command):
        LOGGER.info(f"command handlers: {self.command_handlers}")
        command_handler = self.command_handlers[type(command)]
        result = await command_handler(command)
        return result

    async def _handle_event(self, event):
        pass

    async def _handle_query(self, query):
        pass

    async def handle(self, message):
        if isinstance(message, Command):
            event = await self._handle_command(message)
            if self.is_unplugged:
                return await self.handle(event)

            self.queue.put_nowait(event)
        elif isinstance(message, Event):
            event = await self._handle_event(message)
            if event:
                if self.is_unplugged:
                    return await self.handle(event)

                self.queue.put_nowait(event)
        elif isinstance(message, Query):
            return await self._handle_query(message)
        else:
            raise ValueError(f"Unknown message type: {type(message)}")

    async def handle_queue(self):
        tasks = []
        while self.queue.qsize() > 0:
            message = self.queue.get_nowait()
            task = asyncio.create_task(self.handle(message))
            tasks.append(task)
        await asyncio.gather(*tasks)
