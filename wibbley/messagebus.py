import asyncio
import logging

from wibbley.messages import Command, Event, Query

LOGGER = logging.getLogger(__name__)


class Messagebus:
    def __init__(
        self,
        event_handlers: dict,
        command_handlers: dict,
        query_handlers: dict,
        is_unplugged: bool = False,
    ):
        self.event_handlers = event_handlers
        self.command_handlers = command_handlers
        self.query_handlers = query_handlers
        self.is_unplugged = is_unplugged
        if not is_unplugged:
            self.queue = asyncio.Queue()

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
