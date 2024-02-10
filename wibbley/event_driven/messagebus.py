import logging
from abc import ABC, abstractmethod

from wibbley.event_driven.messages import Command, Event, Query
from wibbley.event_driven.queue import wibbley_queue

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
        self.queue = wibbley_queue

    def is_function(self, obj):
        if callable(obj):
            if hasattr(obj, "__class__") and hasattr(obj, "__call__"):
                return not isinstance(obj, type)
        return False

    def is_class(self, obj):
        if callable(obj):
            if hasattr(obj, "__class__") and hasattr(obj, "__call__"):
                return isinstance(obj, type)
        return False

    def listen(self, message):
        def function_decorator(func):
            if issubclass(message, Event):
                if self.event_handlers.get(message):
                    self.event_handlers[message].append(func)
                else:
                    self.event_handlers[message] = [func]
            elif issubclass(message, Command):
                if self.command_handlers.get(message):
                    raise ValueError(
                        f"Command handler already registered for {message}"
                    )
                self.command_handlers[message] = func
            elif issubclass(message, Query):
                if self.query_handlers.get(message):
                    raise ValueError(f"Query handler already registered for {message}")
                self.query_handlers[message] = func
            else:
                raise ValueError(f"Unknown message type: {type(message)}")
            return func

        def class_decorator(cls):
            if issubclass(message, Event):
                if self.event_handlers.get(message):
                    self.event_handlers[message].append(cls().handle)
                else:
                    self.event_handlers[message] = [cls().handle]
            elif issubclass(message, Command):
                if self.command_handlers.get(message):
                    raise ValueError(
                        f"Command handler already registered for {message}"
                    )
                self.command_handlers[message] = cls().handle
            elif issubclass(message, Query):
                if self.query_handlers.get(message):
                    raise ValueError(f"Query handler already registered for {message}")
                self.query_handlers[message] = cls().handle
            else:
                raise ValueError(f"Unknown message type: {type(message)}")
            return cls

        def decorator(decorated):
            if self.is_function(decorated):
                return function_decorator(decorated)
            elif self.is_class(decorated):
                return class_decorator(decorated)
            else:
                raise ValueError(f"Unknown type decorated: {type(decorated)}")

        return decorator

    async def handle(self, message):
        if isinstance(message, Command):
            if not self.command_handlers.get(type(message)):
                raise ValueError(f"No command handler registered for {type(message)}")
            command_handler = self.command_handlers[type(message)]
            await command_handler(message)
            return True
        elif isinstance(message, Event):
            if not self.event_handlers.get(type(message)):
                raise ValueError(f"No event handler registered for {type(message)}")
            for event_handler in self.event_handlers[type(message)]:
                await event_handler(message)
            return True
        elif isinstance(message, Query):
            if not self.query_handlers.get(type(message)):
                raise ValueError(f"No query handler registered for {type(message)}")
            query_handler = self.query_handlers[type(message)]
            result = await query_handler(message)
            return result
        else:
            raise ValueError(f"Unknown message type: {type(message)}")

    async def handle_queue(self):
        message = self.queue.get_nowait()
        await self.handle(message)
        self.queue.task_done()


async def send(message, queue=wibbley_queue):
    await queue.put(message)
