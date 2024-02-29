import logging
from functools import wraps
from typing import Literal, Union

from wibbley.event_driven.message_broker.queue import wibbley_queue
from wibbley.event_driven.message_client.message_client import (
    AsyncConnectionFactory,
    ConnectionFactory,
)
from wibbley.event_driven.messagebus.messages import Command, Event, Query
from wibbley.utilities.async_retry import AsyncRetry

LOGGER = logging.getLogger(__name__)


class Messagebus:
    def __init__(self):
        self.event_handlers = {}
        self.command_handlers = {}
        self.query_handlers = {}
        self.async_retry = AsyncRetry()
        self.is_durable = False

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

        return decorator

    async def execute_event_handler(self, handler, message):
        @self.async_retry
        @wraps(handler)
        async def inner_handler(message):
            await handler(message)

        await inner_handler(message)

    async def handle(self, message):
        if isinstance(message, Command):
            if not self.command_handlers.get(type(message)):
                LOGGER.error(f"No command handler registered for {type(message)}")
                return False
            command_handler = self.command_handlers[type(message)]
            try:
                await command_handler(message)
            except Exception as e:
                LOGGER.exception(
                    f"Command could not be handled by handler: {command_handler} for command: {message}"
                )
                raise e
            return True
        elif isinstance(message, Event):
            if not self.event_handlers.get(type(message)):
                LOGGER.error(f"No event handler registered for {type(message)}")
                return False
            for event_handler in self.event_handlers[type(message)]:
                try:
                    await self.execute_event_handler(event_handler, message)
                except Exception:
                    LOGGER.exception(
                        f"Event could not be handled by handler: {event_handler} for event: {message}"
                    )
                    continue
            return True
        elif isinstance(message, Query):
            if not self.query_handlers.get(type(message)):
                LOGGER.error(f"No query handler registered for {type(message)}")
                return None
            query_handler = self.query_handlers[type(message)]
            try:
                result = await query_handler(message)
            except Exception as e:
                LOGGER.exception(
                    f"Query could not be handled by handler: {query_handler} for query: {message}"
                )
                raise e
            return result
        else:
            LOGGER.error(f"Unknown message type: {type(message)}")
            return False


async def send(message, queue=wibbley_queue):
    await queue.put(message)
