from wibbley.messages import Command, Event, Query


class Listener:
    def __init__(self):
        self.event_handlers = {}
        self.command_handlers = {}
        self.query_handlers = {}

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
