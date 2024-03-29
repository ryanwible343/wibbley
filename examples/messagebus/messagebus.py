import asyncio

from wibbley.event_driven import Command, Event, Messagebus, send

queue = asyncio.Queue()
messagebus = Messagebus(queue=queue)


class MyEvent(Event):
    pass


class MyCommand(Command):
    pass


@messagebus.listen(MyCommand)
async def my_command_listener(command):
    print(f"Command received: {command}")
    await send(MyEvent())


@messagebus.listen(MyEvent)
class MyEventListener:
    async def handle(self, event):
        print(f"Event received: {event}")
        return None
