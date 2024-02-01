from wibbley.event_driven.messagebus import Messagebus
from wibbley.event_driven.messages import Command, Event

messagebus = Messagebus()


class MyEvent(Event):
    pass


class MyCommand(Command):
    pass


@messagebus.listen(MyEvent)
async def my_event_listener(event):
    print(f"Event received: {event}")
    return None


@messagebus.listen(MyCommand)
async def my_command_listener(command):
    return MyEvent()
