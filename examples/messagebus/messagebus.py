from wibbley.event_driven import Command, Event, Messagebus

messagebus = Messagebus()


class MyEvent(Event):
    pass


class MyCommand(Command):
    pass


@messagebus.listen(MyCommand)
async def my_command_listener(command):
    return MyEvent()


@messagebus.listen(MyEvent)
class MyEventListener:
    async def handle(self, event):
        print(f"Event received: {event}")
        return None


@messagebus.listen(MyEvent)
class MyEventListener:
    async def handle(self, event):
        print(f"Event received: {event}")
        return None
