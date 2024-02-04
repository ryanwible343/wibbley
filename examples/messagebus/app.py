from examples.messagebus.messagebus import MyCommand, messagebus
from wibbley.api import App, EventHandlingSettings

event_handling_settings = EventHandlingSettings(enabled=True, handler=messagebus)


app = App()
app.enable_event_handling(event_handling_settings)


@app.post("/message")
async def message_handler(request):
    await messagebus.handle(MyCommand())
    return {"message": "Message received"}
