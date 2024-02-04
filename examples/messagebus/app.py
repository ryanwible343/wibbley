from wibbley import App, EventHandlingSettings
from wibbley.examples.messagebus.messagebus import MyCommand, messagebus

event_handling_settings = EventHandlingSettings(enabled=True, handler=messagebus)


app = App()
app.enable_event_handling(event_handling_settings)


@app.post("/message")
async def message_handler(request):
    await messagebus.handle(MyCommand())
    return {"message": "Message received"}
