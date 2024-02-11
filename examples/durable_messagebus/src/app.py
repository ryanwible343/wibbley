from src.messagebus import MyCommand, messagebus

from wibbley.api import App

app = App()


@app.post("/message")
async def message_handler(request):
    await messagebus.handle(MyCommand())
    return {"message": "Message received"}
