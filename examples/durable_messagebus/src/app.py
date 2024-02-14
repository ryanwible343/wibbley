from src.messagebus import CreateSquareCommand, messagebus
from src.orm import map_orm_to_model

from wibbley.api import App

app = App()
map_orm_to_model()


@app.post("/message")
async def message_handler(request):
    await messagebus.handle(CreateSquareCommand())
    return {"message": "Message received"}
