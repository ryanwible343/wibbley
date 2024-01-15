import logging

from wibbley.messages import Command
from wibbley.router import Router

router = Router()

LOGGER = logging.getLogger(__name__)


@router.get("/")
async def hello_world(request):
    return "Hello World!"


@router.get("/json")
async def hello_world_json(request):
    return {"message": "Hello World!"}


@router.post("/json")
async def hello_world_json(messagebus):
    command = Command()
    result = await messagebus.handle(command)
    LOGGER.info(f"My result: {result}")
    return {"message": "Hello World!"}
