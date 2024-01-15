import logging

from wibbley.app import App, CORSSettings
from wibbley.examples.hello_world.services.handle_basic_command import listener
from wibbley.messages import Command
from wibbley.router import Router

router = Router()

logging.basicConfig(level=2, format="%(asctime)-15s %(levelname)-8s %(message)s")

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


cors_settings = CORSSettings(
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

listeners = [listener]

app = App(
    router,
    cors_settings,
    listeners,
)
