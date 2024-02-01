import logging

import orjson

from wibbley.app import App
from wibbley.http_handler.cors import CORSSettings
from wibbley.http_handler.event_handling import EventHandlingSettings
from wibbley.http_handler.http_handler import HTTPHandler
from wibbley.http_handler.http_request import HTTPRequestConstructor
from wibbley.http_handler.request_handlers.default_request_handler import (
    DefaultRequestHandler,
)
from wibbley.http_handler.request_handlers.head_request_handler import (
    HeadRequestHandler,
)
from wibbley.http_handler.request_handlers.options_request_handler import (
    OptionsRequestHandler,
)
from wibbley.http_handler.request_handlers.response_sender import ResponseSender
from wibbley.router import Router

logging.basicConfig(level=2, format="%(asctime)-15s %(levelname)-8s %(message)s")

router = Router()


@router.get("/hello")
async def hello(request):
    return {"hello": "world"}


cors_settings = CORSSettings(
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["*"],
)

http_handler = HTTPHandler(
    router=router,
    response_sender=ResponseSender(orjson),
    options_request_handler=OptionsRequestHandler(
        cors_settings=cors_settings, response_sender=ResponseSender(orjson)
    ),
    http_request_constructor=HTTPRequestConstructor(),
    head_request_handler=HeadRequestHandler(response_sender=ResponseSender(orjson)),
    default_request_handler=DefaultRequestHandler(
        response_sender=ResponseSender(orjson)
    ),
    event_handling_settings=EventHandlingSettings(),
)

app = App(
    http_handler=http_handler,
)
