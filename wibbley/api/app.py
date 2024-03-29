import orjson

from wibbley.api.http_handler.cors import CORSSettings
from wibbley.api.http_handler.event_handling import EventHandlingSettings
from wibbley.api.http_handler.handler import HTTPHandler
from wibbley.api.http_handler.request import HTTPRequestConstructor
from wibbley.api.http_handler.request_handlers.default_request_handler import (
    DefaultRequestHandler,
)
from wibbley.api.http_handler.request_handlers.head_request_handler import (
    HeadRequestHandler,
)
from wibbley.api.http_handler.request_handlers.options_request_handler import (
    OptionsRequestHandler,
)
from wibbley.api.http_handler.request_handlers.response_sender import ResponseSender
from wibbley.api.http_handler.route_extractor import RouteExtractor
from wibbley.api.http_handler.router import Router


class App:
    def __init__(
        self,
        http_handler: HTTPHandler = HTTPHandler(
            router=Router(),
            response_sender=ResponseSender(orjson),
            options_request_handler=OptionsRequestHandler(
                cors_settings=None, response_sender=ResponseSender(orjson)
            ),
            http_request_constructor=HTTPRequestConstructor(),
            head_request_handler=HeadRequestHandler(
                response_sender=ResponseSender(orjson)
            ),
            default_request_handler=DefaultRequestHandler(ResponseSender(orjson)),
            event_handling_settings=EventHandlingSettings(),
            route_extractor=RouteExtractor(),
        ),
    ):
        self.http_handler = http_handler

    def add_router(self, router: Router):
        self.http_handler.router = router

    def enable_cors(self, cors_settings: CORSSettings):
        self.http_handler.options_request_handler.cors_settings = cors_settings

    def enable_event_handling(self, event_handling_settings: EventHandlingSettings):
        self.http_handler.event_handling_settings = event_handling_settings

    def get(self, path: str):
        return self.http_handler.router.get(path)

    def post(self, path):
        return self.http_handler.router.post(path)

    def put(self, path):
        return self.http_handler.router.put(path)

    def delete(self, path):
        return self.http_handler.router.delete(path)

    def patch(self, path):
        return self.http_handler.router.patch(path)

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"

        if scope["type"] == "http":
            await self.http_handler.handle(scope, receive, send)
