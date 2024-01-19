import logging

from wibbley.http_handler import HTTPHandler
from wibbley.messagebus import AbstractMessagebus
from wibbley.router import Router

LOGGER = logging.getLogger(__name__)


class CORSSettings:
    def __init__(
        self,
        allow_origins: list[str],
        allow_methods: list[str],
        allow_headers: list[str],
    ):
        self.allow_origins = allow_origins
        self.allow_methods = allow_methods
        self.allow_headers = allow_headers

    @property
    def serialized_allow_origins(self):
        return b",".join([origin.encode("utf-8") for origin in self.allow_origins])

    @property
    def serialized_allow_methods(self):
        return b",".join([method.encode("utf-8") for method in self.allow_methods])

    @property
    def serialized_allow_headers(self):
        return b",".join([header.encode("utf-8") for header in self.allow_headers])


class EventHandlingSettings:
    def __init__(
        self,
        enabled: bool = False,
        handler: AbstractMessagebus = None,
    ):
        self.enabled = enabled
        self.handler = handler


class App:
    def __init__(
        self,
        http_handler: HTTPHandler,
    ):
        self.routes = {}
        self.http_handler = http_handler

    def add_router(self, router: Router):
        self.http_handler.add_router(router)

    # TODO: add get call to http_handler for another layer of passthrough
    def get(self, path: str):
        return self.http_handler.router.get(path)

    # TODO: add post call to http_handler for another layer of passthrough
    def post(self, path):
        return self.http_handler.router.post(path)

    # TODO: add put call to http_handler for another layer of passthrough
    def put(self, path):
        return self.http_handler.router.put(path)

    # TODO: add delete call to http_handler for another layer of passthrough
    def delete(self, path):
        return self.http_handler.router.delete(path)

    # TODO: add patch call to http_handler for another layer of passthrough
    def patch(self, path):
        return self.http_handler.router.patch(path)

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"

        if scope["type"] == "http":
            await self.http_handler.handle(scope, receive)
