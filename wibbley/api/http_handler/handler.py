import logging
import re

from wibbley.api.http_handler.event_handling import EventHandlingSettings
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

LOGGER = logging.getLogger(__name__)


class HTTPHandler:
    def __init__(
        self,
        router: Router,
        response_sender: ResponseSender,
        options_request_handler: OptionsRequestHandler,
        http_request_constructor: HTTPRequestConstructor,
        head_request_handler: HeadRequestHandler,
        default_request_handler: DefaultRequestHandler,
        event_handling_settings: EventHandlingSettings,
        route_extractor: RouteExtractor,
    ):
        self.router = router
        self.response_sender = response_sender
        self.options_request_handler = options_request_handler
        self.head_request_handler = head_request_handler
        self.http_request_constructor = http_request_constructor
        self.default_request_handler = default_request_handler
        self.event_handling_settings = event_handling_settings
        self.route_extractor = route_extractor

    async def handle(self, scope, receive, send):
        path = scope["path"]
        method = scope["method"]
        headers = scope["headers"]
        query_string = scope["query_string"]
        route_info = self.route_extractor.extract(
            routes=self.router.routes, request_path=path, request_method=method
        )
        available_methods = route_info.available_methods
        route_func = route_info.route_func
        path_parameters = route_info.path_parameters

        if len(available_methods) == 0:
            return await self.response_sender.send_response(
                send,
                status_code=404,
                headers=[
                    (b"content-type", b"application/json"),
                ],
                response_body={"detail": "Not Found"},
            )

        if method == "OPTIONS":
            return await self.options_request_handler.handle(send, available_methods)

        if route_func is None:
            return await self.response_sender.send_response(
                send,
                status_code=405,
                headers=[
                    (b"content-type", b"application/json"),
                    (
                        b"allow",
                        b",".join(
                            available_method.encode("utf-8")
                            for available_method in available_methods
                        ),
                    ),
                ],
                response_body={"detail": "Method Not Allowed"},
            )

        if method == "HEAD" and "GET" not in available_methods:
            return await self.response_sender.send_response(
                send,
                status_code=405,
                headers=[
                    (b"content-type", b"application/json"),
                    (
                        b"allow",
                        b",".join(
                            available_method.encode("utf-8")
                            for available_method in available_methods
                        ),
                    ),
                ],
                response_body={"detail": "Method Not Allowed"},
            )

        http_request = await self.http_request_constructor.construct(
            path=path,
            method=method,
            path_params=path_parameters,
            query_string=query_string,
            headers=headers,
            receive=receive,
        )
        try:
            result = await route_func(request=http_request)
        except Exception as e:
            LOGGER.exception(e)
            return await self.response_sender.send_response(
                send,
                status_code=500,
                headers=[
                    (b"content-type", b"application/json"),
                ],
                response_body={"detail": "Internal Server Error"},
            )

        if method == "HEAD":
            await self.head_request_handler.handle(send, result)

        if method in ["GET", "POST", "PUT", "PATCH", "DELETE"]:
            await self.default_request_handler.handle(send, result)
