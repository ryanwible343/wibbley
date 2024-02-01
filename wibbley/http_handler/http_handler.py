import logging

from wibbley.http_handler.event_handling import EventHandlingSettings
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
    ):
        self.router = router
        self.response_sender = response_sender
        self.options_request_handler = options_request_handler
        self.head_request_handler = head_request_handler
        self.http_request_constructor = http_request_constructor
        self.default_request_handler = default_request_handler
        self.event_handling_settings = event_handling_settings

    async def handle(self, scope, receive, send):
        path = scope["path"]
        method = scope["method"]
        headers = scope["headers"]
        query_string = scope["query_string"]
        print("query_string", query_string)
        route_func = self.router.routes.get(path, {}).get(method, None)
        available_methods = self.router.routes.get(path, {}).keys()

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

        http_request = await self.http_request_constructor.construct(
            path=path,
            method=method,
            query_string=query_string,
            headers=headers,
            receive=receive,
        )
        try:
            result = await route_func(request=http_request)
        except Exception as e:
            LOGGER.exception(e, exc_info=True)
            await self.response_sender.send_response(
                send,
                status_code=500,
                headers=[
                    (b"content-type", b"application/json"),
                ],
                response_body={"detail": "Internal Server Error"},
            )
            return

        if method == "HEAD":
            await self.head_request_handler.handle(send, result)
            return

        if method in ["GET", "POST", "PUT", "PATCH", "DELETE"]:
            await self.default_request_handler.handle(send, result)

        if self.event_handling_settings.enabled:
            await self.event_handling_settings.handler.handle(result)
