from abc import ABC, abstractmethod
from typing import Coroutine

import orjson

from wibbley.messagebus import AbstractMessagebus
from wibbley.request_handlers.default_request_handler import DefaultRequestHandler
from wibbley.request_handlers.head_request_handler import HeadRequestHandler
from wibbley.request_handlers.options_request_handler import OptionsRequestHandler
from wibbley.request_handlers.response_sender import ResponseSender
from wibbley.router import Router


class HTTPRequest:
    def __init__(
        self,
        path: str,
        method: str,
        query_params: dict[str, str],
        headers: dict[str, str],
        body: bytes,
    ):
        self.method = method
        self.path = path
        self.query_params = query_params
        self.headers = headers
        self.body = body

    @property
    def body_as_dict(self):
        return orjson.loads(self.body)

    @property
    def body_as_str(self):
        return self.body.decode("utf-8")

    def to_dict(self):
        return {
            "path": self.path,
            "query_params": self.query_params,
            "headers": self.headers,
            "body": self.body,
        }


class HTTPRequestConstructor:
    async def construct(
        self,
        path: str,
        method: str,
        query_string: bytes,
        headers: list[tuple[bytes, bytes]],
        receive: Coroutine,
    ) -> HTTPRequest:
        query_params = self._format_query_params(query_string)
        headers = self._format_headers(headers)
        request_body = await self._read_body(receive)
        return HTTPRequest(
            path=path,
            method=method,
            query_params=query_params,
            headers=headers,
            body=request_body,
        )

    def _format_query_params(self, query_string):
        query_params = {}
        for query in query_string.split(b"&"):
            key, value = query.split(b"=")
            query_params[key.decode("utf-8")] = value.decode("utf-8")
        return query_params

    def _format_headers(self, headers):
        formatted_headers = {}
        for header in headers:
            key, value = header
            formatted_headers[key.decode("utf-8")] = value.decode("utf-8")
        return formatted_headers

    async def _read_body(self, receive):
        body = b""
        more_body = True
        while more_body:
            message = await receive()
            body += message.get("body", b"")
            more_body = message.get("more_body", False)
        return body


class EventHandlingSettings:
    def __init__(
        self,
        enabled: bool = False,
        handler: AbstractMessagebus = None,
    ):
        self.enabled = enabled
        self.handler = handler


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
        route_func = self.router.routes.get(path, {}).get(method, None)

        if route_func is None:
            await self.response_sender.send_response(
                send,
                status_code=404,
                headers=[
                    (b"content-type", b"application/json"),
                ],
                response_body={"detail": "Not Found"},
            )
            return

        if method == "OPTIONS":
            # TODO: OptionsRequestHandler should set Allow header of which methods on path are allowed
            await self.options_request_handler.handle(send)
            return

        http_request = await self.http_request_constructor.construct(
            path=path,
            method=method,
            query_string=query_string,
            headers=headers,
            receive=receive,
        )
        # TODO: handle exceptions from route_func
        result = await route_func(request=http_request)

        if method == "HEAD":
            await self.head_request_handler.handle(send, result)
            return

        if method in ["GET", "POST", "PUT", "PATCH", "DELETE"]:
            await self.default_request_handler.handle(send, result)

        if self.event_handling_settings.enabled:
            await self.event_handling_settings.handler.handle(result)
