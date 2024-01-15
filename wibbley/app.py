import json
import logging
from asyncio import Queue
from typing import Callable, Coroutine
from urllib.parse import quote, unquote

import orjson

from wibbley.listen import Listener
from wibbley.messagebus import Messagebus
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


class Request:
    def __init__(
        self,
        path: str,
        query_params: dict[str, str],
        headers: dict[str, str],
        body: bytes,
    ):
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


class App:
    def __init__(
        self,
        router: Router,
        cors_settings: CORSSettings,
        listeners: list[Listener | None] = [],
    ):
        self.router = router
        self.cors_settings = cors_settings
        self.listeners = listeners

        self.event_handlers = {}
        self.command_handlers = {}
        self.query_handlers = {}
        for listener in self.listeners:
            self.event_handlers.update(listener.event_handlers)
            self.command_handlers.update(listener.command_handlers)
            self.query_handlers.update(listener.query_handlers)

    def _determine_content_type_header(self, result):
        response_types = {
            str: b"text/plain",
            bytes: b"application/octet-stream",
            dict: b"application/json",
        }
        response_header = response_types.get(type(result))
        return response_header

    def _format_response(self, result):
        data_types_to_serializer = {
            str: lambda x: x.encode("utf-8"),
            bytes: lambda x: x,
            dict: lambda x: orjson.dumps(x),
        }

        serializer = data_types_to_serializer.get(type(result))
        return serializer(result)

    async def _read_body(self, receive):
        body = b""
        more_body = True

        while more_body:
            message = await receive()
            body += message.get("body", b"")
            more_body = message.get("more_body", False)

        return body

    def _format_query_params(self, query_params: bytes) -> dict[str, str]:
        if query_params == b"":
            return {}
        query_params = unquote(query_params.decode("utf-8"))
        query_params = query_params.split("&")
        query_params = [param.split("=") for param in query_params]
        query_params = {param[0]: param[1] for param in query_params}
        return query_params

    def _format_headers(self, headers: list[tuple[bytes, bytes]]) -> dict[str, str]:
        headers = {
            header[0].decode("utf-8"): header[1].decode("utf-8") for header in headers
        }
        return headers

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"

        path = scope["path"]
        method = scope["method"]
        route_func = self.router.routes.get(path, {}).get(method, None)

        if route_func is None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [
                        (b"content-type", b"application/json"),
                    ],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": orjson.dumps({"detail": "Not Found"}),
                }
            )
            return

        if method == "OPTIONS":
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (
                            b"Access-Control-Allow-Origin",
                            self.cors_settings.serialized_allow_origins,
                        ),
                        (
                            b"Access-Control-Allow-Methods",
                            self.cors_settings.serialized_allow_methods,
                        ),
                        (
                            b"Access-Control-Allow-Headers",
                            self.cors_settings.serialized_allow_headers,
                        ),
                    ],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b"",
                }
            )
            return

        query_params = self._format_query_params(scope["query_string"])
        headers = self._format_headers(scope["headers"])
        request_body = await self._read_body(receive)
        request = Request(
            path=path,
            query_params=query_params,
            headers=headers,
            body=request_body,
        )

        messagebus = Messagebus(
            event_handlers=self.event_handlers,
            command_handlers=self.command_handlers,
            query_handlers=self.query_handlers,
        )

        result = await route_func(request=request, messagebus=messagebus)

        content_type_header = self._determine_content_type_header(result)

        if method == "HEAD":
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", content_type_header),
                        (
                            b"content-length",
                            str(len(result)).encode("utf-8"),
                        ),
                    ],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b"",
                }
            )
            return

        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", content_type_header),
                ],
            }
        )

        formatted_response = self._format_response(result)

        await send(
            {
                "type": "http.response.body",
                "body": formatted_response,
            }
        )

        await messagebus.handle_queue()
