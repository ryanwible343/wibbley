from typing import Coroutine

import orjson


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
            if not query:
                continue
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
