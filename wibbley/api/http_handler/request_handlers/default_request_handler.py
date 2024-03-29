from typing import Union

from wibbley.api.http_handler.request_handlers.response_sender import ResponseSender
from wibbley.api.http_handler.response import HTTPResponse


class DefaultRequestHandler:
    def __init__(
        self,
        response_sender: ResponseSender,
    ):
        self.response_sender = response_sender

    def _determine_content_type_header(self, result):
        response_types = {
            str: b"text/plain",
            bytes: b"application/octet-stream",
            dict: b"application/json",
            list: b"application/json",
        }
        response_header = response_types.get(type(result))
        return response_header

    async def handle(
        self, send, route_func_result: Union[str, bytes, dict, list, HTTPResponse]
    ):
        if isinstance(route_func_result, HTTPResponse):
            return await self.response_sender.send_response(
                send,
                status_code=route_func_result.status_code,
                headers=route_func_result.headers,
                response_body=route_func_result.body,
            )

        content_type_header = self._determine_content_type_header(route_func_result)
        return await self.response_sender.send_response(
            send,
            status_code=200,
            headers=[
                (b"content-type", content_type_header),
            ],
            response_body=route_func_result,
        )
