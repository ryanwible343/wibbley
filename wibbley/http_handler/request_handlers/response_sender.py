from abc import ABC, abstractmethod
from typing import Coroutine


class JSONSerializer(ABC):
    @abstractmethod
    def dumps(self, obj) -> bytes:
        raise NotImplementedError


class ResponseSender:
    def __init__(self, json_serializer: JSONSerializer):
        self.json_serializer = json_serializer

    async def send_response_start(
        self, send: Coroutine, headers: list[tuple[bytes, bytes]], status_code: int
    ):
        await send(
            {"type": "http.response.start", "status": status_code, "headers": headers}
        )

    async def send_response_body(
        self,
        send: Coroutine,
        response_body: bytes | str | dict | list,
        status_code: int,
    ):
        if type(response_body) is dict or type(response_body) is list:
            response_body = self.json_serializer.dumps(response_body)
        elif type(response_body) is str:
            response_body = response_body.encode("utf-8")

        await send(
            {
                "type": "http.response.body",
                "status": status_code,
                "body": response_body,
            }
        )

    async def send_response(self, send, headers, response_body):
        await self.send_response_start(send, headers)
        await self.send_response_body(send, response_body)
