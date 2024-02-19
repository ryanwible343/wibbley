from abc import ABC, abstractmethod
from typing import Coroutine, List, Tuple, Union


class JSONSerializer(ABC):
    @abstractmethod
    def dumps(self, obj) -> bytes:
        """Serialize an object to a json byte string."""


class ResponseSender:
    def __init__(self, json_serializer: JSONSerializer):
        self.json_serializer = json_serializer

    async def send_response_start(
        self, send: Coroutine, headers: List[Tuple[bytes, bytes]], status_code: int
    ):
        await send(
            {"type": "http.response.start", "status": status_code, "headers": headers}
        )

    async def send_response_body(
        self,
        send: Coroutine,
        response_body: Union[bytes, str, dict, list],
        status_code: int,
    ):
        if isinstance(response_body, dict) or isinstance(response_body, list):
            response_body = self.json_serializer.dumps(response_body)
        elif isinstance(response_body, str):
            response_body = response_body.encode("utf-8")

        await send(
            {
                "type": "http.response.body",
                "status": status_code,
                "body": response_body,
            }
        )

    async def send_response(self, send, headers, response_body, status_code):
        await self.send_response_start(send, headers, status_code)
        await self.send_response_body(send, response_body, status_code)
