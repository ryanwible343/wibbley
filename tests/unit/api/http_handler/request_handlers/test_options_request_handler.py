import pytest

from wibbley.api.http_handler.cors import CORSSettings
from wibbley.api.http_handler.request_handlers.options_request_handler import (
    OptionsRequestHandler,
)


class FakeResponseSender:
    def __init__(self):
        self.calls = []

    async def send_response(self, send, status_code, headers, response_body):
        self.calls.append(
            {
                "send": send,
                "status_code": status_code,
                "headers": headers,
                "response_body": response_body,
            }
        )


@pytest.mark.asyncio
async def test__options_request_handler_handle__when_cors_settings_is_none__calls_response_sender_send_response():
    # ARRANGE
    response_sender = FakeResponseSender()
    options_request_handler = OptionsRequestHandler(None, response_sender)
    available_methods = ["GET", "POST"]

    # ACT
    await options_request_handler.handle(response_sender, available_methods)

    # ASSERT
    assert response_sender.calls[0] == {
        "send": response_sender,
        "status_code": 200,
        "headers": [
            (b"content-type", b"application/json"),
            (b"Allow", b"GET, POST"),
        ],
        "response_body": b"",
    }


@pytest.mark.asyncio
async def test__options_request_handler_handle__when_cors_settings_are_set__calls_response_sender_send_response():
    # ARRANGE
    response_sender = FakeResponseSender()
    cors_settings = CORSSettings(
        allow_origins=["*"],
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    options_request_handler = OptionsRequestHandler(cors_settings, response_sender)
    available_methods = ["GET", "POST"]

    # ACT
    await options_request_handler.handle(response_sender, available_methods)
    print("response_sender_calls", response_sender.calls)

    # ASSERT
    assert response_sender.calls[0] == {
        "send": response_sender,
        "status_code": 200,
        "headers": [
            (b"content-type", b"application/json"),
            (b"Allow", b"GET, POST"),
            (b"Access-Control-Allow-Origin", b"*"),
            (b"Access-Control-Allow-Methods", b"GET, POST"),
            (b"Access-Control-Allow-Headers", b"*"),
        ],
        "response_body": b"",
    }
