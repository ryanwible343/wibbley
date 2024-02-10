import pytest

from wibbley.api.http_handler.request_handlers.default_request_handler import (
    DefaultRequestHandler,
    HTTPResponse,
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
async def test__default_request_handler_handle__when_route_func_result_is_http_response__calls_response_sender_send_response():
    # ARRANGE
    response_sender = FakeResponseSender()
    default_request_handler = DefaultRequestHandler(response_sender)
    result = HTTPResponse(
        status_code=200,
        headers=[(b"key", b"value")],
        body=b'{"key": "value"}',
    )

    # ACT
    await default_request_handler.handle(response_sender, result)

    # ASSERT
    assert response_sender.calls[0] == {
        "send": response_sender,
        "status_code": 200,
        "headers": [(b"key", b"value")],
        "response_body": b'{"key": "value"}',
    }


@pytest.mark.asyncio
async def test__default_request_handler_handle__when_route_func_result_is_not_http_response__calls_response_sender_send_response():
    # ARRANGE
    response_sender = FakeResponseSender()
    default_request_handler = DefaultRequestHandler(response_sender)
    result = "test"

    # ACT
    await default_request_handler.handle(response_sender, result)

    # ASSERT
    assert response_sender.calls[0] == {
        "send": response_sender,
        "status_code": 200,
        "headers": [(b"content-type", b"text/plain")],
        "response_body": "test",
    }


def test__default_request_handler_determine_content_type__when_result_is_str__returns_text_plain():
    # ARRANGE
    default_request_handler = DefaultRequestHandler(None)
    result = "test"

    # ACT
    content_type = default_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"text/plain"


def test__default_request_handler_determine_content_type__when_result_is_dict__returns_application_json():
    # ARRANGE
    default_request_handler = DefaultRequestHandler(None)
    result = {"key": "value"}

    # ACT
    content_type = default_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"application/json"


def test__default_request_handler_determine_content_type__when_result_is_bytes__returns_application_octet_stream():
    # ARRANGE
    default_request_handler = DefaultRequestHandler(None)
    result = b"test"

    # ACT
    content_type = default_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"application/octet-stream"


def test__default_request_handler_determine_content_type__when_result_is_list__returns_application_json():
    # ARRANGE
    default_request_handler = DefaultRequestHandler(None)
    result = ["test"]

    # ACT
    content_type = default_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"application/json"
