import pytest

from wibbley.api.http_handler.request_handlers.head_request_handler import (
    HeadRequestHandler,
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
async def test__head_request_handler_handle__calls_response_sender_send_response():
    # ARRANGE
    response_sender = FakeResponseSender()
    head_request_handler = HeadRequestHandler(response_sender)
    result = "test"

    # ACT
    await head_request_handler.handle(response_sender, result)

    # ASSERT
    assert response_sender.calls[0] == {
        "send": response_sender,
        "status_code": 200,
        "headers": [
            (b"content-type", b"text/plain"),
            (b"content-length", b"4"),
        ],
        "response_body": b"",
    }


def test__head_request_handler_determine_content_type__when_result_is_str__returns_text_plain():
    # ARRANGE
    head_request_handler = HeadRequestHandler(FakeResponseSender())
    result = "test"

    # ACT
    content_type = head_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"text/plain"


def test__head_request_handler_determine_content_type__when_result_is_dict__returns_application_json():
    # ARRANGE
    head_request_handler = HeadRequestHandler(FakeResponseSender())
    result = {"key": "value"}

    # ACT
    content_type = head_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"application/json"


def test__head_request_handler_determine_content_type__when_result_is_bytes__returns_application_octet_stream():
    # ARRANGE
    head_request_handler = HeadRequestHandler(FakeResponseSender())
    result = b"test"

    # ACT
    content_type = head_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"application/octet-stream"


def test__head_request_handler_determine_content_type__when_result_is_list__returns_application_json():
    # ARRANGE
    head_request_handler = HeadRequestHandler(FakeResponseSender())
    result = ["test"]

    # ACT
    content_type = head_request_handler._determine_content_type_header(result)

    # ASSERT
    assert content_type == b"application/json"
