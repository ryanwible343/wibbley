import orjson
import pytest

from wibbley.api.http_handler.request_handlers.response_sender import ResponseSender


class FakeSend:
    def __init__(self):
        self.calls = []

    async def __call__(self, message):
        self.calls.append(message)


@pytest.mark.asyncio
async def test__response_sender_send_response_start__calls_send_with_correct_message():
    # ARRANGE
    send = FakeSend()
    response_sender = ResponseSender(orjson)
    headers = [(b"content-type", b"application/json")]
    status_code = 200

    # ACT
    await response_sender.send_response_start(send, headers, status_code)

    # ASSERT
    assert send.calls[0] == {
        "type": "http.response.start",
        "status": status_code,
        "headers": headers,
    }


@pytest.mark.asyncio
async def test__response_sender_send_response_body__when_response_body_is_dict__calls_send_with_correct_message():
    # ARRANGE
    send = FakeSend()
    response_sender = ResponseSender(orjson)
    response_body = {"key": "value"}
    status_code = 200

    # ACT
    await response_sender.send_response_body(send, response_body, status_code)

    # ASSERT
    assert send.calls[0] == {
        "type": "http.response.body",
        "status": status_code,
        "body": orjson.dumps(response_body),
    }


@pytest.mark.asyncio
async def test__response_sender_send_response_body__when_response_body_is_list__calls_send_with_correct_message():
    # ARRANGE
    send = FakeSend()
    response_sender = ResponseSender(orjson)
    response_body = ["value"]
    status_code = 200

    # ACT
    await response_sender.send_response_body(send, response_body, status_code)

    # ASSERT
    assert send.calls[0] == {
        "type": "http.response.body",
        "status": status_code,
        "body": orjson.dumps(response_body),
    }


@pytest.mark.asyncio
async def test__response_sender_send_response_body__when_response_body_is_str__calls_send_with_correct_message():
    # ARRANGE
    send = FakeSend()
    response_sender = ResponseSender(orjson)
    response_body = "value"
    status_code = 200

    # ACT
    await response_sender.send_response_body(send, response_body, status_code)

    # ASSERT
    assert send.calls[0] == {
        "type": "http.response.body",
        "status": status_code,
        "body": response_body.encode("utf-8"),
    }


@pytest.mark.asyncio
async def test__response_sender_send_response__sends_response_start_and_response_body():
    # ARRANGE
    send = FakeSend()
    response_sender = ResponseSender(orjson)
    headers = [(b"content-type", b"application/json")]
    response_body = "value"
    status_code = 200

    # ACT
    await response_sender.send_response(send, headers, response_body, status_code)

    # ASSERT
    assert send.calls[0] == {
        "type": "http.response.start",
        "status": status_code,
        "headers": headers,
    }
    assert send.calls[1] == {
        "type": "http.response.body",
        "status": status_code,
        "body": response_body.encode("utf-8"),
    }
