import pytest

from wibbley.api.http_handler.request import HTTPRequest, HTTPRequestConstructor


class FakeReceiveTwice:
    def __init__(self):
        self.calls = 0

    async def __call__(self):
        if self.calls == 0:
            self.calls += 1
            return {"body": b"test", "more_body": True}
        return {"body": b"test", "more_body": False}


class FakeReceiveOnce:
    async def __call__(self):
        return {"body": b"test", "more_body": False}


def test__http_request_body_as_dict__returns_dict():
    # ARRANGE
    request = HTTPRequest(
        path="/",
        method="GET",
        query_params={},
        headers={},
        body=b'{"key": "value"}',
    )

    # ACT/ASSERT
    assert request.body_as_dict == {"key": "value"}


def test__http_request_body_as_str__returns_str():
    # ARRANGE
    request = HTTPRequest(
        path="/",
        method="GET",
        query_params={},
        headers={},
        body=b'{"key": "value"}',
    )

    # ACT/ASSERT
    assert request.body_as_str == '{"key": "value"}'


def test__http_request_to_dict__returns_dict():
    # ARRANGE
    request = HTTPRequest(
        path="/",
        method="GET",
        query_params={"key": "value"},
        headers={"key": "value"},
        body=b'{"key": "value"}',
    )

    # ACT/ASSERT
    assert request.to_dict() == {
        "path": "/",
        "query_params": {"key": "value"},
        "headers": {"key": "value"},
        "body": b'{"key": "value"}',
    }


def test__http_request_constructor_format_query_params__when_multiple_query_params__returns_dict():
    # ARRANGE
    request_constructor = HTTPRequestConstructor()

    # ACT
    query_params = request_constructor._format_query_params(b"key=value&key2=value2")

    # ASSERT
    assert query_params == {"key": "value", "key2": "value2"}


def test__http_request_constructor_format_query_params__when_no_query_params__returns_empty_dict():
    # ARRANGE
    request_constructor = HTTPRequestConstructor()

    # ACT
    query_params = request_constructor._format_query_params(b"")

    # ASSERT
    assert query_params == {}


def test__http_request_constructor_format_query_params__when_one_query_param__returns_dict():
    # ARRANGE
    request_constructor = HTTPRequestConstructor()

    # ACT
    query_params = request_constructor._format_query_params(b"key=value")

    # ASSERT
    assert query_params == {"key": "value"}


def test__http_request_constructor_format_headers__returns_dict():
    # ARRANGE
    request_constructor = HTTPRequestConstructor()

    # ACT
    headers = request_constructor._format_headers([(b"key", b"value")])

    # ASSERT
    assert headers == {"key": "value"}


@pytest.mark.asyncio
async def test__http_request_constructor_read_body__when_one_receive_loop__returns_bytes():
    # ARRANGE
    fake_receive = FakeReceiveOnce()
    request_constructor = HTTPRequestConstructor()

    # ACT
    body = await request_constructor._read_body(fake_receive)

    # ASSERT
    assert body == b"test"


@pytest.mark.asyncio
async def test__http_request_constructor_read_body__when_two_receive_loops__returns_bytes():
    # ARRANGE
    fake_receive = FakeReceiveTwice()
    request_constructor = HTTPRequestConstructor()

    # ACT
    body = await request_constructor._read_body(fake_receive)

    # ASSERT
    assert body == b"testtest"


@pytest.mark.asyncio
async def test__http_request_constructor_construct__returns_http_request():
    # ARRANGE
    request_constructor = HTTPRequestConstructor()

    # ACT
    request = await request_constructor.construct(
        path="/",
        method="GET",
        query_string=b"",
        headers=[],
        receive=FakeReceiveOnce(),
    )

    # ASSERT
    assert request.path == "/"
    assert request.method == "GET"
    assert request.query_params == {}
    assert request.headers == {}
    assert request.body == b"test"
