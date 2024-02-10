import pytest

from wibbley.api.http_handler.handler import HTTPHandler
from wibbley.api.http_handler.route_extractor import RouteExtractor


class FakeRouter:
    def __init__(self, routes=None):
        self.routes = routes or {}

    def get(self, path):
        pass

    def post(self, path):
        pass

    def put(self, path):
        pass

    def delete(self, path):
        pass

    def patch(self, path):
        pass


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


class FakeOptionsRequestHandler:
    def __init__(self):
        self.calls = []

    async def handle(self, send, available_method):
        self.calls.append({"send": send, "available_method": available_method})


class FakeHTTPRequestConstructor:
    def __init__(self):
        pass

    async def construct(self, path, method, headers, query_string, receive):
        return "some_request"


class FakeHeadRequestHandler:
    def __init__(self, response_sender):
        self.calls = []

    async def handle(self, send, result):
        self.calls.append({"send": send, "result": result})


class FakeDefaultRequestHandler:
    def __init__(self, response_sender):
        self.calls = []

    async def handle(self, send, result):
        self.calls.append({"send": send, "result": result})


class FakeEventHandlingSettings:
    pass


async def fake_send():
    pass


class FakeRouteFuncFactory:
    def __init__(self):
        self.calls = []

    async def route_func(self, request):
        self.calls.append({"request": request})
        return "test"


@pytest.mark.asyncio
async def test__http_handler_handle__when_no_available_methods__sends_404_response():
    # ARRANGE
    http_handler = HTTPHandler(
        router=FakeRouter(),
        response_sender=FakeResponseSender(),
        options_request_handler=FakeOptionsRequestHandler(),
        http_request_constructor=FakeHTTPRequestConstructor(),
        head_request_handler=FakeHeadRequestHandler(FakeResponseSender()),
        default_request_handler=FakeDefaultRequestHandler(FakeResponseSender()),
        event_handling_settings=FakeEventHandlingSettings(),
        route_extractor=RouteExtractor(),
    )
    scope = {
        "path": "/path",
        "method": "GET",
        "headers": {},
        "query_string": b"",
    }

    # ACT
    await http_handler.handle(scope, None, fake_send)

    # ASSERT
    assert len(http_handler.response_sender.calls) == 1
    assert http_handler.response_sender.calls[0]["status_code"] == 404


@pytest.mark.asyncio
async def test__http_handler_handle__when_route_func_is_none__sends_405_response():
    # ARRANGE
    http_handler = HTTPHandler(
        router=FakeRouter(routes={"/path": {"POST": "some_func"}}),
        response_sender=FakeResponseSender(),
        options_request_handler=FakeOptionsRequestHandler(),
        http_request_constructor=FakeHTTPRequestConstructor(),
        head_request_handler=FakeHeadRequestHandler(FakeResponseSender()),
        default_request_handler=FakeDefaultRequestHandler(FakeResponseSender()),
        event_handling_settings=FakeEventHandlingSettings(),
        route_extractor=RouteExtractor(),
    )
    scope = {
        "path": "/path",
        "method": "GET",
        "headers": {},
        "query_string": b"",
    }

    # ACT
    await http_handler.handle(scope, None, fake_send)

    # ASSERT
    assert len(http_handler.response_sender.calls) == 1
    assert http_handler.response_sender.calls[0]["status_code"] == 405


@pytest.mark.asyncio
async def test__http_handler_handle__when_method_is_OPTIONS__calls_options_request_handler_handle():
    # ARRANGE
    options_request_handler = FakeOptionsRequestHandler()
    http_handler = HTTPHandler(
        router=FakeRouter(routes={"/path": {"GET": "some_func"}}),
        response_sender=FakeResponseSender(),
        options_request_handler=options_request_handler,
        http_request_constructor=FakeHTTPRequestConstructor(),
        head_request_handler=FakeHeadRequestHandler(FakeResponseSender()),
        default_request_handler=FakeDefaultRequestHandler(FakeResponseSender()),
        event_handling_settings=FakeEventHandlingSettings(),
        route_extractor=RouteExtractor(),
    )
    scope = {
        "path": "/path",
        "method": "OPTIONS",
        "headers": {},
        "query_string": b"",
    }

    # ACT
    await http_handler.handle(scope, None, fake_send)

    # ASSERT
    assert list(options_request_handler.calls[0]["available_method"]) == ["GET"]


@pytest.mark.asyncio
async def test__http_handler_handle__when_method_is_default__executes_route_func():
    # ARRANGE
    route_func_factory = FakeRouteFuncFactory()
    http_handler = HTTPHandler(
        router=FakeRouter(routes={"/path": {"GET": route_func_factory.route_func}}),
        response_sender=FakeResponseSender(),
        options_request_handler=FakeOptionsRequestHandler(),
        http_request_constructor=FakeHTTPRequestConstructor(),
        head_request_handler=FakeHeadRequestHandler(FakeResponseSender()),
        default_request_handler=FakeDefaultRequestHandler(FakeResponseSender()),
        event_handling_settings=FakeEventHandlingSettings(),
        route_extractor=RouteExtractor(),
    )
    scope = {
        "path": "/path",
        "method": "GET",
        "headers": {},
        "query_string": b"",
    }

    # ACT
    await http_handler.handle(scope, None, fake_send)

    # ASSERT
    assert route_func_factory.calls[0]["request"] == "some_request"


@pytest.mark.asyncio
async def test__http_handler_handle__when_method_is_head_with_no_get__sends_405_response():
    # ARRANGE
    http_handler = HTTPHandler(
        router=FakeRouter(routes={"/path": {"POST": "some_func"}}),
        response_sender=FakeResponseSender(),
        options_request_handler=FakeOptionsRequestHandler(),
        http_request_constructor=FakeHTTPRequestConstructor(),
        head_request_handler=FakeHeadRequestHandler(FakeResponseSender()),
        default_request_handler=FakeDefaultRequestHandler(FakeResponseSender()),
        event_handling_settings=FakeEventHandlingSettings(),
        route_extractor=RouteExtractor(),
    )
    scope = {
        "path": "/path",
        "method": "HEAD",
        "headers": {},
        "query_string": b"",
    }

    # ACT
    await http_handler.handle(scope, None, fake_send)

    # ASSERT
    assert len(http_handler.response_sender.calls) == 1
    assert http_handler.response_sender.calls[0]["status_code"] == 405


@pytest.mark.asyncio
async def test__http_handler_handle__when_method_is_head__calls_head_request_handler_handle():
    # ARRANGE
    route_func_factory = FakeRouteFuncFactory()
    head_request_handler = FakeHeadRequestHandler(FakeResponseSender())
    http_handler = HTTPHandler(
        router=FakeRouter(
            routes={
                "/path": {
                    "GET": route_func_factory.route_func,
                    "HEAD": route_func_factory.route_func,
                }
            }
        ),
        response_sender=FakeResponseSender(),
        options_request_handler=FakeOptionsRequestHandler(),
        http_request_constructor=FakeHTTPRequestConstructor(),
        head_request_handler=head_request_handler,
        default_request_handler=FakeDefaultRequestHandler(FakeResponseSender()),
        event_handling_settings=FakeEventHandlingSettings(),
        route_extractor=RouteExtractor(),
    )
    scope = {
        "path": "/path",
        "method": "HEAD",
        "headers": {},
        "query_string": b"",
    }

    # ACT
    await http_handler.handle(scope, None, fake_send)

    # ASSERT
    assert head_request_handler.calls[0]["result"] == "test"


@pytest.mark.asyncio
async def test__http_handler_handle__when_method_is_default__calls_default_request_handler_handle():
    # ARRANGE
    route_func_factory = FakeRouteFuncFactory()
    default_request_handler = FakeDefaultRequestHandler(FakeResponseSender())
    http_handler = HTTPHandler(
        router=FakeRouter(
            routes={
                "/path": {
                    "GET": route_func_factory.route_func,
                }
            }
        ),
        response_sender=FakeResponseSender(),
        options_request_handler=FakeOptionsRequestHandler(),
        http_request_constructor=FakeHTTPRequestConstructor(),
        head_request_handler=FakeHeadRequestHandler(FakeResponseSender()),
        default_request_handler=default_request_handler,
        event_handling_settings=FakeEventHandlingSettings(),
        route_extractor=RouteExtractor(),
    )
    scope = {
        "path": "/path",
        "method": "GET",
        "headers": {},
        "query_string": b"",
    }

    # ACT
    await http_handler.handle(scope, None, fake_send)

    # ASSERT
    assert default_request_handler.calls[0]["result"] == "test"
