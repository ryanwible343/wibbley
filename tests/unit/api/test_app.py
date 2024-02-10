import pytest

from wibbley.api.app import App


class FakeOptionsRequestHandler:
    def __init__(self, *args, **kwargs):
        self.cors_settings = None


class FakeRouter:
    def __init__(self, *args, **kwargs):
        self.is_get_called = False
        self.is_post_called = False
        self.is_put_called = False
        self.is_delete_called = False
        self.is_patch_called = False

    def get(self, path):
        self.is_get_called = True

    def post(self, path):
        self.is_post_called = True

    def put(self, path):
        self.is_put_called = True

    def delete(self, path):
        self.is_delete_called = True

    def patch(self, path):
        self.is_patch_called = True


class FakeHTTPHandler:
    def __init__(self, *args, **kwargs):
        self.router = FakeRouter()
        self.is_handle_called = False
        self.options_request_handler = FakeOptionsRequestHandler()

    async def handle(self, scope, receive, send):
        self.is_handle_called = True


def test__app_get__calls_http_handler_router_get():
    # ARRANGE
    app = App(FakeHTTPHandler())

    # ACT
    app.get("/path")

    # ASSERT
    assert app.http_handler.router.is_get_called


def test__app_post__calls_http_handler_router_post():
    # ARRANGE
    app = App(FakeHTTPHandler())

    # ACT
    app.post("/path")

    # ASSERT
    assert app.http_handler.router.is_post_called


def test__app_put__calls_http_handler_router_put():
    # ARRANGE
    app = App(FakeHTTPHandler())

    # ACT
    app.put("/path")

    # ASSERT
    assert app.http_handler.router.is_put_called


def test__app_delete__calls_http_handler_router_delete():
    # ARRANGE
    app = App(FakeHTTPHandler())

    # ACT
    app.delete("/path")

    # ASSERT
    assert app.http_handler.router.is_delete_called


def test__app_patch__calls_http_handler_router_patch():
    # ARRANGE
    app = App(FakeHTTPHandler())

    # ACT
    app.patch("/path")

    # ASSERT
    assert app.http_handler.router.is_patch_called


@pytest.mark.asyncio
async def test__app_call__when_scope_is_http__calls_http_handler():
    # ARRANGE
    app = App(FakeHTTPHandler())
    scope = {"type": "http"}

    # ACT
    await app(scope, None, None)

    # ASSERT
    assert app.http_handler.is_handle_called is True


@pytest.mark.asyncio
async def test__app_call__when_scope_is_not_http__raises_exception():
    # ARRANGE
    app = App(FakeHTTPHandler())
    scope = {"type": "websocket"}

    # ACT
    with pytest.raises(Exception):
        await app(scope, None, None)


def test__app_add_router__sets_http_handler_router():
    # ARRANGE
    app = App(FakeHTTPHandler())
    router = FakeRouter()

    # ACT
    app.add_router(router)

    # ASSERT
    assert app.http_handler.router == router


def test__enable_cors__sets_http_handler_options_request_handler_cors_settings():
    # ARRANGE
    app = App(FakeHTTPHandler())
    cors_settings = "CORS_SETTINGS"

    # ACT
    app.enable_cors(cors_settings)

    # ASSERT
    assert app.http_handler.options_request_handler.cors_settings == cors_settings


def test__enable_event_handling__sets_http_handler_event_handling_settings():
    # ARRANGE
    app = App(FakeHTTPHandler())
    event_handling_settings = "EVENT_HANDLING_SETTINGS"

    # ACT
    app.enable_event_handling(event_handling_settings)

    # ASSERT
    assert app.http_handler.event_handling_settings == event_handling_settings
