from wibbley.http_handler.http_handler import HTTPHandler
from wibbley.router import Router


class App:
    def __init__(
        self,
        http_handler: HTTPHandler,
    ):
        self.http_handler = http_handler

    # TODO: add get call to http_handler for another layer of passthrough
    def get(self, path: str):
        return self.http_handler.router.get(path)

    # TODO: add post call to http_handler for another layer of passthrough
    def post(self, path):
        return self.http_handler.router.post(path)

    # TODO: add put call to http_handler for another layer of passthrough
    def put(self, path):
        return self.http_handler.router.put(path)

    # TODO: add delete call to http_handler for another layer of passthrough
    def delete(self, path):
        return self.http_handler.router.delete(path)

    # TODO: add patch call to http_handler for another layer of passthrough
    def patch(self, path):
        return self.http_handler.router.patch(path)

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"

        if scope["type"] == "http":
            await self.http_handler.handle(scope, receive, send)
