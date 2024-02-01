class HTTPResponse:
    def __init__(
        self,
        status_code: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ):
        self.status_code = status_code
        self.headers = headers
        self.body = body

    def to_dict(self):
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.body,
        }
