from wibbley.http_handler.cors import CORSSettings
from wibbley.http_handler.request_handlers.response_sender import ResponseSender


class OptionsRequestHandler:
    def __init__(self, cors_settings: CORSSettings, response_sender: ResponseSender):
        self.cors_settings = cors_settings
        self.response_sender = response_sender

    async def handle(self, send, available_methods: list[str]):
        if self.cors_settings is None:
            await self.response_sender.send_response(
                send,
                headers=[
                    (b"content-type", b"application/json"),
                ],
                response_body=b"",
                status_code=200,
            )
        else:
            await self.response_sender.send_response(
                send,
                headers=[
                    (b"content-type", b"application/json"),
                    (
                        b"Allow",
                        ", ".join(available_methods).encode("utf-8"),
                    )(
                        b"Access-Control-Allow-Origin",
                        self.cors_settings.serialized_allow_origins,
                    ),
                    (
                        b"Access-Control-Allow-Methods",
                        self.cors_settings.serialized_allow_methods,
                    ),
                    (
                        b"Access-Control-Allow-Headers",
                        self.cors_settings.serialized_allow_headers,
                    ),
                ],
                response_body=b"",
                status_code=200,
            )
