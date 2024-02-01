from wibbley.http_handler.request_handlers.response_sender import ResponseSender


class HeadRequestHandler:
    def __init__(self, response_sender: ResponseSender):
        self.response_sender = response_sender

    def _determine_content_type_header(self, result):
        response_types = {
            str: b"text/plain",
            bytes: b"application/octet-stream",
            dict: b"application/json",
            list: b"application/json",
        }
        response_header = response_types.get(type(result))
        return response_header

    async def handle(self, send, route_func_result: str | bytes | dict | list):
        content_type_header = self._determine_content_type_header(route_func_result)

        await self.response_sender.send_response(
            send,
            status_code=200,
            headers=[
                (b"content-type", content_type_header),
                (
                    b"content-length",
                    str(len(route_func_result)).encode("utf-8"),
                ),
            ],
            response_body=b"",
        )
