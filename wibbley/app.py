import json

import orjson


async def do_something():
    return "Hello World"


def determine_content_type_header(result: str | bytes | dict) -> bytes | None:
    response_types = {
        str: b"text/plain",
        bytes: b"application/octet-stream",
        dict: b"application/json",
    }
    response_header = response_types.get(type(result))
    return response_header


def format_response(result: str | bytes | dict) -> bytes:
    data_types_to_serializer = {
        str: lambda x: x.encode("utf-8"),
        bytes: lambda x: x,
        dict: lambda x: orjson.dumps(x),
    }

    serializer = data_types_to_serializer.get(type(result))
    return serializer(result)


async def app(scope, receive, send):
    assert scope["type"] == "http"

    result = await do_something()

    content_type_header = determine_content_type_header(result)

    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", content_type_header),
            ],
        }
    )

    formatted_response = format_response(result)

    await send(
        {
            "type": "http.response.body",
            "body": formatted_response,
        }
    )
