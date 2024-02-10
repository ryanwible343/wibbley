from wibbley.api.http_handler.response import HTTPResponse


def test__http_response_to_dict__returns_dict():
    # ARRANGE
    response = HTTPResponse(
        status_code=200,
        headers=[(b"key", b"value")],
        body=b'{"key": "value"}',
    )

    # ACT/ASSERT
    assert response.to_dict() == {
        "status_code": 200,
        "headers": [(b"key", b"value")],
        "body": b'{"key": "value"}',
    }
