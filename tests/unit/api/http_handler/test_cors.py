from wibbley.api.http_handler.cors import CORSSettings


def test__serialized_allow_origins__returns_byte_string():
    cors_settings = CORSSettings(
        allow_origins=["http://example.com", "http://example.org"],
        allow_methods=[],
        allow_headers=[],
    )
    assert (
        cors_settings.serialized_allow_origins
        == b"http://example.com, http://example.org"
    )


def test__serialized_allow_methods__returns_byte_string():
    cors_settings = CORSSettings(
        allow_origins=[],
        allow_methods=["GET", "POST"],
        allow_headers=[],
    )
    assert cors_settings.serialized_allow_methods == b"GET, POST"


def test__serialized_allow_headers__returns_byte_string():
    cors_settings = CORSSettings(
        allow_origins=[],
        allow_methods=[],
        allow_headers=["Content-Type", "Authorization"],
    )
    assert cors_settings.serialized_allow_headers == b"Content-Type, Authorization"
