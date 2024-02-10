from wibbley.api.http_handler.route_extractor import RouteExtractor


def test__route_extractor_match_and_extract__when_matches__returns_match_with_parameters():
    # ARRANGE
    route_extractor = RouteExtractor()
    pattern = "/api/v1/{id}"
    path = "/api/v1/123"

    # ACT
    match, path_parameters = route_extractor._match_and_extract(pattern, path)

    # ASSERT
    assert match is True
    assert path_parameters == {"id": "123"}


def test__route_extractor_match_and_extract__when_does_not_match__returns_no_match():
    # ARRANGE
    route_extractor = RouteExtractor()
    pattern = "/api/v1/{id}"
    path = "/api/v1"

    # ACT
    match, path_parameters = route_extractor._match_and_extract(pattern, path)

    # ASSERT
    assert match is False
    assert len(path_parameters) == 0


def test__route_extractor_match_and_extract__when_path_param_in_first_position__returns_match():
    # ARRANGE
    route_extractor = RouteExtractor()
    pattern = "/{id}/api/v1"
    path = "/123/api/v1"

    # ACT
    match, path_parameters = route_extractor._match_and_extract(pattern, path)

    # ASSERT
    assert match is True
    assert path_parameters == {"id": "123"}


def test__route_extractor_extract__when_matching_route__returns_route_info():
    # ARRANGE
    async def fake_route_func(scope, receive, send):
        pass

    route_extractor = RouteExtractor()
    routes = {"/api/v1/{id}": {"GET": fake_route_func}}
    request_path = "/api/v1/123"
    request_method = "GET"

    # ACT
    route_info = route_extractor.extract(routes, request_path, request_method)

    # ASSERT
    assert route_info.route_func is fake_route_func
    assert route_info.available_methods == {"GET"}
    assert route_info.path_parameters == {"id": "123"}


def test__route_extractor_extract__when_no_matching_route__returns_empty_route_info():
    # ARRANGE
    async def fake_route_func(scope, receive, send):
        pass

    route_extractor = RouteExtractor()
    routes = {"/api/v1/{id}": {"GET": fake_route_func}}
    request_path = "/notaroute/v1/123"
    request_method = "POST"

    # ACT
    route_info = route_extractor.extract(routes, request_path, request_method)

    # ASSERT
    assert route_info.route_func is None
    assert len(route_info.available_methods) == 0
    assert len(route_info.path_parameters) == 0


def test__route_extractor_extract__when_matching_route_with_no_route_func_for_request_method__returns_route_info_without_route_func():
    # ARRANGE
    async def fake_route_func(scope, receive, send):
        pass

    route_extractor = RouteExtractor()
    routes = {"/api/v1/{id}": {"GET": fake_route_func}}
    request_path = "/api/v1/123"
    request_method = "POST"

    # ACT
    route_info = route_extractor.extract(routes, request_path, request_method)

    # ASSERT
    assert route_info.route_func is None
    assert route_info.available_methods == {"GET"}
    assert route_info.path_parameters == {"id": "123"}


def test__route_extractor_extract__when_no_path_parameters__returns_route_func_and_available_methods():
    # ARRANGE
    async def fake_route_func(scope, receive, send):
        pass

    route_extractor = RouteExtractor()
    routes = {"/api/v1": {"GET": fake_route_func}}
    request_path = "/api/v1"
    request_method = "GET"

    # ACT
    route_info = route_extractor.extract(routes, request_path, request_method)

    # ASSERT
    assert route_info.route_func is fake_route_func
    assert route_info.available_methods == {"GET"}
    assert len(route_info.path_parameters) == 0
