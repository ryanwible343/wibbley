import pytest

from wibbley.api.http_handler.router import Router


def test__router_get_wrapper__returns_wrapped_function():
    # ARRANGE
    router = Router()

    async def test_func():
        pass

    # ACT
    wrapper = router._get_wrapper(test_func)

    # ASSERT
    assert wrapper.__name__ == "test_func"


@pytest.mark.asyncio
async def test__router_get_wrapper__when_additional_kwargs__strips_kwargs_down_to_defined():
    # ARRANGE
    router = Router()

    async def test_func(a):
        return a

    # ACT
    wrapper = router._get_wrapper(test_func)
    result = await wrapper(a=1, b=2)

    # ASSERT
    assert result == 1


@pytest.mark.asyncio
async def test__router_get__adds_head_and_get_to_routes():
    # ARRANGE
    router = Router()

    @router.get("/")
    async def test_func():
        pass

    # ACT
    result = router.routes["/"]

    # ASSERT
    assert result["GET"].__name__ == "test_func"
    assert result["HEAD"].__name__ == "test_func"


@pytest.mark.asyncio
async def test__router_post__adds_post_to_routes():
    # ARRANGE
    router = Router()

    @router.post("/")
    async def test_func():
        pass

    # ACT
    result = router.routes["/"]

    # ASSERT
    assert result["POST"].__name__ == "test_func"


@pytest.mark.asyncio
async def test__router_put__adds_put_to_routes():
    # ARRANGE
    router = Router()

    @router.put("/")
    async def test_func():
        pass

    # ACT
    result = router.routes["/"]

    # ASSERT
    assert result["PUT"].__name__ == "test_func"


@pytest.mark.asyncio
async def test__router_delete__adds_delete_to_routes():
    # ARRANGE
    router = Router()

    @router.delete("/")
    async def test_func():
        pass

    # ACT
    result = router.routes["/"]

    # ASSERT
    assert result["DELETE"].__name__ == "test_func"


@pytest.mark.asyncio
async def test__router_patch__adds_patch_to_routes():
    # ARRANGE
    router = Router()

    @router.patch("/")
    async def test_func():
        pass

    # ACT
    result = router.routes["/"]

    # ASSERT
    assert result["PATCH"].__name__ == "test_func"
