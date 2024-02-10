import pytest

from wibbley.utilities.async_retry import AsyncRetry


@pytest.mark.asyncio
async def test__async_retry__retries_up_to_max_attempts():
    # Arrange
    async_retry = AsyncRetry(max_attempts=3, base_delay=0)
    attempts = 0

    @async_retry
    async def fake_function():
        nonlocal attempts
        attempts += 1
        raise ValueError("Fake error")

    # Act
    try:
        await fake_function()
    except ValueError:
        pass

    # Assert
    assert attempts == 3


@pytest.mark.asyncio
async def test__async_retry__reraises_on_last_attempt():
    # Arrange
    async_retry = AsyncRetry(max_attempts=1, base_delay=0)

    @async_retry
    async def fake_function():
        raise ValueError("Fake error")

    # Act/Assert
    with pytest.raises(ValueError):
        await fake_function()
