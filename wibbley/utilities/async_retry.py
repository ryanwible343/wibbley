import asyncio
import logging
from functools import wraps

LOGGER = logging.getLogger(__name__)


class AsyncRetry:
    def __init__(self, max_attempts=3, base_delay=1):
        self.max_attempts = max_attempts
        self.base_delay = base_delay

    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < self.max_attempts:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts >= self.max_attempts:
                        raise e
                    delay = self.base_delay * (2**attempts)
                    LOGGER.error(
                        f"Error calling {func.__name__}. Retrying in {delay} seconds"
                    )
                    await asyncio.sleep(delay)

        return wrapper
