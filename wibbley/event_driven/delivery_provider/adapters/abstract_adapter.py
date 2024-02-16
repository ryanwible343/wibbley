from abc import ABC, abstractmethod


class AbstractAdapter(ABC):
    @abstractmethod
    async def execute_async(self, connection_factory, stmt) -> None:
        pass

    @abstractmethod
    async def enable_exactly_once_processing(self, connection_factory) -> None:
        pass

    @abstractmethod
    async def stage(self, event, session) -> None:
        pass

    @abstractmethod
    async def publish(self, event, session) -> None:
        pass

    @abstractmethod
    async def is_duplicate(self, event, session) -> bool:
        pass

    @abstractmethod
    def ack(self, event) -> bool:
        pass

    @abstractmethod
    def nack(self, event) -> bool:
        pass
