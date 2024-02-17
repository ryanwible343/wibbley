from abc import ABC, abstractmethod


class AbstractAdapter(ABC):
    @abstractmethod
    async def execute_async(self, connection_factory, stmt) -> None:
        """
        Create a connection async, execute the statement async,
        commit the transaction async, and close the connection async.
        """

    @abstractmethod
    async def enable_exactly_once_processing(self, connection_factory) -> None:
        """
        Create the schema, outbox, and inbox tables if they don't exist.
        """

    @abstractmethod
    async def stage(self, event, session) -> None:
        """
        Stage the event in the outbox table
        """

    @abstractmethod
    async def publish(self, event, session) -> None:
        """
        Publish the event to the wibbley_queue and update the outbox table to mark the event as delivered
        """

    @abstractmethod
    async def is_duplicate(self, event, session) -> bool:
        """
        Check if the event is a duplicate by checking the inbox table. If not, add the event to the inbox table.
        """

    @abstractmethod
    def ack(self, event) -> bool:
        """
        Acknowledge the event has been processed."""

    @abstractmethod
    def nack(self, event) -> bool:
        """Negatively acknowledge the event has been processed."""
