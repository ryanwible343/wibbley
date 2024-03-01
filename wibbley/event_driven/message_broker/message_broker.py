import asyncio

from wibbley.event_driven.message_broker.queue import wibbley_queue
from wibbley.event_driven.message_client.adapters import ADAPTERS


class MessageBrokerSettings:
    def __init__(self, event_handler_count, outbox_poller_count):
        self.event_handler_count = event_handler_count
        self.outbox_poller_count = outbox_poller_count


class MessageBroker:
    def __init__(
        self,
        messagebus,
        adapter_name,
        connection_factory,
        message_broker_settings,
        queue=wibbley_queue,
        adapters=ADAPTERS,
    ):
        if adapter_name not in adapters:
            raise ValueError("Unavailable adapter selected")
        self.adapter = adapters[adapter_name](connection_factory)
        self.messagebus = messagebus
        self.event_handler_count = message_broker_settings.event_handler_count
        self.outbox_poller_count = message_broker_settings.outbox_poller_count
        self.queue = queue

    async def _poll_outbox(self):
        # ... snip ...
        return self.database_adapter.get_messages()

    async def _handle_message(self):
        message = await self.queue.get()
        count_of_event_handlers = len(self.messagebus.event_handlers[type(message)])
        await message.acknowledgement_queue.put(count_of_event_handlers)
        await self.messagebus.handle(message)
        self.queue.task_done()

    async def _poll_queue(self):
        while True:
            try:
                await self._handle_message()
            except asyncio.CancelledError:
                break

    async def create_durable_tables(self):
        connection = await self.adapter.get_connection()
        for stmt in self.adapter.get_table_creation_statements():
            await self.adapter.execute_stmt_on_connection(stmt, connection)
        await self.adapter.commit_connection(connection)
        await self.adapter.close_connection(connection)

    async def start(self):
        await self.create_durable_tables()

        for _ in range(self.event_handler_count):
            asyncio.create_task(self._poll_queue())

        for _ in range(self.outbox_poller_count):
            asyncio.create_task(self._poll_outbox())
