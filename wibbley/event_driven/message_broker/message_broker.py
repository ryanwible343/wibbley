import asyncio

from wibbley.event_driven.message_broker.queue import wibbley_queue


class MessageBroker:
    def __init__(
        self,
        messagebus,
        message_client,
        event_handler_count,
        outbox_poller_count,
        queue=wibbley_queue,
    ):
        self.messagebus = messagebus
        self.message_client = message_client
        self.event_handler_count = event_handler_count
        self.outbox_poller_count = outbox_poller_count
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
        connection = await self.message_client.adapter.get_connection()
        for stmt in self.message_client.adapter.get_table_creation_statements():
            await self.message_client.adapter.execute_stmt_on_connection(
                stmt, connection
            )
        await self.message_client.adapter.commit_connection(connection)
        await self.message_client.adapter.close_connection(connection)

    async def start(self):
        await self.create_durable_tables()

        for _ in range(self.event_handler_count):
            asyncio.create_task(self._poll_queue())

        for _ in range(self.outbox_poller_count):
            asyncio.create_task(self._poll_outbox())
