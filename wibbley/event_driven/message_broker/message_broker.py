import asyncio
from logging import getLogger

from wibbley.event_driven.message_broker.pollers.event_queue_poller import (
    EventQueuePoller,
)
from wibbley.event_driven.message_broker.pollers.fanout_poller import FanoutPoller
from wibbley.event_driven.message_broker.pollers.outbox_poller import OutboxPoller
from wibbley.event_driven.message_broker.queue import wibbley_queue
from wibbley.event_driven.message_client.adapters import ADAPTERS

LOGGER = getLogger(__name__)


class MessageBrokerSettings:
    def __init__(self, event_handler_count, outbox_poller_count, fanout_poller_count):
        self.event_handler_count = event_handler_count
        self.outbox_poller_count = outbox_poller_count
        self.fanout_poller_count = fanout_poller_count


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
        self.fanout_poller_count = message_broker_settings.fanout_poller_count
        self.max_fanout_delivery_attempts = 3
        self.queue = queue
        self.tasks = []
        self.fanout_poller = FanoutPoller(self.adapter, self.messagebus)
        self.outbox_poller = OutboxPoller(self.adapter, self.messagebus)
        self.event_queue_poller = EventQueuePoller(self.adapter, self.messagebus)

    async def create_durable_tables(self):
        connection = await self.adapter.get_connection()
        for stmt in self.adapter.get_table_creation_statements():
            await self.adapter.execute_stmt_on_connection(stmt, connection)
        await self.adapter.commit_connection(connection)
        await self.adapter.close_connection(connection)

    async def start(self):
        await self.create_durable_tables()

        for _ in range(self.event_handler_count):
            task = asyncio.create_task(self.event_queue_poller.poll())
            self.tasks.append(task)

        for _ in range(self.outbox_poller_count):
            task = asyncio.create_task(self.outbox_poller.poll())
            self.tasks.append(task)

        for _ in range(self.fanout_poller_count):
            task = asyncio.create_task(self.fanout_poller.poll())
            self.tasks.append(task)
