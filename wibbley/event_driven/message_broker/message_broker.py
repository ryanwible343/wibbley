import asyncio
from copy import copy
from logging import getLogger
from uuid import uuid4

import orjson

from wibbley.event_driven.message_broker.queue import wibbley_queue
from wibbley.event_driven.message_client.adapters import ADAPTERS

LOGGER = getLogger(__name__)


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

    async def _wait_for_ack(self, event):
        await self.queue.put(event)
        return await event.acknowledgement_queue.get()

    async def _handle_outbox(self):
        connection = await self.adapter.get_connection()
        select_stmt = self.adapter.get_outbox_outstanding_select_stmt()
        result = await self.adapter.execute_stmt_on_connection(select_stmt, connection)
        records = self.adapter.get_all_rows(result)
        LOGGER.info(f"Records: {records}")

        events = []
        for record in records:
            event_type = record.event["event_type"]
            event = {**record.event}
            del event["event_type"]
            del event["fanout_key"]
            event = self.messagebus.event_type_registry[event_type](**event)
            events.append(event)

        outbox_tasks = []
        for event in events:
            outbox_tasks.append(self._wait_for_ack(event))

        ack_results = await asyncio.gather(*outbox_tasks)
        for index, result in enumerate(ack_results):
            if result:
                update_stmt = self.adapter.get_outbox_update_stmt(
                    events[index].id, records[index].attempts
                )
                await self.adapter.execute_stmt_on_connection(update_stmt, connection)
            else:
                update_stmt = self.adapter.get_outbox_failed_delivery_update_stmt(
                    events[index].id, records[index].attempts
                )
                await self.adapter.execute_stmt_on_connection(update_stmt, connection)
        await self.adapter.commit_connection(connection)
        await self.adapter.close_connection(connection)

    async def _poll_outbox(self):
        while True:
            try:
                await self._handle_outbox()
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break

    async def _poll_fanout(self):
        while True:
            try:
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break

    def _fanout_event(self, event, event_handlers):
        fanout_messages = []
        for handler in event_handlers:
            new_event = copy(event)
            fanout_key = handler["handler_name"]
            fanout_messages.append({"fanout_key": fanout_key, "event": new_event})
        return fanout_messages

    async def _is_fanout_persisted(self, event_id, connection):
        get_stmt = self.adapter.get_fanout_select_all_stmt(event_id)
        result = await self.adapter.execute_stmt_on_connection(get_stmt, connection)
        records = self.adapter.get_all_rows(result)
        return len(records) == 0

    async def _persist_fanout_messages(self, fanout_messages, connection):
        for fanout_message in fanout_messages:
            event = fanout_message["event"]
            event = copy(vars(event))
            event["fanout_key"] = fanout_message["fanout_key"]
            del event["acknowledgement_queue"]
            insert_stmt = self.adapter.get_fanout_insert_stmt(
                fanout_message["event"].id,
                fanout_message["fanout_key"],
                fanout_message["event"].created_at,
                orjson.dumps(event).decode("utf-8"),
            )
            await self.adapter.execute_stmt_on_connection(insert_stmt, connection)

    async def _route_fanout_messages(self, fanout_messages, event_handlers):
        event_handling_tasks = []
        for fanout_message in fanout_messages:
            for handler in event_handlers:
                if handler["handler_name"] == fanout_message["fanout_key"]:
                    fanout_message["event"].acknowledgement_queue = asyncio.Queue()
                    event_handling_tasks.append(
                        asyncio.create_task(
                            self.messagebus.execute_event_handler(
                                handler["handler"], fanout_message["event"]
                            )
                        )
                    )
        return await asyncio.gather(*event_handling_tasks)

    async def _delete_acked_fanout_messages(self, fanout_messages, results, connection):
        for index, result in enumerate(results):
            if result:
                delete_stmt = self.adapter.delete_fanout_stmt(
                    fanout_messages[index]["message"].id,
                    fanout_messages[index]["fanout_key"],
                )
                await self.adapter.execute_stmt_on_connection(delete_stmt, connection)

    async def _handle_event(self):
        event = await self.queue.get()
        LOGGER.info(f"Handling event: {event}")
        event_handlers = self.messagebus.event_handlers[type(event)]

        fanout_messages = self._fanout_event(event, event_handlers)
        connection = await self.adapter.get_connection()

        is_fanout_persisted = await self._is_fanout_persisted(event.id, connection)

        if not is_fanout_persisted:
            await self._persist_fanout_messages(fanout_messages, connection)
            await self.adapter.commit_connection(connection)

        await event.acknowledgement_queue.put(True)
        ack_results = await self._route_fanout_messages(fanout_messages, event_handlers)

        await self._delete_acked_fanout_messages(
            fanout_messages, ack_results, connection
        )
        await self.adapter.commit_connection(connection)

        await self.adapter.close_connection(connection)
        self.queue.task_done()

    async def _poll_queue(self):
        while True:
            try:
                await self._handle_event()
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
