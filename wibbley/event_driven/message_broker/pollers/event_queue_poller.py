import asyncio
from copy import copy
from logging import getLogger

import orjson

from wibbley.event_driven.message_broker.queue import wibbley_queue

LOGGER = getLogger(__name__)


class EventQueuePoller:
    def __init__(self, adapter, messagebus, queue=wibbley_queue):
        self.adapter = adapter
        self.messagebus = messagebus
        self.queue = queue

    async def _wait_for_ack(self, event):
        await self.queue.put(event)
        return await event.acknowledgement_queue.get()

    def _fanout_event(self, event, event_handlers):
        fanout_messages = []
        for handler in event_handlers:
            new_event = copy(event)
            new_event.fanout_key = handler["handler_name"]
            fanout_messages.append(new_event)
        return fanout_messages

    async def _is_fanout_persisted(self, event_id, connection):
        get_stmt = self.adapter.get_fanout_select_all_stmt(event_id)
        result = await self.adapter.execute_stmt_on_connection(get_stmt, connection)
        records = self.adapter.get_all_rows(result)
        return len(records) != 0

    async def _persist_fanout_messages(self, fanout_messages, connection):
        for fanout_message in fanout_messages:
            event = fanout_message["event"]
            event = copy(vars(event))
            del event["acknowledgement_queue"]
            insert_stmt = self.adapter.get_fanout_insert_stmt(
                fanout_message.id,
                fanout_message.fanout_key,
                fanout_message.created_at,
                orjson.dumps(event).decode("utf-8"),
            )
            await self.adapter.execute_stmt_on_connection(insert_stmt, connection)

    async def _handle_message_and_wait_for_ack(self, handler, event):
        await self.messagebus.execute_event_handler(handler, event)
        try:
            return await asyncio.wait_for(event.acknowledgement_queue.get(), timeout=1)
        except asyncio.TimeoutError:
            return False

    async def _route_fanout_messages(self, fanout_messages, event_handlers):
        event_handling_tasks = []
        for fanout_message in fanout_messages:
            for handler in event_handlers:
                if handler["handler_name"] == fanout_message.fanout_key:
                    fanout_message.acknowledgement_queue = asyncio.Queue()
                    event_handling_tasks.append(
                        asyncio.create_task(
                            self._handle_message_and_wait_for_ack(
                                handler["handler"], fanout_message
                            )
                        )
                    )
        return await asyncio.gather(*event_handling_tasks)

    async def _delete_acked_fanout_messages(self, fanout_messages, results, connection):
        LOGGER.info("results: %s", results)
        for index, result in enumerate(results):
            if result:
                LOGGER.info("should see this once")
                delete_stmt = self.adapter.delete_fanout_stmt(
                    fanout_messages[index]["message"].id,
                    fanout_messages[index]["fanout_key"],
                )
                await self.adapter.execute_stmt_on_connection(delete_stmt, connection)
            else:
                update_stmt = self.adapter.get_fanout_failed_delivery_update_stmt(
                    fanout_messages[index].id, fanout_messages[index].fanout_key, 1
                )
                await self.adapter.execute_stmt_on_connection(update_stmt, connection)

    async def _handle_event(self):
        event = await self.queue.get()
        event_handlers = self.messagebus.event_handlers[type(event)]

        fanout_messages = self._fanout_event(event, event_handlers)
        connection = await self.adapter.get_connection()

        transaction = await self.adapter.get_transaction(connection)
        await self.adapter.start_transaction(transaction)
        try:
            is_fanout_persisted = await self._is_fanout_persisted(event.id, connection)

            if not is_fanout_persisted:
                LOGGER.info("Fanout not yet persisted")
                await self._persist_fanout_messages(fanout_messages, connection)
        except Exception as e:
            await self.adapter.rollback_transaction(transaction)
        else:
            await self.adapter.commit_transaction(transaction)

            await event.acknowledgement_queue.put(True)
            ack_results = await self._route_fanout_messages(
                fanout_messages, event_handlers
            )

            try:
                await self._delete_acked_fanout_messages(
                    fanout_messages, ack_results, connection
                )
            except:
                await self.adapter.rollback_transaction(transaction)
            else:
                await self.adapter.commit_connection(connection)
        finally:
            await self.adapter.close_connection(connection)
            self.queue.task_done()

    async def poll(self):
        while True:
            try:
                await self._handle_event()
            except asyncio.CancelledError:
                break
