import asyncio
from logging import getLogger

import orjson

LOGGER = getLogger(__name__)


class FanoutPoller:
    def __init__(self, adapter, messagebus, max_attempts):
        self.adapter = adapter
        self.messagebus = messagebus
        self.max_attempts = max_attempts

    async def _handle_fanout(self):
        connection = await self.adapter.get_connection()
        outstanding_fanout_stmt = self.adapter.get_fanout_outstanding_select_stmt()
        result = await self.adapter.execute_stmt_on_connection(
            outstanding_fanout_stmt, connection
        )
        records = self.adapter.get_all_rows(result)
        if len(records) == 0:
            await self.adapter.close_connection(connection)
            return

        for record in records:
            if record.attempts == self.max_attempts:
                pass

        records = [record for record in records if record.attempts < self.max_attempts]
        events = [self._convert_message_to_event(record) for record in records]
        for event in events:
            event_handlers = self.messagebus.event_handlers[type(event)]
            for handler in event_handlers:
                if handler["handler_name"] == event.fanout_key:
                    asyncio.create_task(
                        self.messagebus.execute_event_handler(handler["handler"], event)
                    )
                    ack_result = await event.acknowledgement_queue.get()
                    if ack_result:
                        delete_stmt = self.adapter.delete_fanout_stmt(
                            event.id, event.fanout_key
                        )
                        await self.adapter.execute_stmt_on_connection(
                            delete_stmt, connection
                        )
                    else:
                        update_stmt = (
                            self.adapter.get_fanout_failed_delivery_update_stmt(
                                event.id, event.fanout_key, records[0].attempts + 1
                            )
                        )
                        await self.adapter.execute_stmt_on_connection(
                            update_stmt, connection
                        )
        await self.adapter.commit_connection(connection)

    async def poll(self):
        while True:
            try:
                LOGGER.info("Polling fanout")
                await self._handle_fanout()
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
