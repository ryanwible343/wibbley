import asyncio
from logging import getLogger

LOGGER = getLogger(__name__)


class FanoutPoller:
    def __init__(self, adapter, messagebus):
        self.adapter = adapter
        self.messagebus = messagebus

    async def _handle_fanout(self):
        connection = await self.adapter.get_connection()
        select_stmt = self.adapter.get_fanout_outstanding_select_stmt()
        result = await self.adapter.execute_stmt_on_connection(select_stmt, connection)
        records = self.adapter.get_all_rows(result)
        LOGGER.info("Fanout records: %s", records)

        fanout_messages = []
        for record in records:
            event_type = record.event["event_type"]
            event = {**record.event}
            del event["event_type"]
            del event["fanout_key"]
            event = self.messagebus.event_type_registry[event_type](**event)
            fanout_messages.append(
                {
                    "id": record.id,
                    "fanout_key": record.fanout_key,
                    "message": event,
                    "attempts": record.attempts,
                }
            )

        fanout_tasks = []
        for fanout_message in fanout_messages:
            for handler in self.messagebus.event_handlers:
                if handler["handler_name"] == fanout_message.fanout_key:
                    fanout_message.acknowledgement_queue = asyncio.Queue()
                    fanout_tasks.append(
                        asyncio.create_task(
                            self.messagebus.execute_event_handler(
                                handler["handler"], fanout_message
                            )
                        )
                    )

        ack_results = await asyncio.gather(*fanout_tasks)
        for index, result in enumerate(ack_results):
            if result:
                update_stmt = self.adapter.get_fanout_update_stmt(
                    fanout_messages[index]["id"],
                    fanout_messages[index]["fanout_key"],
                    fanout_messages[index]["attempts"] + 1,
                )
                await self.adapter.execute_stmt_on_connection(update_stmt, connection)
            else:
                update_stmt = self.adapter.get_fanout_failed_delivery_update_stmt(
                    fanout_messages[index]["id"],
                    fanout_messages[index]["fanout_key"],
                    fanout_messages[index]["attempts"] + 1,
                )
                await self.adapter.execute_stmt_on_connection(update_stmt, connection)
        await self.adapter.commit_connection(connection)
        await self.adapter.close_connection(connection)

    async def poll(self):
        while True:
            try:
                LOGGER.info("Polling fanout")
                await self._handle_fanout()
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
