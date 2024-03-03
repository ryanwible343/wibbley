import asyncio


class OutboxPoller:
    def __init__(self, adapter, messagebus):
        self.adapter = adapter
        self.messagebus = messagebus

    async def _handle_outbox(self):
        connection = await self.adapter.get_connection()
        select_stmt = self.adapter.get_outbox_outstanding_select_stmt()
        result = await self.adapter.execute_stmt_on_connection(select_stmt, connection)
        records = self.adapter.get_all_rows(result)

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
                    events[index].id, records[index].attempts + 1
                )
                await self.adapter.execute_stmt_on_connection(update_stmt, connection)
            else:
                update_stmt = self.adapter.get_outbox_failed_delivery_update_stmt(
                    events[index].id, records[index].attempts + 1
                )
                await self.adapter.execute_stmt_on_connection(update_stmt, connection)
        await self.adapter.commit_connection(connection)
        await self.adapter.close_connection(connection)

    async def poll(self):
        while True:
            try:
                await self._handle_outbox()
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
