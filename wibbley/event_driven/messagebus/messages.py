import asyncio
import datetime
from dataclasses import dataclass, field
from uuid import uuid4


@dataclass
class Event:
    id: str = field(default_factory=lambda: str(uuid4()))
    created_at: str = field(
        default_factory=lambda: datetime.datetime.utcnow().isoformat()
    )
    acknowledgement_queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue()
    )
    fanout_key: str = field(default_factory=lambda: "")

    def __post_init__(self):
        self.event_type = type(self).__name__


class Command:
    pass


class Query:
    pass
