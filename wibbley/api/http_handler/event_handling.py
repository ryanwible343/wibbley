from wibbley.event_driven import AbstractMessagebus


class EventHandlingSettings:
    def __init__(
        self,
        enabled: bool = False,
        handler: AbstractMessagebus = None,
    ):
        self.enabled = enabled
        self.handler = handler
