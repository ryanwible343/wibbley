from wibbley.messagebus.messagebus import AbstractMessagebus


class EventHandlingSettings:
    def __init__(
        self,
        enabled: bool = False,
        handler: AbstractMessagebus = None,
    ):
        self.enabled = enabled
        self.handler = handler
