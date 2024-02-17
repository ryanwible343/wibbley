class EventHandlingSettings:
    def __init__(
        self,
        enabled: bool = False,
        handler=None,
    ):
        self.enabled = enabled
        self.handler = handler
