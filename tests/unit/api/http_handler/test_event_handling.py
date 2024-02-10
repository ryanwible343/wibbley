from wibbley.api.http_handler.event_handling import EventHandlingSettings


def test__construct_event_handling_settings__with_defaults__returns_instance():
    # ARRANGE
    # ACT
    event_handling_settings = EventHandlingSettings()

    # ASSERT
    assert event_handling_settings.enabled == False
    assert event_handling_settings.handler == None
