import asyncio

import pytest
from click.testing import CliRunner

from wibbley.api.app import App
from wibbley.event_driven.messagebus.messages import Event
from wibbley.main import (
    SignalHandlerInstaller,
    install_signal_handlers,
    load_module,
    main,
    print_version,
    serve_app,
    shutdown_handler,
)


async def fake_handler(message):
    pass


class FakeMessageBroker:
    def __init__(self):
        self.start_called = False
        self.tasks = []

    async def start(self):
        self.start_called = True


class FakeTask:
    def __init__(self):
        self.done = False

    def cancel(self):
        self.done = True


class FakeUvicornServer:
    def __init__(self):
        self.handle_exit_called = False

    def handle_exit(self, sig, frame):
        self.handle_exit_called = True


class FakeInstaller:
    def install(self, loop, signal, handler, server, tasks):
        raise NotImplementedError("test")


class FakeServer:
    def __init__(self, config):
        self.config = config
        self.serve_called = False

    async def serve(self):
        self.serve_called = True


class FakeBackgroundTask:
    def __init__(self):
        self.background_task_called = False

    async def run(self, *args):
        self.background_task_called = True


class FakeSignalHandlerInstaller:
    def __init__(self):
        self.install_called = False

    def install(self, *args, **kwargs):
        self.install_called = True


class FakeCTX:
    def __init__(self):
        self.resilient_parsing = False
        self.exit_called = False

    def exit(self, *args):
        self.exit_called = True


class FakeClick:
    def __init__(self):
        self.echo_called = False

    def echo(self, *args):
        self.echo_called = True


class FakeLoadModuleFailure:
    def load(self, *args):
        return False


class FakeLoadModuleSuccess:
    def load(self, *args):
        return True


def test__load_module__when_module_exists__returns_variable_in_module():
    # ACT
    var = load_module("wibbley.api.app:App")

    # ASSERT
    assert var == App


def test__load_module__when_module_does_not_exist__returns_none():
    # ACT
    module = load_module("wibbley.does_not_exist:App")

    # ASSERT
    assert module == None


def test__load_module__when_variable_does_not_exist__returns_none():
    # ACT
    module = load_module("wibbley.api.app:App2")

    # ASSERT
    assert module == None


def test__load_module__when_parameter_does_not_contain_colon__returns_none():
    module = load_module("wibbley.api.app")

    # ASSERT
    assert module == None


def test__shutdown_handler__cancels_tasks_and_calls_server_handle_exit():
    # ARRANGE
    fake_task_1 = FakeTask()
    fake_task_2 = FakeTask()
    tasks = [fake_task_1, fake_task_2]
    fake_server = FakeUvicornServer()

    # ACT
    shutdown_handler(fake_server, tasks, "test")

    # ASSERT
    assert fake_task_1.done == True
    assert fake_task_2.done == True
    assert fake_server.handle_exit_called == True


def test__install_signal_handlers__registers_signal_handlers_on_asyncio_loop():
    # ARRANGE
    loop = asyncio.get_event_loop()
    fake_server = FakeUvicornServer()
    task_1 = FakeTask()
    tasks = [task_1]

    # ACT
    install_signal_handlers(fake_server, tasks, loop)

    # ASSERT
    assert loop._signal_handlers[2] != None
    assert loop._signal_handlers[15] != None


def test__signal_handler_installer_install__registers_signal_handlers_on_asyncio_loop():
    # ARRANGE
    loop = asyncio.get_event_loop()
    fake_server = FakeUvicornServer()
    task_1 = FakeTask()
    tasks = [task_1]
    installer = SignalHandlerInstaller()

    # ACT
    installer.install(loop, 1, lambda: None, fake_server, tasks)

    # ASSERT
    assert loop._signal_handlers[1] != None


def test__install_signal_handlers__when_installer_raises_exception__adds_signal_with_signal():
    # ARRANGE
    loop = asyncio.get_event_loop()
    fake_server = FakeUvicornServer()
    task_1 = FakeTask()
    tasks = [task_1]
    installer = FakeInstaller()

    # ACT
    install_signal_handlers(fake_server, tasks, loop, installer)

    # ASSERT
    assert loop._signal_handlers[1] != None


@pytest.mark.asyncio
async def test__serve_app__no_message_broker__runs_serve_app_task():
    # ARRANGE
    config = {}
    server = FakeServer(config)

    # ACT
    await serve_app(
        message_broker=None,
        server=server,
    )

    # ASSERT
    assert server.serve_called == True


def test__serve_app__when_message_broker__calls_start_and_installs_event_handlers():
    # ARRANGE
    signal_handler_installer = FakeSignalHandlerInstaller()
    config = {}
    server = FakeServer(config)
    fake_message_broker = FakeMessageBroker()

    # ACT
    asyncio.run(
        serve_app(
            fake_message_broker,
            server,
            signal_handler_installer.install,
        )
    )

    # ASSERT
    assert fake_message_broker.start_called == True
    assert signal_handler_installer.install_called == True


def test__print_version__when_value_and_no_resilient_parsing__calls_click_echo():
    # ARRANGE
    click = FakeClick()
    ctx = FakeCTX()
    ctx.resilient_parsing = False

    # ACT
    print_version(ctx, None, True, click)

    # ASSERT
    assert click.echo_called == True


def test__print_version__when_resilient_parsing_enabled__returns():
    # ARRANGE
    click = FakeClick()
    ctx = FakeCTX()
    ctx.resilient_parsing = True

    # ACT
    print_version(ctx, None, True, click)

    # ASSERT
    assert click.echo_called == False


def test__main__when_cannot_load_app__calls_sys_exit_1(mocker):
    # ARRANGE
    load_module = FakeLoadModuleFailure()
    runner = CliRunner()
    mocker.patch("wibbley.main.load_module", load_module.load)

    # ACT
    result = runner.invoke(main, ["--app", "wibbley.api.app:App"])

    # ASSERT
    assert result.exit_code == 1


def test__main__when_cannot_load_message_broker__calls_sys_exit_1():
    # ARRANGE
    runner = CliRunner()

    # ACT
    result = runner.invoke(
        main,
        [
            "--app",
            "wibbley.api.app:App",
            "--message-broker",
            "wibbley.does_not_exist",
        ],
    )

    # ASSERT
    assert result.exit_code == 1


def test__main__when_successfully_loads_app__calls_serve_app(mocker):
    # ARRANGE
    load_module = FakeLoadModuleSuccess()
    serve_app = mocker.patch("wibbley.main.serve_app")
    runner = CliRunner()
    mocker.patch("wibbley.main.load_module", load_module.load)

    # ACT
    result = runner.invoke(main, ["--app", "wibbley.api.app:App"])

    # ASSERT
    assert serve_app.called == True
    assert result.exit_code == 0
