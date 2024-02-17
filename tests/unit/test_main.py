import asyncio

import pytest

from wibbley.api.app import App
from wibbley.main import (
    SignalHandlerInstaller,
    handle_message,
    install_signal_handlers,
    load_module,
    read_from_queue,
    serve_app,
    shutdown_handler,
)


class FakeMessagebus:
    async def handle(self, message):
        pass


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


def test__load_module__when_module_exists__returns_variable_in_module():
    # ACT
    var = load_module("wibbley.api.app:App")

    # ASSERT
    assert var == App


def test__load_module__when_module_does_not_exist__returns_none():
    # ACT
    module = load_module("w_module")

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


@pytest.mark.asyncio
async def test__handle_message__marks_task_as_done():
    # ARRANGE
    message = {"type": "task_done", "task_id": "1234"}
    messagebus = FakeMessagebus()
    queue = asyncio.Queue()
    queue.put_nowait(message)

    # ACT
    await handle_message(queue, messagebus)

    # ASSERT
    assert queue.empty() == True


@pytest.mark.asyncio
async def test__read_from_queue__calls_handle_message():
    # ARRANGE
    messagebus = FakeMessagebus()
    queue = asyncio.Queue()
    queue.put_nowait({"type": "task_done", "task_id": "1234"})

    # ACT
    await asyncio.wait_for(read_from_queue(queue, messagebus), timeout=0.05)

    # ASSERT
    assert queue.empty() == True


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


def test__serve_app__no_messagebus__runs_serve_app_task():
    # ACT
    serve_app(
        app="test",
        messagebus=None,
        task_count=1,
        host="test",
        port="test",
        uds="test",
        fd="test",
        reload=False,
        reload_dirs="test",
        reload_includes="test",
        reload_excludes="test",
        reload_delay=1,
        env_file="test",
        log_level="test",
        proxy_headers=False,
        server_header=False,
        date_header=False,
        forwarded_allow_ips="test",
        root_path="test",
        limit_concurrency=1,
        backlog=1,
        limit_max_requests=1,
        timeout_keep_alive=1,
        timeout_graceful_shutdown=1,
        ssl_keyfile="test",
        ssl_certfile="test",
        ssl_keyfile_password="test",
        ssl_version="test",
        ssl_cert_reqs="test",
        ssl_ca_certs="test",
        ssl_ciphers="test",
        headers=[],
    )

    # ASSERT
    assert server.serve_called == True
