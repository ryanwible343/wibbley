import asyncio
import importlib.util
import os
import signal
import ssl
import sys
from signal import SIGINT, SIGTERM

import click
import uvicorn
import uvloop

import wibbley
from wibbley.event_driven.queue import wibbley_queue

SIGNAL_HANDLERS = [SIGINT, SIGTERM]
DEFAULT_EVENT_HANDLER_TASK_COUNT = 100
TRACE_LOG_LEVEL = 5
LOG_LEVEL_CHOICES = [
    "critical",
    "error",
    "warning",
    "info",
    "debug",
    "trace",
]


def load_module(module_path: str):
    components = module_path.split(":")
    if len(components) != 2:
        print(
            f"Invalid module path, path should contain exactly one colon. You passed: {module_path}"
        )
        return None
    module_path = components[0]
    variable_name = components[1]

    try:
        # Try to import the module
        module = importlib.import_module(module_path)
    except ImportError:
        print(f"Failed to import module '{module_path}'")
        return None

    # Check if the module has the specified attribute (variable_name)
    if hasattr(module, variable_name):
        # Get the messagebus object
        variable = getattr(module, variable_name)
        return variable
    else:
        print(f"Module '{module_path}' does not contain attribute '{variable_name}'")
        return None


async def read_from_queue(queue: asyncio.Queue, messagebus):
    while True:
        try:
            message = await queue.get()
            await messagebus.handle(message)
        except asyncio.CancelledError:
            break


def shutdown_handler(server: uvicorn.Server, tasks, sig):
    for task in tasks:
        task.cancel()
    server.handle_exit(sig, None)


def install_signal_handlers(
    server: uvicorn.Server, tasks, loop: asyncio.AbstractEventLoop
):
    SIGNAL_HANDLERS = [SIGINT, SIGTERM]
    server.install_signal_handlers = lambda: None
    try:
        for sig in [SIGINT, SIGTERM]:
            loop.add_signal_handler(sig, shutdown_handler, server, tasks, sig)
    except NotImplementedError:  # pragma: no cover
        # Windows
        for sig in [SIGINT, SIGTERM]:
            signal.signal(
                sig,
                lambda sig, frame: shutdown_handler(server, tasks, sig),
            )


async def serve_app(
    app,
    messagebus,
    task_count: int,
    host: str,
    port: int,
    uds: str,
    fd: int,
    reload: bool,
    reload_dirs: list[str],
    reload_includes: list[str],
    reload_excludes: list[str],
    reload_delay: float,
    env_file: str,
    log_level: str,
    proxy_headers: bool,
    server_header: bool,
    date_header: bool,
    forwarded_allow_ips: str,
    root_path: str,
    limit_concurrency: int,
    backlog: int,
    limit_max_requests: int,
    timeout_keep_alive: int,
    timeout_graceful_shutdown: int | None,
    ssl_keyfile: str,
    ssl_certfile: str,
    ssl_keyfile_password: str,
    ssl_version: int,
    ssl_cert_reqs: int,
    ssl_ca_certs: str,
    ssl_ciphers: str,
    headers: list[str],
    h11_max_incomplete_event_size: int | None,
):
    loop = asyncio.get_event_loop()
    config = uvicorn.Config(
        app=app,
        loop="uvloop",
        host=host,
        port=port,
        uds=uds,
        fd=fd,
        reload=reload,
        reload_dirs=reload_dirs or None,
        reload_includes=reload_includes or None,
        reload_excludes=reload_excludes or None,
        reload_delay=reload_delay,
        env_file=env_file,
        log_level=log_level,
        proxy_headers=proxy_headers,
        server_header=server_header,
        date_header=date_header,
        forwarded_allow_ips=forwarded_allow_ips,
        root_path=root_path,
        limit_concurrency=limit_concurrency,
        backlog=backlog,
        limit_max_requests=limit_max_requests,
        timeout_keep_alive=timeout_keep_alive,
        timeout_graceful_shutdown=timeout_graceful_shutdown,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_keyfile_password=ssl_keyfile_password,
        ssl_version=ssl_version,
        ssl_cert_reqs=ssl_cert_reqs,
        ssl_ca_certs=ssl_ca_certs,
        ssl_ciphers=ssl_ciphers,
        headers=[header.split(":", 1) for header in headers],
        h11_max_incomplete_event_size=h11_max_incomplete_event_size,
    )
    server = uvicorn.Server(config)
    event_handler_tasks = []
    if messagebus:
        event_handler_tasks = [
            asyncio.create_task(read_from_queue(wibbley_queue, messagebus))
            for _ in range(task_count)
        ]
        install_signal_handlers(server, event_handler_tasks, loop)
    tasks = event_handler_tasks + [asyncio.create_task(server.serve())]
    await asyncio.gather(*tasks)


def print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"wibbley {wibbley.__version__}")
    ctx.exit()


@click.command()
@click.option(
    "--app",
    type=str,
    help="Name of the module containing the ASGI application. Format: module_path.module_name:app_name",
)
@click.option(
    "--messagebus",
    type=str,
    default=None,
    help="Name of the module containing the messagebus. Format: module_path.module_name:messagebus_name",
)
@click.option(
    "--event-handler-task-count",
    type=int,
    default=DEFAULT_EVENT_HANDLER_TASK_COUNT,
    help="Number of event handler tasks to run",
)
@click.option("--host", type=str, help="Bind socket to this host.", default="127.0.0.1")
@click.option("--port", type=int, help="Bind socket to this port.", default=8000)
@click.option("--uds", type=str, default=None, help="Bind to a UNIX domain socket.")
@click.option(
    "--fd", type=int, default=None, help="Bind to socket from this file descriptor."
)
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload.")
@click.option(
    "--reload-dir",
    "reload_dirs",
    multiple=True,
    default=None,
    help="Set reload directories explicitly, instead of using the current working"
    " directory.",
    type=click.Path(exists=True),
)
@click.option(
    "--reload-include",
    "reload_includes",
    multiple=True,
    default=None,
    help="Set glob patterns to include while watching for files. Includes '*.py' "
    "by default; these defaults can be overridden with `--reload-exclude`. "
    "This option has no effect unless watchfiles is installed.",
)
@click.option(
    "--reload-exclude",
    "reload_excludes",
    multiple=True,
    default=None,
    help="Set glob patterns to exclude while watching for files. Includes "
    "'.*, .py[cod], .sw.*, ~*' by default; these defaults can be overridden "
    "with `--reload-include`. This option has no effect unless watchfiles is "
    "installed.",
)
@click.option(
    "--reload-delay",
    type=float,
    default=0.25,
    show_default=True,
    help="Delay between previous and next check if application needs to be."
    " Defaults to 0.25s.",
)
@click.option(
    "--env-file",
    type=click.Path(exists=True),
    default=None,
    help="Environment configuration file.",
    show_default=True,
)
@click.option(
    "--log-level",
    type=LOG_LEVEL_CHOICES,
    default=None,
    help="Log level. [default: info]",
    show_default=True,
)
@click.option(
    "--proxy-headers/--no-proxy-headers",
    is_flag=True,
    default=True,
    help="Enable/Disable X-Forwarded-Proto, X-Forwarded-For, X-Forwarded-Port to "
    "populate remote address info.",
)
@click.option(
    "--server-header/--no-server-header",
    is_flag=True,
    default=True,
    help="Enable/Disable default Server header.",
)
@click.option(
    "--date-header/--no-date-header",
    is_flag=True,
    default=True,
    help="Enable/Disable default Date header.",
)
@click.option(
    "--forwarded-allow-ips",
    type=str,
    default=None,
    help="Comma separated list of IPs to trust with proxy headers. Defaults to"
    " the $FORWARDED_ALLOW_IPS environment variable if available, or '127.0.0.1'.",
)
@click.option(
    "--root-path",
    type=str,
    default="",
    help="Set the ASGI 'root_path' for applications submounted below a given URL path.",
)
@click.option(
    "--limit-concurrency",
    type=int,
    default=None,
    help="Maximum number of concurrent connections or tasks to allow, before issuing"
    " HTTP 503 responses.",
)
@click.option(
    "--backlog",
    type=int,
    default=2048,
    help="Maximum number of connections to hold in backlog",
)
@click.option(
    "--limit-max-requests",
    type=int,
    default=None,
    help="Maximum number of requests to service before terminating the process.",
)
@click.option(
    "--timeout-keep-alive",
    type=int,
    default=5,
    help="Close Keep-Alive connections if no new data is received within this timeout.",
    show_default=True,
)
@click.option(
    "--timeout-graceful-shutdown",
    type=int,
    default=None,
    help="Maximum number of seconds to wait for graceful shutdown.",
)
@click.option(
    "--ssl-keyfile", type=str, default=None, help="SSL key file", show_default=True
)
@click.option(
    "--ssl-certfile",
    type=str,
    default=None,
    help="SSL certificate file",
    show_default=True,
)
@click.option(
    "--ssl-keyfile-password",
    type=str,
    default=None,
    help="SSL keyfile password",
    show_default=True,
)
@click.option(
    "--ssl-version",
    type=int,
    default=int(ssl.PROTOCOL_TLS_SERVER),
    help="SSL version to use (see stdlib ssl module's)",
    show_default=True,
)
@click.option(
    "--ssl-cert-reqs",
    type=int,
    default=int(ssl.CERT_NONE),
    help="Whether client certificate is required (see stdlib ssl module's)",
    show_default=True,
)
@click.option(
    "--ssl-ca-certs",
    type=str,
    default=None,
    help="CA certificates file",
    show_default=True,
)
@click.option(
    "--ssl-ciphers",
    type=str,
    default="TLSv1",
    help="Ciphers to use (see stdlib ssl module's)",
    show_default=True,
)
@click.option(
    "--header",
    "headers",
    multiple=True,
    help="Specify custom default HTTP response headers as a Name:Value pair",
)
@click.option(
    "--version",
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Display the uvicorn version and exit.",
)
@click.option(
    "--h11-max-incomplete-event-size",
    "h11_max_incomplete_event_size",
    type=int,
    default=None,
    help="For h11, the maximum number of bytes to buffer of an incomplete event.",
)
def main(
    app,
    messagebus,
    event_handler_task_count,
    host: str,
    port: int,
    uds: str,
    fd: int,
    reload: bool,
    reload_dirs: list[str],
    reload_includes: list[str],
    reload_excludes: list[str],
    reload_delay: float,
    env_file: str,
    log_level: str,
    proxy_headers: bool,
    server_header: bool,
    date_header: bool,
    forwarded_allow_ips: str,
    root_path: str,
    limit_concurrency: int,
    backlog: int,
    limit_max_requests: int,
    timeout_keep_alive: int,
    timeout_graceful_shutdown: int | None,
    ssl_keyfile: str,
    ssl_certfile: str,
    ssl_keyfile_password: str,
    ssl_version: int,
    ssl_cert_reqs: int,
    ssl_ca_certs: str,
    ssl_ciphers: str,
    headers: list[str],
    h11_max_incomplete_event_size: int | None,
):
    current_dir = os.getcwd()
    sys.path.insert(0, current_dir)

    loaded_app = load_module(app)
    if not loaded_app:
        print("Failed to load ASGI application")
        return

    if messagebus:
        loaded_messagebus = load_module(messagebus)
        if not loaded_messagebus:
            print("Failed to load messagebus")
            return

    uvloop.install()
    uvloop.run(
        serve_app(
            app=loaded_app,
            messagebus=loaded_messagebus,
            task_count=event_handler_task_count,
            host=host,
            port=port,
            uds=uds,
            fd=fd,
            reload=reload,
            reload_dirs=reload_dirs,
            reload_includes=reload_includes,
            reload_excludes=reload_excludes,
            reload_delay=reload_delay,
            env_file=env_file,
            log_level=log_level,
            proxy_headers=proxy_headers,
            server_header=server_header,
            date_header=date_header,
            forwarded_allow_ips=forwarded_allow_ips,
            root_path=root_path,
            limit_concurrency=limit_concurrency,
            backlog=backlog,
            limit_max_requests=limit_max_requests,
            timeout_keep_alive=timeout_keep_alive,
            timeout_graceful_shutdown=timeout_graceful_shutdown,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile_password=ssl_keyfile_password,
            ssl_version=ssl_version,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_ciphers=ssl_ciphers,
            headers=headers,
            h11_max_incomplete_event_size=h11_max_incomplete_event_size,
        )
    )
