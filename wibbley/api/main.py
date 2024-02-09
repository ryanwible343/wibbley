import argparse
import asyncio
import importlib.util
import os
import signal
import sys
from signal import SIGINT, SIGTERM

import uvicorn
import uvloop

from wibbley.event_driven.queue import wibbley_queue


def load_asgi_app(app_path: str):
    try:
        components = app_path.split(":")
        module_name = components[0]
        app_name = components[1]
    except IndexError:
        print(f"Invalid ASGI application path: {app_path}")
        return None
    try:
        # Try to import the module
        module = importlib.import_module(module_name)
    except ImportError:
        print(f"Failed to import module '{module_name}'")
        return None

    # Check if the module has the specified attribute (app_name)
    if hasattr(module, app_name):
        # Get the ASGI application object
        app = getattr(module, app_name)
        return app
    else:
        print(f"Module '{module_name}' does not contain attribute '{app_name}'")
        return None


def load_messagebus(messagebus_path: str):
    try:
        components = messagebus_path.split(":")
        module_name = components[0]
        messagebus_name = components[1]
        print("components", components)
    except IndexError:
        print(f"Invalid messagebus path: {messagebus_path}")
        return None
    try:
        # Try to import the module
        module = importlib.import_module(module_name)
    except ImportError:
        print(f"Failed to import module '{module_name}'")
        return None

    # Check if the module has the specified attribute (messagebus_name)
    if hasattr(module, messagebus_name):
        # Get the messagebus object
        messagebus = getattr(module, messagebus_name)
        return messagebus
    else:
        print(f"Module '{module_name}' does not contain attribute '{messagebus_name}'")
        return None


def get_command_line_arguments():
    parser = argparse.ArgumentParser(description="Load ASGI application from module")
    parser.add_argument(
        "--app", type=str, help="Name of the module containing the ASGI application"
    )
    parser.add_argument(
        "--messagebus",
        type=str,
        help="Name of the module containing the messagebus",
    )
    args = parser.parse_args()
    return args


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


async def serve_app(app, messagebus):
    loop = asyncio.get_event_loop()
    config = uvicorn.Config(app, loop="uvloop")
    server = uvicorn.Server(config)
    consumer_tasks = []
    if messagebus:
        consumer_tasks = [
            asyncio.create_task(read_from_queue(wibbley_queue, messagebus))
            for _ in range(100)
        ]
        server.install_signal_handlers = lambda: None
        try:
            for sig in [SIGINT, SIGTERM]:
                loop.add_signal_handler(
                    sig, shutdown_handler, server, consumer_tasks, sig
                )
        except NotImplementedError:  # pragma: no cover
            # Windows
            for sig in [SIGINT, SIGTERM]:
                signal.signal(
                    sig,
                    lambda sig, frame: shutdown_handler(server, consumer_tasks, sig),
                )
    api_task = [asyncio.create_task(server.serve())]
    tasks = consumer_tasks + api_task
    await asyncio.gather(*tasks)


def main():
    current_dir = os.getcwd()
    sys.path.insert(0, current_dir)

    args = get_command_line_arguments()

    if not args.app:
        print("Module name is required.")
        sys.exit(1)

    app = load_asgi_app(args.app)

    messagebus = None
    if args.messagebus:
        messagebus = load_messagebus(args.messagebus)

    if app:
        uvloop.install()
        try:
            uvloop.run(serve_app(app, messagebus))
        except KeyboardInterrupt:
            print("Shutting down 45")
    else:
        print("Failed to load ASGI application")
