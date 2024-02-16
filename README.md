# Wibbley

An asynchronous framework/toolkit for event-driven architecture

Here's what you get:
- Fully asynchronous http api framework built on uvicorn and uvloop (so it's fast)
- Fast json serialization/deserialization using orjson
- In-process message handling with message routing and acknowledgement toolkit
- Ability to run api and messagebus simultaneously
- Make your message handling bulletproof with auto-magic inbox/outbox pattern

# Suitability
Wibbley is designed for applications that are heavily I/O bound. It uses `await` to handle multiple
http requests concurrently, as well as handling messages produced by those events. If you have long,
synchronous blocks of cpu bound code, this is not the framework for you, as performance will degrade
significantly.

# Requirements
python 3.8+

# Installation
pip install wibbley

# API Example
### app.py
```python
from wibbley.api import App

app = App()

@app.get("/")
async def hello():
    return {"message": "Hello, World!"}
```
Run the application:
```python
wibbley --app app:app
```

# API + Messagebus Example
### messagebus.py
```python
from wibbley.event_driven import Command, Event, Messagebus, publish

class UpdateRecordCommand(Command):
    pass

class RecordUpdatedEvent(Event):
    pass

messagebus = Messagebus()

@messagebus.listen(UpdateRecordCommand)
async def update_record(command):
    print(f"Command received: {command}")

    # Update the record

    record_updated_event = RecordUpdatedEvent()
    await publish(record_updated_event)

@messagebus.listen(RecordUpdatedEvent)
async def send_email(event):
    print(f"Event received: {event}")

    # Send an email that a record has been updated

    return None
```
### app.py
```python
from wibbley.api import App
from messagebus import messagebus, UpdateRecordCommand

app = App()

@app.post("/update_record")
async def update_record():
    update_record_command = UpdateRecordCommand()
    messagebus.handle(update_record_command)
    return {"message": "Record Updated!"}
```
Run the application:
```python
wibbley --app app:app --messagebus messagebus:messagebus
```
# In-Depth Examples
For more in-depth examples, including using the inbox/outbox pattern. Please see the examples
directory. Each example is containerized and can be run using docker compose.

# Dependencies
- [uvloop](https://github.com/MagicStack/uvloop)
- [uvicorn](https://github.com/encode/uvicorn)
- [orjson](https://github.com/ijl/orjson)
- [click](https://github.com/pallets/click)

# API Documentation
