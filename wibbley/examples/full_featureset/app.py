import logging

import orjson

from wibbley.app import App

app = App()


@app.get("/hello")
async def hello(request):
    return "hello world"
