import logging

import orjson

from wibbley import App

app = App()


@app.get("/hello")
async def hello(request):
    return "hello world"
