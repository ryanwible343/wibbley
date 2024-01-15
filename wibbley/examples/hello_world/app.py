from wibbley.app import App

app = App()


@app.get("/")
async def hello_world():
    return "Hello World!"
