from wibbley.api import App

app = App()


@app.get("/hello")
async def hello(request):
    return "hello world"
