from wibbley.api import App

app = App()


@app.get("/hello")
async def hello():
    return {"message": "Hello, World!"}
