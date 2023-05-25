from fastapi import FastAPI

app = FastAPI()

@app.get("/welcome")
async def hello_world():
    return {"message": "Welcome to Python"}
