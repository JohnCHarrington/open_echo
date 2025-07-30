import asyncio
from contextlib import asynccontextmanager
from echo import EchoSettings, EchoReader
import logging
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse


from fastapi.staticfiles import StaticFiles


log = logging.getLogger("uvicorn")

settings = EchoSettings()


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        log.info(f"WebSocket connected: {websocket.client}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast_json(self, data):
        for connection in self.active_connections:
            await connection.send_json(data)


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    with EchoReader() as reader:
        asyncio.create_task(reader.aread_echo(manager.broadcast_json))
        yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # Just here to keep the connection alive
    except Exception as e:
        log.error(f"WebSocket closed: {e}")
    finally:
        await manager.disconnect(websocket)


@app.get("/")
async def home(request: Request):
    return HTMLResponse(open("frontend.html").read())
