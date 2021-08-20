from typing import List, Optional

import uvicorn
from fastapi import FastAPI, responses
from aiohttp import ClientSession
from pydantic import BaseModel


class App(FastAPI):
    def __init__(self, **extra):
        super().__init__(**extra)
        self.session = None


app = App(
    title="lnx demo app!",
    description="A test api that sends requests to lnx",
    docs_url=None,
    redoc_url="/docs"
)

with open("index.html", "r", encoding="utf-8") as file:
    html = file.read()


@app.on_event("startup")
async def start():
    app.session = ClientSession()


@app.on_event("shutdown")
async def start():
    if app.session is not None:
        await app.session.close()


@app.get("/")
def serve_html() -> responses.HTMLResponse:
    return responses.HTMLResponse(html)


class Hit(BaseModel):
    doc: dict
    ratio: Optional[float]
    document_id: str


class SearchData(BaseModel):
    count: int
    hits: List[Hit]
    time_taken: float


class SearchResults(BaseModel):
    data: SearchData
    status: int


class DataPayload(BaseModel):
    query: str
    url: str


@app.post("/check")
async def check(payload: DataPayload) -> dict:
    try:
        async with app.session.get(payload.url, params={"query": payload.query}) as resp:
            return {"status": resp.status}
    except Exception:
        return {"status": -1}


@app.post("/search", response_model=SearchResults)
async def search(payload: DataPayload) -> SearchResults:
    async with app.session.get(payload.url, params={"query": payload.query}) as resp:
        return await resp.json()


if __name__ == '__main__':
    uvicorn.run("main:app", port=5000)



