from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import tags, stock_tags, selector

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(tags.router, prefix="/api/tags", tags=["tags"])
app.include_router(stock_tags.router, prefix="/api/stock-tags", tags=["stock-tags"])
app.include_router(selector.router, prefix="/api/selector", tags=["selector"])


@app.get("/")
def root():
    return {"message": "stock-tagger API"}
