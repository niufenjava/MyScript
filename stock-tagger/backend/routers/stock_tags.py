import csv
import fcntl
import os
from datetime import date
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

STOCK_TAGS_FILE = os.path.join(os.path.dirname(__file__), "../data/stock_tags.csv")
STOCK_BASE_INFO_FILE = "/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/data/stockBaseInfo.csv"


def _get_stock_name(stock_code: str) -> str:
    if not os.path.exists(STOCK_BASE_INFO_FILE):
        return ""
    with open(STOCK_BASE_INFO_FILE, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("A股代码", "").strip() == stock_code:
                return row.get("A股简称", "").strip()
    return ""


def _read_all() -> list[dict]:
    if not os.path.exists(STOCK_TAGS_FILE):
        return []
    with open(STOCK_TAGS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _write_all(rows: list[dict]):
    with open(STOCK_TAGS_FILE, "w", newline="", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            writer = csv.DictWriter(f, fieldnames=["stock_code", "stock_name", "tag_name", "created_at"])
            writer.writeheader()
            writer.writerows(rows)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


@router.get("")
def get_stock_tags(
    tag_name: str | None = Query(None),
    stock_code: str | None = Query(None),
):
    rows = _read_all()
    if tag_name:
        rows = [r for r in rows if r["tag_name"] == tag_name]
    if stock_code:
        rows = [r for r in rows if r["stock_code"] == stock_code]
    return rows


@router.post("")
def add_stock_tag(stock_code: str = Query(...), tag_name: str = Query(...)):
    stock_code = stock_code.strip()
    tag_name = tag_name.strip()

    if not stock_code or not tag_name:
        raise HTTPException(status_code=400, detail="stock_code and tag_name are required")

    stock_name = _get_stock_name(stock_code)

    rows = _read_all()
    for row in rows:
        if row["stock_code"] == stock_code and row["tag_name"] == tag_name:
            raise HTTPException(status_code=409, detail="该股票已有此标签")

    new_row = {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "tag_name": tag_name,
        "created_at": date.today().isoformat(),
    }
    rows.append(new_row)
    _write_all(rows)
    return new_row


@router.delete("")
def remove_stock_tag(
    stock_code: str = Query(...),
    tag_name: str = Query(...),
):
    rows = _read_all()
    original_len = len(rows)
    rows = [r for r in rows if not (r["stock_code"] == stock_code and r["tag_name"] == tag_name)]

    if len(rows) == original_len:
        raise HTTPException(status_code=404, detail="该股票标签组合不存在")

    _write_all(rows)
    return {"message": "标签已移除"}
