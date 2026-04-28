import csv
import os
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

STOCK_TAGS_FILE = os.path.join(os.path.dirname(__file__), "../data/stock_tags.csv")
STOCK_BASE_INFO_FILE = "/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/data/stockBaseInfo.csv"
STOCK_MARKET_DATA_FILE = "/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/data/stockMarketData.csv"


def _read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _build_index(rows: list[dict], key_col: str) -> dict[str, dict]:
    index = {}
    for row in rows:
        k = row.get(key_col, "").strip()
        if k and k not in index:
            index[k] = row
    return index


def _get_or_null(row: dict, key: str):
    val = row.get(key)
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return val if val else None


@router.get("")
def get_selector_stocks(
    tag_name: str | None = Query(None),
    tags: str | None = Query(None),
    search: str | None = Query(None),
    market: str | None = Query(None),
    industry: str | None = Query(None),
    page: int | None = Query(None),
    page_size: int | None = Query(None),
):
    target_tags: set[str] = set()
    if tag_name:
        target_tags.add(tag_name.strip())
    if tags:
        for t in tags.split(","):
            t = t.strip()
            if t:
                target_tags.add(t)

    base_rows = _read_csv(STOCK_BASE_INFO_FILE)
    market_rows = _read_csv(STOCK_MARKET_DATA_FILE)
    base_index = _build_index(base_rows, "A股代码")
    market_index = _build_index(market_rows, "A股代码")
    stock_tags_rows = _read_csv(STOCK_TAGS_FILE)

    stock_tag_map: dict[str, list[str]] = {}
    stock_created_at_map: dict[str, str] = {}
    for row in stock_tags_rows:
        sc = row.get("stock_code", "").strip()
        tn = row.get("tag_name", "").strip()
        ca = row.get("created_at", "").strip()
        if not sc or not tn:
            continue
        stock_tag_map.setdefault(sc, []).append(tn)
        if sc not in stock_created_at_map or ca < stock_created_at_map[sc]:
            stock_created_at_map[sc] = ca

    if target_tags:
        matched_codes = {
            row.get("stock_code", "").strip()
            for row in stock_tags_rows
            if row.get("tag_name", "").strip() in target_tags
            and row.get("stock_code", "").strip()
        }
    else:
        matched_codes = None  # 无标签过滤时，返回全部股票

    result = []
    for sc, base in base_index.items():
        if matched_codes is not None and sc not in matched_codes:
            continue
        mkt = market_index.get(sc, {})
        tags_list = stock_tag_map.get(sc, [])
        if search:
            s = search.strip().lower()
            if s not in sc.lower() and s not in base.get("A股简称", "").lower():
                continue
        result.append({
            "stock_code": sc,
            "stock_name": base.get("A股简称", "").strip() or None,
            "market": base.get("所属市场", "").strip() or None,
            "industry": base.get("所属行业", "").strip() or None,
            "latest_price": _get_or_null(mkt, "最新价"),
            "total_market_cap": _get_or_null(mkt, "总市值(亿)"),
            "pe_ttm": _get_or_null(mkt, "市盈率TTM"),
            "roe": _get_or_null(mkt, "ROE"),
            "tags": tags_list,
            "created_at": stock_created_at_map.get(sc),
        })

    if market:
        m = market.strip()
        result = [r for r in result if r.get("market") == m]

    if industry:
        ind = industry.strip().lower()
        result = [r for r in result if r.get("industry") and ind in r["industry"].lower()]

    total = len(result)
    result.sort(key=lambda x: x["stock_code"])

    page = page if page is not None else 1
    page_size = page_size if page_size is not None else 30
    start = (page - 1) * page_size
    result = result[start:start + page_size]

    return {"total": total, "page": page, "page_size": page_size, "data": result}
