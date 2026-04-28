import csv
import fcntl
import os
from fastapi import APIRouter, HTTPException

router = APIRouter()

TAGS_FILE = os.path.join(os.path.dirname(__file__), "../data/tags_library.csv")
STOCK_TAGS_FILE = os.path.join(os.path.dirname(__file__), "../data/stock_tags.csv")


def _load_tags():
    if not os.path.exists(TAGS_FILE):
        return {}
    with open(TAGS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["tag_name"]: row["color"] for row in reader}


def _save_tags(tags: dict):
    with open(TAGS_FILE, "w", newline="", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            writer = csv.writer(f)
            writer.writerow(["tag_name", "color"])
            for tag_name, color in tags.items():
                writer.writerow([tag_name, color])
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def _tag_in_use(tag_name: str) -> bool:
    if not os.path.exists(STOCK_TAGS_FILE):
        return False
    with open(STOCK_TAGS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["tag_name"] == tag_name:
                return True
    return False


@router.get("")
def get_tags():
    tags = _load_tags()
    return [{"tag_name": k, "color": v} for k, v in tags.items()]


@router.post("")
def create_tag(payload: dict):
    tag_name = payload.get("tag_name", "").strip()
    color = payload.get("color", "").strip()
    if not tag_name or not color:
        raise HTTPException(status_code=400, detail="tag_name and color are required")

    tags = _load_tags()
    if tag_name in tags:
        raise HTTPException(status_code=409, detail="标签已存在")

    tags[tag_name] = color
    _save_tags(tags)
    return {"tag_name": tag_name, "color": color}


@router.put("/{tag_name}")
def update_tag_color(tag_name: str, payload: dict):
    color = payload.get("color", "").strip()
    if not color:
        raise HTTPException(status_code=400, detail="color is required")

    tags = _load_tags()
    if tag_name not in tags:
        raise HTTPException(status_code=404, detail="标签不存在")

    tags[tag_name] = color
    _save_tags(tags)
    return {"tag_name": tag_name, "color": color}


@router.delete("/{tag_name}")
def delete_tag(tag_name: str):
    tags = _load_tags()
    if tag_name not in tags:
        raise HTTPException(status_code=404, detail="标签不存在")

    if _tag_in_use(tag_name):
        raise HTTPException(status_code=409, detail="该标签已被使用，无法删除")

    del tags[tag_name]
    _save_tags(tags)
    return {"message": f"标签 {tag_name} 已删除"}