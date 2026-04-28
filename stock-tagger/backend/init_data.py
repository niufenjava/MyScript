import os
import csv
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

TAGS_FILE = DATA_DIR / "tags_library.csv"
STOCK_TAGS_FILE = DATA_DIR / "stock_tags.csv"

DEFAULT_TAGS = [
    {"tag_name": "珊瑚红", "color": "#FF6B6B"},
    {"tag_name": "橙黄", "color": "#FFA94D"},
    {"tag_name": "柠檬黄", "color": "#FFE066"},
    {"tag_name": "薄荷绿", "color": "#69DB7C"},
    {"tag_name": "天蓝", "color": "#4ECDC4"},
    {"tag_name": "海洋蓝", "color": "#45B7D1"},
    {"tag_name": "薰衣草紫", "color": "#B197FC"},
    {"tag_name": "玫红", "color": "#F06595"},
    {"tag_name": "墨灰", "color": "#868E96"},
    {"tag_name": "深墨", "color": "#1A1A2E"},
]

def init_tags_library():
    if not TAGS_FILE.exists() or TAGS_FILE.stat().st_size == 0:
        with open(TAGS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["tag_name", "color"])
            writer.writeheader()
            writer.writerows(DEFAULT_TAGS)
        print(f"Created {TAGS_FILE}")
    else:
        print(f"{TAGS_FILE} already exists, skipping.")

def init_stock_tags():
    if not STOCK_TAGS_FILE.exists() or STOCK_TAGS_FILE.stat().st_size == 0:
        with open(STOCK_TAGS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["stock_code", "stock_name", "tag_name", "created_at"])
            writer.writeheader()
        print(f"Created {STOCK_TAGS_FILE}")
    else:
        print(f"{STOCK_TAGS_FILE} already exists, skipping.")

if __name__ == "__main__":
    init_tags_library()
    init_stock_tags()
    print("Done.")
