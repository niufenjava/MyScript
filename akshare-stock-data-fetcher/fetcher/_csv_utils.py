# -*- coding: utf-8 -*-
"""
fetcher 内部 CSV 读写工具
供 info_base.py / info_market.py / finance.py 等模块使用
"""
from __future__ import annotations

import csv
import os
from typing import Dict, List, Optional


def load_existing_csv(path: str) -> Dict[str, Dict]:
    """
    加载已有 CSV，返回 {代码: 行dict}。
    第一列（如'A股代码'）作为 key。
    """
    if not os.path.exists(path):
        return {}
    result: Dict[str, Dict] = {}
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = str(row.get("股票代码", "") or row.get("A股代码", "")).strip()
            if key:
                result[key] = dict(row)
    return result


def save_csv(rows: List[Dict], path: str, fields: List[str], mode: str = "w") -> None:
    """
    写入 CSV。
    - mode="w": 覆盖写入（包含 header）
    - mode="a": 追加写入（不写 header，直接 append 行）
    若文件已存在且 mode="w"，先备份为 .bak。
    """
    if mode == "w":
        _backup(path)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    else:
        with open(path, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            for row in rows:
                writer.writerow(row)


def _backup(path: str) -> None:
    """若文件存在，写入 .bak 备份"""
    if os.path.exists(path):
        with open(path, "rb") as src, open(f"{path}.bak", "wb") as dst:
            dst.write(src.read())
