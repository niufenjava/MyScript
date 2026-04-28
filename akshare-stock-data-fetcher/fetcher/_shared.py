# -*- coding: utf-8 -*-
"""
fetcher 内部共享工具函数
供 fetcher/ 下所有模块使用
"""
from __future__ import annotations

import os
import csv
from datetime import date, datetime
from typing import List

import akshare as ak

# ── 路径常量 ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
STOCK_CODES_PATH = os.path.join(DATA_DIR, "stock_codes.txt")


# ── 股票代码工具 ─────────────────────────────────────────────────────────────

def strip_prefix(code: str) -> str:
    """去掉 sh/sz/bj 前缀，返回纯6位数字代码"""
    return code.replace("sh", "").replace("sz", "").replace("bj", "")


def load_stock_codes(prefixed: bool = True) -> List[str]:
    """
    读取 stock_codes.txt。
    - prefixed=True  → 返回带 sh/sz/bj 前缀的代码（用于腾讯接口）
    - prefixed=False → 返回纯6位数字代码
    """
    if not os.path.exists(STOCK_CODES_PATH):
        return []
    codes = []
    with open(STOCK_CODES_PATH, "r", encoding="utf-8") as f:
        for line in f:
            code = line.strip()
            if not code:
                continue
            if prefixed:
                code = code.zfill(6)
                if code.startswith(("60", "68")):
                    codes.append(f"sh{code}")
                elif code.startswith(("00", "30")):
                    codes.append(f"sz{code}")
                elif code.startswith(("4", "8")):
                    codes.append(f"bj{code}")  # 北交所
                else:
                    codes.append(f"sz{code}")
            else:
                codes.append(strip_prefix(code))
    return codes


# ── 交易日工具 ───────────────────────────────────────────────────────────────

def get_latest_trade_date(ref_date: date | None = None) -> date:
    """
    获取最近交易日（默认今天）。
    ref_date 用于指定参考日期（测试/回溯用）。
    """
    today = ref_date or date.today()
    try:
        cal = ak.tool_trade_date_hist_sina()
        trade_dates = sorted(cal["trade_date"].tolist())
        if today in trade_dates:
            return today
        past = [d for d in trade_dates if d <= today]
        return past[-1] if past else today
    except Exception:
        return today
