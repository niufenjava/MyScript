"""
一次性补数脚本：针对指定股票强制从 FULL_START_DATE 开始拉取
"""
import os
import sys
import time
import random
import re
import pandas as pd
from datetime import datetime, timedelta

# 加入项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data", "daily")
FULL_START_DATE = "2025-01-01"

# 腾讯接口
TX_API_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"

def to_tx_symbol(code):
    code = str(code).zfill(6)
    if code.startswith(("60", "68")):
        return f"sh{code}"
    elif code.startswith(("00", "30")):
        return f"sz{code}"
    elif code.startswith(("4", "8")):
        return f"bj{code}"
    return f"sz{code}"

def _fetch_kline_raw(tx_symbol, start_year, end_year, adjust="qfq"):
    all_rows = []
    for year in range(start_year, end_year + 1):
        params = {
            "_var": f"kline_day{adjust}{year}",
            "param": f"{tx_symbol},day,{year}-01-01,{year + 1}-12-31,640,{adjust}",
        }
        try:
            r = requests_get(params)
            if r is None:
                continue
            text = r.text
            rows = re.findall(r'\[([^\[\]]+)\]', text)
            for row in rows:
                cleaned_row = re.sub(r'\{[^}]*\}', '', row)
                parts = [p.strip().strip('"') for p in cleaned_row.split(",")]
                parts = [p for p in parts if p]
                if len(parts) < 6 or len(parts[0]) != 10 or parts[0][4] != '-':
                    continue
                all_rows.append(parts)
        except Exception:
            continue
        time.sleep(random.uniform(0.3, 0.8))
    seen = {}
    for row in all_rows:
        seen[row[0]] = row
    return list(seen.values())

def _parse_to_dataframe(all_rows, start_date, end_date):
    if not all_rows:
        return pd.DataFrame()
    records = []
    for row in all_rows:
        rec = {
            "日期": row[0],
            "开盘": float(row[1]),
            "收盘": float(row[2]),
            "最高": float(row[3]),
            "最低": float(row[4]),
            "成交量": float(row[5]),
        }
        rec["换手率"] = float(row[6]) if len(row) > 6 else 0.0
        rec["成交额"] = float(row[7]) if len(row) > 7 else 0.0
        records.append(rec)
    df = pd.DataFrame(records)
    df["日期"] = pd.to_datetime(df["日期"]).dt.date
    df.sort_values("日期", inplace=True)
    df["涨跌幅(%)"] = (df["收盘"].pct_change() * 100).round(2)
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    df = df[(df["日期"] >= start_dt) & (df["日期"] <= end_dt)].copy()
    df.reset_index(drop=True, inplace=True)
    return df

def requests_get(params):
    import requests
    return requests.get(TX_API_URL, params=params, timeout=10)

def fetch_and_save(code, start_date, end_date):
    tx_symbol = to_tx_symbol(code)
    real_start = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=30)
    start_year = real_start.year
    end_year = datetime.strptime(end_date, "%Y-%m-%d").year
    all_rows = _fetch_kline_raw(tx_symbol, start_year, end_year, "qfq")
    df = _parse_to_dataframe(all_rows, start_date, end_date)
    if df.empty:
        print(f"[WARN] {code} 未拉到任何数据")
        return
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    df.to_parquet(path, index=False)
    print(f"[OK] {code} 写入 {len(df)} 条，范围 {df['日期'].min()} ~ {df['日期'].max()}")

if __name__ == '__main__':
    target = "603659"
    end_date = "2025-12-31"
    print(f"强制拉取 {target} 从 {FULL_START_DATE} 到 {end_date}")
    fetch_and_save(target, FULL_START_DATE, end_date)
