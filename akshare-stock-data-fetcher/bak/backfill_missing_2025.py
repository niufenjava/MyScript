"""
批量补数脚本：所有缺少2025年数据的股票，强制从 2025-01-01 拉取
"""
import os, sys, time, random, re
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data", "daily")
FULL_START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

TX_API_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
NUM_WORKERS = 16

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
            import requests
            r = requests.get(TX_API_URL, params=params, timeout=10)
            if r.status_code != 200:
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

def backfill_one(code):
    tx_symbol = to_tx_symbol(code)
    real_start = datetime.strptime(FULL_START_DATE, "%Y-%m-%d") - timedelta(days=30)
    start_year = real_start.year
    end_year = datetime.strptime(END_DATE, "%Y-%m-%d").year
    all_rows = _fetch_kline_raw(tx_symbol, start_year, end_year, "qfq")
    df = _parse_to_dataframe(all_rows, FULL_START_DATE, END_DATE)
    if df.empty:
        print(f"[WARN] {code} 无数据")
        return code, 0
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    # 先备份原文件
    bak_path = path + ".bak2025"
    if os.path.exists(path) and not os.path.exists(bak_path):
        import shutil
        shutil.copy2(path, bak_path)
    df.to_parquet(path, index=False)
    print(f"[OK] {code} {len(df)} 条 {df['日期'].min()} ~ {df['日期'].max()}")
    return code, len(df)

if __name__ == '__main__':
    missing_codes = [
        "000001","000333","000858","001220","001257","001312",
        "002415","002594","300750","301513","301666","301680",
        "301682","301683","301696","600036","600519","601012",
        "601112","601318","603284","603293","603352","603402",
        "603459","688712","688781","688785","688808","688811",
        "688813","688816","688818","688820"
    ]

    print(f"开始补数，共 {len(missing_codes)} 只股票，并发 {NUM_WORKERS}")
    t0 = time.time()
    success, fail = 0, []

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = {pool.submit(backfill_one, code): code for code in missing_codes}
        for future in as_completed(futures):
            code, n = future.result()
            if n > 0:
                success += 1
            else:
                fail.append(code)

    print(f"\n完成，耗时 {time.time()-t0:.0f}s，成功 {success}，失败 {len(fail)}")
    if fail:
        print(f"失败: {fail}")
