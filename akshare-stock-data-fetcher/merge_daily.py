"""
合并所有单股 Parquet 文件为一张大表 all_daily.parquet
用途：供跨股筛选查询使用（如"近3天缩量的票"）
用法：python merge_daily.py
"""
import os
import time
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAILY_DIR = os.path.join(SCRIPT_DIR, "data", "daily")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "data", "all_daily.parquet")


def merge_all():
    """读取 data/daily/*.parquet，合并为一张大表，新增"代码"列"""
    files = [f for f in os.listdir(DAILY_DIR) if f.endswith(".parquet")]
    if not files:
        print("data/daily/ 下没有 parquet 文件")
        return

    print(f"正在合并 {len(files)} 个 parquet 文件...")
    t = time.time()

    dfs = []
    for f in files:
        code = f.replace(".parquet", "")
        df = pd.read_parquet(os.path.join(DAILY_DIR, f))
        df["代码"] = code
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)
    merged["日期"] = pd.to_datetime(merged["日期"])
    merged.sort_values(["代码", "日期"], inplace=True)
    merged.reset_index(drop=True, inplace=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    merged.to_parquet(OUTPUT_PATH, index=False)

    elapsed = time.time() - t
    print(f"合并完成: {len(files)} 只股票, {len(merged)} 条记录 → {OUTPUT_PATH}")
    print(f"文件大小: {os.path.getsize(OUTPUT_PATH) / 1024 / 1024:.1f} MB, 耗时: {elapsed:.1f}s")


if __name__ == "__main__":
    merge_all()
