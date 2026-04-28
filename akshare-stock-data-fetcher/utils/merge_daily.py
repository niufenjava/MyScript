# -*- coding: utf-8 -*-
"""
合并所有单股 Parquet 文件为一张大表 all_daily.parquet
用途：供跨股筛选查询使用（如"近3天缩量的票"）

用法：
  python -m fetcher.merge_daily        # 完整合并（默认）
  python -m fetcher.merge_daily --incr  # 增量追加（新数据追加到现有大表）

run.py merge 默认走完整合并；daily_incr.py 盘后自动走增量追加。
"""

import os
import time
import argparse
import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAILY_DIR   = os.path.join(PROJECT_DIR, "data", "daily")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "data", "all_daily.parquet")


def merge_all():
    """
    完整合并：读取 data/daily/*.parquet，合并为一张大表。
    新增"代码"列，按 [代码, 日期] 排序，去重（保留最新）。
    """
    files = [f for f in os.listdir(DAILY_DIR) if f.endswith(".parquet")]
    if not files:
        print("data/daily/ 下没有 parquet 文件，跳过。")
        return

    print(f"正在完整合并 {len(files)} 个 parquet 文件...")
    t = time.time()

    dfs = []
    for f in files:
        code = f.replace(".parquet", "")
        df = pd.read_parquet(os.path.join(DAILY_DIR, f))
        df["代码"] = code
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)
    merged["日期"] = pd.to_datetime(merged["日期"])
    merged.drop_duplicates(subset=["代码", "日期"], keep="last", inplace=True)
    merged.sort_values(["代码", "日期"], inplace=True)
    merged.reset_index(drop=True, inplace=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    merged.to_parquet(OUTPUT_PATH, index=False)

    elapsed = time.time() - t
    size_mb = os.path.getsize(OUTPUT_PATH) / 1024 / 1024
    print(f"✅ 完整合并完成: {len(files)} 只股票, {len(merged)} 条记录, {size_mb:.1f} MB, {elapsed:.1f}s")


def merge_incremental(new_rows: list):
    """
    增量追加：将 new_rows（当日采集的日线数据）追加到现有大表。
    - 大表不存在 → 直接写入
    - 大表已存在 → 追加 + 去重（保留最新）
    """
    if not new_rows:
        print("无新数据，跳过增量合并。")
        return

    append_df = pd.DataFrame(new_rows).copy()
    append_df["日期"] = pd.to_datetime(append_df["日期"]).dt.date

    if not os.path.exists(OUTPUT_PATH):
        # 大表不存在，直接写入
        append_df["日期"] = pd.to_datetime(append_df["日期"])
        append_df.sort_values(["代码", "日期"], inplace=True)
        append_df.reset_index(drop=True, inplace=True)
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        append_df.to_parquet(OUTPUT_PATH, index=False)
        print(f"✅ 增量合并完成（新建）: {len(append_df)} 条 → {OUTPUT_PATH}")
        return

    # 大表存在，追加 + 去重
    all_df = pd.read_parquet(OUTPUT_PATH)
    merged = pd.concat([all_df, append_df], ignore_index=True)
    merged.drop_duplicates(subset=["代码", "日期"], keep="last", inplace=True)
    merged.sort_values(["代码", "日期"], inplace=True)
    merged.reset_index(drop=True, inplace=True)
    merged.to_parquet(OUTPUT_PATH, index=False)

    print(f"✅ 增量合并完成: {len(merged)} 条（+{len(append_df)} 条）")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--incr", action="store_true", help="增量追加模式（供 daily_incr 内部调用）")
    args = parser.parse_args()

    if args.incr:
        print("增量追加模式需要由调用方传入 new_rows，请使用 python -m fetcher.merge_daily 做完整合并。")
    else:
        merge_all()
