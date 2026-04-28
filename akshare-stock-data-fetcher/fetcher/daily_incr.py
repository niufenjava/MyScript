#!/usr/bin/env python3
"""
每日增量脚本：通过腾讯批量行情接口一次性获取全市场当日日线数据
相比 stock_zh_a_hist_daily_em.py（逐只拉取，~488s），本脚本约 20s 完成

原理：
  - 腾讯 qt.gtimg.cn 批量行情接口，80只/请求，5199只仅需 ~65 次请求（~12s）
  - 每只返回 88 个字段，包含当日 开/收/高/低/成交量/换手率/成交额/涨跌幅
  - 解析后直接 upsert 到现有 Parquet 文件，与全量脚本数据格式完全一致

适用场景：盘后每日增量更新（非首次全量拉取）
"""
from datetime import datetime
import os
import time

import pandas as pd
import requests

from fetcher._shared import (
    DATA_DIR as _DAILY_DIR,
    PROJECT_ROOT,
    get_latest_trade_date,
    load_stock_codes,
)
from utils.merge_daily import merge_incremental
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "daily")
BATCH_SIZE = 80  # 每批请求的股票数量


def fetch_all_daily(codes):
    """
    通过腾讯批量行情接口获取全部股票当日日线数据
    返回 {纯数字代码: {日期, 开盘, 收盘, 最高, 最低, 成交量, 换手率, 成交额, 涨跌幅}}
    """
    result = {}
    total = len(codes)

    for i in range(0, total, BATCH_SIZE):
        batch = codes[i:i + BATCH_SIZE]
        url = f"http://qt.gtimg.cn/q={','.join(batch)}"

        try:
            r = requests.get(url, timeout=15)
            for line in r.text.strip().split(";"):
                line = line.strip()
                if not line or "~" not in line:
                    continue
                f = line.split("~")
                if len(f) < 80:
                    continue

                pure_code = f[2]  # 纯数字代码
                close = float(f[3])
                open_price = float(f[5])
                high = float(f[33])
                low = float(f[34])
                volume = float(f[36])
                amount_wan = float(f[37])
                turnover = float(f[38])
                change_pct = float(f[32])
                trade_dt = f[30][:8]

                if close == 0 and volume == 0:
                    continue  # 跳过停牌

                result[pure_code] = {
                    "日期": f"{trade_dt[:4]}-{trade_dt[4:6]}-{trade_dt[6:8]}",
                    "开盘": open_price,
                    "收盘": close,
                    "最高": high,
                    "最低": low,
                    "成交量": volume,
                    "换手率": turnover,
                    "成交额": amount_wan * 10000,
                    "涨跌幅(%)": change_pct,
                }
        except Exception as e:
            print(f"  批次 {i // BATCH_SIZE + 1} 请求失败: {e}")

        done = min(i + BATCH_SIZE, total)
        if done % 1000 == 0 or done == total:
            print(f"  [{done}/{total}] 已获取行情数据")

    return result


def upsert_parquet(code, new_row):
    """将一条日线数据 upsert 到 Parquet 文件（快速跳过已有日期）"""
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    new_date = datetime.strptime(new_row["日期"], "%Y-%m-%d").date()
    new_df = pd.DataFrame([new_row])
    new_df["日期"] = pd.to_datetime(new_df["日期"]).dt.date

    if os.path.exists(path):
        old_dates = pd.read_parquet(path, columns=["日期"])
        if new_date in old_dates["日期"].values:
            return 0

        old_df = pd.read_parquet(path)
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged.sort_values("日期", inplace=True)
        merged.reset_index(drop=True, inplace=True)
        merged.to_parquet(path, index=False)
        return 1
    else:
        new_df.to_parquet(path, index=False)
        return 1


def main():
    print("=" * 60)
    print("每日增量更新 - 腾讯批量行情接口")
    print("=" * 60)

    trade_date = get_latest_trade_date()
    trade_date_str = trade_date.strftime("%Y-%m-%d")
    print(f"\n目标交易日: {trade_date_str}")

    codes = load_stock_codes(prefixed=True)  # sh/sz/bj 前缀
    print(f"股票总数: {len(codes)}")

    # 第1步：获取行情
    print(f"\n[第1步] 批量获取当日行情...")
    t0 = time.time()
    daily_data = fetch_all_daily(codes)
    fetch_time = time.time() - t0
    print(f"获取完成: {len(daily_data)} 只有效数据, 耗时 {fetch_time:.1f}s")

    # 第2步：写入 Parquet
    print(f"\n[第2步] 写入 Parquet 文件...")
    os.makedirs(DATA_DIR, exist_ok=True)
    t0 = time.time()
    new_count = skip_count = err_count = 0

    for code_with_prefix in codes:
        pure_code = code_with_prefix.replace("sh", "").replace("sz", "").replace("bj", "")
        if pure_code not in daily_data:
            skip_count += 1
            continue
        try:
            added = upsert_parquet(pure_code, daily_data[pure_code])
            if added > 0:
                new_count += 1
        except Exception as e:
            err_count += 1
            if err_count <= 10:
                print(f"  [ERR] {pure_code}: {e}")

    write_time = time.time() - t0
    print(f"写入完成: 新增 {new_count} 只, 跳过 {skip_count} 只, 失败 {err_count} 只, "
          f"耗时 {write_time:.1f}s")

    # 第3步：增量合并大表
    print(f"\n[第3步] 增量合并大表...")
    t0 = time.time()
    new_rows = []
    for code_with_prefix in codes:
        pure_code = code_with_prefix.replace("sh", "").replace("sz", "").replace("bj", "")
        if pure_code in daily_data:
            row = daily_data[pure_code].copy()
            row["代码"] = pure_code
            new_rows.append(row)
    merge_incremental(new_rows)
    merge_time = time.time() - t0

    total_time = fetch_time + write_time + merge_time
    print(f"\n{'=' * 60}")
    print(f"全部完成! 总耗时 {total_time:.1f}s")
    print(f"  获取行情: {fetch_time:.1f}s  写入文件: {write_time:.1f}s  合并大表: {merge_time:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
