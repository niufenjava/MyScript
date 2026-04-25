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

import os
import time
import requests
import pandas as pd
import akshare as ak
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data", "daily")
BATCH_SIZE = 80  # 每批请求的股票数量


def load_stock_codes():
    """从 stock_codes.txt 读取股票代码（带 sh/sz 前缀）"""
    path = os.path.join(SCRIPT_DIR, 'data' ,'stock_codes.txt')
    codes = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                codes.append(line)
    return codes


def get_latest_trade_date():
    """获取最近的交易日"""
    today = datetime.now().date()
    try:
        calendar_df = ak.tool_trade_date_hist_sina()
        trade_dates = sorted(calendar_df['trade_date'].tolist())
        if today in trade_dates:
            return today
        past = [d for d in trade_dates if d <= today]
        if past:
            last_trade = past[-1]
            print(f"今天({today})不是交易日，使用最近交易日: {last_trade}")
            return last_trade
    except Exception as e:
        print(f"获取交易日历失败: {e}")
    return None


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
            for line in r.text.strip().split(';'):
                line = line.strip()
                if not line or '~' not in line:
                    continue
                f = line.split('~')
                if len(f) < 80:
                    continue

                pure_code = f[2]        # 纯数字代码
                close = float(f[3])     # 最新价/收盘
                open_price = float(f[5])  # 今开
                high = float(f[33])     # 最高
                low = float(f[34])      # 最低
                volume = float(f[36])   # 成交量(手)
                amount_wan = float(f[37])  # 成交额(万元)
                turnover = float(f[38])  # 换手率(%)
                change_pct = float(f[32])  # 涨跌幅(%)
                trade_dt = f[30][:8]     # 日期 YYYYMMDD

                # 跳过无效数据（未开盘/停牌：成交量为0且价格为0）
                if close == 0 and volume == 0:
                    continue

                result[pure_code] = {
                    "日期": f"{trade_dt[:4]}-{trade_dt[4:6]}-{trade_dt[6:8]}",
                    "开盘": open_price,
                    "收盘": close,
                    "最高": high,
                    "最低": low,
                    "成交量": volume,
                    "换手率": turnover,
                    "成交额": amount_wan * 10000,  # 万元 → 元，与全量脚本一致
                    "涨跌幅(%)": change_pct,
                }
        except Exception as e:
            print(f"  批次 {i // BATCH_SIZE + 1} 请求失败: {e}")

        done = min(i + BATCH_SIZE, total)
        if done % 1000 == 0 or done == total:
            print(f"  [{done}/{total}] 已获取行情数据")

    return result


def upsert_parquet(code, new_row):
    """将一条日线数据 upsert 到对应的 Parquet 文件（快速跳过已有日期）"""
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    new_date = datetime.strptime(new_row["日期"], "%Y-%m-%d").date()
    new_df = pd.DataFrame([new_row])
    new_df["日期"] = pd.to_datetime(new_df["日期"]).dt.date

    if os.path.exists(path):
        # 只读日期列，快速判断是否需要写入
        old_df = pd.read_parquet(path, columns=["日期"])
        if new_date in old_df["日期"].values:
            return 0  # 已有该日期数据，跳过

        # 需要新增：完整读取 + 追加
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

    # 1. 获取交易日
    trade_date = get_latest_trade_date()
    if not trade_date:
        print("无法获取交易日，退出")
        return
    trade_date_str = trade_date.strftime("%Y-%m-%d")
    print(f"\n目标交易日: {trade_date_str}")

    # 2. 加载股票代码
    codes = load_stock_codes()
    print(f"股票总数: {len(codes)}")

    # 3. 批量获取当日行情
    print(f"\n[第1步] 批量获取当日行情...")
    t0 = time.time()
    daily_data = fetch_all_daily(codes)
    fetch_time = time.time() - t0
    print(f"获取完成: {len(daily_data)} 只有效数据, 耗时 {fetch_time:.1f}s")

    # 4. 写入 Parquet
    print(f"\n[第2步] 写入 Parquet 文件...")
    os.makedirs(DATA_DIR, exist_ok=True)
    t0 = time.time()
    new_count = 0
    skip_count = 0
    err_count = 0

    for code_with_prefix in codes:
        pure_code = code_with_prefix.replace('sh', '').replace('sz', '')
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
    print(f"写入完成: 新增 {new_count} 只, 跳过 {skip_count} 只(停牌/无数据), "
          f"失败 {err_count} 只, 耗时 {write_time:.1f}s")

    # 5. 增量合并大表（只追加新数据，不全量重建）
    print(f"\n[第3步] 增量合并大表...")
    t0 = time.time()
    try:
        all_parquet = os.path.join(SCRIPT_DIR, "data", "all_daily.parquet")
        if new_count > 0 and os.path.exists(all_parquet):
            # 有新数据：追加到大表
            all_df = pd.read_parquet(all_parquet)
            new_rows = []
            for code_with_prefix in codes:
                pure_code = code_with_prefix.replace('sh', '').replace('sz', '')
                if pure_code in daily_data:
                    row = daily_data[pure_code].copy()
                    row["代码"] = pure_code
                    new_rows.append(row)
            if new_rows:
                append_df = pd.DataFrame(new_rows)
                append_df["日期"] = pd.to_datetime(append_df["日期"]).dt.date
                merged = pd.concat([all_df, append_df], ignore_index=True)
                merged.drop_duplicates(subset=["代码", "日期"], keep="last", inplace=True)
                merged.sort_values(["代码", "日期"], inplace=True)
                merged.reset_index(drop=True, inplace=True)
                merged.to_parquet(all_parquet, index=False)
                print(f"增量追加完成: {len(merged)} 条记录")
        else:
            # 无大表或首次：全量重建
            from utils.merge_daily import merge_all
            merge_all()
    except Exception as e:
        print(f"合并失败: {e}")
        # 降级为全量重建
        try:
            from utils.merge_daily import merge_all
            merge_all()
        except Exception:
            pass
    merge_time = time.time() - t0

    # 6. 汇总
    total_time = fetch_time + write_time + merge_time
    print(f"\n{'=' * 60}")
    print(f"全部完成! 总耗时 {total_time:.1f}s")
    print(f"  获取行情: {fetch_time:.1f}s")
    print(f"  写入文件: {write_time:.1f}s")
    print(f"  合并大表: {merge_time:.1f}s")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
