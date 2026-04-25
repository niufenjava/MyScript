#!/usr/bin/env python3
"""
桥接脚本: 将 all_daily.parquet 转换为 qing-skills technical-analysis 的 data.json 格式

用法:
    python parquet-to-tech-analysis.py <股票代码> [--days N] [--date YYYY-MM-DD]
    python parquet-to-tech-analysis.py 600519 --date 2026-04-24 --days 120
"""
import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

# === 配置 ===
PARQUET_PATH = "/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/data/all_daily.parquet"
DEFAULT_DAYS = 120


def load_parquet() -> pd.DataFrame:
    """加载 parquet 数据"""
    df = pd.read_parquet(PARQUET_PATH)
    # 列名标准化：中文 → 英文
    col_map = {
        '日期': 'date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '涨跌幅(%)': 'pct_chg',
        '代码': 'code'
    }
    df = df.rename(columns=col_map)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    # 股票代码统一为 6 位字符串
    df['code'] = df['code'].astype(str).str.zfill(6)
    return df


def extract_stock(code: str, days: int, date_str: str) -> dict:
    """
    提取单只股票数据，输出 data.json 格式
    """
    df = load_parquet()

    # 筛选股票
    code_padded = code.zfill(6)
    stock_df = df[df['code'] == code_padded].sort_values('date')

    if stock_df.empty:
        raise ValueError(f"股票代码 {code} 未找到")

    # 取最近 N 天数据
    stock_df = stock_df.tail(days).reset_index(drop=True)

    # 构建 klines
    klines = []
    for _, row in stock_df.iterrows():
        klines.append({
            "date": row['date'],
            "open": round(float(row['open']), 2),
            "high": round(float(row['high']), 2),
            "low": round(float(row['low']), 2),
            "close": round(float(row['close']), 2),
            "volume": int(row['volume']),
            "amount": round(float(row['amount']), 2) if pd.notna(row['amount']) else 0.0,
            "pct_chg": round(float(row['pct_chg']), 2) if pd.notna(row['pct_chg']) else 0.0
        })

    # 判断市场
    market_map = {
        '6': 'A股',
        '0': 'A股',
        '3': 'A股',
        '8': 'A股'
    }
    first_digit = str(code_padded)[0]
    market = market_map.get(first_digit, 'A股')

    # 判断股票名称（从代码推断或用代码代替）
    name = code_padded  # 简化处理，可后续优化

    result = {
        "code": code_padded,
        "name": name,
        "market": market,
        "source": "akshare-parquet",
        "klines": klines
    }

    return result


def main():
    parser = argparse.ArgumentParser(description='parquet → technical-analysis data.json 转换器')
    parser.add_argument('code', help='股票代码，如 600519')
    parser.add_argument('--days', type=int, default=DEFAULT_DAYS, help=f'获取天数（默认 {DEFAULT_DAYS}）')
    parser.add_argument('--date', required=True, help='日期标识，格式 YYYY-MM-DD')
    parser.add_argument('--output', help='输出路径（默认 output/<code>/<date>/data.json）')
    args = parser.parse_args()

    try:
        data = extract_stock(args.code, args.days, args.date)

        if args.output:
            out_path = args.output
        else:
            # 与 skill 目录结构兼容
            script_dir = Path(__file__).parent.resolve()
            out_path = script_dir / 'output' / args.code / args.date / 'data.json'

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"[OK] 数据已保存: {out_path}", file=sys.stderr)
        print(f"[OK] 共 {len(data['klines'])} 条 K 线，股票 {data['code']}", file=sys.stderr)

        # 同时打印 data.json 内容供调试
        print(json.dumps(data, ensure_ascii=False, indent=2))

    except Exception as e:
        print(f"[错误] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
