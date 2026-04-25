#!/usr/bin/env python3
"""
从 stock_codes.txt 读取股票列表，获取实时市场数据（腾讯扩展行情），输出到 stockMarketData.csv
每次执行都会刷新全部数据
"""

import os
import csv
import time
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, 'data', 'stockMarketData.csv')

# 市场数据字段（腾讯）
MARKET_FIELDS = ['A股代码', '最新价', '总市值(亿)', '每股收益', '每股净资产', '市净率',
                 '股息率(%)', '动态市盈率', '静态市盈率', '市盈率TTM', 'ROE']

# 腾讯扩展行情字段索引
# [3]最新价 [39]动态PE [44]总市值(亿) [46]PB
# [52]PE_TTM [53]静态PE [56]股息率(%) [66]EPS [67]每股净资产
# [68]ROE [72]总股本(股) [75]户均持股(万股)
# 腾讯扩展行情字段索引
# [3]最新价 [39]动态PE [44]总市值(亿) [46]PB
# [52]PE_TTM [53]静态PE [56]股息率(%) [66]EPS [67]每股净资产
# [68]ROE [75]户均持股(万股)
TENCENT_FIELD_MAP = {
    '最新价': 3,
    '动态市盈率': 39,
    '总市值(亿)': 44,
    '市净率': 46,
    '市盈率TTM': 52,
    '静态市盈率': 53,
    '每股收益': 66,
    '每股净资产': 67,
    '股息率(%)': 56,
    'ROE': 68,
}

INVALID_VALUES = {'', '-', 'N/A', 'None', None}


def is_valid_info(info):
    """判断是否有效数据（非交易时段字段为空/占位则判为无效）"""
    key_fields = ['最新价', '动态市盈率', '市盈率TTM']
    for k in key_fields:
        val = info.get(k, '').strip()
        if val and val not in INVALID_VALUES:
            return True
    return False


def load_stock_codes():
    """从 stock_codes.txt 读取股票代码列表"""
    path = os.path.join(SCRIPT_DIR, 'data/stock_codes.txt')
    codes = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                codes.append(line)
    return codes


def code_to_tencent(code):
    """统一转为腾讯格式 (sh/sz前缀)"""
    if code.startswith('sh') or code.startswith('sz'):
        return code
    if code.startswith('6'):
        return f'sh{code}'
    else:
        return f'sz{code}'


def fetch_market_batch(codes, batch_size=50):
    """批量获取腾讯扩展行情，返回 {sh/sz代码: {字段dict}}"""
    result = {}
    total = len(codes)
    start_time = time.time()

    for batch_start in range(0, total, batch_size):
        batch = codes[batch_start:batch_start + batch_size]
        tencent_codes = [code_to_tencent(c) for c in batch]
        url = f"http://qt.gtimg.cn/q={','.join(tencent_codes)}"

        try:
            r = requests.get(url, timeout=15)
            for line in r.text.strip().split(';'):
                line = line.strip()
                if not line or '~' not in line:
                    continue
                fields = line.split('~')
                if len(fields) < 80:
                    continue
                pure_code = fields[2]
                if pure_code.startswith('6'):
                    full_code = f'sh{pure_code}'
                else:
                    full_code = f'sz{pure_code}'
                info = {}
                for name, idx in TENCENT_FIELD_MAP.items():
                    try:
                        info[name] = fields[idx]
                    except (IndexError, ValueError):
                        info[name] = ''
                if not is_valid_info(info):
                    continue
                result[full_code] = info
        except Exception as e:
            print(f"  批次 {batch_start//batch_size+1} 请求失败: {e}")

        done = min(batch_start + batch_size, total)
        if done % 500 == 0 or done == total:
            elapsed = time.time() - start_time
            print(f"  [{done}/{total}] 已获取市场数据 ({elapsed:.1f}秒)")

    return result


def save_csv(codes, market_data):
    """按 stock_codes.txt 顺序保存 CSV"""
    rows = []
    for code in codes:
        lookup_key = code_to_tencent(code)
        info = market_data.get(lookup_key, {})
        row = {'A股代码': code}
        for f in MARKET_FIELDS[1:]:
            row[f] = info.get(f, '')
        rows.append(row)

    with open(CSV_PATH, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=MARKET_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n已保存到: {CSV_PATH} ({len(rows)} 行 x {len(MARKET_FIELDS)} 列)")


def main():
    codes = load_stock_codes()
    print(f"共 {len(codes)} 只股票\n")

    print(f"[市场数据] 开始获取 {len(codes)} 只股票的实时数据...")
    market_data = fetch_market_batch(codes)
    print(f"[市场数据] 完成! 获取 {len(market_data)} 只股票数据")

    save_csv(codes, market_data)


if __name__ == '__main__':
    main()