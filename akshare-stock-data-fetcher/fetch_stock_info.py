#!/usr/bin/env python3
"""
从 stock_codes.txt 读取股票列表，获取股票基本信息 + 实时市场数据，输出到 stockInfo.csv
分两步：
  1. 基础信息（巨潮资讯 cninfo）: 名称/行业/市场等 —— 已有则跳过
  2. 市场数据（腾讯扩展行情）: 股价/总市值/PE/PB等 —— 每次刷新
"""

import os
import csv
import time
import requests
import akshare as ak
from concurrent.futures import ProcessPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, 'stockInfo.csv')

# 基础信息字段（巨潮，慢，只拉一次）
BASE_FIELDS = ['A股代码', 'A股简称', 'H股代码', 'H股简称', '成立日期', '上市日期',
               '所属市场', '所属行业', '入选指数', '主营业务']

# 市场数据字段（腾讯，快，每次刷新）
MARKET_FIELDS = ['最新价', '总市值(亿)', '每股收益', '每股净资产', '市净率',
                 '股息率(%)', '动态市盈率', '静态市盈率', '市盈率TTM']

ALL_FIELDS = ['A股代码', 'A股简称', 'H股代码', 'H股简称',
              '最新价', '总市值(亿)', '每股收益', '每股净资产', '市净率',
              '股息率(%)', '动态市盈率', '静态市盈率', '市盈率TTM',
              '成立日期', '上市日期', '所属市场', '所属行业', '入选指数', '主营业务']


def load_stock_codes():
    """从 stock_codes.txt 读取股票代码列表，保留 sh/sz 前缀"""
    path = os.path.join(SCRIPT_DIR, 'stock_codes.txt')
    codes = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                codes.append(line)  # 保留 sh600000 / sz000001 原始格式
    return codes


def strip_prefix(code):
    """去掉 sh/sz 前缀，返回纯数字代码（用于调用 cninfo 等接口）"""
    return code.replace('sh', '').replace('sz', '')


def load_existing_csv():
    """加载已有 CSV，返回 {代码: {字段dict}} """
    if not os.path.exists(CSV_PATH):
        return {}
    existing = {}
    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = str(row.get('A股代码', '')).strip()
            if code:
                existing[code] = dict(row)
    return existing


def is_base_complete(row):
    """判断基础信息是否已完整（有名称和行业即视为完整）"""
    return bool(row.get('A股简称', '').strip()) and bool(row.get('所属行业', '').strip())


# ==================== 第1步：基础信息（巨潮） ====================

def fetch_base_one(code, retries=2):
    """获取单只股票的基础信息，code 为 sh/sz 前缀格式"""
    pure_code = strip_prefix(code)
    for attempt in range(retries + 1):
        try:
            df = ak.stock_profile_cninfo(symbol=pure_code)
            if df.empty:
                info = {f: '' for f in BASE_FIELDS}
                info['A股代码'] = code
                return (info, None)
            row = df.iloc[0]
            info = {}
            for f in BASE_FIELDS:
                val = row.get(f, '')
                info[f] = '' if val is None or str(val) == 'None' else str(val)
            # A股代码统一用 sh/sz 前缀格式
            info['A股代码'] = code
            return (info, None)
        except Exception as e:
            if attempt < retries:
                time.sleep(1)
            else:
                info = {f: '' for f in BASE_FIELDS}
                info['A股代码'] = code
                return (info, str(e))


def fetch_base_info(codes, existing):
    """批量获取基础信息，跳过已有的"""
    need_fetch = [c for c in codes if c not in existing or not is_base_complete(existing[c])]

    if not need_fetch:
        print(f"[基础信息] 全部 {len(codes)} 只股票已有基础信息，跳过")
        return existing

    print(f"[基础信息] 需获取 {len(need_fetch)} 只（已有 {len(codes)-len(need_fetch)} 只跳过）")
    success = fail = 0
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_base_one, code): code for code in need_fetch}
        for i, future in enumerate(as_completed(futures), 1):
            info, err = future.result()
            code = info['A股代码']
            if err:
                fail += 1
                if fail <= 20:
                    print(f"  [{i}/{len(need_fetch)}] {code} 失败: {err}")
            else:
                success += 1
            # 合并到 existing
            if code not in existing:
                existing[code] = {f: '' for f in ALL_FIELDS}
            for f in BASE_FIELDS:
                existing[code][f] = info[f]

            if i % 100 == 0 or i == len(need_fetch):
                elapsed = time.time() - start_time
                speed = i / elapsed if elapsed > 0 else 0
                eta = (len(need_fetch) - i) / speed if speed > 0 else 0
                print(f"  [{i}/{len(need_fetch)}] 成功:{success} 失败:{fail} "
                      f"速度:{speed:.1f}只/秒 剩余:{eta/60:.1f}分钟")

    print(f"[基础信息] 完成! 成功:{success} 失败:{fail}")
    return existing


# ==================== 第2步：市场数据（腾讯扩展行情） ====================

def code_to_tencent(code):
    """统一转为腾讯格式 (sh/sz前缀)，输入已有前缀则原样返回"""
    if code.startswith('sh') or code.startswith('sz'):
        return code
    if code.startswith('6'):
        return f'sh{code}'
    else:
        return f'sz{code}'


# 腾讯扩展行情字段索引
# [3]最新价 [39]动态PE [44]总市值(亿) [46]PB [52]PE_TTM [53]静态PE
# [66]EPS [67]每股净资产 [73]总股本 [79]股息率
TENCENT_FIELD_MAP = {
    '最新价': 3,
    '动态市盈率': 39,
    '总市值(亿)': 44,
    '市净率': 46,
    '市盈率TTM': 52,
    '静态市盈率': 53,
    '每股收益': 66,
    '每股净资产': 67,
    '股息率(%)': 79,
}


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
                # 还原为 sh/sz 前缀格式
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
                result[full_code] = info
        except Exception as e:
            print(f"  批次 {batch_start//batch_size+1} 请求失败: {e}")

        done = min(batch_start + batch_size, total)
        if done % 500 == 0 or done == total:
            elapsed = time.time() - start_time
            print(f"  [{done}/{total}] 已获取市场数据 ({elapsed:.1f}秒)")

    return result


def fetch_market_data(codes, existing):
    """获取所有股票的市场数据并合并到 existing"""
    print(f"\n[市场数据] 开始获取 {len(codes)} 只股票的实时数据...")
    market = fetch_market_batch(codes)

    updated = 0
    for code in codes:
        if code in market:
            if code not in existing:
                existing[code] = {f: '' for f in ALL_FIELDS}
                existing[code]['A股代码'] = code
            for f in MARKET_FIELDS:
                existing[code][f] = market[code].get(f, '')
            updated += 1

    print(f"[市场数据] 完成! 更新 {updated} 只股票")
    return existing


# ==================== 输出 ====================

def save_csv(codes, existing):
    """按 stock_codes.txt 顺序保存 CSV"""
    rows = []
    for code in codes:
        if code in existing:
            row = {}
            for f in ALL_FIELDS:
                row[f] = existing[code].get(f, '')
            rows.append(row)

    with open(CSV_PATH, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ALL_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n已保存到: {CSV_PATH} ({len(rows)} 行 x {len(ALL_FIELDS)} 列)")


def main():
    codes = load_stock_codes()
    print(f"共 {len(codes)} 只股票\n")

    # 加载已有数据
    existing = load_existing_csv()
    if existing:
        print(f"已有 CSV 包含 {len(existing)} 条记录")

    # 第1步：基础信息（已有则跳过）
    existing = fetch_base_info(codes, existing)

    # 第2步：市场数据（每次刷新）
    existing = fetch_market_data(codes, existing)

    # 保存
    save_csv(codes, existing)


if __name__ == '__main__':
    main()
