#!/usr/bin/env python3
"""
从 stock_codes.txt 读取股票列表，获取股票基本信息（巨潮资讯 cninfo），输出到 stockBaseInfo.csv
已有完整基础信息的股票自动跳过
"""

import os
import csv
import time
import akshare as ak
from concurrent.futures import ProcessPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, 'data', 'stockBaseInfo.csv')

# 基础信息字段（巨潮）
BASE_FIELDS = ['A股代码', 'A股简称', 'H股代码', 'H股简称', '成立日期', '上市日期',
               '所属市场', '所属行业', '入选指数', '主营业务']


def load_stock_codes():
    """从 stock_codes.txt 读取股票代码列表，保留 sh/sz 前缀"""
    path = os.path.join(SCRIPT_DIR, 'data/stock_codes.txt')
    codes = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                codes.append(line)
    return codes


def strip_prefix(code):
    """去掉 sh/sz 前缀，返回纯数字代码"""
    return code.replace('sh', '').replace('sz', '')


def load_existing_csv():
    """加载已有 CSV，返回 {代码: {字段dict}}"""
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
            if code not in existing:
                existing[code] = {f: '' for f in BASE_FIELDS}
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


def save_csv(codes, existing):
    """按 stock_codes.txt 顺序保存 CSV"""
    rows = []
    for code in codes:
        if code in existing:
            row = {}
            for f in BASE_FIELDS:
                row[f] = existing[code].get(f, '')
            rows.append(row)

    with open(CSV_PATH, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=BASE_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n已保存到: {CSV_PATH} ({len(rows)} 行 x {len(BASE_FIELDS)} 列)")


def main():
    codes = load_stock_codes()
    print(f"共 {len(codes)} 只股票\n")

    existing = load_existing_csv()
    if existing:
        print(f"已有 CSV 包含 {len(existing)} 条记录")

    existing = fetch_base_info(codes, existing)
    save_csv(codes, existing)


if __name__ == '__main__':
    main()
