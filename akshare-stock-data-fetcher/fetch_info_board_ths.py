#!/usr/bin/env python3
"""
从同花顺获取各股票所属概念板块，输出到 stockBoardConcept.csv
数据来源：同花顺 (q.10jqka.com.cn)
改进：
  - 使用 py_mini_racer 生成认证 cookie，突破 AJAX 反爬
  - 完整抓取所有分页数据
  - 低并发（2 workers）+ 长延迟（0.3s）避免触发 IP 封禁
  - concept_list.csv 缓存 24h，减少网络请求
"""

import os
import sys
import csv
import time
import json
import requests
import py_mini_racer
from bs4 import BeautifulSoup
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
import threading

# akshare 内部模块
from akshare.datasets import get_ths_js

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, 'data', 'stockBoardConcept.csv')

# ─────────────────────────────────────────────
# 线程安全的 v_code 获取（关键！py_mini_racer 非线程安全，必须加锁）
# ─────────────────────────────────────────────
_v_lock = threading.Lock()
_v_cache = ""

def get_v_code():
    global _v_cache
    if _v_cache:
        return _v_cache
    with _v_lock:
        if _v_cache:
            return _v_cache
        js = py_mini_racer.MiniRacer()
        with open(get_ths_js("ths.js"), encoding="utf-8") as f:
            js.eval(f.read())
        _v_cache = js.call("v")
        return _v_cache


# ─────────────────────────────────────────────
# 东方财富：获取全部概念板块名称和代码（备用数据源）
# ─────────────────────────────────────────────
def get_concepts_from_em():
    """东方财富概念板块名称列表，返回 [(name, code), ...]"""
    try:
        # 东方财富概念板块 JSON API
        url = "https://quote.eastmoney.com/center/boardlist.html"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
        }
        r = requests.get(url, headers=headers, timeout=15)
        # 从页面中提取概念数据
        soup = BeautifulSoup(r.text, 'lxml')
        result = []
        # 东方财富概念板块在页面的 table 中
        links = soup.find_all('a', href=lambda h: h and 'quote.eastmoney.com/boards' in h)
        seen = set()
        for a in links:
            name = a.text.strip()
            href = a.get('href', '')
            # href 格式: /boards/BK1234.html
            if '/boards/BK' in href:
                code = href.split('/')[-1].replace('BK', '').replace('.html', '').strip()
                if code not in seen and len(code) > 0:
                    seen.add(code)
                    result.append((name, 'EM:' + code))  # 加 EM: 前缀区分来源
        if result:
            return result
    except Exception as e:
        print(f"  [EM] 获取概念列表失败: {e}", flush=True)
    return []


# ─────────────────────────────────────────────
# 同花顺：从概念详情页 AJAX 分页抓取成分股
# ─────────────────────────────────────────────
def fetch_ths_concept_stocks(concept_name, concept_code, retries=3):
    """抓取单个 THS 概念的全部成分股代码，返回 set"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
        "Referer": "http://q.10jqka.com.cn/",
    }
    all_codes = set()

    for attempt in range(retries):
        try:
            v = get_v_code()
            sess = requests.Session()
            sess.cookies.set('v', v)
            adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1)
            sess.mount('http://', adapter)

            # 第1页
            url = f"http://q.10jqka.com.cn/gn/detail/code/{concept_code}/page/1/ajax/1/"
            r = sess.get(url, headers=headers, timeout=15)
            r.encoding = 'gbk'

            if r.status_code == 403 or r.status_code == 401 or 'Nginx forbidden' in r.text:
                if attempt < retries - 1:
                    time.sleep(5)
                    global _v_cache
                    _v_cache = ""  # 强制刷新 cookie
                    continue
                return set()

            if '暂无成份股数据' in r.text:
                return set()

            soup = BeautifulSoup(r.text, 'lxml')
            rows = soup.find_all('tr')
            for row in rows:
                tds = row.find_all('td')
                if len(tds) >= 3:
                    code = tds[1].text.strip()
                    if code.isdigit() and len(code) == 6:
                        all_codes.add(code)

            page_info = soup.find('span', attrs={'class': 'page_info'})
            total_pages = 1
            if page_info:
                parts = page_info.text.split('/')
                if len(parts) == 2:
                    total_pages = int(parts[1])

            # 后续页
            for page in range(2, total_pages + 1):
                url = f"http://q.10jqka.com.cn/gn/detail/code/{concept_code}/page/{page}/ajax/1/"
                r2 = sess.get(url, headers=headers, timeout=15)
                r2.encoding = 'gbk'
                soup2 = BeautifulSoup(r2.text, 'lxml')
                for row in soup2.find_all('tr'):
                    tds = row.find_all('td')
                    if len(tds) >= 3:
                        code = tds[1].text.strip()
                        if code.isdigit() and len(code) == 6:
                            all_codes.add(code)
                time.sleep(0.3)  # 页间延迟，避免触发反爬

            return all_codes

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                return set()

    return set()


# ─────────────────────────────────────────────
# 从本地缓存读取概念列表（24h 有效期）
# ─────────────────────────────────────────────
def load_cached_concepts():
    csv_path = os.path.join(SCRIPT_DIR, 'data', 'concept_list.csv')
    if os.path.exists(csv_path):
        age = time.time() - os.path.getmtime(csv_path)
        if age < 86400:
            result = []
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    n = row.get('概念名称', '').strip()
                    c = row.get('概念代码', '').strip()
                    if n and c:
                        result.append((n, c))
            if result:
                return result
    return None


def save_concepts_to_cache(concepts):
    csv_path = os.path.join(SCRIPT_DIR, 'data', 'concept_list.csv')
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['概念名称', '概念代码'])
        writer.writeheader()
        for name, code in concepts:
            writer.writerow({'概念名称': name, '概念代码': code})


# ─────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────
def main():
    print("=" * 60, flush=True)
    print("同花顺概念板块数据抓取", flush=True)
    print("=" * 60, flush=True)

    # 1. 加载概念列表（优先读缓存）
    concepts = load_cached_concepts()
    if concepts:
        print(f"从缓存加载概念列表: {len(concepts)} 个\n", flush=True)
    else:
        print("正在从同花顺获取概念列表...", flush=True)
        try:
            from akshare import stock_board_concept_name_ths
            df = stock_board_concept_name_ths()
            concepts = list(zip(df['name'], df['code'].astype(str)))
            save_concepts_to_cache(concepts)
            print(f"共 {len(concepts)} 个概念，已缓存\n", flush=True)
        except Exception as e:
            print(f"akshare 获取概念失败: {e}", flush=True)
            print("尝试东方财富备选方案...\n", flush=True)
            concepts = get_concepts_from_em()
            if concepts:
                save_concepts_to_cache(concepts)
                print(f"东方财富获取到 {len(concepts)} 个概念，已缓存\n", flush=True)
            else:
                print("无法获取概念列表，退出", flush=True)
                return

    # 2. 抓取各概念成分股（2并发 + 长延迟）
    print(f"开始抓取成分股（共 {len(concepts)} 个概念，2并发）...\n", flush=True)
    stock_concept_map = defaultdict(set)
    fail_count = 0
    start_time = time.time()

    def do_fetch(name, code):
        # 东方财富的概念代码带 EM: 前缀，不走 THS 逻辑
        if code.startswith('EM:'):
            return name, code, set(), None
        codes = fetch_ths_concept_stocks(name, code)
        return name, code, codes, None

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(do_fetch, name, code): (name, code)
            for name, code in concepts
        }
        for i, future in enumerate(as_completed(futures), 1):
            concept_name, _, stock_codes, err = future.result()
            if err:
                fail_count += 1
                if fail_count <= 5:
                    print(f"  [{i}/{len(concepts)}] 失败: {err}", flush=True)
            else:
                for sc in stock_codes:
                    stock_concept_map[sc].add(concept_name)

            if i % 20 == 0 or i == len(concepts):
                elapsed = time.time() - start_time
                speed = i / elapsed if elapsed > 0 else 0
                eta = (len(concepts) - i) / speed if speed > 0 else 0
                print(f"  [{i}/{len(concepts)}] 失败:{fail_count} 速度:{speed:.1f}个/秒 ETA:{eta/60:.1f}分钟", flush=True)

    print(f"\n抓取完成! 共映射 {len(stock_concept_map)} 只股票的概念\n", flush=True)

    # 3. 输出结果
    codes = load_stock_codes()
    rows = []
    for code in codes:
        pure = code.replace('sh', '').replace('sz', '')
        concepts_str = ','.join(sorted(stock_concept_map.get(pure, set())))
        rows.append({'A股代码': code, '所属概念': concepts_str})

    with open(OUTPUT_PATH, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['A股代码', '所属概念'])
        writer.writeheader()
        writer.writerows(rows)

    has_concept = sum(1 for r in rows if r['所属概念'])
    multi_concept = sum(1 for r in rows if ',' in r['所属概念'])
    print(f"已保存到: {OUTPUT_PATH} ({len(rows)} 行)", flush=True)
    print(f"  有概念: {has_concept} 只", flush=True)
    print(f"  多概念(>=2): {multi_concept} 只", flush=True)


def load_stock_codes():
    path = os.path.join(SCRIPT_DIR, 'data/stock_codes.txt')
    codes = []
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    codes.append(line)
    return codes


if __name__ == '__main__':
    main()
