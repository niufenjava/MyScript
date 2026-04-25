"""
批量抓取A股日线历史数据 → Parquet（多进程 + 重试）
参考 stock_zh_a_hist_min_em.py 架构，改造点：
  - 数据源：腾讯证券原始接口（直连不被封，无需代理）
  - 存储：MongoDB → Parquet（每只股票一个文件，按日期 upsert）
  - 增量：根据本地最新日期自动增量拉取
  - 字段：日期/开盘/收盘/最高/最低/成交量/换手率/成交额/涨跌幅(%)
"""

import multiprocessing
import time
import re
import random
from datetime import datetime, timedelta
import akshare as ak
import requests
import schedule
import os
import pandas as pd
from pebble import ProcessPool
from concurrent.futures import TimeoutError as PebbleTimeout

# ── 配置 ──────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "daily")
FULL_START_DATE = "2025-01-01"  # 全量拉取起始日期
NUM_WORKERS = 8                 # 多进程并发数（腾讯接口不封IP，可并发）
MAX_TIMEOUT = 30                # 单只股票超时（秒）
MAX_RETRIES = 6                 # 最大重试轮数
REQUEST_INTERVAL = (0.3, 0.8)   # 每次请求随机间隔（秒），礼貌访问


# ── 腾讯接口：代码转换 ───────────────────────────────
def to_tx_symbol(code):
    """
    纯数字代码 → 腾讯格式（带市场前缀）
    60/68开头 → sh, 00/30开头 → sz, 4/8开头 → bj
    """
    code = str(code).zfill(6)
    if code.startswith(("60", "68")):
        return f"sh{code}"
    elif code.startswith(("00", "30")):
        return f"sz{code}"
    elif code.startswith(("4", "8")):
        return f"bj{code}"
    return f"sz{code}"


# ── 腾讯接口：原始数据拉取 ───────────────────────────
TX_API_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"

def _fetch_kline_raw(tx_symbol, start_year, end_year, adjust="qfq"):
    """按年请求腾讯原始接口，返回所有数据行"""
    all_rows = []
    for year in range(start_year, end_year + 1):
        params = {
            "_var": f"kline_day{adjust}{year}",
            "param": f"{tx_symbol},day,{year}-01-01,{year + 1}-12-31,640,{adjust}",
        }
        try:
            r = requests.get(TX_API_URL, params=params, timeout=10)
            if r.status_code != 200:
                continue
            text = r.text
            rows = re.findall(r'\[([^\[\]]+)\]', text)
            for row in rows:
                # 先移除 JSON 对象 {...}（可能含分红信息），再分割
                cleaned_row = re.sub(r'\{[^}]*\}', '', row)
                parts = [p.strip().strip('"') for p in cleaned_row.split(",")]
                # 过滤空值
                parts = [p for p in parts if p]
                # 数据行首字段是日期 YYYY-MM-DD
                if len(parts) < 6 or len(parts[0]) != 10 or parts[0][4] != '-':
                    continue
                all_rows.append(parts)
        except Exception:
            continue
        time.sleep(random.uniform(*REQUEST_INTERVAL))
    # 按日期去重，保留最后出现的（即复权数据覆盖原始数据）
    seen = {}
    for row in all_rows:
        seen[row[0]] = row
    return list(seen.values())


def _parse_to_dataframe(all_rows, start_date, end_date):
    """解析原始数据行为 DataFrame，含涨跌幅计算"""
    if not all_rows:
        return pd.DataFrame()

    # 列: 日期, 开盘, 收盘, 最高, 最低, 成交量, 换手率(第7), 成交额(第8，可能缺失)
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

    # 计算涨跌幅（原始接口的涨跌幅字段恒为0，必须自算）
    df.sort_values("日期", inplace=True)
    df["涨跌幅(%)"] = (df["收盘"].pct_change() * 100).round(2)

    # 按日期范围裁切
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    df = df[(df["日期"] >= start_dt) & (df["日期"] <= end_dt)].copy()
    df.reset_index(drop=True, inplace=True)

    return df


def fetch_stock_daily(code, start_date, end_date, adjust="qfq"):
    """
    通过腾讯证券原始接口拉取日线数据
    :param code: 股票代码，如 '000001'
    :param start_date: 起始日期，格式 'YYYY-MM-DD'
    :param end_date: 结束日期，格式 'YYYY-MM-DD'
    :param adjust: 复权类型 'qfq'前复权 / 'hfq'后复权 / ''不复权
    :return: DataFrame
    """
    tx_symbol = to_tx_symbol(code)

    # 向前多取30天，用于计算涨跌幅首行（消除 NaN）
    real_start = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=30)
    start_year = real_start.year
    end_year = datetime.strptime(end_date, "%Y-%m-%d").year

    all_rows = _fetch_kline_raw(tx_symbol, start_year, end_year, adjust)
    df = _parse_to_dataframe(all_rows, start_date, end_date)
    return df


# ── Parquet 读写工具 ──────────────────────────────────
def read_stock_parquet(code):
    """读取某只股票的 Parquet 文件，不存在则返回空 DataFrame"""
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()


def upsert_stock_parquet(code, new_df):
    """按日期去重合并写入 Parquet，返回新增行数"""
    if new_df.empty:
        return 0
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    old_df = read_stock_parquet(code)
    if old_df.empty:
        new_df.to_parquet(path, index=False)
        return len(new_df)
    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged.drop_duplicates(subset=["日期"], keep="last", inplace=True)
    merged.sort_values("日期", inplace=True)
    merged.reset_index(drop=True, inplace=True)
    added = len(merged) - len(old_df)
    merged.to_parquet(path, index=False)
    return added


def get_last_date(code):
    """获取某只股票本地已有的最新日期，无数据返回 None"""
    df = read_stock_parquet(code)
    if df.empty:
        return None
    return str(df["日期"].max())


# ── 单只股票处理（子进程入口） ────────────────────────
def process_single_stock(code, date):
    """子进程中运行：拉取日线 + 写入 Parquet"""
    try:
        # 增量拉取：从本地最新日期的下一天开始
        last = get_last_date(code)
        if last:
            start = (datetime.strptime(last[:10], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start = FULL_START_DATE

        end = date

        # 起始日期已超过结束日期，无需拉取
        if start > end:
            return

        daily_data = fetch_stock_daily(code, start, end)

        if daily_data is None or daily_data.empty:
            return

        added = upsert_stock_parquet(code, daily_data)
        if added > 0:
            print(f"[OK] {code} 新增 {added} 条日线")
    except requests.exceptions.RequestException as e:
        print(f"[NET_ERR] {code} 网络请求失败: {e}")
        raise
    except Exception as e:
        print(f"[ERR] {code} 获取日线数据出错: {e}")
        raise


# ── 工具函数 ──────────────────────────────────────────
def load_stock_codes(file_path):
    """从文件加载股票代码"""
    with open(file_path, 'r', encoding='utf-8') as file:
        return [line.strip()[2:] for line in file if line.strip()]


def get_today_trading_date():
    """获取最近的交易日（今天是交易日返回今天，否则返回最近一个交易日）"""
    today = datetime.now().date()
    try:
        calendar_df = ak.tool_trade_date_hist_sina()
        trade_dates = sorted(calendar_df['trade_date'].tolist())
        if today in trade_dates:
            return today.strftime("%Y-%m-%d")
        # 非交易日：取最近一个过去的交易日
        past = [d for d in trade_dates if d <= today]
        if past:
            last_trade = past[-1]
            print(f"今天({today})不是交易日，使用最近交易日: {last_trade}")
            return last_trade.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"获取交易日历失败: {str(e)}")
    return None


# ── 批量调度（Pebble 多进程 + 超时 + 重试） ──────────
def scheduled_task(stock_codes, num_workers, max_timeout=60, max_retries=3):
    """定时任务：使用 Pebble 进程池处理股票数据，支持超时与自动终止"""
    date = get_today_trading_date()
    if not date:
        print("无法获取交易日历，跳过处理")
        return

    # 确保存储目录存在
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"开始处理 {len(stock_codes)} 个股票代码，使用 {num_workers} 个进程")
    retry_list = []

    def submit_tasks(codes):
        with ProcessPool(max_workers=num_workers) as pool:
            futures = {
                pool.schedule(
                    process_single_stock,
                    args=[code, date],
                    timeout=max_timeout
                ): code for code in codes
            }

            for future in futures:
                code = futures[future]
                try:
                    future.result()
                except PebbleTimeout:
                    print(f"[TIMEOUT] {code} 处理超时")
                    retry_list.append(code)
                except Exception as e:
                    print(f"[FAIL] {code}: {e}")
                    retry_list.append(code)

    # 初次执行
    submit_tasks(stock_codes)

    # 重试执行
    for retry in range(max_retries):
        if not retry_list:
            file_path = "./perfect.txt"
            current_date = datetime.now().strftime("%Y-%m-%d")
            content = f"{current_date}: 今日所有股票日线任务均成功完成"
            with open(file_path, "a", encoding="utf-8") as file:
                file.write(content + "\n")
                file.flush()
                os.fsync(file.fileno())
            break
        print(f"正在进行第 {retry + 1} 次重试，任务数: {len(retry_list)}")
        time.sleep(3)
        current_retry = retry_list
        retry_list = []
        submit_tasks(current_retry)

    if retry_list:
        print(f"以下股票任务最终仍失败: {retry_list}")
        file_path = "./retry.txt"
        current_date = datetime.now().strftime("%Y-%m-%d")
        content = f"{current_date}：{retry_list}"
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(content + "\n")
            file.flush()
            os.fsync(file.fileno())


def main():
    # 加载股票代码
    stock_codes = load_stock_codes(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock_codes.txt'))
    num_workers = NUM_WORKERS

    # 设置定时任务（每天16:00运行）
    # schedule.every().day.at("16:00").do(
    #     scheduled_task,
    #     stock_codes=stock_codes,
    #     num_workers=num_workers,
    #     max_timeout=MAX_TIMEOUT,
    #     max_retries=MAX_RETRIES
    # )

    # 立即运行一次（取消注释以启用）
    t = time.time()
    scheduled_task(
        stock_codes=stock_codes,
        num_workers=num_workers,
        max_timeout=MAX_TIMEOUT,
        max_retries=MAX_RETRIES
    )
    print(f"耗时: {time.time() - t:.1f}s")

    # 抓取完成后自动合并为大表（供跨股筛选使用）
    try:
        from utils.merge_daily import merge_all
        merge_all()
    except Exception as e:
        print(f"合并大表失败: {e}")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序被手动终止")


if __name__ == '__main__':
    # 设置多进程启动方法
    multiprocessing.set_start_method('spawn')
    main()
