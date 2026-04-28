#!/usr/bin/env python3
"""
股票异动事件采集系统
功能：每日盘后采集涨停/跌停/炸板/强势股/龙虎榜事件，关联公告归因，按异动类型分 CSV 存储

用法：
  # 每日增量（今日数据）
  python fetch_events.py

  # 历史回溯（指定日期区间）
  python fetch_events.py --start 20260301 --end 20260331

  # 强制刷新（覆盖已有数据）
  python fetch_events.py --force

数据源：
  - 东方财富涨停/跌停/炸板/强势股池（stock_zt_pool_*）
  - 东方财富龙虎榜（stock_lhb_detail_em）
  - 东方财富个股公告（stock_individual_notice_report）→ 用于归因

输出（分文件）：
  data/events/stock_events_涨停.csv
  data/events/stock_events_跌停.csv
  data/events/stock_events_炸板.csv
  data/events/stock_events_强势股.csv
  data/events/stock_events_强势股(回调).csv
  data/events/stock_events_龙虎榜.csv

  字段：股票代码, 股票名称, 日期, 异动类型, 异动原因, 关联公告, 公告链接, 数据来源

去重键：(股票代码, 日期, 异动类型)
"""

import os
import csv
import time
import argparse
import logging
from datetime import date, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from fetcher._shared import PROJECT_ROOT, get_latest_trade_date

# ── 路径配置 ───────────────────────────────────────────────────────────────
DATA_DIR   = os.path.join(PROJECT_ROOT, "data")
EVENTS_DIR = os.path.join(DATA_DIR, "events")

os.makedirs(EVENTS_DIR, exist_ok=True)

# ── 日志配置 ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(EVENTS_DIR, "fetch_events.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── CSV 表头 ───────────────────────────────────────────────────────────────
CSV_FIELDS = ["股票代码", "股票名称", "日期", "异动类型", "异动原因",
              "关联公告", "公告链接", "数据来源"]

# ── 异动类型常量 ──────────────────────────────────────────────────────────
EVENT_ZT    = "涨停"      # 东方财富涨停池
EVENT_DT    = "跌停"      # 东方财富跌停池
EVENT_ZB    = "炸板"      # 东方财富炸板池（曾涨停后开板）
EVENT_QSG   = "强势股"    # 东方财富强势股池
EVENT_LHB   = "龙虎榜"    # 龙虎榜（机构/营业部席位）

EVENT_TYPES = [EVENT_ZT, EVENT_DT, EVENT_ZB, EVENT_QSG, "强势股(回调)", EVENT_LHB]  # 全部异动类型（含强势股回调子类型）

# ── 归因关键词映射 ────────────────────────────────────────────────────────
# 公告标题含以下关键词 → 映射为对应异动原因
REASON_KEYWORDS = {
    "业绩预告":       "业绩超预期",
    "业绩增长":       "业绩超预期",
    "净利润增长":     "业绩超预期",
    "项目投资":       "投资新项目",
    "对外投资":       "投资新项目",
    "马来西亚":       "海外扩产",
    "建生产线":       "扩产",
    "扩产":          "扩产",
    "股权激励":       "股权激励",
    "行权":          "股权激励行权",
    "授予期权":       "股权激励",
    "资产重组":       "资产重组",
    "收购":          "收购资产",
    "发行H股":       "H股上市推进",
    "港交所":        "H股上市推进",
    "分红":          "分红派息",
    "权益分派":       "分红派息",
    "回购":          "股份回购",
    "增持":          "股东增持",
    "战略合作":       "战略合作",
    "供货":          "订单/供货",
    "订单":          "订单",
    "AI":            "AI概念",
    "人工智能":       "AI概念",
    "机器人":         "机器人概念",
    "新能源":         "新能源概念",
    "固态电池":       "固态电池",
    "储能":          "储能概念",
    "光伏":          "光伏概念",
    "芯片":          "半导体/芯片",
    "半导体":        "半导体/芯片",
    "北交所":        "北交所上市",
    "科创板":        "科创板",
}

# ── 工具函数 ───────────────────────────────────────────────────────────────

def get_event_csv_path(event_type):
    """根据异动类型返回对应的 CSV 文件路径"""
    fname = f"stock_events_{event_type}.csv"
    return os.path.join(EVENTS_DIR, fname)


def _load_single_type(event_type):
    """读取单个异动类型 CSV，返回 {(代码, 日期, 异动类型): 行dict}"""
    fpath = get_event_csv_path(event_type)
    if not os.path.exists(fpath):
        return {}
    result = {}
    with open(fpath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["股票代码"], row["日期"], row["异动类型"])
            result[key] = row
    return result


def load_existing_events_by_type():
    """
    按异动类型读取各自分文件的已有数据。
    返回 {(代码, 日期, 异动类型): 行dict}。
    去重键：(股票代码, 日期, 异动类型)
    """
    result = {}
    for event_type in EVENT_TYPES:
        result.update(_load_single_type(event_type))
    logger.info(f"已加载 {len(result)} 条历史事件记录")
    return result


def save_events(all_rows, mode="a"):
    """
    按异动类型分文件写入 CSV。
    - mode="w": 强制覆盖（读取全部已有 + 新数据，重写全文件）
    - mode="a": 增量追加（只追加不重复的新行，不读全文件）

    去重键：(股票代码, 日期, 异动类型)
    """
    by_type = {}
    for row in all_rows:
        by_type.setdefault(row["异动类型"], []).append(row)

    for event_type, rows in by_type.items():
        fpath = get_event_csv_path(event_type)

        if mode == "w":
            # 全量模式：读已有 + 新增 → 整体重写
            existing = _load_single_type(event_type)
            for row in rows:
                key = (row["股票代码"], row["日期"], row["异动类型"])
                existing[key] = row
            with open(fpath, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writeheader()
                for row in sorted(existing.values(),
                                  key=lambda r: (r["日期"], r["股票代码"])):
                    writer.writerow(row)
            logger.info(f"[{event_type}] 重写 {len(existing)} 条（本次 {len(rows)} 条）")
        else:
            # 追加模式：只写不存在的新 key，不读全文件
            existing_keys = set(_load_single_type(event_type).keys())
            file_exists = os.path.exists(fpath)
            new_count = 0
            with open(fpath, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if not file_exists:
                    writer.writeheader()
                for row in rows:
                    key = (row["股票代码"], row["日期"], row["异动类型"])
                    if key not in existing_keys:
                        writer.writerow(row)
                        existing_keys.add(key)
                        new_count += 1
            logger.info(f"[{event_type}] 追加 {new_count} 条（跳过 {len(rows)-new_count} 条重复）")


def infer_reason_from_announces(code, trade_date, lookback_days=5):
    """
    拉取涨停日前N天内的公告，匹配关键词推断异动原因。
    若单次调用超 5 秒，自动放弃（返回空），避免卡住。
    返回: (异动原因字符串, 关联公告标题, 公告URL)
    """
    import signal

    def _timeout_handler(signum, frame):
        raise TimeoutError()

    begin = (trade_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end   = trade_date.strftime("%Y-%m-%d")
    try:
        # 设置 5 秒超时
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(5)
        try:
            df = ak.stock_individual_notice_report(
                security=code,
                symbol="全部",
                begin_date=begin,
                end_date=end,
            )
        finally:
            signal.alarm(0)  # 取消闹钟
    except (Exception, TimeoutError) as e:
        logger.debug(f"公告拉取失败/超时 {code}: {e}")
        return "", "", ""

    if df is None or df.empty:
        return "", "", ""

    reason_found = ""
    matched_title = ""
    matched_url = ""

    for _, row in df.iterrows():
        title = str(row.get("公告标题", ""))
        url   = str(row.get("网址", ""))
        for keyword, reason in REASON_KEYWORDS.items():
            if keyword in title:
                reason_found = reason
                matched_title = title
                matched_url = url
                break
        if reason_found:
            break

    return reason_found, matched_title, matched_url


def get_stock_change(code, trade_dt):
    """
    从本地 Parquet 读取指定股票在 trade_dt 当日的涨跌幅。
    返回 float（涨跌幅%）或 None（数据不存在）
    """
    path = os.path.join(DATA_DIR, "daily", f"{code}.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        df["日期"] = pd.to_datetime(df["日期"]).dt.date
        row = df[df["日期"] == trade_dt]
        if row.empty:
            return None
        return float(row["涨跌幅(%)"].values[0])
    except Exception:
        return None


def _enrich_single(row_data, event_type, trade_dt):
    """对单行数据进行归因处理（供并发调用）"""
    code  = str(row_data.get("代码", "")).zfill(6)
    stock_name = str(row_data.get("名称", ""))
    if not code:
        return None
    date_str = trade_dt.strftime("%Y-%m-%d")

    # 归因
    reason, ann_title, ann_url = infer_reason_from_announces(code, trade_dt)

    # 涨停原因补充：所属板块
    industry = str(row_data.get("所属行业", ""))
    if reason and industry:
        reason = f"{reason}（{industry}板块）"
    elif not reason and industry:
        reason = f"{industry}板块带动"

    # 强势股特殊处理：检查当日是否下跌，标注为"强势股(回调)"
    final_event_type = event_type
    if event_type == EVENT_QSG:
        change_pct = get_stock_change(code, trade_dt)
        if change_pct is not None and change_pct < 0:
            final_event_type = "强势股(回调)"
            if reason:
                reason = f"{reason}，当日跌{int(change_pct)}%"
            else:
                reason = f"连续强势后当日下跌{int(abs(change_pct))}%"

    return {
        "股票代码": code,
        "股票名称": stock_name,
        "日期": date_str,
        "异动类型": final_event_type,
        "异动原因": reason,
        "关联公告": ann_title,
        "公告链接": ann_url,
        "数据来源": "东方财富",
    }


def enrich_zt_pool(df, event_type, trade_dt):
    """
    对东方财富股池 DataFrame 补全字段，转为标准事件行。
    归因：并发查近5天公告（每只5秒超时），强势股额外判断当日涨跌。
    """
    rows = []
    rows_raw = [row for _, row in df.iterrows()]

    # 并发归因（最多20线程，每只5秒超时）
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(_enrich_single, row_data, event_type, trade_dt): row_data
            for row_data in rows_raw
        }
        for future in as_completed(futures, timeout=60):
            try:
                result = future.result()
                if result:
                    rows.append(result)
            except Exception as e:
                logger.debug(f"归因异常: {e}")

    return rows


def fetch_lhb_events(trade_dt, existing_keys=None):
    """
    拉取指定交易日的龙虎榜事件。
    - existing_keys: 已存在的 (代码, 日期, 类型) 集合，跳过的股票不查公告直接复用
    - 对新股票做公告归因（提升原因质量）
    - 对已在 CSV 的股票只保留上榜原因，不查公告（节省时间）
    """
    existing_keys = existing_keys or set()
    date_str = trade_dt.strftime("%Y%m%d")
    try:
        df = ak.stock_lhb_detail_em(start_date=date_str, end_date=date_str)
    except Exception as e:
        logger.warning(f"龙虎榜拉取失败 {date_str}: {e}")
        return []

    if df is None or df.empty:
        return []

    seen = {}
    for _, row in df.iterrows():
        code     = str(row.get("代码", "")).zfill(6)
        name     = str(row.get("名称", ""))
        reason   = str(row.get("上榜原因", ""))
        date_fmt = trade_dt.strftime("%Y-%m-%d")

        key = (code, date_fmt, EVENT_LHB)
        if key not in seen:
            # 已存在的股票跳过公告归因（省时间）
            if key in existing_keys:
                seen[key] = {
                    "股票代码": code, "股票名称": name, "日期": date_fmt,
                    "异动类型": EVENT_LHB, "异动原因": reason,
                    "关联公告": "", "公告链接": "", "数据来源": "东方财富",
                }
            else:
                # 新股票做公告归因
                ann_reason, ann_title, ann_url = infer_reason_from_announces(code, trade_dt)
                final_reason = ann_reason if ann_reason else reason
                seen[key] = {
                    "股票代码": code, "股票名称": name, "日期": date_fmt,
                    "异动类型": EVENT_LHB, "异动原因": final_reason,
                    "关联公告": ann_title, "公告链接": ann_url, "数据来源": "东方财富",
                }

    return list(seen.values())


# ── 主采集函数 ─────────────────────────────────────────────────────────────

def fetch_daily_events(trade_dt, existing_keys):
    """
    采集指定交易日的所有异动事件。
    返回新增事件列表（排除已在 existing_keys 的）。
    """
    date_str = trade_dt.strftime("%Y-%m-%d")
    all_rows = []

    # ── 1. 涨停池 ──────────────────────────────────────────────────────────
    try:
        logger.info(f"[{date_str}] 拉取涨停池...")
        df_zt = ak.stock_zt_pool_em(date=trade_dt.strftime("%Y%m%d"))
        if not df_zt.empty:
            rows = enrich_zt_pool(df_zt, EVENT_ZT, trade_dt)
            logger.info(f"  涨停: {len(rows)} 只")
            all_rows.extend(rows)
        else:
            logger.info(f"  涨停: 0 只（今日无涨停或非交易日）")
    except Exception as e:
        logger.warning(f"涨停池拉取失败: {e}")

    # ── 2. 跌停池 ─────────────────────────────────────────────────────────
    try:
        logger.info(f"[{date_str}] 拉取跌停池...")
        df_dt = ak.stock_zt_pool_dtgc_em(date=trade_dt.strftime("%Y%m%d"))
        if not df_dt.empty:
            rows = enrich_zt_pool(df_dt, EVENT_DT, trade_dt)
            logger.info(f"  跌停: {len(rows)} 只")
            all_rows.extend(rows)
    except ValueError as e:
        logger.warning(f"跌停池: {e}")
    except Exception as e:
        logger.warning(f"跌停池拉取失败: {e}")

    # ── 3. 炸板池 ─────────────────────────────────────────────────────────
    try:
        logger.info(f"[{date_str}] 拉取炸板池...")
        df_zb = ak.stock_zt_pool_zbgc_em(date=trade_dt.strftime("%Y%m%d"))
        if not df_zb.empty:
            rows = enrich_zt_pool(df_zb, EVENT_ZB, trade_dt)
            logger.info(f"  炸板: {len(rows)} 只")
            all_rows.extend(rows)
    except Exception as e:
        logger.warning(f"炸板池拉取失败: {e}")

    # ── 4. 强势股池 ───────────────────────────────────────────────────────
    try:
        logger.info(f"[{date_str}] 拉取强势股池...")
        df_qsg = ak.stock_zt_pool_strong_em(date=trade_dt.strftime("%Y%m%d"))
        if not df_qsg.empty:
            rows = enrich_zt_pool(df_qsg, EVENT_QSG, trade_dt)
            logger.info(f"  强势股: {len(rows)} 只")
            all_rows.extend(rows)
    except Exception as e:
        logger.warning(f"强势股池拉取失败: {e}")

    # ── 5. 龙虎榜 ─────────────────────────────────────────────────────────
    try:
        logger.info(f"[{date_str}] 拉取龙虎榜...")
        lhb_rows = fetch_lhb_events(trade_dt, existing_keys)
        logger.info(f"  龙虎榜: {len(lhb_rows)} 只")
        all_rows.extend(lhb_rows)
    except Exception as e:
        logger.warning(f"龙虎榜拉取失败: {e}")

    # 过滤已有
    new_rows = [
        r for r in all_rows
        if (r["股票代码"], r["日期"], r["异动类型"]) not in existing_keys
    ]
    logger.info(f"[{date_str}] 新增事件 {len(new_rows)} 条（已有 {len(all_rows)-len(new_rows)} 条跳过）")

    return new_rows


def run_daily():
    """每日增量：采集最近交易日数据"""
    trade_dt = get_latest_trade_date()
    if trade_dt is None:
        logger.error("无法获取交易日，退出")
        return

    existing = load_existing_events_by_type()
    rows = fetch_daily_events(trade_dt, set(existing.keys()))
    if rows:
        save_events(rows, mode="a")
        logger.info(f"今日({trade_dt})采集完成，新增 {len(rows)} 条")
    else:
        logger.info(f"今日({trade_dt})无新事件（可能非交易日或数据已存在）")


def run_historical(start_date, end_date, force=False):
    """历史回溯：采集指定日期区间数据"""
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end   = datetime.strptime(end_date, "%Y%m%d").date()

    cal = ak.tool_trade_date_hist_sina()
    trade_dates = sorted([d for d in cal["trade_date"].tolist() if start <= d <= end])

    logger.info(f"历史回溯: {start} ~ {end}，共 {len(trade_dates)} 个交易日")

    existing = {} if force else load_existing_events_by_type()
    all_new = []

    for i, trade_dt in enumerate(trade_dates, 1):
        logger.info(f"[{i}/{len(trade_dates)}] {trade_dt}")
        rows = fetch_daily_events(trade_dt, set(existing.keys()))
        for r in rows:
            key = (r["股票代码"], r["日期"], r["异动类型"])
            existing[key] = r
            all_new.append(r)
        logger.info(f"  新增 {len(rows)} 条")
        time.sleep(1)  # 避免请求过快

    # 全量重写（force=True 时）或合并写入
    mode = "w" if force else "a"
    if existing:
        save_events(list(existing.values()), mode=mode)
        logger.info(f"历史回溯完成！共 {len(existing)} 条记录（新增 {len(all_new)} 条）")


# ── 入口 ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="股票异动事件采集")
    parser.add_argument("--start", help="历史回溯起始日期 YYYYMMDD")
    parser.add_argument("--end",   help="历史回溯结束日期 YYYYMMDD")
    parser.add_argument("--force", action="store_true", help="强制覆盖已有数据")
    args = parser.parse_args()

    if args.start and args.end:
        run_historical(args.start, args.end, force=args.force)
    else:
        run_daily()
