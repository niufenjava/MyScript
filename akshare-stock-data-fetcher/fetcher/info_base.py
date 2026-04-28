#!/usr/bin/env python3
"""
从 stock_codes.txt 读取股票列表，获取股票基本信息（巨潮资讯 cninfo），输出到 stockBaseInfo.csv
已有完整基础信息的股票自动跳过
"""
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import akshare as ak

from fetcher._csv_utils import load_existing_csv, save_csv
from fetcher._shared import PROJECT_ROOT, load_stock_codes, strip_prefix

# ── 日志 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── 路径 ───────────────────────────────────────────────────────────────────
CSV_PATH = os.path.join(PROJECT_ROOT, "data", "stockBaseInfo.csv")

# 基础信息字段（巨潮）
BASE_FIELDS = [
    "A股代码", "A股简称", "H股代码", "H股简称",
    "成立日期", "上市日期", "所属市场", "所属行业",
    "入选指数", "主营业务",
]


def is_base_complete(row: dict) -> bool:
    """判断基础信息是否已完整（有名称和行业即视为完整）"""
    return bool(row.get("A股简称", "").strip()) and bool(row.get("所属行业", "").strip())


def fetch_base_one(code: str, retries: int = 2) -> tuple[dict, str | None]:
    """获取单只股票的基础信息，code 为 sh/sz 前缀格式"""
    pure_code = strip_prefix(code)
    for attempt in range(retries + 1):
        try:
            df = ak.stock_profile_cninfo(symbol=pure_code)
            if df.empty:
                info = {f: "" for f in BASE_FIELDS}
                info["A股代码"] = code
                return info, None
            row = df.iloc[0]
            info = {}
            for f in BASE_FIELDS:
                val = row.get(f, "")
                info[f] = "" if val is None or str(val) == "None" else str(val)
            info["A股代码"] = code
            return info, None
        except Exception as e:
            if attempt < retries:
                time.sleep(1)
            else:
                info = {f: "" for f in BASE_FIELDS}
                info["A股代码"] = code
                return info, str(e)
    return {f: "" for f in BASE_FIELDS}, "max retries"


def fetch_base_info(codes: list[str], existing: dict) -> dict:
    """批量获取基础信息，跳过已有的"""
    need_fetch = [c for c in codes if c not in existing or not is_base_complete(existing[c])]

    if not need_fetch:
        logger.info("[基础信息] 全部 %d 只股票已有基础信息，跳过", len(codes))
        return existing

    logger.info("[基础信息] 需获取 %d 只（已有 %d 只跳过）", len(need_fetch), len(codes) - len(need_fetch))
    success = fail = 0
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_base_one, code): code for code in need_fetch}
        for i, future in enumerate(as_completed(futures), 1):
            info, err = future.result()
            code = info["A股代码"]
            if err:
                fail += 1
                if fail <= 20:
                    logger.warning("  [%d/%d] %s 失败: %s", i, len(need_fetch), code, err)
            else:
                success += 1
            if code not in existing:
                existing[code] = {f: "" for f in BASE_FIELDS}
            for f in BASE_FIELDS:
                existing[code][f] = info[f]

            if i % 100 == 0 or i == len(need_fetch):
                elapsed = time.time() - start_time
                speed = i / elapsed if elapsed > 0 else 0
                eta = (len(need_fetch) - i) / speed if speed > 0 else 0
                logger.info(
                    "  [%d/%d] 成功:%d 失败:%d 速度:%.1f只/秒 剩余:%.1f分钟",
                    i, len(need_fetch), success, fail, speed, eta / 60
                )

    logger.info("[基础信息] 完成! 成功:%d 失败:%d", success, fail)
    return existing


def save_info(codes: list[str], existing: dict) -> None:
    """按 stock_codes.txt 顺序保存 CSV"""
    rows = []
    for code in codes:
        if code in existing:
            row = {f: existing[code].get(f, "") for f in BASE_FIELDS}
            rows.append(row)
    save_csv(rows, CSV_PATH, BASE_FIELDS, mode="w")
    logger.info("已保存到: %s (%d 行)", CSV_PATH, len(rows))


def _is_main() -> bool:
    """判断是否为直接执行，防止 fork 出问题（macOS 默认 spawn）"""
    return __import__("sys").argv[0].endswith("info_base.py") or __import__("sys").argv[0].endswith("run.py")


def main():
    codes = load_stock_codes(prefixed=True)
    logger.info("共 %d 只股票", len(codes))

    existing = load_existing_csv(CSV_PATH)
    if existing:
        logger.info("已有 CSV 包含 %d 条记录", len(existing))

    existing = fetch_base_info(codes, existing)
    save_info(codes, existing)


if __name__ == "__main__" and _is_main():
    main()
