#!/usr/bin/env python3
"""
从 stock_codes.txt 读取股票列表，获取实时市场数据（腾讯扩展行情），输出到 stockMarketData.csv
每次执行都会刷新全部数据
"""
import logging
import os
import time
from typing import Dict

import requests

from fetcher._csv_utils import save_csv
from fetcher._shared import PROJECT_ROOT, load_stock_codes

# ── 日志 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── 路径 ───────────────────────────────────────────────────────────────────
CSV_PATH = os.path.join(PROJECT_ROOT, "data", "stockMarketData.csv")

# 市场数据字段（腾讯）
MARKET_FIELDS = [
    "A股代码", "最新价", "总市值(亿)", "每股收益", "每股净资产", "市净率",
    "股息率(%)", "动态市盈率", "静态市盈率", "市盈率TTM", "ROE",
]

# 腾讯扩展行情字段索引
# [3]最新价 [39]动态PE [44]总市值(亿) [46]PB
# [52]PE_TTM [53]静态PE [56]股息率(%) [66]EPS [67]每股净资产 [68]ROE
TENCENT_FIELD_MAP: Dict[str, int] = {
    "最新价": 3,
    "动态市盈率": 39,
    "总市值(亿)": 44,
    "市净率": 46,
    "市盈率TTM": 52,
    "静态市盈率": 53,
    "每股收益": 66,
    "每股净资产": 67,
    "股息率(%)": 56,
    "ROE": 68,
}

INVALID_VALUES = {"", "-", "N/A", "None", None}
BATCH_SIZE = 50


def _to_tencent(code: str) -> str:
    """统一转为腾讯格式 (sh/sz前缀)"""
    if code.startswith(("sh", "sz")):
        return code
    code = code.zfill(6)
    return f"sh{code}" if code.startswith("6") else f"sz{code}"


def _is_valid_info(info: dict) -> bool:
    """判断是否有效数据（非交易时段字段为空/占位则判为无效）"""
    for k in ("最新价", "动态市盈率", "市盈率TTM"):
        val = info.get(k, "").strip()
        if val and val not in INVALID_VALUES:
            return True
    return False


def fetch_market_batch(codes: list[str]) -> dict:
    """批量获取腾讯扩展行情，返回 {sh/sz代码: {字段dict}}"""
    result: dict = {}
    total = len(codes)
    start_time = time.time()

    for batch_start in range(0, total, BATCH_SIZE):
        batch = codes[batch_start:batch_start + BATCH_SIZE]
        tx_codes = [_to_tencent(c) for c in batch]
        url = f"http://qt.gtimg.cn/q={','.join(tx_codes)}"

        try:
            r = requests.get(url, timeout=15)
            for line in r.text.strip().split(";"):
                line = line.strip()
                if not line or "~" not in line:
                    continue
                fields = line.split("~")
                if len(fields) < 80:
                    continue
                pure_code = fields[2]
                full_code = f"sh{pure_code}" if pure_code.startswith("6") else f"sz{pure_code}"
                info = {}
                for name, idx in TENCENT_FIELD_MAP.items():
                    try:
                        info[name] = fields[idx]
                    except (IndexError, ValueError):
                        info[name] = ""
                if not _is_valid_info(info):
                    continue
                result[full_code] = info
        except Exception as e:
            logger.warning("  批次 %d 请求失败: %s", batch_start // BATCH_SIZE + 1, e)

        done = min(batch_start + BATCH_SIZE, total)
        if done % 500 == 0 or done == total:
            elapsed = time.time() - start_time
            logger.info("  [%d/%d] 已获取市场数据 (%.1f秒)", done, total, elapsed)

    return result


def main() -> None:
    codes = load_stock_codes(prefixed=False)  # 纯6位数字
    logger.info("共 %d 只股票", len(codes))

    logger.info("[市场数据] 开始获取 %d 只股票的实时数据...", len(codes))
    market_data = fetch_market_batch(codes)
    logger.info("[市场数据] 完成! 获取 %d 只股票数据", len(market_data))

    # 保存
    rows = []
    for code in codes:
        tx_code = _to_tencent(code)
        info = market_data.get(tx_code, {})
        row = {"A股代码": code}
        for f in MARKET_FIELDS[1:]:
            row[f] = info.get(f, "")
        rows.append(row)
    save_csv(rows, CSV_PATH, MARKET_FIELDS, mode="w")
    logger.info("已保存到: %s (%d 行)", CSV_PATH, len(rows))


if __name__ == "__main__":
    main()
