# -*- coding: utf-8 -*-
"""
获取沪深 A 股股票代码列表，持久化到 data/stock_codes.txt。

数据源
----
    新浪财经 vip.stock.finance.sina.com.cn（Market_Center.getHQNodeDataSimple）

为何用 curl 而非 akshare / requests
----
    LobsterAI 在其进程环境中注入了 LOBSTER_PROXY_TOKEN 环境变量，
    Python 的 urllib / requests 会自动读取 HTTP_PROXY / HTTPS_PROXY 并
    尝试通过该代理发送请求，但该代理地址对外网无效，导致请求直接失败。
    通过 subprocess 调用系统 curl，并将代理相关环境变量从子进程中清除，
    可彻底规避该问题。

过滤规则
----
    北交所股票以 "bj" 前缀出现，在此统一过滤，仅保留上交所 + 深交所 A 股。

输出格式
----
    每行一个 6 位股票代码，如：
        600519
        000001
    去重并保持原始顺序。
"""

import subprocess
import json
import os
from datetime import datetime
from typing import List

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE  = os.path.join(PROJECT_ROOT, "data", "stock_codes.txt")

_BASE_URL = (
    "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
    "/Market_Center.getHQNodeDataSimple"
)
_PARAMS = {
    "node": "hs_a",   # 沪深 A 股
    "num":  100,
}


def _clean_env() -> dict:
    """将所有代理相关环境变量从子进程中移除，防止 curl 被路由到无效代理。"""
    env = os.environ.copy()
    for key in [
        "LOBSTER_PROXY_TOKEN",
        "HTTP_PROXY", "HTTPS_PROXY",
        "http_proxy", "https_proxy",
        "ALL_PROXY",  "all_proxy",
    ]:
        env.pop(key, None)
    return env


def fetch_page(page: int, num: int = 100) -> List[dict]:
    """获取指定页码的股票列表，空列表表示已无数据或请求失败。"""
    params = _PARAMS.copy()
    params["page"] = page
    params["num"]  = num

    query = "&".join(f"{k}={v}" for k, v in params.items())
    url   = f"{_BASE_URL}?{query}"

    result = subprocess.run(
        ["curl", "-s", "--max-time", "15",
         "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/120.0.0.0 Safari/537.36",
         url],
        capture_output=True,
        text=True,
        env=_clean_env(),
    )

    if result.returncode != 0:
        raise RuntimeError(f"curl 执行失败 (code={result.returncode}): {result.stderr}")

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"JSON 解析失败（第 {page} 页）: {exc}，"
            f"原始响应前 200 字符: {result.stdout[:200]}"
        )
    return data


def get_stock_codes() -> List[str]:
    """遍历所有分页，获取全量股票代码并写入文件。"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    print(f"[{current_date}] 开始获取股票代码...")

    all_codes: List[str] = []
    page = 1

    while True:
        rows = fetch_page(page)
        if not rows:
            print(f"  第 {page} 页为空，停止翻页。")
            break

        for r in rows:
            symbol = str(r.get("symbol", ""))
            # 北交所前缀为 "bj"（如 "bj920992"），跳过
            if symbol.startswith("bj"):
                continue
            code = r.get("code", "")
            if code:
                all_codes.append(str(code))

        print(f"  第 {page} 页 → {len(rows):3d} 条 | 累计 {len(all_codes):4d} 条")

        if len(rows) < 100:   # 不满一页说明是最后一页
            break
        page += 1

    # 去重，保持原始顺序
    seen = set()
    unique_codes = []
    for c in all_codes:
        if c not in seen:
            seen.add(c)
            unique_codes.append(c)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for code in unique_codes:
            f.write(code + "\n")
        f.flush()
        os.fsync(f.fileno())

    print(f"\n✅ 完成！共写入 {len(unique_codes)} 条 → {OUTPUT_FILE}")
    return unique_codes


if __name__ == "__main__":
    get_stock_codes()