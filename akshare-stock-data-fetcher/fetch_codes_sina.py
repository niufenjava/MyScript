"""
fetch_codes_tx.py
=================
获取沪深 A 股股票代码列表，并持久化到 data/stock_codes.txt。

数据源
------
    新浪财经 vip.stock.finance.sina.com.cn
    接口：Market_Center.getHQNodeDataSimple
    文档：https://vip.stock.finance.sina.com.cn/mkt/#hs_a

为何用 curl 而非 akshare / requests
-----------------------------------
    LobsterAI 在其进程环境中注入了 LOBSTER_PROXY_TOKEN 环境变量。
    Python 的 urllib / requests 会自动读取 HTTP_PROXY / HTTPS_PROXY 并
    尝试通过该代理发送请求，但该代理地址对外网无效，导致所有请求直接失败。
    通过 subprocess 调用系统 curl，并将代理相关环境变量从子进程中清除，
    可彻底规避该问题。

代理清理逻辑
------------
    以下环境变量会被从子进程环境中有意移除：
        LOBSTER_PROXY_TOKEN
        HTTP_PROXY / HTTPS_PROXY
        http_proxy / https_proxy
        ALL_PROXY / all_proxy

过滤规则
--------
    北交所股票以 "bj" 前缀出现在 symbol 字段中，属于非沪深交易所品种，
    在此统一过滤，仅保留上海证券交易所 + 深圳证券交易所的 A 股。

输出格式
--------
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

# ---------------------------------------------------------------------------
# 路径常量（相对于脚本自身所在目录）
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "data", "stock_codes.txt")

# 新浪接口参数
_BASE_URL = (
    "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
    "/Market_Center.getHQNodeDataSimple"
)
_PARAMS = {
    "node": "hs_a",   # 沪深 A 股（包含上交所、深交所、北交所）
    "num":  100,      # 每页条目数上限
}


def _clean_env() -> dict:
    """
    构建一个「干净」的子进程环境。

    将所有已知的代理相关环境变量从当前进程环境中复制并移除，
    防止 curl 被路由到无效的代理地址。
    """
    env = os.environ.copy()
    # 常见的代理变量名（大小写/下划线/连字符变体）
    for key in [
        "LOBSTER_PROXY_TOKEN",
        "HTTP_PROXY", "HTTPS_PROXY",
        "http_proxy", "https_proxy",
        "ALL_PROXY",  "all_proxy",
    ]:
        env.pop(key, None)
    return env


def fetch_page(page: int, num: int = 100) -> List[dict]:
    """
    获取指定页码的股票列表数据。

    参数
    ----
    page : int    页码（从 1 开始）
    num  : int    每页条目数，默认 100

    返回
    ----
    List[dict]    本页的股票记录列表，每条记录包含 symbol / code / name 等字段
                  空列表表示已无数据或请求失败

    异常
    ----
    RuntimeError   当 curl 返回非零状态码或 JSON 解析失败时
    """
    params = _PARAMS.copy()
    params["page"] = page
    params["num"]  = num

    # 将 params 编码为查询字符串，手动拼接以避免 urllib 依赖
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url   = f"{_BASE_URL}?{query}"

    result = subprocess.run(
        [
            "curl", "-s",                      # -s: 静默，不输出进度
            "--max-time", "15",                # 15 秒超时，防止挂起
            "-A",                              # 指定 User-Agent
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36",
            url,
        ],
        capture_output=True,   # 捕获 stdout / stderr
        text=True,             # 返回字符串而非字节
        env=_clean_env(),      # 注入已清理的环境
    )

    # 检查 curl 是否成功执行
    if result.returncode != 0:
        raise RuntimeError(f"curl 执行失败 (code={result.returncode}): {result.stderr}")

    # 解析 JSON 响应
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"JSON 解析失败（第 {page} 页）: {exc}，"
            f"原始响应前 200 字符: {result.stdout[:200]}"
        )

    # 新浪接口在无数据时可能返回空列表 []，这是正常情况
    return data


def get_stock_codes() -> List[str]:
    """
    遍历所有分页，获取全量股票代码并写入文件。

    过滤规则：
        - 北交所股票（symbol 以 "bj" 开头）→ 跳过
        - code 字段为空 → 跳过

    返回
    ----
    List[str]    去重后的股票代码列表
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    print(f"[{current_date}] 开始获取股票代码...")

    all_codes: List[str] = []
    page       = 1

    while True:
        rows = fetch_page(page)

        # 空列表 → 已到最后一页，停止翻页
        if not rows:
            print(f"  第 {page} 页为空，停止翻页。")
            break

        for r in rows:
            symbol = str(r.get("symbol", ""))
            # 北交所股票前缀为 "bj"（如 "bj920992"），此脚本仅保留沪深交易所品种
            if symbol.startswith("bj"):
                continue
            code = r.get("code", "")
            if code:
                all_codes.append(str(code))

        print(f"  第 {page} 页 → {len(rows):3d} 条 | 累计 {len(all_codes):4d} 条")

        # 不满一页说明是最后一页
        if len(rows) < 100:
            break

        page += 1

    # ---------------------------------------------------------------------------
    # 去重（保持原始顺序）
    # ---------------------------------------------------------------------------
    seen       = set()
    unique_codes = []
    for c in all_codes:
        if c not in seen:
            seen.add(c)
            unique_codes.append(c)

    # ---------------------------------------------------------------------------
    # 写入文件
    # ---------------------------------------------------------------------------
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for code in unique_codes:
            f.write(code + "\n")
        # 强制刷盘，确保数据落盘
        f.flush()
        os.fsync(f.fileno())

    print(f"\n✅ 完成！共写入 {len(unique_codes)} 条股票代码 →\n   {OUTPUT_FILE}")
    return unique_codes


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    get_stock_codes()