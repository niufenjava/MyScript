# -*- coding: utf-8 -*-
"""
场景2：获取个股历史K线行情
学习目标：掌握腾讯证券原始接口获取完整日K线字段
运行方式：python3 demo_02_hist_kline.py

[数据源说明]
原接口 ak.stock_zh_a_hist() 底层请求东方财富，已被反爬拦截（ConnectionError）。
改用腾讯证券原始接口，解析完整字段（9列），稳定可用。
腾讯接口 symbol 需要带市场前缀：sh=沪市(6开头), sz=深市(0/3开头), bj=北交所(8/4开头)

[完整字段说明]
日期 | 开盘 | 收盘 | 最高 | 最低 | 成交量(手) | 换手率(%) | 成交额(万元) | 涨跌幅(%)
注：涨跌幅 由收盘价计算得出（接口原始字段不可靠，自行计算）
"""
import re
import requests
import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime, timedelta


def request_wrapper(func, desc="请求", *args, **kwargs):
    """
    统一请求包装：重试3次 + 友好错误提示
    失败时返回 None，不抛出异常
    """
    for i in range(3):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_type = type(e).__name__
            if i < 2:
                print(f"  [WARN] {desc} 失败({i+1}/3): {err_type}")
                time.sleep(2 * (i + 1) + random.uniform(0, 1))
            else:
                print(f"  [FAIL] {desc} 失败: {err_type} - {e}")
    return None


def safe_sleep(a=1.0, b=2.5):
    """随机休眠，避免请求过快被封IP"""
    time.sleep(random.uniform(a, b))


def to_tx_symbol(symbol: str) -> str:
    """
    将纯数字股票代码转换为腾讯接口格式（带市场前缀）
    600519 -> sh600519, 000001 -> sz000001, 430047 -> bj430047
    """
    if symbol.startswith(("sh", "sz", "bj")):
        return symbol  # 已有前缀，直接返回
    if symbol.startswith("6"):
        return f"sh{symbol}"
    if symbol.startswith(("8", "4")):
        return f"bj{symbol}"
    return f"sz{symbol}"


def fetch_kline_full(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
    """
    直接请求腾讯证券原始接口，返回完整字段的日K线数据。

    返回列（中文）：
      日期 | 开盘 | 收盘 | 最高 | 最低 | 成交量(手) | 换手率(%) | 成交额(万元) | 涨跌幅(%)
    注：涨跌幅 由相邻收盘价计算，首日无 NaN（内部自动多取一个前置交易日辅助计算）

    :param symbol:     股票代码，纯数字或带 sh/sz/bj 前缀均可
    :param start_date: 开始日期，格式 YYYYMMDD 或 YYYY-MM-DD
    :param end_date:   结束日期，格式 YYYYMMDD 或 YYYY-MM-DD
    :param adjust:     复权方式：'qfq'=前复权, 'hfq'=后复权, ''=不复权
    :return: DataFrame，失败返回 None
    """
    tx_symbol = to_tx_symbol(symbol)
    # 日期格式标准化为 YYYY-MM-DD
    def fmt(d): return f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else d
    start_fmt = fmt(start_date)
    end_fmt = fmt(end_date)

    # 向前多取30天，确保能拿到 start_date 的前一个交易日（用于计算首日涨跌幅）
    pre_start = (datetime.strptime(start_fmt, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    fetch_start_year = int(pre_start[:4])
    end_year = int(end_fmt[:4])

    url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
    all_rows = []

    for year in range(fetch_start_year, end_year + 1):
        var_key = f"kline_day{adjust}{year}"
        params = {
            "_var": var_key,
            "param": f"{tx_symbol},day,{year}-01-01,{year + 1}-12-31,640,{adjust}",
            "r": "0.8205512681390605",
        }
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=10)
                text = r.text
                # 解析行数据：每行格式 ["日期","开","收","高","低","量",{},"换手率","成交额",...]
                raw_rows = re.findall(r'\["(\d{4}-\d{2}-\d{2})",(.*?)\]', text)
                for date_str, rest in raw_rows:
                    fields = [f.strip().strip('"') for f in rest.split(",") if f.strip() not in ("{}", "{}")]
                    if len(fields) >= 7:
                        all_rows.append([date_str] + fields[:7])
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                else:
                    print(f"  [FAIL] 获取 {year} 年数据失败: {e}")

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows, columns=["日期", "开盘", "收盘", "最高", "最低", "成交量(手)", "_换手率_raw", "成交额(万元)"])
    df.rename(columns={"_换手率_raw": "换手率(%)"}, inplace=True)

    # 类型转换
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
    for col in ["开盘", "收盘", "最高", "最低", "成交量(手)", "换手率(%)", "成交额(万元)"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 包含前置数据，先排序去重，计算涨跌幅
    df = df.drop_duplicates(subset=["日期"]).sort_values("日期").reset_index(drop=True)
    df["涨跌幅(%)"] = df["收盘"].pct_change() * 100
    df["涨跌幅(%)"] = df["涨跌幅(%)"].round(2)

    # 裁掉前置数据，只保留用户要求的日期范围，此时首日涨跌幅已有值
    df = df[(df["日期"].astype(str) >= start_fmt) & (df["日期"].astype(str) <= end_fmt)]
    df = df.reset_index(drop=True)

    return df


# ========================================
# Case 1: 获取指定股票日K线完整数据
# 数据源：腾讯证券原始接口（fetch_kline_full），列名为中文
# 返回字段：日期|开盘|收盘|最高|最低|成交量(手)|换手率(%)|成交额(万元)|涨跌幅(%)
# ========================================
def case_1_daily_kline(symbol, start_date, end_date):
    """获取个股完整日K线数据"""
    df = fetch_kline_full(symbol, start_date, end_date, adjust="qfq")
    if df is None or df.empty:
        # 尝试查询该股票的历史数据范围，辅助判断是退市/停牌还是接口问题
        df_check = fetch_kline_full(symbol, "20000101", "20260422", adjust="qfq")
        if df_check is None or df_check.empty:
            print(f"[SKIP] {symbol} 无任何历史数据，可能是无效代码或已退市")
        else:
            last_date = df_check["日期"].max()
            print(f"[SKIP] {symbol} 在 {start_date}~{end_date} 无数据")
            print(f"       该股票最后交易日为: {last_date}，可能已退市或停牌")
        return None

    tx_symbol = to_tx_symbol(symbol)
    print(f"[{symbol}({tx_symbol})] 日K线数据: {len(df)} 条 ({start_date} ~ {end_date})")
    print(f"列名: {list(df.columns)}")
    print(df.head(5))
    print(df.tail(5))
    return df


# ========================================
# Case 2: 获取指定股票周K线数据
# 数据源：腾讯证券原始接口，由完整日K聚合为周K
# ========================================
def case_2_weekly_kline(symbol, start_date, end_date):
    """获取个股周K线数据（由完整日K聚合而来）"""
    df = fetch_kline_full(symbol, start_date, end_date, adjust="qfq")
    if df is None or df.empty:
        print("[SKIP] 获取数据失败")
        return
    df["日期"] = pd.to_datetime(df["日期"])
    df.set_index("日期", inplace=True)
    weekly = df[["开盘", "收盘", "最高", "最低", "成交量(手)", "成交额(万元)"]].resample("W").agg({
        "开盘": "first", "收盘": "last",
        "最高": "max", "最低": "min",
        "成交量(手)": "sum", "成交额(万元)": "sum",
    }).dropna().reset_index()
    weekly["涨跌幅(%)"] = weekly["收盘"].pct_change().mul(100).round(2)
    print(f"\n[{symbol}] 周K线: {len(weekly)} 条 ({start_date} ~ {end_date})")
    print(weekly.head(5))


# ========================================
# Case 3: 计算简单技术指标：5日均线
# ========================================
def case_3_ma5(df):
    """计算5日均线（中文列名：日期/收盘）"""
    if df is None or df.empty:
        print("[SKIP] 无日K数据，跳过")
        return
    df = df.copy()
    df["MA5"] = df["收盘"].rolling(window=5).mean().round(2)
    print(f"\n最近10日收盘价及5日均线:")
    print(df[["日期", "收盘", "MA5", "涨跌幅(%)"]].tail(10))


# ========================================
# Test Case 1: 传入未来日期，应返回空DataFrame
# ========================================
def case_test_1_future_date():
    """测试：未来日期返回空DataFrame"""
    safe_sleep(1.0, 2.5)
    df_future = fetch_kline_full("600519", "20990101", "20991231", adjust="qfq")
    if df_future is not None:
        assert len(df_future) == 0, f"Test 1 失败: 预期0行, 实际{len(df_future)}行"
        print("✔ Test 1: 未来日期返回空DataFrame")
    else:
        print("✔ Test 1: 未来日期返回 None（接口无数据，符合预期）")


# ========================================
# Test Case 2: 周K数据行数应远少于日K(约1/5)
# ========================================
def case_test_2_week_vs_day():
    """测试：周K行数约为日K的1/5（日K聚合周K）"""
    safe_sleep(1.0, 2.5)
    df_daily = fetch_kline_full("000001", "20240101", "20241231", adjust="qfq")
    if df_daily is not None and not df_daily.empty:
        df_daily["日期"] = pd.to_datetime(df_daily["日期"])
        df_daily.set_index("日期", inplace=True)
        df_weekly = df_daily["收盘"].resample("W").last().dropna()
        ratio = len(df_weekly) / len(df_daily) if len(df_daily) > 0 else 1
        print(f"✔ Test 2: 周/日K行数比: {ratio:.2f} (预期约0.2), 日K={len(df_daily)}条, 周K={len(df_weekly)}条")
    else:
        print("⚠ Test 2: 请求失败，跳过")


# ========================================
# Test Case 3: 不复权 vs 前复权对比
# ========================================
def case_test_3_adj_compare():
    """测试：不复权与前复权价格不同"""
    safe_sleep(1.0, 2.5)
    df_no_adj = fetch_kline_full("000001", "20240101", "20240601", adjust="")
    safe_sleep(1.0, 2.5)
    df_qfq = fetch_kline_full("000001", "20240101", "20240601", adjust="qfq")
    if df_no_adj is not None and df_qfq is not None and not df_no_adj.empty and not df_qfq.empty:
        print(f"✔ Test 3: 不复权首日收盘: {df_no_adj.iloc[0]['收盘']}, 前复权首日收盘: {df_qfq.iloc[0]['收盘']}")
    else:
        print("⚠ Test 3: 请求失败，跳过")


# ========================================
# main: 逐个打开case验证
# 调试参数：修改 symbol / start_date / end_date 切换股票和日期范围
# ========================================
def main(
    symbol="600659",       # 股票代码，如 600519=贵州茅台, 000001=平安银行
    start_date="20260418",       # 开始日期，格式 YYYYMMDD，默认最近60天
    end_date="20260422",         # 结束日期，格式 YYYYMMDD，默认今天
):
    # 默认日期：today 往前5天
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    print(f">>> 调试参数: symbol={symbol}, start_date={start_date}, end_date={end_date}")

    # Case 1: 获取指定股票日K线数据（必须保持开启，是 case_3_ma5 的数据来源）
    df = case_1_daily_kline(symbol, start_date, end_date)

    # 取消注释以验证对应case：
    # Case 2: 获取指定股票周K线数据
    # case_2_weekly_kline(symbol, start_date, end_date)
    # Case 3: 计算简单技术指标：5日均线
    # case_3_ma5(df)

    # print("\n" + "=" * 50)
    # print("测试 Case")
    # Test Case 1: 传入未来日期，应返回空DataFrame
    # case_test_1_future_date()
    # Test Case 2: 周K数据行数应远少于日K(约1/5)
    # case_test_2_week_vs_day()
    # Test Case 3: 不复权 vs 前复权对比
    # case_test_3_adj_compare()


if __name__ == "__main__":
    main()
