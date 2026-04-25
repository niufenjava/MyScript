"""
跨股筛选工具 — 基于 all_daily.parquet 大表快速筛选
用法:
  python screen_stock.py 缩量 3          # 近3天连续缩量
  python screen_stock.py 放量 3          # 近3天连续放量
  python screen_stock.py 连涨 3          # 近3天连续上涨
  python screen_stock.py 连跌 3          # 近3天连续下跌
  python screen_stock.py 新高 20         # 近20日创新高
  python screen_stock.py 地量 20         # 近20日成交量最低（今天是近20日最小量）
"""
import os
import sys
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ALL_DAILY_PATH = os.path.join(SCRIPT_DIR, "data", "all_daily.parquet")


def load_data():
    if not os.path.exists(ALL_DAILY_PATH):
        print(f"未找到合并大表: {ALL_DAILY_PATH}")
        print("请先运行: python -m utils.merge_daily")
        sys.exit(1)
    df = pd.read_parquet(ALL_DAILY_PATH)
    df["日期"] = pd.to_datetime(df["日期"])
    return df


def screen_volume_shrink(df, days=3):
    """近N天连续缩量：每天成交量 < 前一天"""
    print(f"\n{'='*60}")
    print(f"  筛选: 近 {days} 天连续缩量")
    print(f"{'='*60}")

    results = []
    for code, g in df.groupby("代码"):
        g = g.sort_values("日期").tail(days + 1)  # 多取1天用于比较
        if len(g) < days + 1:
            continue
        vols = g["成交量"].values
        # 检查最近 days 天是否每天都比前一天小
        shrinking = all(vols[i] > vols[i + 1] for i in range(len(vols) - days, len(vols) - 1))
        if shrinking:
            last = g.iloc[-1]
            results.append({
                "代码": code,
                "最新日期": last["日期"].strftime("%Y-%m-%d"),
                "收盘": last["收盘"],
                "涨跌幅(%)": last["涨跌幅(%)"],
                "最新成交量": int(last["成交量"]),
            })

    return pd.DataFrame(results)


def screen_volume_expand(df, days=3):
    """近N天连续放量：每天成交量 > 前一天"""
    print(f"\n{'='*60}")
    print(f"  筛选: 近 {days} 天连续放量")
    print(f"{'='*60}")

    results = []
    for code, g in df.groupby("代码"):
        g = g.sort_values("日期").tail(days + 1)
        if len(g) < days + 1:
            continue
        vols = g["成交量"].values
        expanding = all(vols[i] < vols[i + 1] for i in range(len(vols) - days, len(vols) - 1))
        if expanding:
            last = g.iloc[-1]
            results.append({
                "代码": code,
                "最新日期": last["日期"].strftime("%Y-%m-%d"),
                "收盘": last["收盘"],
                "涨跌幅(%)": last["涨跌幅(%)"],
                "最新成交量": int(last["成交量"]),
            })

    return pd.DataFrame(results)


def screen_consecutive_up(df, days=3):
    """近N天连续上涨"""
    print(f"\n{'='*60}")
    print(f"  筛选: 近 {days} 天连续上涨")
    print(f"{'='*60}")

    results = []
    for code, g in df.groupby("代码"):
        g = g.sort_values("日期").tail(days)
        if len(g) < days:
            continue
        if all(g["涨跌幅(%)"].values > 0):
            last = g.iloc[-1]
            total_pct = ((1 + g["涨跌幅(%)"] / 100).prod() - 1) * 100
            results.append({
                "代码": code,
                "最新日期": last["日期"].strftime("%Y-%m-%d"),
                "收盘": last["收盘"],
                f"{days}日累计涨幅(%)": round(total_pct, 2),
                "最新成交量": int(last["成交量"]),
            })

    return pd.DataFrame(results)


def screen_consecutive_down(df, days=3):
    """近N天连续下跌"""
    print(f"\n{'='*60}")
    print(f"  筛选: 近 {days} 天连续下跌")
    print(f"{'='*60}")

    results = []
    for code, g in df.groupby("代码"):
        g = g.sort_values("日期").tail(days)
        if len(g) < days:
            continue
        if all(g["涨跌幅(%)"].values < 0):
            last = g.iloc[-1]
            total_pct = ((1 + g["涨跌幅(%)"] / 100).prod() - 1) * 100
            results.append({
                "代码": code,
                "最新日期": last["日期"].strftime("%Y-%m-%d"),
                "收盘": last["收盘"],
                f"{days}日累计跌幅(%)": round(total_pct, 2),
                "最新成交量": int(last["成交量"]),
            })

    return pd.DataFrame(results)


def screen_new_high(df, days=20):
    """近N日创新高：今日收盘 = 近N日最高"""
    print(f"\n{'='*60}")
    print(f"  筛选: 近 {days} 日创新高")
    print(f"{'='*60}")

    results = []
    for code, g in df.groupby("代码"):
        g = g.sort_values("日期").tail(days)
        if len(g) < days:
            continue
        last = g.iloc[-1]
        if last["收盘"] >= g["收盘"].max():
            results.append({
                "代码": code,
                "最新日期": last["日期"].strftime("%Y-%m-%d"),
                "收盘": last["收盘"],
                f"{days}日最高": g["最高"].max(),
                "涨跌幅(%)": last["涨跌幅(%)"],
            })

    return pd.DataFrame(results)


def screen_low_volume(df, days=20):
    """近N日地量：今日成交量 = 近N日最低"""
    print(f"\n{'='*60}")
    print(f"  筛选: 近 {days} 日地量")
    print(f"{'='*60}")

    results = []
    for code, g in df.groupby("代码"):
        g = g.sort_values("日期").tail(days)
        if len(g) < days:
            continue
        last = g.iloc[-1]
        if last["成交量"] <= g["成交量"].min():
            results.append({
                "代码": code,
                "最新日期": last["日期"].strftime("%Y-%m-%d"),
                "收盘": last["收盘"],
                "涨跌幅(%)": last["涨跌幅(%)"],
                "今日成交量": int(last["成交量"]),
                f"{days}日均量": int(g["成交量"].mean()),
            })

    return pd.DataFrame(results)


STRATEGIES = {
    "缩量": screen_volume_shrink,
    "放量": screen_volume_expand,
    "连涨": screen_consecutive_up,
    "连跌": screen_consecutive_down,
    "新高": screen_new_high,
    "地量": screen_low_volume,
}


def main():
    if len(sys.argv) < 2:
        print("用法: python screen_stock.py <策略> [天数]")
        print(f"可用策略: {', '.join(STRATEGIES.keys())}")
        print("示例: python screen_stock.py 缩量 3")
        sys.exit(0)

    strategy_name = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    if strategy_name not in STRATEGIES:
        print(f"未知策略: {strategy_name}")
        print(f"可用策略: {', '.join(STRATEGIES.keys())}")
        sys.exit(1)

    df = load_data()
    result = STRATEGIES[strategy_name](df, days)

    if result.empty:
        print("未找到符合条件的股票")
    else:
        print(f"\n共 {len(result)} 只符合条件:")
        print(result.to_string(index=False))
    print()


if __name__ == "__main__":
    main()
