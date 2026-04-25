"""
Parquet 股票日线数据查询工具
用法: python -m utils.query_daily <股票代码> [天数]
示例: python -m utils.query_daily 603659 20
"""
import os
import sys
import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "daily")


def query(code, n=20):
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    if not os.path.exists(path):
        print(f"未找到 {code} 的数据文件: {path}")
        return

    df = pd.read_parquet(path)
    df["日期"] = pd.to_datetime(df["日期"])
    df.sort_values("日期", ascending=False, inplace=True)
    df = df.head(n)

    # 格式化输出
    df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
    print(f"\n{'='*75}")
    print(f"  {code} 近 {n} 个交易日日线（倒序）  共 {len(df)} 条")
    print(f"{'='*75}")
    print(df.to_string(index=False))
    print()


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "603659"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    query(code, n)
