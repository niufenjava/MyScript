"""快速测试 fetch_daily_full.py（腾讯接口版）"""
import sys, os
# 将项目根目录加入搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fetch_daily_full import (
    fetch_stock_daily, to_tx_symbol,
    upsert_stock_parquet, read_stock_parquet, get_last_date, DATA_DIR
)
os.makedirs(DATA_DIR, exist_ok=True)

# ── 测试1: 腾讯接口连通性 + 字段完整性 ──
codes = ["000001", "600519", "300750", "002594", "603659"]
names = ["平安银行", "贵州茅台", "宁德时代", "比亚迪", "璞泰来"]

print("=" * 70)
print("测试1: 腾讯接口连通性（5只股票，直连无代理）")
print("=" * 70)

for code, name in zip(codes, names):
    df = fetch_stock_daily(code, "2025-04-14", "2025-04-23")
    if df is not None and not df.empty:
        print(f"\n[OK] {code} {name}: {len(df)}条, 列: {list(df.columns)}")
        print(df.tail(3).to_string(index=False))
    else:
        print(f"\n[EMPTY] {code} {name}")

# ── 测试2: Parquet upsert + 增量去重 ──
print("\n" + "=" * 70)
print("测试2: Parquet 写入 → 增量追加 → 去重验证")
print("=" * 70)

test_code = "000001"

# 第一批: 4/1 - 4/10
df1 = fetch_stock_daily(test_code, "2025-04-01", "2025-04-10")
added1 = upsert_stock_parquet(test_code, df1)
last1 = get_last_date(test_code)
print(f"[第1批] 写入 {added1} 条, 最新日期: {last1}")

# 第二批: 4/8 - 4/23（有3天重叠）
df2 = fetch_stock_daily(test_code, "2025-04-08", "2025-04-23")
added2 = upsert_stock_parquet(test_code, df2)
last2 = get_last_date(test_code)
print(f"[第2批] 新增 {added2} 条（重叠去重后）, 最新日期: {last2}")

# 验证总数据
final = read_stock_parquet(test_code)
print(f"[合计] {len(final)} 条")
print(final.to_string(index=False))

# ── 测试3: Parquet 查询示例 ──
print("\n" + "=" * 70)
print("测试3: Parquet 查询示例")
print("=" * 70)

import pandas as pd
from datetime import date

df = read_stock_parquet(test_code)

# 3.1 查看基本信息
print(f"\n--- 3.1 基本信息 ---")
print(f"股票: {test_code}, 总行数: {len(df)}, 列: {list(df.columns)}")
print(f"日期范围: {df['日期'].min()} ~ {df['日期'].max()}")

# 3.2 按日期范围查询
print(f"\n--- 3.2 按日期范围查询（4/14 ~ 4/18）---")
mask = (df["日期"] >= date(2025, 4, 14)) & (df["日期"] <= date(2025, 4, 18))
print(df[mask].to_string(index=False))

# 3.3 按条件筛选：涨跌幅 > 1%
print(f"\n--- 3.3 条件筛选：涨跌幅 > 1% ---")
up = df[df["涨跌幅(%)"] > 1]
print(up.to_string(index=False) if not up.empty else "无符合条件的数据")

# 3.4 按条件筛选：跌幅 > 1%
print(f"\n--- 3.4 条件筛选：跌幅 > 1% ---")
down = df[df["涨跌幅(%)"] < -1]
print(down.to_string(index=False) if not down.empty else "无符合条件的数据")

# 3.5 统计汇总
print(f"\n--- 3.5 统计汇总 ---")
print(f"平均收盘价: {df['收盘'].mean():.2f}")
print(f"最高价: {df['最高'].max():.2f} ({df.loc[df['最高'].idxmax(), '日期']})")
print(f"最低价: {df['最低'].min():.2f} ({df.loc[df['最低'].idxmin(), '日期']})")
print(f"日均成交量: {df['成交量'].mean():.0f}")
print(f"日均换手率: {df['换手率'].mean():.2f}%")
print(f"区间涨跌幅: {((df['收盘'].iloc[-1] / df['收盘'].iloc[0]) - 1) * 100:.2f}%")

# 3.6 最近N条
print(f"\n--- 3.6 最近5条 ---")
print(df.tail(5).to_string(index=False))

# 清理
test_path = os.path.join(DATA_DIR, f"{test_code}.parquet")
if os.path.exists(test_path):
    os.remove(test_path)
    print(f"\n[清理] 已删除 {test_path}")
