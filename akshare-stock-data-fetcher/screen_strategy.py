#!/usr/bin/env python3
"""
A股策略选股：连续3天缩量 + MA5/MA10均线交叉可视化
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── 路径配置 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
DATA_DIR   = SCRIPT_DIR / "data"
INFO_PATH  = SCRIPT_DIR / "stockInfo.csv"
OUTPUT_DIR = SCRIPT_DIR / "screening_output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── 读取数据 ──────────────────────────────────────────────
print("📂 加载数据文件...")
df_daily = pd.read_parquet(DATA_DIR / "all_daily.parquet")
df_info  = pd.read_csv(INFO_PATH)

# 标准化代码字段
df_daily["代码"] = df_daily["代码"].astype(str).str.zfill(6)
df_info["A股代码"] = df_info["A股代码"].astype(str).str.zfill(6)

# 建立代码→名称映射
code2name = dict(zip(df_info["A股代码"], df_info["A股简称"]))

print(f"   日线记录: {len(df_daily):,} 条")
print(f"   股票信息: {len(df_info):,} 条")

# ── Step 1: 筛选 00/60 开头 ───────────────────────────────
df_main = df_daily[df_daily["代码"].str.startswith(("00", "60"))].copy()
df_main = df_main.sort_values(["代码", "日期"]).reset_index(drop=True)
print(f"\n✅ 主板股票筛选后: {df_main['代码'].nunique():,} 只")

# ── Step 2: 计算最近3天是否连续缩量 ─────────────────────
df_main["成交量_前日"] = df_main.groupby("代码")["成交量"].shift(1)
df_main["缩量"] = df_main["成交量"] < df_main["成交量_前日"]

def check_consecutive_shrink(code_grp):
    """传入单个股票DataFrame，检查最后3条是否连续缩量"""
    grp = code_grp.sort_values("日期")
    if len(grp) < 3:
        return False
    return bool(all(grp["缩量"].tail(3)))

print("   正在计算缩量条件...")
cond_series = df_main.groupby("代码", sort=False).apply(check_consecutive_shrink)
shrink_stocks = cond_series[cond_series].index.tolist()
print(f"✅ 连续3天缩量: {len(shrink_stocks)} 只")

# ── Step 3: 提取这些股票最近20天数据 ─────────────────────
df_shrunk = df_main[df_main["代码"].isin(shrink_stocks)].copy()

# 每个股票取最后20条
df_viz = (df_shrunk
          .sort_values(["代码", "日期"])
          .groupby("代码", sort=False, group_keys=True)
          .tail(20)
          .reset_index(drop=True))

print(f"   提取最近20日数据完成")

# ── Step 4: 计算MA5/MA10 ────────────────────────────────
df_viz = df_viz.sort_values(["代码", "日期"]).reset_index(drop=True)

# 用循环比groupby+apply更稳
for code in shrink_stocks:
    mask = df_viz["代码"] == code
    prices = df_viz.loc[mask, "收盘"].values
    df_viz.loc[mask, "MA5"]  = pd.Series(prices).rolling(5).mean().values
    df_viz.loc[mask, "MA10"] = pd.Series(prices).rolling(10).mean().values

print("✅ MA5/MA10 计算完成")

# ── Step 5: 生成报告 ─────────────────────────────────────
def detect_crossovers(sub_df):
    """检测金叉/死叉信号列表"""
    g = sub_df.dropna(subset=["MA5", "MA10"]).sort_values("日期")
    if len(g) < 2:
        return []
    g = g.copy()
    g["ma5_le_ma10"] = g["MA5"] <= g["MA10"]
    signals = []
    for i in range(1, len(g)):
        prev = g.iloc[i-1]
        curr = g.iloc[i]
        if prev["ma5_le_ma10"] and not curr["ma5_le_ma10"]:
            signals.append(("金叉买入", curr["日期"], curr["收盘"]))
        elif not prev["ma5_le_ma10"] and curr["ma5_le_ma10"]:
            signals.append(("死叉卖出", curr["日期"], curr["收盘"]))
    return signals

def ascii_chart(prices, volumes, dates, ma5_list, ma10_list, name, code):
    n = len(prices)
    price_min  = min(prices)
    price_max  = max(prices)
    price_range = price_max - price_min or 1
    vol_min   = min(volumes)
    vol_max   = max(volumes)
    vol_range = vol_max - vol_min or 1

    rows_p = 12
    price_lines = []
    for i in range(rows_p, -1, -1):
        lvl = price_min + price_range * i / rows_p
        line = f"{lvl:>8.2f} │"
        for j, p in enumerate(prices):
            if i == 0:
                line += "───"
            elif p >= lvl:
                m5  = ma5_list[j]
                m10 = ma10_list[j]
                if not (isinstance(m5,  (float, int)) and np.isnan(m5)) and \
                   not (isinstance(m10, (float, int)) and np.isnan(m10)):
                    if abs(p - m5)  < price_range / rows_p * 0.7:
                        line += " ● "   # 接近MA5
                    elif abs(p - m10) < price_range / rows_p * 0.7:
                        line += " ○ "   # 接近MA10
                    else:
                        line += " ║ "
                else:
                    line += " ║ "
            else:
                line += "   "
        price_lines.append(line)

    x_axis  = "─" * 9 + "".join(["───" for _ in range(n)])
    x_label = "       " + "  ".join([d for d in dates])

    rows_v = 6
    vol_lines = []
    for i in range(rows_v, -1, -1):
        lvl = vol_min + vol_range * i / rows_v
        line = f"{lvl:>10,.0f} │"
        for v in volumes:
            line += "  █ " if v >= lvl else "    "
        vol_lines.append(line)

    chart = "\n".join(price_lines)
    vol_part = "\n".join(vol_lines)

    return f"""
┌{'─'*68}┐
│  {name} ({code})  近{n}日价格走势                          │
├{'─'*68}┤
{chart}
{x_axis}
{x_label}
├{'─'*68}┤
{vol_part}
{'─'*(9+n*3)}
{' '*12}成交量（█ 表示放量）                                     """

# ── 生成所有标的报告 ──────────────────────────────────────
report = []
report.append("=" * 70)
report.append("  A股策略选股报告：连续3天缩量 + MA5/MA10均线交叉")
report.append(f"  筛选时间: {pd.Timestamp('today').strftime('%Y-%m-%d')}")
report.append(f"  满足条件股票数: {len(shrink_stocks)}")
report.append("=" * 70)

count = 0
for code in shrink_stocks:
    sub = df_viz[df_viz["代码"] == code].sort_values("日期")
    if len(sub) < 10:
        continue

    name       = code2name.get(code, "未知")
    prices     = sub["收盘"].tolist()
    volumes    = sub["成交量"].tolist()
    dates      = sub["日期"].dt.strftime("%m-%d").tolist()
    ma5_list   = sub["MA5"].tolist()
    ma10_list  = sub["MA10"].tolist()
    last       = sub.iloc[-1]
    last_price = last["收盘"]
    last_ma5   = last["MA5"]
    last_ma10  = last["MA10"]

    # 均线状态
    if pd.isna(last_ma5) or pd.isna(last_ma10):
        ma_status = "数据不足"
    elif last_price > last_ma5 > last_ma10:
        ma_status = "多头排列（价格>MA5>MA10）"
    elif last_price < last_ma5 < last_ma10:
        ma_status = "空头排列（价格<MA5<MA10）"
    elif last_ma5 > last_ma10:
        ma_status = "MA5 > MA10（偏多）"
    elif last_ma5 < last_ma10:
        ma_status = "MA5 < MA10（偏空）"
    else:
        ma_status = "MA5 ≈ MA10（缠绕）"

    # 最近3天缩量数据
    shrink_rows = sub.tail(3)[["日期", "成交量"]]
    shrink_lines = "\n    ".join(
        f"{d.strftime('%Y-%m-%d')}  成交量={v:>12,.0f}"
        for d, v in shrink_rows.values
    )

    # 交叉信号
    signals    = detect_crossovers(sub)
    sig_text   = "".join(
        f"  • {t} @ {d.strftime('%Y-%m-%d')} 收盘={p:.2f}\n"
        for t, d, p in signals[-3:]
    ) or "  • 近期无明显交叉信号\n"

    chart = ascii_chart(prices[-20:], volumes[-20:], dates[-20:],
                        ma5_list[-20:], ma10_list[-20:], name, code)

    block = f"""
{'='*70}
## 标的: {code} {name}
{'='*70}
### 筛选条件满足情况
  • 代码前缀: {'60' if code.startswith('60') else '00'} 开头 ✓
  • 连续3天缩量 ✓
    最近3日成交量:
    {shrink_lines}

### 近20日走势（文本图）
{chart}

### 技术分析信号
  • MA5/MA10 当前状态: {ma_status}
  • MA5:  {f'{last_ma5:.2f}' if not pd.isna(last_ma5) else 'N/A'}
  • MA10: {f'{last_ma10:.2f}' if not pd.isna(last_ma10) else 'N/A'}
  • 最近买卖点信号:
{sig_text}  • 最新收盘价: {last_price:.2f}
  • 相对MA5/MA10位置: {'价格>' + ('MA5>MA10（多头）' if last_ma5>last_ma10 else 'MA5<MA10（空头）') if not (pd.isna(last_ma5) or pd.isna(last_ma10)) else '数据不足'}

"""
    report.append(block)
    count += 1

report.append(f"\n{'='*70}")
report.append(f"共筛选出 {count} 只满足条件的股票")
report.append("=" * 70)

# ── 写入文件 & 打印 ──────────────────────────────────────
out_path = OUTPUT_DIR / f"screening_report_{pd.Timestamp('today').strftime('%Y%m%d')}.txt"
with open(out_path, "w", encoding="utf-8") as f:
    f.write("\n".join(report))

print(f"\n📄 报告已保存: {out_path}")
print("\n".join(report))
