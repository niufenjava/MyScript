#!/usr/bin/env python3
"""
A股策略选股：连续3天缩量 + MA金叉死叉
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ============ 路径配置 ============
PARQUET_PATH = Path.home() / "claw/MyScript/akshare-stock-data-fetcher/data/all_daily.parquet"
STOCKINFO_PATH = Path.home() / "claw/MyScript/akshare-stock-data-fetcher/stockInfo.csv"
OUTPUT_PATH = Path("/Users/niufen/claw/MyScript/prompt/AStockSelect-Result-20260423.md")

# ============ 读取数据 ============
print("📂 加载数据...")
df = pd.read_parquet(PARQUET_PATH)
df["日期"] = pd.to_datetime(df["日期"])
df = df.sort_values(["代码", "日期"]).reset_index(drop=True)

info = pd.read_csv(STOCKINFO_PATH, dtype={"A股代码": str})
info["A股代码"] = info["A股代码"].str.strip().str.zfill(6)
info_dict = info.set_index("A股代码")[["A股简称", "所属行业"]].to_dict("index")

# ============ 过滤ST ============
st_codes = set(info[info["A股简称"].str.contains(r"ST|\*ST", na=False, regex=True)]["A股代码"].tolist())
print(f"📛 ST股票数量: {len(st_codes)}")

# ============ 过滤房地产业 ============
realestate_codes = set(info[info["所属行业"].str.contains("房地产", na=False)]["A股代码"].tolist())
print(f"🏠 房地产股票数量: {len(realestate_codes)}")

# ============ 筛选：60/00 开头 ============
df = df[df["代码"].astype(str).str.match(r"^(00|60)\d{4}$")].copy()
df = df[~df["代码"].isin(st_codes)]
df = df[~df["代码"].isin(realestate_codes)]
print(f"✅ 60/00 开头股票数量（去ST+房地产后）: {df['代码'].nunique()}")

# ============ 逐股计算缩量信号 ============
print("🔍 扫描连续3天缩量...")

# 为每只股票计算成交量变化
df = df.sort_values(["代码", "日期"])
df["成交量_prev"] = df.groupby("代码")["成交量"].shift(1)
df["成交量_change_pct"] = (df["成交量"] - df["成交量_prev"]) / df["成交量_prev"] * 100

# 连续3天缩量：最近3天每天都比前一天低
def check_shrink3(group):
    if len(group) < 5:
        return False
    recent = group.tail(4).reset_index(drop=True)  # 4行：index0=前第4天,1=前第3天,2=前2天,3=昨天
    # 需要 recent[1] < recent[0] AND recent[2] < recent[1] AND recent[3] < recent[2]
    if len(recent) < 4:
        return False
    return (recent.iloc[1]["成交量"] < recent.iloc[0]["成交量"] and
            recent.iloc[2]["成交量"] < recent.iloc[1]["成交量"] and
            recent.iloc[3]["成交量"] < recent.iloc[2]["成交量"])

# 快速扫描：取每只股票最后4条记录
last4 = df.groupby("代码").tail(4).reset_index(drop=True)
candidates = []
for code, g in last4.groupby("代码"):
    g = g.sort_values("日期").reset_index(drop=True)
    if len(g) < 4:
        continue
    shrink = (g.iloc[1]["成交量"] < g.iloc[0]["成交量"] and
             g.iloc[2]["成交量"] < g.iloc[1]["成交量"] and
             g.iloc[3]["成交量"] < g.iloc[2]["成交量"])
    if shrink:
        candidates.append(code)

print(f"🎯 满足连续3天缩量: {len(candidates)} 只")

# ============ 获取近20日数据并计算指标 ============
def get_stock_data(code, n=23):
    """获取最近n条日线（含足够计算MA的数据）"""
    g = df[df["代码"] == code].sort_values("日期").tail(n).reset_index(drop=True)
    return g

def compute_indicators(g):
    """计算MA5、MA10、成交量变化"""
    g = g.copy()
    g["MA5"] = g["收盘"].rolling(5).mean()
    g["MA10"] = g["收盘"].rolling(10).mean()
    g["成交量_MA5"] = g["成交量"].rolling(5).mean()
    g["成交量_prev1"] = g["成交量"].shift(1)
    g["成交量变化"] = (g["成交量"] - g["成交量_prev1"]) / g["成交量_prev1"] * 100
    return g

def find_cross(g):
    """找出MA5/MA10金叉和死叉"""
    signals = []
    g = g.reset_index(drop=True)
    for i in range(1, len(g)):
        ma5_y = g.iloc[i-1]["MA5"]
        ma10_y = g.iloc[i-1]["MA10"]
        ma5_t = g.iloc[i]["MA5"]
        ma10_t = g.iloc[i]["MA10"]
        date = g.iloc[i]["日期"]
        close = g.iloc[i]["收盘"]
        if pd.isna(ma5_y) or pd.isna(ma10_y) or pd.isna(ma5_t) or pd.isna(ma10_t):
            continue
        # 金叉：前一日MA5<=MA10，当日MA5>MA10
        if ma5_y <= ma10_y and ma5_t > ma10_t:
            signals.append(("买入(金叉)", date, close, "MA5上穿MA10"))
        # 死叉：前一日MA5>=MA10，当日MA5<MA10
        if ma5_y >= ma10_y and ma5_t < ma10_t:
            signals.append(("卖出(死叉)", date, close, "MA5下穿MA10"))
    return signals

# ============ 生成报告 ============
output_lines = []
output_lines.append("# A股策略选股结果\n")
output_lines.append(f"**生成时间:** 2026-04-23  \n")
output_lines.append(f"**筛选条件:** 60/00开头主板 + 过滤ST + 过滤房地产 + 连续3天缩量 + 技术信号  \n")
output_lines.append(f"**满足条件股票数:** {len(candidates)}  \n\n")
output_lines.append("---\n")

if not candidates:
    output_lines.append("*未找到满足条件的股票*\n")
else:
    # ---- 总结表格 ----
    summary_rows = []
    for code in sorted(candidates):
        name = info_dict.get(code, {}).get("A股简称", "未知")
        industry = info_dict.get(code, {}).get("所属行业", "未知")
        g_all = df[df["代码"] == code].sort_values("日期")
        g = compute_indicators(g_all.tail(23).reset_index(drop=True))
        recent20 = g.tail(20).reset_index(drop=True)
        last_row = recent20.iloc[-1]
        ma5 = last_row["MA5"]
        ma10 = last_row["MA10"]
        close = last_row["收盘"]
        arr = "多头" if (not np.isnan(ma5) and not np.isnan(ma10) and ma5 > ma10) else ("空头" if (not np.isnan(ma5) and not np.isnan(ma10) and ma5 < ma10) else "缠绕")
        last4_data = g.tail(4).sort_values("日期").reset_index(drop=True)
        shrink_days = 0
        for i in range(len(last4_data) - 1, 0, -1):
            if last4_data.iloc[i]["成交量"] < last4_data.iloc[i-1]["成交量"]:
                shrink_days += 1
            else:
                break
        summary_rows.append({
            "代码": code, "名称": name, "行业": industry,
            "最新价": f"{close:.2f}", "MA5": f"{ma5:.2f}" if not np.isnan(ma5) else "N/A",
            "MA10": f"{ma10:.2f}" if not np.isnan(ma10) else "N/A",
            "均线": arr, "缩量天数": shrink_days
        })

    summary_df = pd.DataFrame(summary_rows)
    output_lines.append("## 汇总表\n\n")
    output_lines.append("| 代码 | 名称 | 行业 | 最新价 | MA5 | MA10 | 均线排列 | 缩量天数 |\n")
    output_lines.append("|------|------|------|--------|-----|------|---------|----------|\n")
    for _, r in summary_df.iterrows():
        output_lines.append(f"| {r['代码']} | {r['名称']} | {r['行业']} | {r['最新价']} | {r['MA5']} | {r['MA10']} | {r['均线']} | {r['缩量天数']}天 |\n")
    output_lines.append("\n---\n")
    for code in sorted(candidates):
        name = info_dict.get(code, {}).get("A股简称", "未知")
        industry = info_dict.get(code, {}).get("所属行业", "未知")

        g = get_stock_data(code, n=23)
        g = compute_indicators(g)

        # 取最近20条（足够计算MA10的最早数据）
        recent20 = g.tail(20).reset_index(drop=True)

        # ---- 连续缩量数据 ----
        last4_data = g.tail(4).sort_values("日期").reset_index(drop=True)
        shrink_ok = (last4_data.iloc[1]["成交量"] < last4_data.iloc[0]["成交量"] and
                     last4_data.iloc[2]["成交量"] < last4_data.iloc[1]["成交量"] and
                     last4_data.iloc[3]["成交量"] < last4_data.iloc[2]["成交量"])

        # ---- 技术信号 ----
        signals = find_cross(g)
        # 只保留近20日内的信号
        min_date = recent20["日期"].min()
        max_date = recent20["日期"].max()
        signals_in_window = [(t, d, p, desc) for t, d, p, desc in signals
                              if min_date <= d <= max_date]

        # ---- 多头/空头判断 ----
        last_row = recent20.iloc[-1]
        ma5 = last_row["MA5"]
        ma10 = last_row["MA10"]
        close = last_row["收盘"]
        if pd.isna(ma5) or pd.isna(ma10):
            arrangement = "数据不足"
        elif ma5 > ma10:
            arrangement = "多头排列"
        elif ma5 < ma10:
            arrangement = "空头排列"
        else:
            arrangement = "缠绕"

        # 连续缩量天数
        shrink_days = 0
        for i in range(len(last4_data) - 1, 0, -1):
            if last4_data.iloc[i]["成交量"] < last4_data.iloc[i-1]["成交量"]:
                shrink_days += 1
            else:
                break

        # ---- 输出 ----
        output_lines.append(f"## 标的: {code} {name} {industry}\n")
        output_lines.append("### 筛选条件满足情况\n")
        output_lines.append(f"- 代码前缀: 60/00 ✓\n")
        output_lines.append(f"- 连续3天缩量 ✓（附每日成交量数据）\n")
        output_lines.append(f"  - 第1天缩量: {last4_data.iloc[0]['日期'].strftime('%m-%d')} 成交量={last4_data.iloc[0]['成交量']:.0f}\n")
        output_lines.append(f"  - 第2天缩量: {last4_data.iloc[1]['日期'].strftime('%m-%d')} 成交量={last4_data.iloc[1]['成交量']:.0f}\n")
        output_lines.append(f"  - 第3天缩量: {last4_data.iloc[2]['日期'].strftime('%m-%d')} 成交量={last4_data.iloc[2]['成交量']:.0f}\n")
        output_lines.append(f"  - 第4天缩量: {last4_data.iloc[3]['日期'].strftime('%m-%d')} 成交量={last4_data.iloc[3]['成交量']:.0f}\n")

        # 构建信号字典：日期 -> (信号类型, 说明)
        sig_map = {}
        for sig_type, date, price, desc in signals_in_window:
            sig_map[date] = (sig_type, desc)

        output_lines.append("\n### 近20日走势数据表\n")
        output_lines.append("| 日期 | 收盘价 | 涨跌幅 | 成交量 | 成交量变化 | 信号类型 | 说明 |\n")
        output_lines.append("|------|--------|--------|--------|----------|--------|------|\n")
        # 倒序输出（最新日在前）
        for _, row in recent20.iloc[::-1].iterrows():
            date_val = row["日期"]
            date_str = date_val.strftime("%m-%d")
            close_p = row["收盘"]
            chg_pct = row["涨跌幅(%)"]
            vol = row["成交量"]
            vol_chg = row["成交量变化"]
            chg_str = f"{chg_pct:+.2f}%" if not np.isnan(chg_pct) else "N/A"
            vol_chg_str = f"{vol_chg:+.1f}%" if not np.isnan(vol_chg) else "N/A"
            if vol >= 1e8:
                vol_str = f"{vol/1e8:.2f}亿"
            elif vol >= 1e4:
                vol_str = f"{vol/1e4:.0f}万"
            else:
                vol_str = f"{vol:.0f}"
            sig_info = sig_map.get(date_val, (None, None))
            sig_type_str = sig_info[0] if sig_info[0] else ""
            sig_desc_str = sig_info[1] if sig_info[1] else ""
            output_lines.append(f"| {date_str} | {close_p:.2f} | {chg_str} | {vol_str} | {vol_chg_str} | {sig_type_str} | {sig_desc_str} |\n")

        output_lines.append("\n### 关键数据\n")
        output_lines.append(f"- 最新收盘价: {close:.2f}\n")
        output_lines.append(f"- MA5/MA10: {ma5:.2f} / {ma10:.2f}（{arrangement}）\n")
        output_lines.append(f"- 连续缩量: 第{shrink_days}天\n")
        output_lines.append("\n---\n\n")

        print(f"✅ {code} {name} 完成")

# ============ 写入文件 ============
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
content = "".join(output_lines)
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(content)

print(f"\n📄 报告已生成: {OUTPUT_PATH}")
print(f"共 {len(candidates)} 只股票")
