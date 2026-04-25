"""
季度财报数据批量拉取 — 东方财富批量接口
功能：按季度批量拉取全市场 A 股财报数据（2024Q1 起），保存到 data/FinanceReport.csv
数据源：
  - stock_yjbb_em  — 业绩报表（营收/净利润/每股指标/ROE/毛利率）
  - stock_zcfz_em  — 资产负债表（资产负债率）
  - stock_lrb_em   — 利润表（可算销售净利率）
特性：
  - 按季度批量获取全市场数据（~3接口/季度，~20秒/季度）
  - 累计值自动转单季度（Q2=中报-Q1, Q3=三季报-中报, Q4=年报-三季报）
  - 增量更新：封板季度跳过，披露中季度每次刷新
用法: python fetch_finance.py
"""

import os
import time
import pandas as pd
import akshare as ak
from datetime import date

# ── 路径配置 ──────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
FINANCE_RAW_FILE = os.path.join(DATA_DIR, "FinanceReport_raw.csv")   # 原始累计值
FINANCE_FILE = os.path.join(DATA_DIR, "FinanceReport.csv")           # 转换后单季度值

# ── 拉取参数 ──────────────────────────────────────────────
START_QUARTER = "2020-03-31"

# 需要累计转单季度的字段
AMOUNT_COLUMNS = ["营业总收入", "净利润"]

# 季度顺序映射
QUARTER_ORDER = {"03-31": 1, "06-30": 2, "09-30": 3, "12-31": 4}

# CSV 输出列名
OUTPUT_COLUMNS = [
    "股票代码", "股票名称", "季度",
    "营业总收入", "营业总收入同比增长率",
    "净利润", "净利润同比增长率",
    "净资产收益率", "销售毛利率", "销售净利率",
    "基本每股收益", "每股净资产", "每股经营现金流",
    "资产负债率",
]


# =========================
# 动态季度计算
# =========================
def get_expected_quarters():
    """
    动态生成截至当前应已公布的季度列表。
    返回: [(季度字符串, 是否已封板), ...]

    财报披露窗口与截止日：
    - 1季报(03-31): 4月1日起披露，4月30日截止
    - 中报  (06-30): 7月1日起披露，8月31日截止
    - 3季报(09-30): 10月1日起披露，10月31日截止
    - 年报  (12-31): 次年1月起披露，次年4月30日截止

    封板 = 当前日期已过截止日（不会再有新披露）
    披露中 = 当前日期在披露窗口内（每次运行都应刷新）
    """
    today = date.today()
    quarters = []
    start_year = int(START_QUARTER[:4])
    year = start_year
    while True:
        # (季度, 披露起始日, 披露截止日)
        candidates = [
            (f"{year}-03-31", date(year, 4, 1),       date(year, 4, 30)),
            (f"{year}-06-30", date(year, 7, 1),       date(year, 8, 31)),
            (f"{year}-09-30", date(year, 10, 1),      date(year, 10, 31)),
            (f"{year}-12-31", date(year + 1, 1, 1),   date(year + 1, 4, 30)),
        ]
        any_added = False
        for q_str, start_date, deadline in candidates:
            if today >= start_date:
                sealed = today > deadline
                quarters.append((q_str, sealed))
                any_added = True
        if not any_added:
            break
        year += 1
    return quarters


def quarter_to_em_date(quarter_str):
    """将 '2024-03-31' 转为东方财富格式 '20240331'"""
    return quarter_str.replace("-", "")


# =========================
# 批量数据拉取
# =========================
def fetch_quarter_batch(quarter_str):
    """
    批量拉取一个季度的全市场数据。
    合并业绩报表 + 资产负债表 + 利润表，返回统一 DataFrame。
    """
    em_date = quarter_to_em_date(quarter_str)

    # 1. 业绩报表
    try:
        df_yjbb = ak.stock_yjbb_em(date=em_date)
        df_yjbb = df_yjbb.rename(columns={
            "股票简称": "股票名称",
            "每股收益": "基本每股收益",
            "营业总收入-营业总收入": "营业总收入",
            "营业总收入-同比增长": "营业总收入同比增长率",
            "净利润-净利润": "净利润",
            "净利润-同比增长": "净利润同比增长率",
            "每股经营现金流量": "每股经营现金流",
            "销售毛利率": "销售毛利率",
            "净资产收益率": "净资产收益率",
            "每股净资产": "每股净资产",
        })
        df_yjbb = df_yjbb[["股票代码", "股票名称", "基本每股收益",
                            "营业总收入", "营业总收入同比增长率",
                            "净利润", "净利润同比增长率",
                            "每股净资产", "净资产收益率", "每股经营现金流",
                            "销售毛利率"]]
        print(f"    业绩报表: {len(df_yjbb)} 只")
    except Exception as e:
        print(f"    [WARN] 业绩报表拉取失败: {e}")
        return None

    # 2. 资产负债表
    try:
        df_zcfz = ak.stock_zcfz_em(date=em_date)
        df_zcfz = df_zcfz[["股票代码", "资产负债率"]]
        print(f"    资产负债表: {len(df_zcfz)} 只")
    except Exception as e:
        print(f"    [WARN] 资产负债表拉取失败: {e}")
        df_zcfz = pd.DataFrame(columns=["股票代码", "资产负债率"])

    # 3. 利润表（用于算销售净利率）
    try:
        df_lrb = ak.stock_lrb_em(date=em_date)
        # 计算销售净利率 = 净利润 / 营业总收入 * 100
        df_lrb["销售净利率"] = df_lrb.apply(
            lambda r: round(r["净利润"] / r["营业总收入"] * 100, 2)
            if pd.notna(r["营业总收入"]) and r["营业总收入"] != 0
            else None, axis=1)
        df_lrb = df_lrb[["股票代码", "销售净利率"]]
        print(f"    利润表: {len(df_lrb)} 只")
    except Exception as e:
        print(f"    [WARN] 利润表拉取失败: {e}")
        df_lrb = pd.DataFrame(columns=["股票代码", "销售净利率"])

    # 合并三张表
    merged = df_yjbb.merge(df_zcfz, on="股票代码", how="left")
    merged = merged.merge(df_lrb, on="股票代码", how="left")
    merged["季度"] = quarter_str

    return merged


# =========================
# 累计转单季度
# =========================
def convert_cumulative_to_single_quarter(df):
    """
    将累计财务数据转换为单季度数据（向量化实现）。
    Q1 保持不变，Q2=中报-Q1，Q3=三季报-中报，Q4=年报-三季报。
    前一季度缺失则保留累计值。
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df["_year"] = df["季度"].str[:4].astype(int)
    df["_suffix"] = df["季度"].str[-5:]
    df["_q_order"] = df["_suffix"].map(QUARTER_ORDER)
    df = df.sort_values(["股票代码", "_year", "_q_order"]).reset_index(drop=True)

    # 前一季度映射表: 用 (股票代码, 年份, 季度后缀) 做快速查找
    prev_suffix_map = {"06-30": "03-31", "09-30": "06-30", "12-31": "09-30"}

    for col in AMOUNT_COLUMNS:
        if col not in df.columns:
            continue

        # 构建 (股票代码, 年份, 后缀) → 值 的字典
        lookup = {}
        for t in df[["股票代码", "_year", "_suffix", col]].itertuples(index=False):
            lookup[(t[0], t[1], t[2])] = t[3]

        new_values = []
        for _, row in df.iterrows():
            suffix = row["_suffix"]
            current_val = row[col]
            if suffix == "03-31" or pd.isna(current_val):
                new_values.append(current_val)
                continue
            prev_suffix = prev_suffix_map.get(suffix)
            if prev_suffix:
                prev_val = lookup.get((row["股票代码"], row["_year"], prev_suffix))
                if prev_val is not None and not pd.isna(prev_val):
                    new_values.append(current_val - prev_val)
                    continue
            new_values.append(current_val)
        df[col] = new_values

    df = df.drop(columns=["_year", "_suffix", "_q_order"])
    return df


# =========================
# 已有数据读取
# =========================
def load_existing_raw():
    """读取已有原始累计数据（从 _raw.csv），返回 DataFrame 和每季度股票数统计"""
    if not os.path.exists(FINANCE_RAW_FILE):
        return pd.DataFrame(columns=OUTPUT_COLUMNS), {}

    df = pd.read_csv(FINANCE_RAW_FILE, dtype={"股票代码": str}, low_memory=False)
    original_len = len(df)
    df = df.drop_duplicates(subset=["股票代码", "季度"], keep="last")
    dedup_len = len(df)

    if dedup_len < original_len:
        print(f"  检测到重复数据: {original_len} → {dedup_len} 行（清理 {original_len - dedup_len} 行）")
        df.to_csv(FINANCE_RAW_FILE, index=False, encoding="utf-8-sig")

    # 统计每个季度的覆盖数
    quarter_counts = {}
    print(f"  已存在原始累计数据: {dedup_len} 条，覆盖 {df['季度'].nunique()} 个季度")
    for q in sorted(df["季度"].unique()):
        cnt = len(df[df["季度"] == q])
        quarter_counts[q] = cnt
        print(f"    {q}: {cnt} 只")

    return df, quarter_counts


# =========================
# 主流程
# =========================
def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    t_start = time.time()

    # 已有原始累计数据
    exist_df, quarter_counts = load_existing_raw()

    # 计算需要拉取的季度
    expected = get_expected_quarters()
    all_quarter_strs = [q for q, _ in expected]

    print(f"\n应公布季度: {all_quarter_strs[0]} ~ {all_quarter_strs[-1]}（共 {len(expected)} 个）")

    # 分类：需要拉取的季度
    to_fetch = []
    for q_str, sealed in expected:
        existing_count = quarter_counts.get(q_str, 0)
        if sealed and existing_count > 0:
            print(f"  {q_str}: 已封板，已有 {existing_count} 只 → 跳过")
        elif not sealed and existing_count > 0:
            print(f"  {q_str}: 披露中，已有 {existing_count} 只 → 刷新")
            to_fetch.append(q_str)
        else:
            status = "已封板" if sealed else "披露中"
            print(f"  {q_str}: {status}，无数据 → 拉取")
            to_fetch.append(q_str)

    if not to_fetch:
        print("\n所有季度数据均已封板且完整，无需更新。")
        return

    print(f"\n需要拉取/刷新: {to_fetch}（{len(to_fetch)} 个）\n")

    # 逐季度批量拉取
    all_new = []
    for quarter in to_fetch:
        print(f"[{quarter}] 拉取中...")
        t = time.time()
        df = fetch_quarter_batch(quarter)
        if df is None or df.empty:
            print(f"  无数据，跳过")
            continue

        # 过滤9开头的股票
        df = df[~df["股票代码"].str.startswith("9")]
        print(f"    合并后: {len(df)} 只，耗时 {time.time() - t:.1f}s")
        all_new.append(df)

    if not all_new:
        print("\n无新数据。")
        return

    # 合并所有新季度数据
    new_df = pd.concat(all_new, ignore_index=True)
    print(f"\n拉取数据: {len(new_df)} 条")

    # ── 保存原始累计数据到 _raw.csv ──
    if not exist_df.empty:
        raw_combined = pd.concat([exist_df, new_df], ignore_index=True)
        # 新数据覆盖旧数据（刷新披露中季度的关键）
        raw_combined = raw_combined.drop_duplicates(subset=["股票代码", "季度"], keep="last")
    else:
        raw_combined = new_df.copy()

    raw_combined = raw_combined.reindex(columns=OUTPUT_COLUMNS)
    raw_combined = raw_combined.sort_values(["股票代码", "季度"]).reset_index(drop=True)
    raw_combined.to_csv(FINANCE_RAW_FILE, index=False, encoding="utf-8-sig")
    print(f"原始累计数据已保存: {FINANCE_RAW_FILE} ({len(raw_combined)} 条)")

    # ── 累计转单季度 → 输出到 FinanceReport.csv ──
    print("执行累计转单季度...")
    converted = convert_cumulative_to_single_quarter(raw_combined)
    converted.to_csv(FINANCE_FILE, index=False, encoding="utf-8-sig")

    elapsed = time.time() - t_start
    n_stocks = converted["股票代码"].nunique()
    n_quarters = converted["季度"].nunique()
    print(f"\n全部完成! 耗时 {elapsed:.1f}s")
    print(f"  股票数: {n_stocks}")
    print(f"  季度数: {n_quarters}")
    print(f"  总记录: {len(converted)} 条")
    print(f"  原始累计文件: {FINANCE_RAW_FILE}")
    print(f"  单季度文件: {FINANCE_FILE}")


if __name__ == "__main__":
    main()
