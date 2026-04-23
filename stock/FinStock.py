import os
import time
import random
import re
import json
import pandas as pd
import akshare as ak
from tqdm import tqdm
from datetime import date

BASE_PATH = "/Users/niufen/claw/MyClawScript/stock/data"
STOCK_INFO_FILE = os.path.join(BASE_PATH, "AStockInfo.csv")
FINANCE_FILE = os.path.join(BASE_PATH, "FinanceReport.csv")

# 无数据缓存文件（按天轮换，避免对未披露公司重复请求）
NODATA_CACHE_FILE = os.path.join(BASE_PATH, f"no_data_cache_{date.today().strftime('%Y%m%d')}.json")

# 只拉取 2024年1季度 及之后的数据
START_QUARTER = "2024-03-31"

# 需要转换金额单位+累计转单季度的字段
AMOUNT_COLUMNS = ["营业总收入", "净利润", "扣非净利润"]

# 季度顺序映射
QUARTER_ORDER = {"03-31": 1, "06-30": 2, "09-30": 3, "12-31": 4}


def get_expected_quarters():
    """
    动态生成截至当前应已公布的季度列表。
    财报披露时间规则：
    - 1季报(03-31): 4月1日起开始披露
    - 2季报/中报(06-30): 8月1日后可拉取
    - 3季报(09-30): 11月1日后可拉取
    - 年报(12-31): 次年4月30日后可拉取
    """
    today = date.today()
    quarters = []
    year = 2024
    while True:
        candidates = [
            (f"{year}-03-31", date(year, 4, 1)),       # 一季报：4月1日起开始披露
            (f"{year}-06-30", date(year, 8, 1)),       # 中报
            (f"{year}-09-30", date(year, 11, 1)),      # 三季报
            (f"{year}-12-31", date(year + 1, 4, 30)),  # 年报
        ]
        for q_str, publish_date in candidates:
            if today >= publish_date:
                quarters.append(q_str)
        if today < date(year, 4, 1):
            break
        year += 1
    return quarters


# CSV 输出的列名（中文）
OUTPUT_COLUMNS = [
    "股票代码", "股票名称", "季度",
    "营业总收入", "营业总收入同比增长率",
    "净利润", "净利润同比增长率",
    "扣非净利润", "扣非净利润同比增长率",
    "净资产收益率", "销售净利率",
    "基本每股收益", "每股净资产",
    "每股资本公积金", "每股未分配利润", "每股经营现金流",
    "资产负债率", "流动比率", "速动比率",
    "产权比率",
]


# =========================
# 金额解析
# =========================
def parse_amount(val):
    """
    将中文金额格式转换为纯数字。
    例如: '387.70亿' → 38770000000.0, '3272.82万' → 32728200.0
    """
    if val is None or val == "" or val == "False":
        return None
    s = str(val).strip()
    if s == "" or s == "False":
        return None
    match = re.match(r'^([+-]?\d+(?:\.\d+)?)\s*(亿|万)$', s)
    if match:
        num = float(match.group(1))
        unit = match.group(2)
        if unit == "亿":
            return num * 1e8
        elif unit == "万":
            return num * 1e4
    try:
        return float(s)
    except ValueError:
        return None


def convert_amounts(df):
    """对 DataFrame 中的金额列进行单位转换"""
    for col in AMOUNT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(parse_amount)
    return df


def convert_cumulative_to_single_quarter(df):
    """
    将累计财务数据转换为单季度数据。
    规则（同一年度内）：
    - Q1(03-31): 保持不变（本身就是单季度）
    - Q2(06-30): 中报 - 一季报
    - Q3(09-30): 三季报 - 中报
    - Q4(12-31): 年报 - 三季报
    如果前一季度缺失，则保留累计值（不做减法）。
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # 解析年份和季度后缀
    df["_year"] = df["季度"].str[:4].astype(int)
    df["_suffix"] = df["季度"].str[-5:]
    df["_q_order"] = df["_suffix"].map(QUARTER_ORDER)

    # 按股票代码、年份、季度顺序排序
    df = df.sort_values(["股票代码", "_year", "_q_order"])

    # 前一季度映射
    prev_quarter = {"06-30": "03-31", "09-30": "06-30", "12-31": "09-30"}

    for col in AMOUNT_COLUMNS:
        if col not in df.columns:
            continue

        new_values = []
        for idx, row in df.iterrows():
            suffix = row["_suffix"]
            current_val = row[col]

            # Q1 或者值为空，直接用原值
            if suffix == "03-31" or pd.isna(current_val):
                new_values.append(current_val)
                continue

            # 找同一年度的前一季度
            prev_suffix = prev_quarter.get(suffix)
            if prev_suffix:
                prev_row = df[
                    (df["股票代码"] == row["股票代码"])
                    & (df["_year"] == row["_year"])
                    & (df["_suffix"] == prev_suffix)
                ]
                if len(prev_row) > 0:
                    prev_val = prev_row[col].values[0]
                    if not pd.isna(prev_val):
                        new_values.append(current_val - prev_val)
                        continue

            # 前一季度缺失或值不可用，保留累计值
            new_values.append(current_val)

        df[col] = new_values

    # 清理临时列
    df = df.drop(columns=["_year", "_suffix", "_q_order"])
    return df


# =========================
# 工具函数
# =========================
def ensure_dir():
    os.makedirs(BASE_PATH, exist_ok=True)


def safe_sleep(a=1.0, b=2.5):
    """随机休眠，避免请求过快被封IP"""
    time.sleep(random.uniform(a, b))


def retry(func, max_retry=3, wait_base=3, *args, **kwargs):
    """带指数退避的重试机制"""
    for i in range(max_retry):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"  [WARN] 重试 {i+1}/{max_retry} 失败: {e}")
            if i < max_retry - 1:
                sleep_time = wait_base * (i + 1) + random.uniform(0, 2)
                print(f"  等待 {sleep_time:.1f}s 后重试...")
                time.sleep(sleep_time)
    return None


# =========================
# 1. 获取股票基础信息
# =========================
def load_or_fetch_stock_info():
    """如果 AStockInfo.csv 已存在则直接读取，否则通过 akshare 获取"""
    ensure_dir()

    if os.path.exists(STOCK_INFO_FILE):
        print(f"✔ 股票基础信息已存在，直接读取: {STOCK_INFO_FILE}")
        df = pd.read_csv(STOCK_INFO_FILE, dtype={"股票代码": str})
        print(f"  共 {len(df)} 只股票")
        return df

    print("⬇ 正在通过 akshare 拉取A股上市公司信息...")
    df = ak.stock_info_a_code_name()
    df = df.rename(columns={"code": "股票代码", "name": "股票名称"})
    df = df[["股票代码", "股票名称"]]
    df["股票代码"] = df["股票代码"].astype(str)
    df.to_csv(STOCK_INFO_FILE, index=False, encoding="utf-8-sig")
    print(f"✔ 已保存: {STOCK_INFO_FILE}  共 {len(df)} 只股票")
    return df


# =========================
# 2. 已有财务数据读取
# =========================
def load_existing_finance():
    """读取已有财务数据，启动时自动检测并清理重复数据"""
    if not os.path.exists(FINANCE_FILE):
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.read_csv(FINANCE_FILE, dtype={"股票代码": str}, low_memory=False)
    original_len = len(df)
    df = df.drop_duplicates(subset=["股票代码", "季度"], keep="last")
    dedup_len = len(df)

    if dedup_len < original_len:
        print(f"⚠ 检测到重复数据: {original_len} 行 → 去重后 {dedup_len} 行（清理 {original_len - dedup_len} 行）")
        df.to_csv(FINANCE_FILE, index=False, encoding="utf-8-sig")
        print(f"  已自动清理并保存")
    else:
        print(f"✔ 已存在财务数据: {dedup_len} 条")
    return df


def build_exist_set(df):
    """构建已存在数据的去重集合 (股票代码, 季度)"""
    if df.empty:
        return set()
    return set(zip(df["股票代码"].astype(str), df["季度"].astype(str)))


def build_stock_quarter_map(df):
    """构建每只股票已有的季度集合"""
    if df.empty:
        return {}
    result = {}
    for code, quarter in zip(df["股票代码"].astype(str), df["季度"].astype(str)):
        result.setdefault(code, set()).add(quarter)
    return result


# =========================
# 无数据缓存（避免对未披露公司重复请求）
# =========================
def load_no_data_cache():
    """读取本月无数据缓存集合"""
    if os.path.exists(NODATA_CACHE_FILE):
        with open(NODATA_CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_no_data_cache(no_data_set):
    """保存无数据缓存到本月文件"""
    with open(NODATA_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(list(no_data_set), f)


# =========================
# 3. 拉取单个股票季度财务
# =========================
def fetch_stock_finance(stock_code, stock_name):
    """
    使用 akshare 同花顺财务摘要接口获取单只股票的季度财务数据。
    返回的数据已经过：单位转换（亿/万→纯数字）+ 累计转单季度。
    """
    def _fetch():
        df = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
        if df is None or df.empty:
            return None

        df = df.copy()
        if "报告期" in df.columns:
            df = df[df["报告期"] >= START_QUARTER]
        if df.empty:
            return None

        # 清洗 False 值
        df = df.replace(False, "")
        df = df.rename(columns={"报告期": "季度"})
        df["股票代码"] = stock_code
        df["股票名称"] = stock_name

        # 只保留目标列
        existing_cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
        df = df[existing_cols]
        if df.empty:
            return None

        # 1. 金额单位转换（亿/万 → 纯数字）
        df = convert_amounts(df)

        # 2. 累计转单季度（06-30=中报-Q1, 09-30=三季报-中报, 12-31=年报-三季报）
        df = convert_cumulative_to_single_quarter(df)

        return df

    return retry(_fetch, max_retry=3, wait_base=3)


# =========================
# 4. 批量保存到CSV
# =========================
def batch_save(records, file_path):
    """将一批记录追加保存到CSV文件"""
    if not records:
        return
    save_df = pd.DataFrame(records)
    header = not os.path.exists(file_path)
    save_df.to_csv(file_path, mode="a", header=header, index=False, encoding="utf-8-sig")


# =========================
# 5. 主流程
# =========================
def main():
    ensure_dir()

    stock_df = load_or_fetch_stock_info()
    exist_df = load_existing_finance()
    exist_set = build_exist_set(exist_df)
    stock_quarter_map = build_stock_quarter_map(exist_df)

    expected_quarters = get_expected_quarters()

    all_result = []
    total_stocks = len(stock_df)
    skipped_filter = 0
    skipped_exist = 0
    skipped_cache = 0     # 无数据缓存跳过
    skipped_nodata = 0
    failed = 0
    fetched = 0

    # 加载本月无数据缓存
    no_data_cache = load_no_data_cache()
    if no_data_cache:
        print(f"   本月无数据缓存: {len(no_data_cache)} 只（将跳过）\n")

    print(f"\n🚀 开始处理股票数量: {total_stocks}")
    print(f"   拉取范围: {START_QUARTER} 至今")
    print(f"   当前应已公布季度: {expected_quarters[0]}~{expected_quarters[-1]} ({len(expected_quarters)}个)")
    print(f"   去重维度: 股票代码 + 季度\n")

    for idx, row in tqdm(stock_df.iterrows(), total=total_stocks, desc="拉取进度"):
        code = str(row["股票代码"]).strip()
        name = str(row["股票名称"]).strip()

        # 过滤9开头
        if code.startswith("9"):
            skipped_filter += 1
            continue

        # 无数据缓存跳过（本月内已确认无数据的公司）
        if code in no_data_cache:
            skipped_cache += 1
            continue

        # 请求前去重：该股票所有季度都已拉取则跳过
        if code in stock_quarter_map:
            missing = [q for q in expected_quarters if q not in stock_quarter_map[code]]
            if not missing:
                skipped_exist += 1
                continue

        print(f"  📡 [{idx+1}/{total_stocks}] 拉取 {code} {name} ...")

        try:
            data = fetch_stock_finance(code, name)
            if data is None or data.empty:
                print(f"     ⚠ 无数据，跳过")
                no_data_cache.add(code)
                skipped_nodata += 1
                continue

            new_count = 0
            dup_count = 0
            for _, r in data.iterrows():
                quarter = str(r.get("季度", "")).strip()
                key = (code, quarter)
                if key in exist_set:
                    dup_count += 1
                    continue

                record = {}
                for col in OUTPUT_COLUMNS:
                    val = r.get(col)
                    if isinstance(val, bool):
                        val = ""
                    record[col] = val
                record["股票代码"] = code
                record["股票名称"] = name
                record["季度"] = quarter
                all_result.append(record)

                exist_set.add(key)
                stock_quarter_map.setdefault(code, set()).add(quarter)
                new_count += 1

            fetched += new_count
            if new_count > 0:
                print(f"     ✅ 新增 {new_count} 条" + (f"，去重 {dup_count} 条" if dup_count > 0 else ""))
            elif dup_count > 0:
                print(f"     ✔ 全部已存在，去重 {dup_count} 条")

            safe_sleep(1.0, 2.5)

        except Exception as e:
            print(f"  [ERROR] {code} {name}: {e}")
            failed += 1
            safe_sleep(3, 6)

        if len(all_result) >= 30:
            batch_save(all_result, FINANCE_FILE)
            print(f"  💾 批量写入 {len(all_result)} 条 (累计新增: {fetched})")
            all_result = []

    if all_result:
        batch_save(all_result, FINANCE_FILE)
        print(f"  💾 最后写入 {len(all_result)} 条")

    # 保存无数据缓存
    save_no_data_cache(no_data_cache)

    print(f"\n🎉 全部完成!")
    print(f"   总股票数: {total_stocks}")
    print(f"   新增财务数据: {fetched} 条")
    print(f"   过滤跳过(9开头): {skipped_filter} 只")
    print(f"   已拉取跳过(免请求): {skipped_exist} 只")
    print(f"   缓存跳过(本月无数据): {skipped_cache} 只")
    print(f"   无数据跳过: {skipped_nodata} 只")
    print(f"   失败: {failed} 只")
    print(f"   输出文件: {FINANCE_FILE}")


if __name__ == "__main__":
    main()
