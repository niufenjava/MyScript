# -*- coding: utf-8 -*-
"""
场景4：获取个股财务摘要
学习目标：掌握 stock_financial_abstract_ths() 获取同花顺财务数据
运行方式：python3 demo_04_financial_abstract.py
"""
import akshare as ak
import pandas as pd
import re
import time
import random


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


def parse_amount(val):
    """将中文金额格式转换为纯数字"""
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


# ========================================
# Case 1: 获取贵州茅台按报告期排列的财务摘要
# ========================================
def case_1_quarterly():
    """获取按报告期排列的财务摘要"""
    df = request_wrapper(
        ak.stock_financial_abstract_ths, desc="获取财务摘要(报告期)",
        symbol="600519", indicator="按报告期",
    )
    if df is None:
        print("[SKIP] 获取财务摘要失败，跳过后续操作")
        return None

    print(f"贵州茅台 财务摘要: {len(df)} 条")
    print(f"列名: {list(df.columns)}")
    print(df.head(5))
    return df


# ========================================
# Case 2: 获取按年度排列的财务摘要
# ========================================
def case_2_annual():
    """获取按年度排列的财务摘要"""
    safe_sleep(1.0, 2.5)
    df_year = request_wrapper(
        ak.stock_financial_abstract_ths, desc="获取财务摘要(年度)",
        symbol="600519", indicator="按年度",
    )
    if df_year is None:
        print("[SKIP] 获取年度财务摘要失败")
        return
    print(f"\n按年度: {len(df_year)} 条")
    print(df_year.head(5))


# ========================================
# Case 3: 提取最近4个季度关键财务指标
# ========================================
def case_3_key_metrics(df):
    """提取最近4个季度关键指标"""
    if df is None or df.empty:
        print("[SKIP] 无数据，跳过")
        return
    if "报告期" in df.columns and "净利润" in df.columns:
        recent = df.head(4)
        print(f"\n最近4个季度关键数据:")
        cols = [c for c in ["报告期", "营业总收入", "净利润", "净资产收益率", "每股收益"] if c in df.columns]
        print(recent[cols])


# ========================================
# Case 4: 对比两家公司最新财务数据
# ========================================
def case_4_compare():
    """对比两家公司财务数据"""
    safe_sleep(1.0, 2.5)
    codes = ["600519", "000858"]
    for code in codes:
        data = request_wrapper(
            ak.stock_financial_abstract_ths, desc=f"获取{code}财务摘要",
            symbol=code, indicator="按报告期",
        )
        if data is not None and not data.empty:
            print(f"\n{code} 最新报告期: {data.iloc[0].get('报告期', 'N/A')}")
            key_cols = [c for c in ["营业总收入", "净利润", "净资产收益率"] if c in data.columns]
            if key_cols:
                print(f"  {data[key_cols].head(1).to_string(index=False)}")
        safe_sleep(1.0, 2.5)


# ========================================
# Test Case 1: 金额单位解析测试
# ========================================
def case_test_1_amount_parse():
    """测试：金额单位解析"""
    assert parse_amount("387.70亿") == 387.70e8, "Test 1a 失败"
    assert parse_amount("3272.82万") == 3272.82e4, "Test 1b 失败"
    assert parse_amount("") is None, "Test 1c 失败"
    assert parse_amount("False") is None, "Test 1d 失败"
    assert parse_amount("150.00") == 150.0, "Test 1e 失败"
    print("✔ Test 1: 金额单位解析正确 (亿/万/纯数字/空值)")


# ========================================
# Test Case 2: indicator 传非法值
# ========================================
def case_test_2_invalid_indicator():
    """测试：非法indicator参数"""
    safe_sleep(1.0, 2.5)
    try:
        bad_df = ak.stock_financial_abstract_ths(symbol="600519", indicator="按月度")
        print(f"✔ Test 2: 非法 indicator 返回: {type(bad_df)}, 行数: {len(bad_df) if bad_df is not None else 'None'}")
    except Exception as e:
        print(f"✔ Test 2: 非法 indicator 抛出异常: {type(e).__name__}: {e}")


# ========================================
# Test Case 3: 注意金额字段可能带中文单位
# ========================================
def case_test_3_has_unit(df):
    """测试：净利润字段含中文单位"""
    if df is None or "净利润" not in df.columns:
        print("[SKIP] 无数据，跳过")
        return
    sample = str(df["净利润"].iloc[0])
    has_unit = "亿" in sample or "万" in sample
    print(f"✔ Test 3: 净利润示例值 '{sample}', 含中文单位: {has_unit}")


# ========================================
# main: 逐个打开case验证
# ========================================
def main():
    df = case_1_quarterly()

    # 取消注释以验证对应case：
    # case_2_annual()
    # case_3_key_metrics(df)
    # case_4_compare()

    # print("\n" + "=" * 50)
    # print("测试 Case")
    # case_test_1_amount_parse()
    # case_test_2_invalid_indicator()
    # case_test_3_has_unit(df)


if __name__ == "__main__":
    main()
