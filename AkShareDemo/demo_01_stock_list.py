# -*- coding: utf-8 -*-
"""
场景1：获取A股上市公司列表
学习目标：掌握 stock_info_a_code_name() 获取基础股票元数据
运行方式：python3 demo_01_stock_list.py
"""
import akshare as ak
import pandas as pd


def request_wrapper(func, desc="请求", *args, **kwargs):
    """
    统一请求包装：重试3次 + 友好错误提示
    失败时返回 None，不抛出异常
    """
    import time
    import random
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
    import time
    import random
    time.sleep(random.uniform(a, b))


# ========================================
# Case 1: 获取A股所有上市公司代码和名称
# ========================================
def case_1_get_all_stocks():
    """获取全部A股上市公司列表"""
    df = request_wrapper(ak.stock_info_a_code_name, desc="获取股票列表")
    if df is None:
        print("[SKIP] 获取股票列表失败，跳过后续操作")
        return None

    print(f"A股上市公司总数: {len(df)}")
    print(f"列名: {list(df.columns)}")
    print(df.head(10))
    return df


# ========================================
# Case 2: 筛选沪市主板（代码以60开头）
# ========================================
def case_2_filter_sh_main(df):
    """筛选沪市主板股票"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    sh_main = df[df["code"].str.startswith("60")]
    print(f"\n沪市主板数量: {len(sh_main)}")
    print(sh_main.head(5))


# ========================================
# Case 3: 筛选创业板（代码以30开头）
# ========================================
def case_3_filter_cyb(df):
    """筛选创业板股票"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    cyb = df[df["code"].str.startswith("30")]
    print(f"\n创业板数量: {len(cyb)}")
    print(cyb.head(5))


# ========================================
# Case 4: 模糊搜索股票名称
# ========================================
def case_4_fuzzy_search(df):
    """按名称模糊搜索股票"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    keyword = "银行"
    result = df[df["name"].str.contains(keyword)]
    print(f"\n名称含'{keyword}'的股票: {len(result)} 只")
    print(result.head(5))


# ========================================
# Test Case 1: 传入不存在的关键词，应返回0行
# ========================================
def case_test_1_empty_keyword(df):
    """测试：不存在的关键词返回空结果"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    empty_result = df[df["name"].str.contains("不存在的公司XYZ")]
    assert len(empty_result) == 0, f"Case 1 失败: 预期0行, 实际{len(empty_result)}行"
    print("✔ Test 1: 不存在的关键词返回0行")


# ========================================
# Test Case 2: 验证 code 列全为6位字符串
# ========================================
def case_test_2_code_length(df):
    """测试：所有股票代码均为6位"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    code_lengths = df["code"].str.len().unique()
    assert all(cl == 6 for cl in code_lengths), f"Case 2 失败: 代码长度不全是6: {code_lengths}"
    print("✔ Test 2: 所有股票代码均为6位")


# ========================================
# Test Case 3: 筛选北交所(8开头/4开头)，检查数量合理性
# ========================================
def case_test_3_bse_count(df):
    """测试：北交所股票数量"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    bse = df[df["code"].str.startswith(("8", "4"))]
    print(f"✔ Test 3: 北交所股票数: {len(bse)} (合理范围: 200-400)")


# ========================================
# main: 逐个打开case验证
# ========================================
def main():

    # Case 1: 获取A股所有上市公司代码和名称（必须保持开启，是所有case的数据来源）
    df = case_1_get_all_stocks()

    # Case 2: 筛选沪市主板（代码以60开头）
    # case_2_filter_sh_main(df)

    # Case 3: 筛选创业板（代码以30开头）
    case_3_filter_cyb(df)
    
    # Case 4: 模糊搜索股票名称
    # case_4_fuzzy_search(df)

    # print("\n" + "=" * 50)
    # print("测试 Case")
    # Test Case 1: 传入不存在的关键词，应返回0行
    # case_test_1_empty_keyword(df)
    # Test Case 2: 验证 code 列全为6位字符串
    # case_test_2_code_length(df)
    # Test Case 3: 筛选北交所(8开头/4开头)，检查数量合理性
    # case_test_3_bse_count(df)


if __name__ == "__main__":
    main()
