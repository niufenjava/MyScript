# -*- coding: utf-8 -*-
"""
场景5：获取行业板块行情
学习目标：掌握行业/概念板块列表与成分股查询
运行方式：python3 demo_05_industry_board.py
"""
import akshare as ak
import pandas as pd
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


# ========================================
# Case 1: 获取东方财富行业板块列表及行情
# ========================================
def case_1_industry_list():
    """获取行业板块列表"""
    df = request_wrapper(ak.stock_board_industry_name_em, desc="获取行业板块列表")
    if df is None:
        print("[SKIP] 获取行业板块列表失败，跳过后续操作")
        return None

    print(f"行业板块数量: {len(df)}")
    print(f"列名: {list(df.columns)}")
    print(df.head(10))
    return df


# ========================================
# Case 2: 查找涨幅前5的行业
# ========================================
def case_2_top5_up(df):
    """查找涨幅前5的行业"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    top5 = df.nlargest(5, "涨跌幅")
    cols = [c for c in ["板块名称", "涨跌幅", "总市值", "换手率"] if c in df.columns]
    print(f"\n今日涨幅前5行业:")
    print(top5[cols])


# ========================================
# Case 3: 获取银行板块成分股
# ========================================
def case_3_bank_members():
    """获取银行板块成分股"""
    safe_sleep(1.0, 2.5)
    bank_members = request_wrapper(ak.stock_board_industry_cons_em, desc="获取银行成分股", symbol="银行")
    if bank_members is None:
        print("[SKIP] 获取银行成分股失败")
        return
    print(f"\n银行板块成分股: {len(bank_members)} 只")
    print(f"列名: {list(bank_members.columns)}")
    print(bank_members.head(5))


# ========================================
# Case 4: 获取概念板块列表
# ========================================
def case_4_concept_list():
    """获取概念板块列表"""
    safe_sleep(1.0, 2.5)
    df_concept = request_wrapper(ak.stock_board_concept_name_em, desc="获取概念板块列表")
    if df_concept is None:
        print("[SKIP] 获取概念板块列表失败")
        return None
    print(f"\n概念板块数量: {len(df_concept)}")
    print(df_concept.head(5))
    return df_concept


# ========================================
# Case 5: 获取某概念板块成分股
# ========================================
def case_5_concept_members(df_concept):
    """获取第一个概念板块的成分股"""
    if df_concept is None or df_concept.empty:
        print("[SKIP] 无概念板块数据，跳过")
        return
    first_concept = df_concept.iloc[0]["板块名称"]
    safe_sleep(1.0, 2.5)
    concept_members = request_wrapper(
        ak.stock_board_concept_cons_em, desc=f"获取'{first_concept}'成分股",
        symbol=first_concept,
    )
    if concept_members is None:
        print("[SKIP] 获取概念成分股失败")
        return
    print(f"\n'{first_concept}' 概念成分股: {len(concept_members)} 只")
    print(concept_members.head(5))


# ========================================
# Test Case 1: 板块名称不存在
# ========================================
def case_test_1_invalid_sector():
    """测试：不存在的行业板块"""
    safe_sleep(1.0, 2.5)
    try:
        bad_result = ak.stock_board_industry_cons_em(symbol="不存在的行业XYZ")
        print(f"✔ Test 1: 不存在的行业返回: {type(bad_result)}, 行数: {len(bad_result) if bad_result is not None else 'None'}")
    except Exception as e:
        print(f"✔ Test 1: 不存在的行业抛出异常: {type(e).__name__}")


# ========================================
# Test Case 2: 行业板块 + 成分股联动
# ========================================
def case_test_2_industry_linkage(df):
    """测试：涨幅最高行业的成分股涨幅前3"""
    if df is None or df.empty:
        print("[SKIP] 无数据，跳过")
        return
    safe_sleep(1.0, 2.5)
    best_industry = df.nlargest(1, "涨跌幅").iloc[0]["板块名称"]
    best_members = request_wrapper(
        ak.stock_board_industry_cons_em, desc=f"获取'{best_industry}'成分股",
        symbol=best_industry,
    )
    if best_members is not None and "涨跌幅" in best_members.columns:
        top3 = best_members.nlargest(3, "涨跌幅")
        name_col = "名称" if "名称" in top3.columns else "股票名称"
        print(f"✔ Test 2: 涨幅最高行业'{best_industry}'的涨幅前3: {list(top3[name_col])}")
    else:
        print(f"⚠ Test 2: 无法获取'{best_industry}'成分股，跳过")


# ========================================
# Test Case 3: 概念板块数量应远多于行业板块
# ========================================
def case_test_3_count_compare(df_industry, df_concept):
    """测试：概念板块数量 > 行业板块数量"""
    if df_industry is None or df_concept is None:
        print("[SKIP] 数据不完整，跳过")
        return
    print(f"✔ Test 3: 概念板块({len(df_concept)}) vs 行业板块({len(df_industry)}): 概念板块更多? {len(df_concept) > len(df_industry)}")


# ========================================
# main: 逐个打开case验证
# ========================================
def main():
    df = case_1_industry_list()

    # 取消注释以验证对应case：
    # case_2_top5_up(df)
    # case_3_bank_members()
    # df_concept = case_4_concept_list()
    # case_5_concept_members(df_concept)

    # print("\n" + "=" * 50)
    # print("测试 Case")
    # case_test_1_invalid_sector()
    # case_test_2_industry_linkage(df)
    # df_concept = case_4_concept_list()  # Test 3 需要概念板块数据
    # case_test_3_count_compare(df, df_concept)


if __name__ == "__main__":
    main()
