# -*- coding: utf-8 -*-
"""
场景6：获取个股资金流向
学习目标：掌握资金流向相关接口，分析主力资金动态
运行方式：python3 demo_06_fund_flow.py
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
# Case 1: 获取个股资金流向
# ========================================
def case_1_individual_flow():
    """获取个股资金流向"""
    df = request_wrapper(
        ak.stock_individual_fund_flow, desc="获取个股资金流向",
        stock="600519", market="sh",
    )
    if df is None:
        print("[SKIP] 获取个股资金流向失败，跳过后续操作")
        return None

    print(f"贵州茅台 资金流向: {len(df)} 条")
    print(f"列名: {list(df.columns)}")
    print(df.head(5))
    print(df.tail(5))
    return df


# ========================================
# Case 2: 计算最近N日主力净流入合计
# ========================================
def case_2_main_net_inflow(df):
    """计算最近5日主力净流入"""
    if df is None or df.empty:
        print("[SKIP] 无数据，跳过")
        return

    main_flow_col = None
    for col in df.columns:
        if "主力净流入" in col and "净额" in col:
            main_flow_col = col
            break

    if main_flow_col:
        recent_5 = df.head(5)
        total_main_flow = pd.to_numeric(recent_5[main_flow_col], errors="coerce").sum()
        print(f"\n最近5日主力净流入合计: {total_main_flow / 1e8:.2f} 亿")


# ========================================
# Case 3: 大盘资金流向概览
# ========================================
def case_3_market_flow():
    """获取大盘资金流向"""
    safe_sleep(1.0, 2.5)
    df_market = request_wrapper(ak.stock_market_fund_flow, desc="获取大盘资金流向")
    if df_market is None:
        print("[SKIP] 获取大盘资金流向失败")
        return None

    print(f"\n大盘资金流向: {len(df_market)} 条")
    print(f"列名: {list(df_market.columns)}")
    print(df_market.head(5))
    return df_market


# ========================================
# Case 4: 行业板块资金流向
# ========================================
def case_4_sector_flow():
    """获取行业板块资金流向排名"""
    safe_sleep(1.0, 2.5)
    df_sector = request_wrapper(
        ak.stock_sector_fund_flow_rank, desc="获取行业资金流向",
        indicator="今日", sector_type="行业资金流",
    )
    if df_sector is None:
        print("[SKIP] 获取行业资金流向失败")
        return
    print(f"\n行业资金流排名: {len(df_sector)} 条")
    print(df_sector.head(10))


# ========================================
# Test Case 1: market 参数错误
# ========================================
def case_test_1_invalid_market():
    """测试：错误的market参数"""
    safe_sleep(1.0, 2.5)
    try:
        bad_result = ak.stock_individual_fund_flow(stock="600519", market="abc")
        print(f"✔ Test 1: 错误market返回: {type(bad_result)}")
    except Exception as e:
        print(f"✔ Test 1: 错误market抛出异常: {type(e).__name__}: {e}")


# ========================================
# Test Case 2: 验证主力净流入 = 超大单+大单
# ========================================
def case_test_2_main_equals_super_big(df):
    """测试：主力净流入 = 超大单净流入 + 大单净流入"""
    if df is None or df.empty:
        print("[SKIP] 无数据，跳过")
        return

    super_col = None
    big_col = None
    main_col = None
    for col in df.columns:
        if "超大单" in col and "净额" in col:
            super_col = col
        if "大单" in col and "净额" in col and "超大" not in col:
            big_col = col
        if "主力" in col and "净额" in col:
            main_col = col

    if super_col and big_col and main_col:
        df_calc = df.head(10).copy()
        df_calc[super_col] = pd.to_numeric(df_calc[super_col], errors="coerce").fillna(0)
        df_calc[big_col] = pd.to_numeric(df_calc[big_col], errors="coerce").fillna(0)
        df_calc[main_col] = pd.to_numeric(df_calc[main_col], errors="coerce").fillna(0)
        df_calc["_sum"] = df_calc[super_col] + df_calc[big_col]
        diff = (df_calc[main_col] - df_calc["_sum"]).abs().max()
        print(f"✔ Test 2: 主力净流入 vs (超大单+大单) 最大偏差: {diff:.2f}")
    else:
        print("⚠ Test 2: 未找到对应列，跳过")


# ========================================
# Test Case 3: 大盘资金流向日期升序
# ========================================
def case_test_3_date_asc(df_market):
    """测试：大盘资金流向日期为升序排列"""
    if df_market is None or "日期" not in df_market.columns:
        print("[SKIP] 无大盘数据，跳过")
        return
    is_asc = df_market["日期"].iloc[0] <= df_market["日期"].iloc[-1]
    print(f"✔ Test 3: 大盘资金流向日期升序(从早到晚): {is_asc}")


# ========================================
# main: 逐个打开case验证
# ========================================
def main():
    df = case_1_individual_flow()

    # 取消注释以验证对应case：
    # case_2_main_net_inflow(df)
    # df_market = case_3_market_flow()
    # case_4_sector_flow()

    # print("\n" + "=" * 50)
    # print("测试 Case")
    # case_test_1_invalid_market()
    # case_test_2_main_equals_super_big(df)
    # df_market = case_3_market_flow()  # Test 3 需要大盘数据
    # case_test_3_date_asc(df_market)


if __name__ == "__main__":
    main()
