# -*- coding: utf-8 -*-
"""
场景3：获取A股实时行情
学习目标：掌握 stock_zh_a_spot_em() 获取全市场实时行情
运行方式：python3 demo_03_realtime_quote.py
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
# Case 1: 获取全部A股实时行情
# ========================================
def case_1_all_spot():
    """获取全市场实时行情"""
    df = request_wrapper(ak.stock_zh_a_spot_em, desc="获取实时行情")
    if df is None:
        print("[SKIP] 获取实时行情失败，跳过后续操作")
        return None

    print(f"A股实时行情: {len(df)} 只")
    print(f"列名: {list(df.columns)}")
    print(df.head(3))
    return df


# ========================================
# Case 2: 查找涨幅前10
# ========================================
def case_2_top10_up(df):
    """查找涨幅前10的股票"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    top10 = df.nlargest(10, "涨跌幅")
    cols = [c for c in ["序号", "代码", "名称", "最新价", "涨跌幅", "成交额"] if c in df.columns]
    print(f"\n涨幅前10:")
    print(top10[cols])


# ========================================
# Case 3: 查找跌幅前10
# ========================================
def case_3_top10_down(df):
    """查找跌幅前10的股票"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    bottom10 = df.nsmallest(10, "涨跌幅")
    cols = [c for c in ["序号", "代码", "名称", "最新价", "涨跌幅", "成交额"] if c in df.columns]
    print(f"\n跌幅前10:")
    print(bottom10[cols])


# ========================================
# Case 4: 筛选特定股票的实时数据
# ========================================
def case_4_selected_stocks(df):
    """筛选指定股票的实时行情"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    targets = ["600519", "000858", "601318"]
    selected = df[df["代码"].isin(targets)]
    sel_cols = [c for c in ["代码", "名称", "最新价", "涨跌幅", "成交量", "成交额", "换手率"] if c in df.columns]
    print(f"\n指定股票实时行情:")
    print(selected[sel_cols])


# ========================================
# Test Case 1: 涨停股筛选
# ========================================
def case_test_1_limit_up(df):
    """测试：涨停股筛选"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    limit_up_main = df[df["涨跌幅"] >= 9.9]
    limit_up_cyb = df[(df["代码"].str.startswith("30")) & (df["涨跌幅"] >= 19.9)]
    limit_up_kcb = df[(df["代码"].str.startswith("68")) & (df["涨跌幅"] >= 19.9)]
    print(f"✔ Test 1: 涨停股 主板:{len(limit_up_main)} 创业板:{len(limit_up_cyb)} 科创板:{len(limit_up_kcb)}")


# ========================================
# Test Case 2: 成交额为0的股票（停牌股）
# ========================================
def case_test_2_suspended(df):
    """测试：停牌/零成交股票"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    suspended = df[df["成交额"] == 0]
    print(f"✔ Test 2: 停牌/零成交股: {len(suspended)} 只")


# ========================================
# Test Case 3: 涨跌幅范围应在合理区间
# ========================================
def case_test_3_change_range(df):
    """测试：涨跌幅范围"""
    if df is None:
        print("[SKIP] 无数据，跳过")
        return
    max_change = df["涨跌幅"].max()
    min_change = df["涨跌幅"].min()
    print(f"✔ Test 3: 涨跌幅范围: [{min_change:.2f}%, {max_change:.2f}%]")


# ========================================
# main: 逐个打开case验证
# ========================================
def main():
    df = case_1_all_spot()

    # 取消注释以验证对应case：
    # case_2_top10_up(df)
    # case_3_top10_down(df)
    # case_4_selected_stocks(df)

    # print("\n" + "=" * 50)
    # print("测试 Case")
    # case_test_1_limit_up(df)
    # case_test_2_suspended(df)
    # case_test_3_change_range(df)


if __name__ == "__main__":
    main()
