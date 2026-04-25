#!/usr/bin/env python3
"""
A股缩量回调选股系统 v3（璞泰来策略版）
按标日期筛选「缩量回调」标的，判断买点，输出结构化 Markdown 表格

优化点（基于璞泰来603659走势规律）:
- 近期涨幅过滤：仅保留 10~30% 区间（有趋势但未高位）
- 距高点回调限制：< 12%（上涨趋势中的正常回调）
- MACD DIF > 0（零轴上方才考虑）
- RSI 边界收严：45~68（非超买非超卖）
- MA 支撑容忍度放宽至 ±3%
"""

import os
import sys
import json
import argparse
from datetime import datetime
import pandas as pd
import numpy as np

# === 路径配置 ===
PARQUET_PATH = "/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/data/all_daily.parquet"
STOCKINFO_PATH = "/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/stockInfo.csv"
OUTPUT_DIR = "/Users/niufen/claw/MyScript/prompt"

# === 交易参数（璞泰来策略 v3）===
BIAS_THRESHOLD = 5.0          # 乖离率阈值（%）
MA_SUPPORT_TOLERANCE = 0.03  # MA 支撑容忍度（±3%，璞泰来策略放宽）
MIN_DAYS = 60                # 最少数据天数

# === 璞泰来策略新增条件 ===
RECENT_GAIN_MIN   = 0.05    # 近期最小涨幅（5%起，有一定趋势即可）
RECENT_GAIN_MAX   = 0.50    # 近期最大涨幅（超过50%视为高位）
RSI_MIN           = 40.0     # RSI下限（过低可能是下跌中继）
RSI_MAX           = 72.0     # RSI上限（超过72视为超买）
PULLBACK_MAX      = 0.20    # 距近期高点最大回调幅度（20%以内）
LOOKBACK_DAYS     = 20       # 近期高低点计算窗口

# === 向量化预筛选参数 ===
PRE_FILTER_DAYS = 5          # 预筛选看最近N天
SHRINK_THRESHOLD = 0.85      # 缩量阈值（当天/前一天 < 此值视为缩量）

# ============================================================
# 指标计算
# ============================================================

def calc_ma(close: np.ndarray, windows=[5, 10, 20]) -> dict:
    """计算均线（numpy版本）"""
    result = {}
    for w in windows:
        if len(close) >= w:
            result[f'MA{w}'] = round(float(np.mean(close[-w:])), 2)
        else:
            result[f'MA{w}'] = np.nan
    return result

def calc_macd(close: np.ndarray) -> dict:
    """计算 MACD 状态"""
    if len(close) < 26:
        return {'status': 'UNKNOWN', 'signal': '数据不足', 'dif': -999}

    ema_fast = pd.Series(close).ewm(span=12, adjust=False).mean().values
    ema_slow = pd.Series(close).ewm(span=26, adjust=False).mean().values
    dif_arr = ema_fast - ema_slow
    dea_arr = pd.Series(dif_arr).ewm(span=9, adjust=False).mean().values

    latest_dif = dif_arr[-1]
    latest_dea = dea_arr[-1]
    prev_dif = dif_arr[-2]
    prev_dea = dea_arr[-2]

    prev_diff = prev_dif - prev_dea
    curr_diff = latest_dif - latest_dea

    if prev_diff <= 0 < curr_diff and latest_dif > 0:
        return {'status': 'GOLDEN_CROSS_ZERO', 'signal': '零轴上金叉', 'dif': latest_dif}
    elif prev_diff <= 0 < curr_diff:
        return {'status': 'GOLDEN_CROSS', 'signal': '金叉，趋势向上', 'dif': latest_dif}
    elif prev_diff >= 0 > curr_diff:
        return {'status': 'DEATH_CROSS', 'signal': '死叉，趋势向下', 'dif': latest_dif}
    elif latest_dif > 0 and latest_dea > 0:
        return {'status': 'BULLISH', 'signal': '多头排列', 'dif': latest_dif}
    elif latest_dif < 0:
        return {'status': 'BEARISH', 'signal': '空头排列', 'dif': latest_dif}
    return {'status': 'NEUTRAL', 'signal': '中性', 'dif': latest_dif}

def calc_rsi(close: np.ndarray, period=14) -> float:
    if len(close) < period + 1:
        return 50.0
    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=period).mean().values[-1]
    avg_loss = pd.Series(loss).rolling(window=period).mean().values[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(float(100 - 100 / (1 + rs)), 1)

def count_consecutive_shrink(vol_arr: np.ndarray, max_days=5) -> tuple:
    """
    从最新一天往前数，连续缩量多少天
    返回: (连续缩量天数, [每天变化率])
    """
    if len(vol_arr) < 2:
        return 0, []

    shrink_days = 0
    changes = []

    # 从倒数第二天开始往前比
    for i in range(len(vol_arr) - 1, 0, -1):
        if vol_arr[i] < vol_arr[i - 1]:
            pct = round((vol_arr[i] - vol_arr[i - 1]) / vol_arr[i - 1] * 100, 1)
            changes.insert(0, pct)
            shrink_days += 1
            if shrink_days >= max_days:
                break
        else:
            break

    return shrink_days, changes

def check_ma_support(price: float, ma5: float, ma10: float) -> dict:
    """检查价格是否回踩 MA5/MA10"""
    result = {'on_ma5': False, 'on_ma10': False, 'which_ma': '无'}

    if not np.isnan(ma5) and ma5 > 0:
        if abs(price - ma5) / ma5 <= MA_SUPPORT_TOLERANCE:
            result['on_ma5'] = True
            result['which_ma'] = 'MA5'

    if not np.isnan(ma10) and ma10 > 0:
        if abs(price - ma10) / ma10 <= MA_SUPPORT_TOLERANCE:
            if result['on_ma5']:
                result['which_ma'] = 'MA5+MA10'
            else:
                result['on_ma10'] = True
                result['which_ma'] = 'MA10'

    return result

def analyze_volume_trend(vol_arr: np.ndarray, close_arr: np.ndarray) -> str:
    """综合量能趋势判断"""
    if len(vol_arr) < 5:
        return '量能正常'

    vol_5d_avg = np.mean(vol_arr[-6:-1])
    latest_vol = vol_arr[-1]
    ratio = latest_vol / vol_5d_avg if vol_5d_avg > 0 else 1

    price_change = (close_arr[-1] - close_arr[-2]) / close_arr[-2] * 100 if len(close_arr) >= 2 else 0

    if ratio >= 1.5:
        return '放量上涨' if price_change > 0 else '放量下跌'
    elif ratio <= 0.7:
        return '缩量回调' if price_change <= 0 else '缩量上涨'
    return '量能正常'

def generate_score(ma_status: str, bias_ma5: float, macd_status: str,
                   volume_trend: str, on_ma5: bool, on_ma10: bool,
                   rsi: float) -> dict:
    """综合评分（100分）"""
    score = 0
    reasons, risks = [], []

    # 趋势（30分）
    trend_map = {'多头排列': 26, 'MA5>MA10': 20, '盘整': 12, 'MA5<MA10': 8, '空头排列': 4}
    score += trend_map.get(ma_status, 12)
    if ma_status == '多头排列':
        reasons.append('均线多头排列，顺势做多')
    elif ma_status == 'MA5>MA10':
        reasons.append('MA5在MA10上方，上涨趋势')

    # 乖离率（20分）
    if bias_ma5 < 0:
        if bias_ma5 > -3:
            score += 20
            reasons.append(f'价格略低于MA5({bias_ma5:.1f}%)，回踩买点')
        elif bias_ma5 > -5:
            score += 16
            reasons.append(f'价格回踩MA5({bias_ma5:.1f}%)，观察支撑')
        else:
            score += 8
            risks.append(f'乖离率过大({bias_ma5:.1f}%)，可能破位')
    elif bias_ma5 < 2:
        score += 18
        reasons.append(f'价格贴近MA5({bias_ma5:.1f}%)，介入好时机')
    elif bias_ma5 < BIAS_THRESHOLD:
        score += 14
        reasons.append(f'价格略高于MA5({bias_ma5:.1f}%)')
    else:
        score += 4
        risks.append(f'乖离率过高({bias_ma5:.1f}%>5%)，严禁追高')

    # 量能（15分）
    vol_map = {'缩量回调': 15, '缩量上涨': 6, '放量上涨': 12, '放量下跌': 0, '量能正常': 10}
    score += vol_map.get(volume_trend, 8)
    if volume_trend == '缩量回调':
        reasons.append('缩量回调，主力洗盘特征')
    elif volume_trend == '放量下跌':
        risks.append('放量下跌，注意风险')

    # 支撑（10分）
    if on_ma5:
        score += 5
        reasons.append('MA5支撑有效')
    if on_ma10:
        score += 5
        reasons.append('MA10支撑有效')

    # MACD（15分）
    macd_map = {'GOLDEN_CROSS_ZERO': 15, 'GOLDEN_CROSS': 12, 'BULLISH': 8,
                'CROSSING_UP': 10, 'BEARISH': 2, 'CROSSING_DOWN': 0, 'DEATH_CROSS': 0}
    score += macd_map.get(macd_status, 5)
    if macd_status in ['GOLDEN_CROSS_ZERO', 'GOLDEN_CROSS']:
        reasons.append(f'MACD{macd_status=="GOLDEN_CROSS_ZERO" and "零轴上金叉" or "金叉"}')
    elif macd_status in ['DEATH_CROSS', 'CROSSING_DOWN']:
        risks.append('MACD死叉信号')

    # RSI（10分）
    if rsi < 30:
        score += 10
        reasons.append(f'RSI超卖({rsi})，反弹机会大')
    elif rsi < 45:
        score += 6
        reasons.append(f'RSI偏弱({rsi})，低位蓄力中')
    elif rsi <= 60:
        score += 9
        reasons.append(f'RSI适中({rsi})，多头控盘')
    elif rsi <= RSI_MAX:
        score += 5
        risks.append(f'RSI偏高({rsi})，注意短线回调风险')
    else:
        score += 0
        risks.append(f'RSI超买({rsi})，短期风险极大')

    # 信号判定
    if score >= 75:
        action = '强烈买入'
    elif score >= 60:
        action = '买入'
    elif score >= 45:
        action = '持有'
    elif score >= 30:
        action = '观望'
    else:
        action = '卖出'

    return {'score': score, 'action': action, 'reasons': reasons, 'risks': risks}

# ============================================================
# 主流程
# ============================================================

def run_screening(target_date: str, max_results: int = 50):
    date_ts = pd.to_datetime(target_date)

    print(f"[{datetime.now()}] 开始选股筛选: {target_date}", file=sys.stderr)

    # === 1. 加载数据 ===
    print("[1/6] 加载日线数据...", file=sys.stderr)
    df = pd.read_parquet(PARQUET_PATH)
    df['date'] = pd.to_datetime(df['日期'])
    df['code'] = df['代码'].astype(str).str.zfill(6)
    df = df.rename(columns={'收盘': 'close', '成交量': 'volume', '涨跌幅(%)': 'pct_chg'})

    # 过滤 00/60 开头主板
    df_main = df[df['code'].str.match(r'^(00|60)')].copy()
    print(f"    主板股票数: {df_main['code'].nunique()}", file=sys.stderr)

    # === 2. 加载股票信息 & ST排除 ===
    print("[2/6] 加载股票信息...", file=sys.stderr)
    stock_info = pd.read_csv(STOCKINFO_PATH)
    stock_info['code'] = stock_info['A股代码'].astype(str).str.zfill(6)
    code_to_name = dict(zip(stock_info['code'], stock_info['A股简称']))
    code_to_industry = dict(zip(stock_info['code'], stock_info['所属行业'].fillna('未知')))

    st_set = set(stock_info[stock_info['A股简称'].str.contains('ST', na=False)]['code'].tolist())
    print(f"    ST股票数: {len(st_set)}", file=sys.stderr)

    # === 3. 向量化预筛选：计算每个股票近5天是否满足基本条件 ===
    print("[3/6] 向量化预筛选...", file=sys.stderr)
    all_codes = df_main['code'].unique()
    pre_candidates = []

    # 分批处理加速
    batch_size = 500
    total = len(all_codes)
    for batch_start in range(0, total, batch_size):
        batch_codes = all_codes[batch_start:batch_start+batch_size]
        batch_df = df_main[df_main['code'].isin(batch_codes)].copy()
        batch_df = batch_df.sort_values(['code', 'date'])

        for code in batch_codes:
            if code in st_set:
                continue

            cdf = batch_df[batch_df['code'] == code].tail(60).reset_index(drop=True)
            if len(cdf) < 40:
                continue

            # 最新一天数据
            latest_close = cdf['close'].iloc[-1]
            latest_pct = cdf['pct_chg'].iloc[-1]

            # 排除涨停
            if latest_pct > 9.0:
                continue

            close_arr = cdf['close'].values
            vol_arr = cdf['volume'].values

            # === 璞泰来策略条件 ===

            # 近期涨幅过滤（10~30%区间）
            if len(close_arr) >= LOOKBACK_DAYS:
                recent_low = np.min(close_arr[-LOOKBACK_DAYS:])
                recent_high = np.max(close_arr[-LOOKBACK_DAYS:])
                if recent_low <= 0:
                    continue
                recent_gain = (recent_high - recent_low) / recent_low
                if recent_gain < RECENT_GAIN_MIN or recent_gain > RECENT_GAIN_MAX:
                    continue

                # 距高点回调幅度（璞泰来策略：距高点 < 12%）
                pullback = (recent_high - latest_close) / recent_high
                if pullback > PULLBACK_MAX:
                    continue

            # 计算 MA
            ma5_val = np.mean(close_arr[-5:]) if len(close_arr) >= 5 else np.nan
            ma10_val = np.mean(close_arr[-10:]) if len(close_arr) >= 10 else np.nan

            # 均线状态判断
            if not (ma5_val > ma10_val):
                continue  # 必须 MA5 > MA10

            # 连续缩量天数（从最新往前数）
            shrink_days, _ = count_consecutive_shrink(vol_arr, max_days=5)
            if shrink_days < 3:
                continue

            # 乖离率检查（璞泰来策略：±3%）
            bias_ma5 = (latest_close - ma5_val) / ma5_val * 100 if ma5_val > 0 else 0
            if abs(bias_ma5) > BIAS_THRESHOLD:
                continue

            # RSI 检查（璞泰来策略）
            rsi = calc_rsi(close_arr)
            if rsi < RSI_MIN or rsi > RSI_MAX:
                continue

            # MACD DIF > 0（预筛参考，不过滤）
            # 注：DIF 过滤仅在精筛阶段生效

            pre_candidates.append(code)

            pre_candidates.append(code)

    pre_candidates = list(dict.fromkeys(pre_candidates))  # 去重
    print(f"    预筛选通过: {len(pre_candidates)} 只（去重后）", file=sys.stderr)
    if not pre_candidates:
        print("[!] 没有找到符合条件的标的", file=sys.stderr)
        return

    # === 4. 精筛：对预筛选标的计算详细指标 ===
    print("[4/6] 精确筛选与指标计算...", file=sys.stderr)
    candidates = []

    # 构建全量数据字典（避免重复读文件）
    cdf_dict = {}
    all_df_sorted = df_main.sort_values(['code', 'date'])
    for code in pre_candidates:
        cdf = all_df_sorted[all_df_sorted['code'] == code].tail(60).reset_index(drop=True)
        cdf_dict[code] = cdf

    for idx, code in enumerate(pre_candidates):
        if idx % 100 == 0:
            print(f"    精筛进度 {idx}/{len(pre_candidates)}", file=sys.stderr)

        cdf = cdf_dict[code]
        close_arr = cdf['close'].values
        vol_arr = cdf['volume'].values

        latest_close = close_arr[-1]
        latest_pct = cdf['pct_chg'].iloc[-1]

        # === 璞泰来策略条件 ===
        # 近期涨幅 + 距高点回调（精筛再次确认）
        if len(close_arr) >= LOOKBACK_DAYS:
            recent_low = np.min(close_arr[-LOOKBACK_DAYS:])
            recent_high = np.max(close_arr[-LOOKBACK_DAYS:])
            if recent_low <= 0:
                continue
            recent_gain = (recent_high - recent_low) / recent_low
            if recent_gain < RECENT_GAIN_MIN or recent_gain > RECENT_GAIN_MAX:
                continue
            pullback = (recent_high - latest_close) / recent_high
            if pullback > PULLBACK_MAX:
                continue
        else:
            continue

        # 均线
        ma5_val = np.mean(close_arr[-5:])
        ma10_val = np.mean(close_arr[-10:])
        ma20_val = np.mean(close_arr[-20:])

        # 均线状态
        if ma5_val > ma10_val > ma20_val:
            ma_status = '多头排列'
        elif ma5_val > ma10_val:
            ma_status = 'MA5>MA10'
        elif ma5_val < ma10_val:
            ma_status = 'MA5<MA10'
        else:
            ma_status = '盘整'

        # 乖离率（璞泰来策略 ±3%）
        bias_ma5 = (latest_close - ma5_val) / ma5_val * 100
        if abs(bias_ma5) > BIAS_THRESHOLD:
            continue

        # 回踩均线判断（璞泰来策略 ±3%）
        support = check_ma_support(latest_close, ma5_val, ma10_val)
        if not (support['on_ma5'] or support['on_ma10']):
            continue

        # 技术指标
        macd_info = calc_macd(close_arr)

        # MACD DIF > 0（零轴上方）
        if macd_info.get('dif', -999) <= 0:
            continue

        rsi = calc_rsi(close_arr)
        if rsi < RSI_MIN or rsi > RSI_MAX:
            continue

        volume_trend = analyze_volume_trend(vol_arr, close_arr)
        score_result = generate_score(
            ma_status, bias_ma5, macd_info['status'],
            volume_trend, support['on_ma5'], support['on_ma10'], rsi
        )

        # 连续缩量天数
        shrink_days, vol_changes = count_consecutive_shrink(vol_arr, max_days=5)

        # MA 多头排列强化
        if ma5_val <= ma10_val:
            continue

        candidates.append({
            'code': code,
            'name': code_to_name.get(code, code),
            'industry': code_to_industry.get(code, '未知'),
            'shrink_days': shrink_days,
            'vol_changes': vol_changes,
            'recent_gain': round(recent_gain * 100, 1),
            'pullback': round(pullback * 100, 1),
            'recent_high': round(recent_high, 2),
            'recent_low': round(recent_low, 2),
            'which_ma': support['which_ma'],
            'ma_status': ma_status,
            'score': score_result['score'],
            'action': score_result['action'],
            'reasons': score_result['reasons'],
            'risks': score_result['risks'],
            'bias_ma5': round(bias_ma5, 2),
            'macd_status': macd_info['status'],
            'macd_signal': macd_info['signal'],
            'macd_dif': round(macd_info.get('dif', 0), 3),
            'rsi': rsi,
            'volume_trend': volume_trend,
            'close': round(latest_close, 2),
            'pct_chg': round(latest_pct, 2),
            'ma5': round(ma5_val, 2),
            'ma10': round(ma10_val, 2),
            'ma20': round(ma20_val, 2),
            'stock_df': cdf.tail(20).copy(),
            'all_df': cdf.copy(),
        })

    print(f"    精筛完成，符合条件: {len(candidates)} 只", file=sys.stderr)

    if not candidates:
        print("[!] 没有找到符合条件的标的", file=sys.stderr)
        return

    # 按评分 + 缩量天数排序
    candidates.sort(key=lambda x: (x['score'], x['shrink_days']), reverse=True)
    candidates = candidates[:max_results]

    # === 5. 生成 Markdown 报告 ===
    print("[5/6] 生成报告...", file=sys.stderr)
    md = []

    md.append(f"# A股缩量回调选股结果")
    md.append(f"")
    md.append(f"**筛选日期**: {target_date}")
    md.append(f"**筛选时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md.append(f"**选股条件**: 代码前缀 00/60、非ST、非涨停(涨跌幅≤9%)、连续缩量3天+、")
    md.append(f"回踩MA5/MA10支撑、MA5>MA10、乖离率<5%、RSI<70")
    md.append(f"**符合条件标的**: {len(candidates)} 只（按评分+缩量天数排序）")
    md.append(f"")

    # 总览表
    md.append(f"## 一、选股结果总览")
    md.append(f"")
    md.append(f"| 序号 | 代码 | 名称 | 行业 | 连续缩量天数 | 回踩均线 | 均线状态 | 评分 | 信号 |")
    md.append(f"|------|------|------|------|------------|---------|--------|------|------|")

    for i, c in enumerate(candidates, 1):
        md.append(f"| {i} | {c['code']} | {c['name']} | {c['industry']} | {c['shrink_days']}天 | {c['which_ma']} | {c['ma_status']} | {c['score']} | {c['action']} |")

    md.append(f"")
    md.append(f"> 💡 评分标准: 75+=强烈买入 60-74=买入 45-59=持有 30-44=观望 <30=卖出")
    md.append(f"> 📌 排序规则: 按评分降序，评分相同则按连续缩量天数降序")
    md.append(f"")

    # 详情部分
    md.append(f"## 二、标的详情")
    md.append(f"")

    for i, c in enumerate(candidates, 1):
        stock_df = c['stock_df']
        all_df = c['all_df']
        close_arr = all_df['close'].values

        # 历史 MACD 金叉/死叉
        hist_signals = []
        if len(all_df) >= 27:
            ema_fast = pd.Series(close_arr).ewm(span=12, adjust=False).mean().values
            ema_slow = pd.Series(close_arr).ewm(span=26, adjust=False).mean().values
            dif = ema_fast - ema_slow
            dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values

            for j in range(1, len(all_df)):
                pdiff = dif[j-1] - dea[j-1]
                cdiff = dif[j] - dea[j]
                if pdiff <= 0 < cdiff:
                    hist_signals.append({
                        'type': '买入(金叉)',
                        'date': all_df.iloc[j]['date'].strftime('%Y-%m-%d'),
                        'price': round(all_df.iloc[j]['close'], 2),
                        'note': 'DIF上穿DEA'
                    })
                elif pdiff >= 0 > cdiff:
                    hist_signals.append({
                        'type': '卖出(死叉)',
                        'date': all_df.iloc[j]['date'].strftime('%Y-%m-%d'),
                        'price': round(all_df.iloc[j]['close'], 2),
                        'note': 'DIF下穿DEA'
                    })
        hist_signals = hist_signals[-8:]

        md.append(f"### 标的: {c['code']} {c['name']} {c['industry']}")
        md.append(f"")
        md.append(f"#### 近20日走势数据表")
        md.append(f"")
        md.append(f"| 日期 | 收盘价 | 涨跌幅 | 成交量 | 成交量变化 | MA5 | MA10 | MA20 |")
        md.append(f"|------|--------|--------|--------|----------|-----|------|------|")

        ma5s = stock_df['close'].rolling(5).mean()
        ma10s = stock_df['close'].rolling(10).mean()
        ma20s = stock_df['close'].rolling(20).mean()

        for j in range(len(stock_df)):
            row = stock_df.iloc[j]
            vol = row['volume']
            vol_str = f"{vol/1e4:.0f}万" if vol < 1e6 else f"{vol/1e8:.2f}亿"
            pct = round(row['pct_chg'], 2)

            if j > 0:
                prev_vol = stock_df.iloc[j-1]['volume']
                vol_chg = (vol - prev_vol) / prev_vol * 100
                vol_chg_str = f"{vol_chg:+.1f}%"
            else:
                vol_chg_str = '-'

            ma5v = round(ma5s.iloc[j], 2) if not pd.isna(ma5s.iloc[j]) else '-'
            ma10v = round(ma10s.iloc[j], 2) if not pd.isna(ma10s.iloc[j]) else '-'
            ma20v = round(ma20s.iloc[j], 2) if not pd.isna(ma20s.iloc[j]) else '-'

            md.append(f"| {row['date'].strftime('%m-%d')} | {round(row['close'], 2)} | {pct:+.2f}% | {vol_str} | {vol_chg_str} | {ma5v} | {ma10v} | {ma20v} |")

        md.append(f"")
        md.append(f"#### 技术指标")
        md.append(f"")
        md.append(f"| 指标 | 数值 | 信号 |")
        md.append(f"|------|------|------|")
        md.append(f"| MA5 | {c['ma5']} | {'在MA10上方' if c['ma5'] > c['ma10'] else '在MA10下方'} |")
        md.append(f"| MA10 | {c['ma10']} | {'向上' if c['ma_status'] in ['多头排列', 'MA5>MA10'] else '向下'} |")
        md.append(f"| MA20 | {c['ma20']} | - |")
        md.append(f"| MACD | {c['macd_status']} | {c['macd_signal']} |")
        md.append(f"| RSI(14) | {c['rsi']} | {'超买⚠️' if c['rsi'] >= 70 else '超卖✨' if c['rsi'] < 30 else '中性' if c['rsi'] < 50 else '偏多' if c['rsi'] < 65 else '偏弱⚠️'} |")
        md.append(f"| 乖离率(MA5) | {c['bias_ma5']}% | {'安全(<5%)' if abs(c['bias_ma5']) < 5 else '危险(>5%)'} |")
        md.append(f"| 成交量 | {c['volume_trend']} | {'✅ 缩量回调' if c['volume_trend'] == '缩量回调' else '⚠️ 注意'} |")
        md.append(f"")

        md.append(f"#### 买点判断")
        md.append(f"")
        md.append(f"| 项目 | 内容 |")
        md.append(f"|------|------|")
        md.append(f"| 是否买点 | {c['action']} |")
        md.append(f"| 买点评分 | {c['score']}/100 |")
        md.append(f"| 买入信号 | {' + '.join(c['reasons'][:3]) if c['reasons'] else '暂无明显信号'} |")
        md.append(f"| 风险提示 | {'; '.join(c['risks']) if c['risks'] else '无'} |")
        md.append(f"")

        if hist_signals:
            md.append(f"#### 历史买卖点信号")
            md.append(f"")
            md.append(f"| 信号类型 | 日期 | 价格 | 说明 |")
            md.append(f"|---------|------|------|------|")
            for sig in hist_signals:
                md.append(f"| {sig['type']} | {sig['date']} | {sig['price']} | {sig['note']} |")
            md.append(f"")

        md.append(f"---\n")

    # === 6. 保存文件 ===
    print("[6/6] 保存报告...", file=sys.stderr)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = date_ts.strftime('%Y%m%d')
    output_path = os.path.join(OUTPUT_DIR, f"AStockSelect-Result-{date_str}.md")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md))

    print(f"[完成] 报告已保存: {output_path}", file=sys.stderr)
    print(f"[完成] 共筛选出 {len(candidates)} 只标的", file=sys.stderr)

    # 打印摘要
    print(f"\n{'='*50}")
    print(f"选股结果摘要 ({target_date})")
    print(f"{'='*50}")
    print(f"| 序号 | 代码 | 名称 | 缩量天数 | 评分 | 信号 |")
    print(f"|------|------|------|---------|------|------|")
    for i, c in enumerate(candidates, 1):
        print(f"| {i} | {c['code']} | {c['name']} | {c['shrink_days']}天 | {c['score']} | {c['action']} |")

    return output_path

# ============================================================
# 入口
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A股缩量回调选股系统 v2')
    parser.add_argument('--date', required=True, help='筛选日期 YYYY-MM-DD')
    parser.add_argument('--max', type=int, default=50, help='最多输出标的数')
    args = parser.parse_args()

    run_screening(args.date, args.max)