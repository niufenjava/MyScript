#!/usr/bin/env python3
"""
统一入口 - A股数据采集工具集

用法：
  python run.py                  # 运行全套每日采集（codes → info → finance → daily → events → merge）
  python run.py daily            # 增量更新日线
  python run.py events           # 采集异动事件
  python run.py codes            # 更新股票代码列表
  python run.py finance          # 更新财报数据
  python run.py info             # 更新市场信息（腾讯行情 + 巨潮基础）
  python run.py merge            # 合并日线大表

  python run.py --help           # 查看帮助
"""

import sys
import argparse
import time
import traceback
from datetime import datetime

sys.path.insert(0, sys.path[0])

# ── 日志颜色 ─────────────────────────────────────────────────────────────────
RED   = "\033[91m"
GREEN = "\033[92m"
YELLOW= "\033[93m"
CYAN  = "\033[96m"
BOLD  = "\033[1m"
RESET = "\033[0m"


def log(msg, color=""):
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = f"{color}[{ts}]{RESET}" if color else f"[{ts}]"
    print(f"{prefix} {msg}")


def ok(msg):
    log(f"{GREEN}✅ {msg}{RESET}", CYAN)


def fail(msg):
    log(f"{RED}❌ {msg}{RESET}", RED)


def skip(msg):
    log(f"{YELLOW}⏭️  {msg}{RESET}", YELLOW)


# ── 各模块执行函数 ────────────────────────────────────────────────────────────

def step(name, fn, *args, **kwargs):
    """执行单个步骤，捕获异常，返回 (success, elapsed, error)"""
    start = time.time()
    color = CYAN
    try:
        fn(*args, **kwargs)
        elapsed = time.time() - start
        return True, elapsed, None
    except Exception as e:
        elapsed = time.time() - start
        fail(f"{name} 失败: {e}")
        traceback.print_exc()
        return False, elapsed, str(e)


# ── 全套每日采集流程 ─────────────────────────────────────────────────────────

def run_all():
    """每日盘后一键采集所有数据，按顺序执行"""
    print()
    log(f"{BOLD}{'='*60}", CYAN)
    log(f"{BOLD}  A股数据采集 · 每日盘后一键运行{RESET}", CYAN)
    log(f"{BOLD}{'='*60}{RESET}", CYAN)
    print()

    results = []   # (step_name, success, elapsed, error)

    # 1. 更新股票代码列表
    ok("开始更新股票代码列表...")
    from fetcher.codes import get_stock_codes as gc
    s, t, e = step("更新股票代码", gc)
    results.append(("股票代码", s, t, e))
    if s: ok(f"完成股票代码更新")
    else: fail("股票代码更新失败，跳过后续依赖步骤")

    # 2. 更新市场信息（腾讯行情 + 巨潮基础）
    from fetcher.info_market import main as market_main
    from fetcher.info_base import main as base_main
    s, t, e = step("腾讯行情", market_main)
    results.append(("市场数据", s, t, e))
    if s: ok("完成腾讯行情更新")
    else: fail("腾讯行情更新失败")

    s, t, e = step("巨潮基础信息", base_main)
    results.append(("基础信息", s, t, e))
    if s: ok("完成巨潮基础信息更新")
    else: fail("巨潮基础信息更新失败")

    # 3. 更新财报数据
    from fetcher.finance import main as finance_main
    s, t, e = step("财报数据", finance_main)
    results.append(("财报数据", s, t, e))
    if s: ok("完成财报数据更新")
    else: fail("财报数据更新失败")

    # 4. 增量更新日线（腾讯批量接口）
    from fetcher.daily_incr import main as incr_main
    s, t, e = step("增量日线", incr_main)
    results.append(("增量日线", s, t, e))
    if s: ok("完成增量日线更新")
    else: fail("增量日线更新失败")

    # 5. 采集异动事件
    from fetcher.events import run_daily as events_daily
    s, t, e = step("异动事件", events_daily)
    results.append(("异动事件", s, t, e))
    if s: ok("完成异动事件采集")
    else: fail("异动事件采集失败")

    # 6. 合并日线大表
    from utils.merge_daily import merge_all as ma
    s, t, e = step("合并大表", ma)
    results.append(("合并大表", s, t, e))
    if s: ok("完成日线大表合并")
    else: fail("日线大表合并失败")

    # ── 汇总报告 ──────────────────────────────────────────────────────────────
    print()
    log(f"{BOLD}{'='*60}", CYAN)
    log(f"{BOLD}  每日采集汇总报告{RESET}", CYAN)
    log(f"{BOLD}{'='*60}{RESET}", CYAN)
    print()

    total_time = 0
    ok_count = sum(1 for r in results if r[1])
    fail_count = len(results) - ok_count

    print(f"{'步骤':<12} {'状态':<8} {'耗时':>8} {'说明'}")
    print(f"{'-'*12} {'-'*8} {'-'*8} {'-'*20}")
    for name, success, elapsed, error in results:
        status = f"{GREEN}✅ 成功{RESET}" if success else f"{RED}❌ 失败{RESET}"
        note = error[:40] + "..." if error and len(error) > 40 else (error or "")
        print(f"{name:<12} {status:<24} {elapsed:>7.1f}s   {note}")

    print(f"{'-'*12} {'-'*8} {'-'*8}")
    total_time = sum(r[2] for r in results)
    print(f"{'合计':<12} {f'{GREEN}✅ {ok_count} 成功{RESET}' if fail_count == 0 else f'{RED}❌ {fail_count} 失败{RESET}':<24} {total_time:>7.1f}s")
    print()

    if fail_count == 0:
        log(f"{GREEN}{BOLD}🎉 全部完成！总耗时 {total_time:.1f}s{RESET}", GREEN)
    else:
        log(f"{YELLOW}{BOLD}⚠️  完成，但 {fail_count} 个步骤失败，请检查日志{RESET}", YELLOW)

    return fail_count == 0


# ── 单步命令 ──────────────────────────────────────────────────────────────────

def run_daily():
    from fetcher.daily_incr import main
    step("增量日线", main)

def run_events():
    from fetcher.events import run_daily
    step("异动事件", run_daily)

def run_codes():
    from fetcher.codes import get_stock_codes
    step("股票代码", get_stock_codes)

def run_finance():
    from fetcher.finance import main
    step("财报数据", main)

def run_info():
    from fetcher.info_market import main as market_main
    from fetcher.info_base import main as base_main
    ok("开始更新市场数据（腾讯行情 + 巨潮基础）...")
    step("腾讯行情", market_main)
    step("巨潮基础信息", base_main)

def run_merge():
    from utils.merge_daily import merge_all
    step("合并大表", merge_all)


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A股数据采集工具集",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python run.py                  # 盘后一键采集（推荐每日 16:30 运行）
  python run.py daily           # 仅更新日线
  python run.py events          # 仅采集异动事件
  python run.py info            # 仅更新市场信息
  python run.py merge           # 仅合并日线大表
        """
    )
    parser.add_argument("cmd", nargs="?", choices=["all","daily","events","codes","finance","info","merge"],
                        help="all=全套每日采集（默认）; 其他为单步执行")
    parser.add_argument("--start", help="历史回溯起始日期 YYYYMMDD（仅 events）")
    parser.add_argument("--end",   help="历史回溯结束日期 YYYYMMDD（仅 events）")
    parser.add_argument("--force", action="store_true", help="强制覆盖已有数据（仅 events）")
    args = parser.parse_args()

    if args.cmd is None or args.cmd == "all":
        run_all()
    elif args.cmd == "daily":
        run_daily()
    elif args.cmd == "events":
        if args.start and args.end:
            from fetcher.events import run_historical
            step("异动事件历史回溯", run_historical, args.start, args.end, force=args.force)
        else:
            run_events()
    elif args.cmd == "codes":
        run_codes()
    elif args.cmd == "finance":
        run_finance()
    elif args.cmd == "info":
        run_info()
    elif args.cmd == "merge":
        run_merge()