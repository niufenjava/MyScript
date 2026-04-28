# A股数据采集工具集

基于 [akshare](https://github.com/akfamily/akshare) + 腾讯行情接口的 A 股数据采集、筛选、查询工具集。

## 项目结构

```
akshare-stock-data-fetcher/
├── fetch_codes.py              # 同步沪深股票代码 → data/stock_codes.txt
├── fetch_info.py               # 股票基本信息 + 市场数据 → data/stockInfo.csv
├── fetch_finance.py            # 季度财报数据 → data/FinanceReport.csv
├── fetch_events.py             # 异动事件采集（涨停/跌停/炸板/强势股/龙虎榜）
├── fetch_daily_full.py         # 全量日线拉取 → data/daily/*.parquet
├── fetch_daily_incr.py         # 每日增量日线（腾讯批量接口，~40s）
├── screen_daily.py              # 日线指标筛选（CLI，6种策略）
├── screen_strategy.py           # 策略选股（缩量+均线交叉）
├── bak/                         # 历史备份/回填脚本
├── utils/                       # 工具/辅助脚本
│   ├── merge_daily.py          # 合并日线大表 → data/all_daily.parquet
│   ├── query_daily.py          # 单股日线查询
│   └── test_daily.py           # 日线接口测试
├── data/
│   ├── daily/                  # 单股日线 Parquet 文件（5200+股票）
│   ├── all_daily.parquet      # 合并日线大表
│   ├── FinanceReport.csv       # 季度财报（单季度值）
│   ├── FinanceReport_raw.csv   # 季度财报（原始累计值）
│   ├── stockInfo.csv           # 股票信息表（19列）
│   ├── stock_codes.txt         # 沪深股票代码列表（sh/sz前缀）
│   └── events/                 # 异动事件数据
│       ├── stock_events_涨停.csv          约1043条
│       ├── stock_events_跌停.csv           约149条
│       ├── stock_events_炸板.csv           约302条
│       ├── stock_events_强势股.csv        约2755条
│       ├── stock_events_强势股(回调).csv   约620条
│       └── stock_events_龙虎榜.csv        约3472条
├── requirements.txt
├── LICENSE
└── README.md
```

## 安装依赖

```bash
pip install -r requirements.txt
```

**注意**：必须使用项目自带的 venv（不是系统 Python）：

```bash
source /Users/niufen/claw/MyScript/.venv/bin/activate
```

## 脚本说明

### 数据采集类（fetch_*）

| 脚本 | 功能 | 数据源 | 输出 |
|------|------|--------|------|
| `fetch_codes.py` | 同步沪深股票代码列表 | 腾讯行情 (akshare) | `data/stock_codes.txt` |
| `fetch_info.py` | 股票基本信息 + 实时市场数据 | 巨潮资讯 + 腾讯行情 | `stockInfo.csv` |
| `fetch_finance.py` | 季度财报数据（2020Q1起） | 东方财富 (akshare) | `data/FinanceReport.csv` |
| `fetch_daily_full.py` | 全量历史日线（逐只拉取） | 腾讯K线接口 | `data/daily/*.parquet` |
| `fetch_daily_incr.py` | 每日增量日线（批量拉取） | 腾讯批量行情 | `data/daily/*.parquet` + `data/all_daily.parquet` |
| `fetch_events.py` | 异动事件采集（5类事件 + 公告归因，并发拉取） | 东方财富 (akshare) | `data/events/*.csv` |

> `fetch_events.py` 使用 20 线程并发归因，单日采集 ~45s 完成（含 200+ 强势股公告查询）。

### 查询与筛选类

| 脚本 | 功能 | 用法示例 |
|------|------|----------|
| `utils/query_daily.py` | 查询单只股票近N日日线 | `python -m utils.query_daily 603659 20` |
| `screen_daily.py` | 按指标快速筛选（6种策略） | `python screen_daily.py 缩量 3` |
| `screen_strategy.py` | 策略选股（缩量+均线交叉，输出报告） | `python screen_strategy.py` |

### 工具类（utils/ 目录）

| 脚本 | 功能 |
|------|------|
| `utils/merge_daily.py` | 合并所有单股 Parquet 为 `all_daily.parquet` 大表 |
| `utils/query_daily.py` | 查询单只股票近N日日线 |
| `utils/test_daily.py` | 日线接口测试（开发用） |

## 典型使用流程

```bash
# 1. 同步股票代码列表
python fetch_codes.py

# 2. 拉取股票基本信息和市场数据
python fetch_info.py

# 3. 拉取季度财报数据
python fetch_finance.py

# 4. 首次全量拉取历史日线（耗时较长，约8分钟）
python fetch_daily_full.py

# 5. 之后每天运行增量更新（~40秒）
python fetch_daily_incr.py

# 6. 每日盘后采集异动事件（~1-2分钟）
python fetch_events.py

# 7. 查询/筛选/工具
python -m utils.query_daily 600519 20     # 查询茅台近20日
python screen_daily.py 缩量 3             # 筛选连续3天缩量
python screen_daily.py 连涨 5             # 筛选连续5天上涨
python screen_daily.py 新高 20            # 筛选近20日创新高
python -m utils.merge_daily               # 手动合并日线大表
```

## fetch_events.py 详细说明

### 功能

每日盘后自动采集5类异动事件，关联公告归因，分文件存储。

### 异动类型

| 类型 | 说明 | 数据源 |
|------|------|--------|
| 涨停 | 当日涨停股票 | `stock_zt_pool_em` |
| 跌停 | 当日跌停股票（30天内首次） | `stock_zt_pool_dtgc_em` |
| 炸板 | 曾涨停后开板的股票 | `stock_zt_pool_zbgc_em` |
| 强势股 | 近期趋势强势股票 | `stock_zt_pool_strong_em` |
| 强势股(回调) | 强势股中当日下跌的（标注回调风险） | 同上，配合本地日线判断 |
| 龙虎榜 | 上榜股票 | `stock_lhb_detail_em` |

### 用法

```bash
# 每日增量（今日数据，默认）
python fetch_events.py

# 历史回溯（指定日期区间）
python fetch_events.py --start 20260301 --end 20260331

# 强制刷新（覆盖已有数据，重新拉取）
python fetch_events.py --force
```

### 公告归因

系统自动拉取异动日前5天内的个股公告，匹配关键词映射为结构化原因：

| 关键词 | 归因结果 |
|--------|---------|
| 业绩预告/业绩增长/净利润增长 | 业绩超预期 |
| 项目投资/对外投资 | 投资新项目 |
| 股权激励/授予期权 | 股权激励 |
| 分红/权益分派 | 分红派息 |
| AI/人工智能 | AI概念 |
| 机器人 | 机器人概念 |
| 固态电池 | 固态电池 |
| 芯片/半导体 | 半导体/芯片 |
| ... | ... |

若公告无匹配，则使用**所属板块**作为原因（如"通信设备板块带动"）。

### 字段说明

| 字段 | 说明 |
|------|------|
| 股票代码 | 6位数字代码 |
| 股票名称 | 股票简称 |
| 日期 | 异动日期（YYYY-MM-DD） |
| 异动类型 | 涨停/跌停/炸板/强势股/强势股(回调)/龙虎榜 |
| 异动原因 | 公告归因或板块带动原因 |
| 关联公告 | 归因依据的公告标题 |
| 公告链接 | 公告原始链接 |
| 数据来源 | 东方财富 |

### 存储结构

每个异动类型独立存储为 CSV 文件，**去重键**为 `(股票代码, 日期, 异动类型)`。

同一股票同日同一类型不会重复写入，适合每日增量追加。

## 数据格式

### stockInfo.csv（股票信息表）

| 字段 | 说明 |
|------|------|
| A股代码 | 带 sh/sz 前缀（如 sh600519） |
| A股简称 | 股票名称 |
| H股代码 / H股简称 | H股信息（如有） |
| 最新价 / 总市值(亿) | 实时市场数据 |
| 每股收益 / 每股净资产 / 市净率 | 估值指标 |
| 股息率(%) / 动态市盈率 / 静态市盈率 / 市盈率TTM | 估值指标 |
| 成立日期 / 上市日期 | 公司基本信息 |
| 所属市场 / 所属行业 / 入选指数 | 分类信息 |
| 主营业务 | 业务描述 |

### FinanceReport.csv（季度财报，单季度值）

| 字段 | 说明 |
|------|------|
| 股票代码 / 股票名称 / 季度 | 基础标识 |
| 营业总收入 / 净利润 | 利润表（单季度，单位：元） |
| 营业总收入同比增长率 / 净利润同比增长率 | 增长指标（%） |
| 净资产收益率 / 销售毛利率 / 销售净利率 | 盈利能力 |
| 基本每股收益 / 每股净资产 / 每股经营现金流 | 每股指标 |
| 资产负债率 | 偿债能力 |

> 原始累计值保存在 `FinanceReport_raw.csv`，脚本自动将累计值转换为单季度值。

### 日线数据（Parquet）

| 字段 | 说明 |
|------|------|
| 日期 | 交易日期 |
| 开盘 / 收盘 / 最高 / 最低 | OHLC 价格 |
| 成交量 / 成交额 | 量价数据 |
| 换手率 / 涨跌幅(%) | 交易指标 |

## screen_daily.py 筛选策略

```
python screen_daily.py <策略> <参数>
```

| 策略 | 说明 | 示例 |
|------|------|------|
| 缩量 N | 近N天连续缩量 | `python screen_daily.py 缩量 3` |
| 放量 N | 近N天连续放量 | `python screen_daily.py 放量 3` |
| 连涨 N | 近N天连续上涨 | `python screen_daily.py 连涨 5` |
| 连跌 N | 近N天连续下跌 | `python screen_daily.py 连跌 3` |
| 新高 N | 近N日创新高 | `python screen_daily.py 新高 20` |
| 地量 N | 近N日成交量最低 | `python screen_daily.py 地量 20` |

## 定时任务

建议每日 16:30（A股收盘后）自动运行：

```bash
# 增量更新日线
python fetch_daily_incr.py

# 采集当日异动事件
python fetch_events.py
```

可配合系统 cron 或 OpenClaw 定时任务功能实现自动化。
