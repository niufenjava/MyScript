# A股数据采集工具集

基于 [akshare](https://github.com/akfamily/akshare) + 腾讯行情接口的 A 股数据采集、筛选、查询工具集。

## 项目结构

```
akshare-stock-data-fetcher/
├── fetch_codes.py          # 同步沪深股票代码 → data/stock_codes.txt
├── fetch_info.py            # 股票基本信息 + 市场数据 → data/stockInfo.csv
├── fetch_finance.py         # 季度财报数据 → data/FinanceReport.csv
├── fetch_events.py          # 异动事件采集（涨停/跌停/炸板/龙虎榜）→ data/events/stock_events.csv
├── fetch_daily_full.py      # 全量日线拉取 → data/daily/*.parquet
├── fetch_daily_incr.py      # 每日增量日线（腾讯批量接口，~40s）
├── screen_daily.py          # 日线指标筛选（CLI，6种策略）
├── screen_strategy.py       # 策略选股（缩量+均线交叉）
├── utils/                   # 工具/辅助脚本
│   ├── merge_daily.py      # 合并日线大表 → data/all_daily.parquet
│   ├── query_daily.py      # 单股日线查询
│   └── test_daily.py       # 日线接口测试
├── data/
│   ├── daily/              # 单股日线 Parquet 文件
│   ├── all_daily.parquet   # 合并日线大表
│   ├── FinanceReport.csv   # 季度财报（单季度值）
│   ├── FinanceReport_raw.csv # 季度财报（原始累计值）
│   ├── stockInfo.csv       # 股票信息表（19列）
│   ├── stock_codes.txt     # 沪深股票代码列表（sh/sz前缀）
│   └── events/             # 异动事件数据
│       └── stock_events.csv # 异动事件记录（涨停/跌停/炸板/龙虎榜）
├── requirements.txt
└── LICENSE
```

## 安装依赖

```bash
pip install -r requirements.txt
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

### 查询与筛选类

| 脚本 | 功能 | 用法示例 |
|------|------|----------|
| `utils/query_daily.py` | 查询单只股票近N日日线 | `python -m utils.query_daily 603659 20` |
| `screen_daily.py` | 按指标快速筛选（6种策略） | `python screen_daily.py 缩量 3` |
| `screen_strategy.py` | 策略选股（缩量+均线交叉，输出报告） | `python screen_strategy.py` |

### 工具类（utils/ 目录）

| 脚本 | 功能 | 用法示例 |
|------|------|----------|
| `utils/merge_daily.py` | 合并所有单股 Parquet 为 `all_daily.parquet` 大表 | `python -m utils.merge_daily` |
| `utils/query_daily.py` | 查询单只股票近N日日线 | `python -m utils.query_daily 603659 20` |
| `utils/test_daily.py` | 日线接口测试（开发用） | `python -m utils.test_daily` |

## 典型使用流程

```bash
# 1. 同步股票代码列表
python fetch_codes.py

# 2. 拉取股票基本信息和市场数据
python fetch_info.py

# 3. 拉取季度财报数据
python fetch_finance.py

# 4. 首次全量拉取历史日线（耗时较长）
python fetch_daily_full.py

# 5. 之后每天运行增量更新（~40秒）
python fetch_daily_incr.py

# 6. 查询/筛选/工具
python -m utils.query_daily 600519 20     # 查询茅台近20日
python screen_daily.py 缩量 3             # 筛选连续3天缩量
python screen_daily.py 连涨 5             # 筛选连续5天上涨
python screen_daily.py 新高 20            # 筛选近20日创新高
python -m utils.merge_daily               # 手动合并日线大表
```

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
| 换手率 / 涨跌幅 | 交易指标 |

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
