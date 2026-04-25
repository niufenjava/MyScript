
## 🎯 项目目标

基于 AKShare 构建**个人本地 A 股日线数据拉取系统**，满足以下核心要求：

- **数据格式**：Parquet（禁止使用 MongoDB）
- **并发策略**：多进程并行拉取 + 超时自动重试
- **防封机制**：代理轮转 + 直连兜底
- **自动更新**：定时任务 + 交易日自动跳过
- **项目路径** /Users/niufen/claw/MyScript/AStockData/

---

## 📚 参考项目

[VeKiner/akshare-stock-data-fetcher](https://github.com/VeKiner/akshare-stock-data-fetcher)

参考其代理轮转、防封、进程超时管理思路，但**存储层从 MongoDB 替换为 Parquet**。

---

## 🔧 技术栈

- `akshare` — A 股数据接口
- `pandas` — 数据处理
- `pyarrow` / `fastparquet` — Parquet 读写
- `pebble` — 支持超时的进程池（多进程并发）
- `schedule` — 定时任务调度
- `requests` — 代理 HTTP 请求
- `asyncio` / `aiohttp` — 可选：异步增强并发

---

## 📋 功能需求清单

### 1. 股票代码同步

- 每日自动从 AKShare 获取全量 A 股代码列表
- 持久化到 `stock_codes.parquet`（字段：`code`, `name`, `list_date`）
- 支持增量判断：新股票加入、停牌股票标记

### 2. 日线历史数据拉取

- 批量抓取全市场 A 股日线 K 线（`akshare.stock_zh_a_hist_em`）
- 字段至少包含：`date`, `open`, `high`, `low`, `close`, `volume`, `turnover`
- 存储为分区 Parquet：`data/daily/{code}/{year}.parquet`
- 多进程并行，默认 32 workers（可配置）
- 单只股票超时 30s 自动跳过，失败重试 6 次

### 3. 实时行情快照（可选扩展）

- 定时抓取全量实时行情（`stock_zh_a_spot_em`）
- 存储为：`data/spot/{date}.parquet`
- 代理轮转策略同上

### 4. 代理轮转防封

- 从代理 API 动态获取代理 IP 池（格式：`ip:port:user:password`）
- 每请求轮换代理，失败自动切换下一个
- 支持直连兜底（`include_direct=True`）
- 内置 `urllib3.Retry` 自动应对 429/5xx 退避重试
- 使用 `unittest.mock.patch` 无侵入替换 AKShare 内部 `requests.get`

### 5. 交易日判断

- 自动跳过非交易日（节假日、周末）
- 基于当日 K 线是否可获取判断，或维护 `trading_calendar.parquet`

### 6. 定时调度

- 日线拉取：每日 16:00 执行（收盘后）
- 代码同步：每日开盘前 9:00 执行
- 使用 `schedule` 库实现，支持多任务注册

---

## 🗂️ 目录结构约定

```
AStockData/
├── config.py              # 全局配置（代理地址、并发数、路径等）
├── requirements.txt
├── data/
│   ├── daily/             # 日线数据
│   │   └── {code}/         # 每只股票一个目录
│   │       └── {year}.parquet
│   ├── spot/              # 实时快照
│   │   └── {date}.parquet
│   ├── stock_codes.parquet
│   └── trading_calendar.parquet
├── scripts/
│   ├── sync_codes.py      # 股票代码同步
│   ├── fetch_daily.py      # 日线历史拉取
│   ├── fetch_spot.py       # 实时行情拉取
│   └── scheduler.py        # 定时调度入口
├── utils/
│   ├── proxy.py           # 代理轮转工具
│   ├── parquet_ops.py     # Parquet 读写封装
│   └── trading_day.py     # 交易日判断工具
└── logs/                   # 日志目录
```

---

## ⚙️ 配置规范（config.py）

```python
# 并发配置
NUM_WORKERS = 32           # 并行进程数
MAX_TIMEOUT = 30           # 单只股票超时（秒）
MAX_RETRIES = 6            # 失败重试次数

# 代理配置
PROXY_API_URL = ""         # 代理获取地址（返回 ip:port:user:password）
INCLUDE_DIRECT = True      # 直连兜底

# 路径配置
DATA_DIR = "data"
DAILY_DIR = f"{DATA_DIR}/daily"
SPOT_DIR = f"{DATA_DIR}/spot"

# 调度时间
SYNC_CODES_HOUR = 9       # 代码同步时间（时）
FETCH_DAILY_HOUR = 16     # 日线拉取时间（时）
```

---

## 🚀 执行要求

1. **先确认目录结构**，创建必要目录和空 parquet 脚手架
2. **优先实现核心链路**：代码同步 → 日线拉取 → Parquet 存储
3. **代理部分**作为可插拔模块，优先用直连模式跑通流程
4. **每次产出**：先给方案 → 再写代码 → 解释关键设计
5. **测试驱动**：关键函数写单元测试（如交易日判断、Parquet upsert）

---

## 🔒 防封最佳实践（实现时必须遵守）

1. 请求间隔：同 IP 同股票间隔 ≥ 0.5s
2. 代理轮换：每请求更换代理，不重复使用同一代理
3. 429 处理：遇到限流立即切换代理，等待 5s 再重试
4. 进程隔离：超时进程强制 kill，不阻塞整批任务
5. 增量拉取：只拉最新一天数据，已有的不重复拉取（upsert 逻辑）

---

## 📌 输出格式约定

每次回复请遵循：
- **方案**：给出技术选型和实现思路
- **代码**：提供完整可运行的 Python 文件
- **解释**：关键设计决策的原因
- **下一步**：后续可改进方向

---

## 🔄 后续可扩展方向（暂不实现，但预留接口）

- 异步 IO 增强并发（`aiohttp` 替代 `requests`）
- 因子计算模块（动量、波动率等）
- 数据库查询服务（DuckDB 替代 Parquet 查询）
- Web 可视化面板（Streamlit）

---

> **免责声明**：本工具仅供个人学习研究使用，请遵守 AKShare 及数据源的使用协议，切勿用于商业目的或高频违规请求。