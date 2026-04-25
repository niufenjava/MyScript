# akshare-stock-data-fetcher

> A 股日线历史数据批量采集工具，基于腾讯证券原始接口，直连无需代理，支持多进程并行抓取、增量更新、Parquet 本地存储，开箱即用。

---

## ✨ Features

- **日线历史数据**：批量抓取全市场 A 股日 K 线（前复权），多进程并行 + 超时自动重试
- **腾讯证券接口**：直连不封 IP，无需代理，稳定高效
- **Parquet 存储**：每只股票一个 `.parquet` 文件，按日期 upsert 去重，支持增量更新
- **交易日判断**：自动跳过非交易日，无需手动维护日历
- **股票代码同步**：一键更新沪深全量股票代码（自动过滤北交所）
- **命令行查询**：内置查询工具，快速查看任意股票近 N 日行情

---

## 📁 Project Structure

```
akshare-stock-data-fetcher/
├── stock_zh_a_hist_daily_em.py   # 批量抓取日线历史数据 → Parquet（多进程 + 重试）
├── get_stock_code_every_day.py   # 同步沪深全量股票代码到 stock_codes.txt
├── query_stock.py                # 命令行查询工具（查看近 N 日行情）
├── test_daily.py                 # 测试脚本（接口连通性 + Parquet 读写验证）
├── stock_codes.txt               # 沪深股票代码（运行后自动生成）
├── data/daily/                   # Parquet 数据存储目录
└── requirements.txt
```

---

## 🚀 Quick Start

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 同步股票代码

```bash
python get_stock_code_every_day.py
```

生成 `stock_codes.txt`，包含沪深全量股票代码（已过滤北交所）。

### 3. 抓取日线历史数据

```bash
python stock_zh_a_hist_daily_em.py
```

默认拉取 `stock_codes.txt` 中所有股票从 `2025-01-01` 至今的日线数据，8 进程并发，失败自动重试 6 次。

### 4. 查询股票数据

```bash
# 查看 603659 近 20 个交易日（默认）
python query_stock.py 603659

# 查看 000001 近 50 个交易日
python query_stock.py 000001 50
```

---

## ⚙️ Configuration

配置项位于 `stock_zh_a_hist_daily_em.py` 顶部：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `FULL_START_DATE` | `2025-01-01` | 全量拉取起始日期 |
| `NUM_WORKERS` | `8` | 并行进程数 |
| `MAX_TIMEOUT` | `30` | 单只股票最长处理时间（秒） |
| `MAX_RETRIES` | `6` | 失败重试轮数 |
| `REQUEST_INTERVAL` | `(0.3, 0.8)` | 每次请求随机间隔（秒） |

---

## 🔧 数据字段

每只股票的 Parquet 文件包含以下字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| 日期 | date | 交易日期 |
| 开盘 | float | 开盘价（前复权） |
| 收盘 | float | 收盘价（前复权） |
| 最高 | float | 最高价（前复权） |
| 最低 | float | 最低价（前复权） |
| 成交量 | float | 成交量 |
| 换手率 | float | 换手率 |
| 成交额 | float | 成交额 |
| 涨跌幅(%) | float | 涨跌幅百分比（自算） |

---

## 📦 Dependencies

- [akshare](https://github.com/akfamily/akshare) — 交易日历查询
- [requests](https://docs.python-requests.org/) — 腾讯接口 HTTP 请求
- [pandas](https://pandas.pydata.org/) — 数据处理
- [pyarrow](https://arrow.apache.org/docs/python/) — Parquet 读写
- [pebble](https://github.com/noxdafox/pebble) — 支持超时的进程池
- [schedule](https://github.com/dbader/schedule) — 定时任务调度

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.
