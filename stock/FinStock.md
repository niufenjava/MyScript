# FinStock.py 脚本提示词模板

## 任务描述
编写一个 Python 脚本，通过 akshare 库拉取 A 股上市公司的季度财务数据，进行数据清洗和转换后保存为 CSV 文件。支持增量更新、去重、无数据缓存等机制。

## 数据来源
- **库**: `akshare`
- **股票列表接口**: `ak.stock_info_a_code_name()`
- **财务数据接口**: `ak.stock_financial_abstract_ths(symbol=股票代码, indicator="按报告期")`

## 输入与输出

### 输入
- 无外部输入参数，脚本运行时自动判断

### 输出文件
| 文件 | 说明 |
|------|------|
| `AStockInfo.csv` | A股基础信息（股票代码、股票名称） |
| `FinanceReport.csv` | 季度财务数据（去重后追加写入） |
| `no_data_cache_YYYYMMDD.json` | 本月无数据缓存（避免重复请求未披露公司） |

### 输出字段（CSV列）
```
股票代码, 股票名称, 季度,
营业总收入, 营业总收入同比增长率,
净利润, 净利润同比增长率,
扣非净利润, 扣非净利润同比增长率,
净资产收益率, 销售净利率,
基本每股收益, 每股净资产,
每股资本公积金, 每股未分配利润, 每股经营现金流,
资产负债率, 流动比率, 速动比率, 产权比率
```

## 核心功能模块

### 1. 动态季度计算
根据当前日期动态判断应已公布的财报季度：
- **1季报(03-31)**: 4月1日起可拉取
- **2季报/中报(06-30)**: 8月1日后可拉取
- **3季报(09-30)**: 11月1日后可拉取
- **年报(12-31)**: 次年4月30日后可拉取

### 2. 金额单位转换
将中文金额格式转换为纯数字：
- `"387.70亿"` → `38770000000.0`
- `"3272.82万"` → `32728200.0`

### 3. 累计转单季度
同一年度内，将累计财务数据转为单季度数据：
- **Q1(03-31)**: 保持不变（本身就是单季度）
- **Q2(06-30)**: 中报 - 一季报
- **Q3(09-30)**: 三季报 - 中报
- **Q4(12-31)**: 年报 - 三季报
- 如果前一季度缺失，则保留累计值（不做减法）

### 4. 增量更新机制
- 启动时读取已有的 `FinanceReport.csv`
- 自动检测并清理重复数据（按"股票代码+季度"去重，保留最后一条）
- 只拉取缺失的季度数据，已存在的跳过

### 5. 无数据缓存
- 按天生成缓存文件 `no_data_cache_YYYYMMDD.json`
- 本月内已确认无数据的公司不再重复请求
- 跨月自动重置（新月份重新尝试）

### 6. 重试与防封机制
- 接口请求失败时自动重试（最多3次，指数退避）
- 每次请求后随机休眠 1.0~2.5 秒
- 失败后休眠 3~6 秒

## 技术实现要点

### 依赖包
```python
import pandas as pd
import akshare as ak
from tqdm import tqdm
```

### 关键代码模式

#### 金额解析（正则+单位映射）
```python
def parse_amount(val):
    match = re.match(r'^([+-]?\d+(?:\.\d+)?)\s*(亿|万)$', str(val))
    if match:
        num = float(match.group(1))
        unit = match.group(2)
        if unit == "亿": return num * 1e8
        elif unit == "万": return num * 1e4
    try:
        return float(val)
    except ValueError:
        return None
```

#### 累计转单季度（DataFrame 分组计算）
```python
def convert_cumulative_to_single_quarter(df):
    # 解析年份和季度后缀
    df["_year"] = df["季度"].str[:4].astype(int)
    df["_suffix"] = df["季度"].str[-5:]
    df["_q_order"] = df["_suffix"].map({"03-31": 1, "06-30": 2, "09-30": 3, "12-31": 4})
    df = df.sort_values(["股票代码", "_year", "_q_order"])
    
    # Q2=中报-Q1, Q3=三季报-中报, Q4=年报-三季报
    # 前一季度缺失则保留累计值
    # ...
    
    df = df.drop(columns=["_year", "_suffix", "_q_order"])
    return df
```

#### 重试机制
```python
def retry(func, max_retry=3, wait_base=3, *args, **kwargs):
    for i in range(max_retry):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i < max_retry - 1:
                sleep_time = wait_base * (i + 1) + random.uniform(0, 2)
                time.sleep(sleep_time)
    return None
```

#### 批量追加写入 CSV
```python
def batch_save(records, file_path):
    save_df = pd.DataFrame(records)
    header = not os.path.exists(file_path)
    save_df.to_csv(file_path, mode="a", header=header, index=False, encoding="utf-8-sig")
```

## 主流程逻辑
1. 确保输出目录存在
2. 读取/拉取股票基础信息
3. 读取已有财务数据，去重
4. 动态计算应公布的季度列表
5. 遍历每只股票：
   - 过滤 9 开头的代码
   - 检查无数据缓存，跳过已确认无数据的公司
   - 检查该股票是否所有季度都已存在，是则跳过
   - 拉取财务数据 → 金额转换 → 累计转单季度
   - 逐条去重后追加到结果列表
   - 每满 30 条批量写入 CSV
6. 保存无数据缓存
7. 输出统计信息

## 边界处理
- 数据中的 `"False"` 字符串需清洗为空值
- 布尔类型值需转为空字符串
- 股票代码统一按字符串类型处理（保留前导零）
- CSV 编码使用 `utf-8-sig`（兼容 Excel）
