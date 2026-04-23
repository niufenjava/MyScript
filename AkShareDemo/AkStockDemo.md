# Python AkShare 导师

> 通过「可运行代码 + 测试用例」的方式教会你使用 akshare。
> 每个场景都有对应的 .py 脚本，直接 `python demo_0x_xxx.py` 即可运行。

---

## 场景 1：获取A股上市公司列表

### 脚本 (copy and run)
```python
# demo_01_stock_list.py
import akshare as ak
import pandas as pd

# 1. 获取A股所有上市公司代码和名称
df = ak.stock_info_a_code_name()
print(f"A股上市公司总数: {len(df)}")
print(f"列名: {list(df.columns)}")
print(df.head(10))

# 2. 筛选沪市主板（代码以60开头）
sh_main = df[df["code"].str.startswith("60")]
print(f"\n沪市主板数量: {len(sh_main)}")
print(sh_main.head(5))

# 3. 筛选创业板（代码以30开头）
cyb = df[df["code"].str.startswith("30")]
print(f"\n创业板数量: {len(cyb)}")
print(cyb.head(5))

# 4. 模糊搜索股票名称
keyword = "银行"
result = df[df["name"].str.contains(keyword)]
print(f"\n名称含'{keyword}'的股票: {len(result)} 只")
print(result.head(5))
```

### 预期输出
```
A股上市公司总数: 5300+
列名: ['code', 'name']
   code   name
0  000001  平安银行
1  000002  万科A
...

沪市主板数量: 1700+
   code   name
0  600000  浦发银行
...

创业板数量: 1300+
   code   name
0  300001  特锐德
...

名称含'银行'的股票: 42 只
   code   name
0  000001  平安银行
...
```

### 测试 Case
- Case 1: 传入不存在的关键词如 `df[df["name"].str.contains("不存在的公司XYZ")]`, 应返回 0 行
- Case 2: 验证 code 列全为6位字符串 `df["code"].str.len().unique()`, 应全为 6
- Case 3: 筛选北交所(8开头/4开头) `df[df["code"].str.startswith(("8","4"))]`, 检查数量合理性

### 注释说明（简短，3-5 句）
1. `stock_info_a_code_name()` 返回所有A股上市公司的代码和名称，是最基础的元数据接口。
2. 返回的是 DataFrame，列名为 `code`(股票代码) 和 `name`(股票名称)。
3. 代码为6位字符串，通过前缀可区分板块：60=沪主板，00=深主板，30=创业板，68=科创板，8/4=北交所。
4. 该接口无需参数，直接调用即可，适合作为后续查询的基础数据源。

---

## 场景 2：获取个股历史K线行情

### 脚本 (copy and run)
```python
# demo_02_hist_kline.py
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

# 1. 获取贵州茅台(600519)日K线数据（最近60天）
end_date = datetime.now().strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")

df = ak.stock_zh_a_hist(
    symbol="600519",       # 股票代码，不含交易所后缀
    period="daily",        # 周期: daily/weekly/monthly
    start_date=start_date, # 开始日期 YYYYMMDD
    end_date=end_date,     # 结束日期 YYYYMMDD
    adjust="qfq",          # 复权: qfq=前复权, hfq=后复权, ""=不复权
)
print(f"贵州茅台 日K线数据: {len(df)} 条")
print(f"列名: {list(df.columns)}")
print(df.head(5))
print(df.tail(5))

# 2. 获取周K线
df_week = ak.stock_zh_a_hist(
    symbol="000001",
    period="weekly",
    start_date="20250101",
    end_date=end_date,
    adjust="qfq",
)
print(f"\n平安银行 周K线: {len(df_week)} 条")
print(df_week.head(5))

# 3. 计算简单技术指标：5日均线
df["MA5"] = df["收盘"].rolling(window=5).mean()
print(f"\n最近5日收盘价及5日均线:")
print(df[["日期", "收盘", "MA5"]].tail(10))
```

### 预期输出
```
贵州茅台 日K线数据: 42 条
列名: ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
         日期     开盘     收盘  ...
0  2025-02-2x  1500.00  1510.50  ...
...

平安银行 周K线: 16 条
         日期    开盘    收盘  ...
0  2025-01-0x  12.50  12.80  ...
...

最近5日收盘价及5日均线:
           日期      收盘      MA5
37  2025-04-1x  1520.30  1515.26
...
```

### 测试 Case
- Case 1: 传入未来日期 `start_date="20990101"`, 应返回空 DataFrame
- Case 2: 不复权 vs 前复权对比: `adjust=""` 与 `adjust="qfq"` 的收盘价应不同(除权日附近)
- Case 3: 周K数据行数应远少于日K(约1/5)

### 注释说明（简短，3-5 句）
1. `stock_zh_a_hist` 是获取A股历史行情的核心接口，支持日/周/月K线。
2. `adjust` 参数非常重要：前复权(qfq)适合看历史走势，后复权(hfq)适合算真实收益，不复权看原始价格。
3. 返回列包括OHLCV(开高低收量)和涨跌幅、换手率等，数据来源为东方财富。
4. `symbol` 只需6位代码，不需要加交易所后缀（如 .SH / .SZ）。

---

## 场景 3：获取A股实时行情

### 脚本 (copy and run)
```python
# demo_03_realtime_quote.py
import akshare as ak
import pandas as pd

# 1. 获取全部A股实时行情（东方财富数据源）
df = ak.stock_zh_a_spot_em()
print(f"A股实时行情: {len(df)} 只")
print(f"列名: {list(df.columns)}")
print(df.head(3))

# 2. 查找涨幅前10
top10 = df.nlargest(10, "涨跌幅")
print(f"\n涨幅前10:")
print(top10[["序号", "代码", "名称", "最新价", "涨跌幅", "成交额"]])

# 3. 查找跌幅前10
bottom10 = df.nsmallest(10, "涨跌幅")
print(f"\n跌幅前10:")
print(bottom10[["序号", "代码", "名称", "最新价", "涨跌幅", "成交额"]])

# 4. 筛选特定股票的实时数据
targets = ["600519", "000858", "601318"]
selected = df[df["代码"].isin(targets)]
print(f"\n指定股票实时行情:")
print(selected[["代码", "名称", "最新价", "涨跌幅", "成交量", "成交额", "换手率"]])
```

### 预期输出
```
A股实时行情: 5300+ 只
列名: ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', ...]

涨幅前10:
   序号    代码    名称   最新价  涨跌幅    成交额
...

跌幅前10:
   序号    代码    名称   最新价  涨跌幅    成交额
...

指定股票实时行情:
      代码    名称    最新价  涨跌幅  成交量  成交额  换手率
0  600519  贵州茅台  1520.30  1.25  ...
1  000858   五粮液   145.60  0.85  ...
2  601318  中国平安   52.30  -0.38  ...
```

### 测试 Case
- Case 1: 收盘后运行，涨跌幅应不再变化（非交易时段数据静止）
- Case 2: 涨停股筛选: `df[df["涨跌幅"] >= 9.9]`(主板) 或 `>= 19.9`(创业板/科创板)
- Case 3: 成交额为0的股票: `df[df["成交额"] == 0]`（停牌股），数量应较少

### 注释说明（简短，3-5 句）
1. `stock_zh_a_spot_em()` 一次性拉取全市场实时行情，数据来自东方财富。
2. 返回字段丰富，包括最新价、涨跌幅、成交量/额、换手率、市盈率等。
3. 交易时间内调用获取的是实时数据，非交易时间获取的是最近收盘数据。
4. 数据量较大(5300+行)，如只需个别股票建议先获取再用 `isin()` 筛选，避免频繁请求。

---

## 场景 4：获取个股财务摘要

### 脚本 (copy and run)
```python
# demo_04_financial_abstract.py
import akshare as ak
import pandas as pd

# 1. 获取贵州茅台(600519)按报告期排列的财务摘要
df = ak.stock_financial_abstract_ths(symbol="600519", indicator="按报告期")
print(f"贵州茅台 财务摘要: {len(df)} 条")
print(f"列名: {list(df.columns)}")
print(df.head(5))

# 2. 获取按年度排列的财务摘要
df_year = ak.stock_financial_abstract_ths(symbol="600519", indicator="按年度")
print(f"\n按年度: {len(df_year)} 条")
print(df_year.head(5))

# 3. 提取关键财务指标
if "报告期" in df.columns and "净利润" in df.columns:
    # 筛选最近4个季度
    recent = df.head(4)
    print(f"\n最近4个季度关键数据:")
    cols_to_show = [c for c in ["报告期", "营业总收入", "净利润", "净资产收益率", "每股收益"] if c in df.columns]
    print(recent[cols_to_show])

# 4. 对比两家公司最新财务数据
codes = ["600519", "000858"]
for code in codes:
    data = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
    if data is not None and not data.empty:
        print(f"\n{code} 最新报告期: {data.iloc[0].get('报告期', 'N/A')}")
        key_cols = [c for c in ["营业总收入", "净利润", "净资产收益率"] if c in data.columns]
        if key_cols:
            print(f"  {data[key_cols].head(1).to_string(index=False)}")
```

### 预期输出
```
贵州茅台 财务摘要: 30+ 条
列名: ['报告期', '营业总收入', '营业总收入同比增长率', '净利润', '净利润同比增长率', ...]

按年度: 10+ 条
         报告期  营业总收入  净利润  ...
0  2024-12-31  1741.44亿  862.28亿  ...

最近4个季度关键数据:
         报告期   营业总收入    净利润  净资产收益率   每股收益
0  2025-03-31  508.13亿  268.43亿  ...
...
```

### 测试 Case
- Case 1: 新股(上市不足1年)查询财务数据，部分季度可能返回空或无数据
- Case 2: `indicator` 传非法值如 "按月度"，应抛出异常或返回空
- Case 3: 注意金额单位：部分字段带"亿"/"万"后缀，使用前需要解析转换（参考本项目 FinStock.py 的 parse_amount）

### 注释说明（简短，3-5 句）
1. `stock_financial_abstract_ths` 来自同花顺数据源，提供核心财务指标摘要。
2. `indicator` 支持 "按报告期"（每季度一条）和 "按年度"（每年一条）两种排列方式。
3. 返回的金额字段可能带中文单位（如"387.70亿"），使用前需解析转换（本项目 FinStock.py 有 parse_amount 函数）。
4. 数据为累计值（如中报是1-6月累计），需做"累计转单季度"计算才能得到Q2单季数据。

---

## 场景 5：获取行业板块行情

### 脚本 (copy and run)
```python
# demo_05_industry_board.py
import akshare as ak
import pandas as pd

# 1. 获取东方财富行业板块列表及行情
df = ak.stock_board_industry_name_em()
print(f"行业板块数量: {len(df)}")
print(f"列名: {list(df.columns)}")
print(df.head(10))

# 2. 查找涨幅前5的行业
top5 = df.nlargest(5, "涨跌幅")
print(f"\n今日涨幅前5行业:")
print(top5[["板块名称", "涨跌幅", "总市值", "换手率"]])

# 3. 获取某个行业板块内的成分股
# 以"银行"行业为例
bank_members = ak.stock_board_industry_cons_em(symbol="银行")
print(f"\n银行板块成分股: {len(bank_members)} 只")
print(f"列名: {list(bank_members.columns)}")
print(bank_members.head(5))

# 4. 获取概念板块列表
df_concept = ak.stock_board_concept_name_em()
print(f"\n概念板块数量: {len(df_concept)}")
print(df_concept.head(5))

# 5. 获取某概念板块成分股
ai_members = ak.stock_board_concept_cons_em(symbol="AI手机")
print(f"\nAI手机概念成分股: {len(ai_members)} 只")
print(ai_members.head(5))
```

### 预期输出
```
行业板块数量: 100+
列名: ['板块名称', '涨跌幅', '总市值', '换手率', '上涨家数', '下跌家数', ...]

今日涨幅前5行业:
  板块名称  涨跌幅     总市值   换手率
...

银行板块成分股: 42 只
列名: ['代码', '名称', '涨跌幅', '最新价', ...]

概念板块数量: 400+
  板块名称  涨跌幅  ...

AI手机概念成分股: 30 只
  代码    名称   涨跌幅  ...
```

### 测试 Case
- Case 1: 板块名称不存在: `ak.stock_board_industry_cons_em(symbol="不存在的行业")`, 应报错或返回空
- Case 2: 行业板块 + 成分股联动: 先查涨幅最高行业，再查其成分股涨幅前3
- Case 3: 概念板块名称可能随时间变化（新概念增加、旧概念淘汰），不要硬编码

### 注释说明（简短，3-5 句）
1. `stock_board_industry_name_em()` 获取行业分类板块，`stock_board_concept_name_em()` 获取概念板块，两者结构类似。
2. 每个板块可进一步查询成分股：`stock_board_industry_cons_em(symbol="板块名称")`。
3. 东方财富的行业分类不同于证监会的行业分类，注意区分使用场景。
4. 概念板块数量远多于行业板块(400+ vs 100+)，且会动态增减，不要硬编码板块名。

---

## 场景 6：获取个股资金流向

### 脚本 (copy and run)
```python
# demo_06_fund_flow.py
import akshare as ak
import pandas as pd

# 1. 获取个股资金流向（东方财富数据源）
df = ak.stock_individual_fund_flow(stock="600519", market="sh")
print(f"贵州茅台 资金流向: {len(df)} 条")
print(f"列名: {list(df.columns)}")
print(df.head(5))
print(df.tail(5))

# 2. 计算最近N日主力净流入合计
if "主力净流入-净额" in df.columns:
    recent_5 = df.head(5)
    total_main_flow = pd.to_numeric(recent_5["主力净流入-净额"], errors="coerce").sum()
    print(f"\n最近5日主力净流入合计: {total_main_flow/1e8:.2f} 亿")

# 3. 大盘资金流向概览
df_market = ak.stock_market_fund_flow()
print(f"\n大盘资金流向: {len(df_market)} 条")
print(f"列名: {list(df_market.columns)}")
print(df_market.head(5))

# 4. 行业板块资金流向
df_sector = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
print(f"\n行业资金流排名: {len(df_sector)} 条")
print(df_sector.head(10))
```

### 预期输出
```
贵州茅台 资金流向: 100+ 条
列名: ['日期', '收盘价', '涨跌幅', '主力净流入-净额', '主力净流入-净占比', ...]

最近5日主力净流入合计: X.XX 亿

大盘资金流向: 100+ 条
列名: ['日期', '上证-收盘价', '深证-收盘价', ...]

行业资金流排名: 100+ 条
  名称  今日涨跌幅  主力净流入-净额  ...
...
```

### 测试 Case
- Case 1: market 参数错误: `market="abc"` 应报错，合法值为 "sh"(沪) / "sz"(深)
- Case 2: 验证主力净流入 = 超大单净流入 + 大单净流入（数值应一致）
- Case 3: 大盘资金流向的日期列应为降序（最近日期在前）

### 注释说明（简短，3-5 句）
1. `stock_individual_fund_flow` 的 `market` 参数必须指定：沪市 "sh"，深市 "sz"，与股票代码对应。
2. 返回的主力净流入 = 超大单 + 大单的净流入，散户资金 = 中单 + 小单净流入。
3. 资金流向数据反映的是大单交易方向，可作为辅助参考，但不应作为唯一决策依据。
4. 行业板块资金流向排名可快速定位当日资金关注的热点板块。

---

## 快速参考卡

| 场景 | 核心函数 | 用途 |
|------|---------|------|
| 1 | `stock_info_a_code_name()` | 获取A股股票列表 |
| 2 | `stock_zh_a_hist()` | 获取历史K线行情 |
| 3 | `stock_zh_a_spot_em()` | 获取实时行情 |
| 4 | `stock_financial_abstract_ths()` | 获取财务摘要 |
| 5 | `stock_board_industry_name_em()` | 获取行业板块 |
| 5 | `stock_board_concept_name_em()` | 获取概念板块 |
| 6 | `stock_individual_fund_flow()` | 获取个股资金流向 |

## 重要提示

1. **请求频率**：akshare 拉取的是公开网站数据，请控制请求频率（建议每次间隔1-2秒），避免被封IP。
2. **数据更新时间**：不同数据源更新频率不同，实时行情在交易时段更新，财务数据按季度披露。
3. **版本兼容**：akshare 更新频繁，API 可能变化，建议固定版本: `pip install akshare==1.x.x`
4. **安装**：`pip install akshare pandas`
