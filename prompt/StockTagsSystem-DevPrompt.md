# 股票打标签系统 - 开发提示词

> 你是一个全栈 Python 开发者，请帮我从零构建一套**股票标签系统**。

## 技术栈

- 后端：Python 3.11 + **FastAPI**（不要用 Django，太重）
- 前端：**Vue3** + Vite + TailwindCSS（简单清晰即可）
- 数据存储：**CSV 文件**（两个文件，不上数据库）
- 项目根路径：`/Users/niufen/claw/MyScript/stock-tagger`

## 数据模型

**文件 1：`/Users/niufen/claw/MyScript/stock-tagger/backend/data/tags_library.csv`**
列：`tag_name,color`（颜色使用 8位 hex 如 #FF6B6B）

**文件 2：`/Users/niufen/claw/MyScript/stock-tagger/backend/data/stock_tags.csv`**
列：`stock_code,stock_name,tag_name,created_at`

**股票基本信息（只读，供展示）：**
路径：`/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/data/stockBaseInfo.csv`
关键列：A股代码, A股简称, 所属市场, 所属行业, 上市日期, 主营业务

**股票市场数据（只读，供展示）：**
路径：`/Users/niufen/claw/MyScript/akshare-stock-data-fetcher/data/stockMarketData.csv`
关键列：A股代码, 最新价, 总市值(亿), 市盈率TTM, ROE, 股息率(%)

> 股票基础信息和市场数据仅供展示，不写入。股票表格按 stock_tags.csv 的 tag 筛选后，再通过 A股代码 关联以上两个 CSV 补充展示字段。

## 默认配色方案（10个，直接做按钮选择，不需要输入颜色代码）

| 颜色名称 | 色值 |
|----------|------|
| 珊瑚红 | #FF6B6B |
| 橙黄 | #FFA94D |
| 柠檬黄 | #FFE066 |
| 薄荷绿 | #69DB7C |
| 天蓝 | #4ECDC4 |
| 海洋蓝 | #45B7D1 |
| 薰衣草紫 | #B197FC |
| 玫红 | #F06595 |
| 墨灰 | #868E96 |
| 深墨 | #1A1A2E |

前端标签库页添加标签时，展示上述 10 个颜色按钮，点击即选，不提供颜色输入框。

## 后端要求（FastAPI）

1. CSV 文件不存在时自动创建（含表头），路径：`/Users/niufen/claw/MyScript/stock-tagger/backend/data/`
2. CSV 写入用 `fcntl.flock` 文件锁防并发
3. 核心 API：
   - `GET /api/tags/library` — 返回所有标签
   - `POST /api/tags/library` — 添加标签 `{tag_name, color}`
   - `PUT /api/tags/library/{name}` — 更新标签颜色
   - `DELETE /api/tags/library/{name}` — 删除标签
   - `GET /api/stocks` — 返回股票列表，支持 `?tag=龙头` 筛选
   - `POST /api/stocks/tags` — 打标签 `{stock_code, stock_name, tag_name}`
   - `DELETE /api/stocks/{code}/tags/{tag}` — 移除标签
   - `POST /api/tags/batch` — AI 批量打标，**自动创建不存在的标签**，请求体 `{stock_code, stock_name, tags: []}`
4. CORS 允许前端 localhost 访问

## 前端要求（Vue3）

1. **首页/选股视图**：
   - 左侧标签列表（每个标签带颜色圆点 + 股票数量）
   - 点击标签筛选股票，全选显示所有股票
   - 右侧股票表格（股票代码、名称、标签 chips）
   - 每只股票的标签 chips 可点击删除
   - 顶部搜索框支持模糊搜索股票
   - "添加股票"按钮弹出对话框，输入股票代码+名称，然后多选已有标签打标

2. **标签库管理页**：
   - 标签列表（颜色预览 + 名称 + 操作按钮）
   - 添加标签表单：输入名称 + 10个颜色按钮（上述配色）
   - 编辑标签颜色：同样用这 10 个颜色按钮选择
   - 删除标签

3. **股票表格（选股视图 & 添加股票弹窗）：**
   - 展示列：股票代码 | 股票名称 | 所属市场 | 最新价 | 总市值(亿) | 市盈率TTM | ROE | 股息率 | 标签 chips
   - 添加股票弹窗中，输入股票代码后自动从 stockBaseInfo.csv + stockMarketData.csv 带出股票名称和市场等基础信息

4. **AI 接入页**（给外部 AI 调用的说明页）：
   - 显示 API 地址、请求格式、示例 curl 命令
   - 显示 API Key（从 `/Users/niufen/claw/MyScript/stock-tagger/.env` 读取）

## 项目结构

```
/Users/niufen/claw/MyScript/stock-tagger/
├── backend/
│   ├── main.py
│   ├── router/
│   │   ├── __init__.py
│   │   ├── tags.py
│   │   └── stocks.py
│   ├── service/
│   │   ├── __init__.py
│   │   ├── tag_service.py
│   │   └── stock_service.py
│   ├── data/
│   │   ├── tags_library.csv
│   │   └── stock_tags.csv
│   └── requirements.txt
├── frontend/
│   ├── (Vite + Vue3 项目)
│   └── package.json
└── .env（存放 API_KEY=sk-xxx 和 PORT=8000）
```

## 开发约定

1. 先完成 backend，跑通 API，再用 curl 测试每个接口
2. 前端联调时 backend 必须先跑起来
3. 不需要用户登录，简单工具
4. 启动命令：
   - backend：`cd /Users/niufen/claw/MyScript/stock-tagger/backend && uvicorn main:app --reload --port 8000`
   - frontend：`cd /Users/niufen/claw/MyScript/stock-tagger/frontend && npm run dev`

请开始开发，完成后告诉我如何启动。
