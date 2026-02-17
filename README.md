# Polymarket 盘口数据采集与回放工具箱

对 [Polymarket](https://polymarket.com) 预测市场进行多粒度订单簿（Order Book）采集——从 5 秒低频到 ~150 次/秒高频——数据落库 SQLite，支持导出 `replay.json` 并在浏览器中回放动画。

---

## 目录

- [功能概览](#功能概览)
- [环境准备](#环境准备)
- [快速开始](#快速开始)
- [三种采集模式详解](#三种采集模式详解)
- [数据导出与回放](#数据导出与回放)
- [数据存储说明](#数据存储说明)
- [项目文件说明](#项目文件说明)
- [技术架构](#技术架构)
- [常见问题](#常见问题)

---

## 功能概览

- **盘口实时监控**：通过 REST 轮询 + WebSocket 混合方式，采集 Polymarket 预测市场的订单簿（Order Book）快照与成交事件
- **三种采集模式**：通用低频录制、多市场全量监听、BTC 5min 高频自动采集，满足不同场景
- **双向盘口**：同时记录 YES 和 NO 两个代币的完整买卖盘数据
- **本地存储**：SQLite + WAL 模式，轻量无需额外数据库服务
- **一键导出**：自动识别数据库 schema，统一导出 `replay.json`
- **浏览器回放**：纯前端播放器，拖入文件即可逐帧回看盘口变化

---

## 环境准备

### 系统要求

- Python 3.8+
- 稳定的网络连接（需访问 Polymarket API）

### 安装依赖

```bash
git clone https://github.com/Alchemist-X/1s-OB-Vol.git
cd 1s-OB-Vol
pip install -r requirements.txt
```

依赖包：
| 包名 | 用途 |
|------|------|
| `requests` | REST API 轮询订单簿 |
| `websocket-client` | WebSocket 监听实时成交 |
| `aiohttp` | 模式 3 高频异步 HTTP 请求 |

> 模式 1 和模式 2 仅需 `requests` + `websocket-client`；模式 3 额外需要 `aiohttp`。

---

## 快速开始

### 1. 选择一个 Polymarket 市场

在 [polymarket.com](https://polymarket.com) 上找到你感兴趣的市场，复制其 URL。例如：

```
https://polymarket.com/event/btc-updown-5m-1770965100
```

### 2. 启动监控并录制

```bash
# 最简单的用法：录制 5 分钟，每秒采样，录完自动导出回放文件
python3 main.py record https://polymarket.com/event/your-market-slug -d 300 -i 1 -e
```

### 3. 回放查看

用浏览器打开 `player.html`，将生成的 `replay.json` 拖入页面即可回放。

---

## 三种采集模式详解

### 模式 1：`main.py record` — 通用单市场录制

适合**任意 Polymarket 市场**（体育、政治、加密等），可自定义采样间隔和档位数。

```bash
python3 main.py record <url> [选项]
```

| 参数 | 缩写 | 说明 | 默认值 |
|------|------|------|--------|
| `url` | — | Polymarket 赛事链接（必填） | — |
| `--duration` | `-d` | 录制时长（秒） | 300 |
| `--interval` | `-i` | 采样间隔（秒） | 5.0 |
| `--top` | `-t` | 记录前 N 档挂单 | 5 |
| `--output` | `-o` | 数据输出目录 | `./data` |
| `--export` | `-e` | 录完自动导出 `replay.json` | 否 |

**使用示例：**

```bash
# 录制 10 分钟，每秒采样，自动导出
python3 main.py record https://polymarket.com/event/nba-gsw-phx-2026-02-05 \
  -d 600 -i 1 -e

# 录制 3 分钟，每 10 秒采样，仅记录前 3 档
python3 main.py record https://polymarket.com/event/lol-tsw-mvk-2026-02-06 \
  -d 180 -i 10 -t 3
```

**适用场景**：初次体验、低频跟踪、任意类型的市场

---

### 模式 2：`monitor.py` — 多市场全量盘口监听

同时监听**多个市场**的**所有价位级别**订单簿，固定 1 秒采样间隔。支持自动检测市场结算后停止录制。

```bash
python3 monitor.py <url1> [url2] ... [选项]
```

| 参数 | 缩写 | 说明 | 默认值 |
|------|------|------|--------|
| `urls` | — | 一个或多个 Polymarket 链接 | — |
| `--duration` | `-d` | 最大录制时长（秒），省略则等到结算 | 无限 |
| `--output` | `-o` | 数据输出目录 | `./data` |

**使用示例：**

```bash
# 监听单个 BTC 5min 市场 600 秒
python3 monitor.py https://polymarket.com/event/btc-updown-5m-1770965100 -d 600

# 同时监听三个市场，直到全部结算自动停止
python3 monitor.py \
  https://polymarket.com/event/btc-updown-5m-1770963600 \
  https://polymarket.com/event/btc-updown-5m-1770964200 \
  https://polymarket.com/event/btc-updown-5m-1770964500
```

**适用场景**：多市场对比分析、完整盘口深度研究、赛事全程录制

**特别说明**：
- 固定 1 秒间隔记录全部价位级别（非截断），数据量较大
- 自动检测结算：每 30 秒查询 Gamma API，所有市场结算后再录制 60 秒缓冲后自动退出
- 同时记录原始成交事件到 `pm_trades_raw` 表

---

### 模式 3：`auto_monitor.py` — BTC 5min 自动发现 + 高频采集

全自动高频模式：自动发现当前/下一个 BTC 5 分钟市场，使用异步并发以 ~150 req/s 的速率采集全量订单簿。

```bash
python3 auto_monitor.py [选项]
```

| 参数 | 缩写 | 说明 | 默认值 |
|------|------|------|--------|
| `--duration` | `-d` | 录制时长（秒） | 300 |
| `--concurrency` | `-c` | 并发连接数 | 50 |
| `--output` | `-o` | 数据输出目录 | `./data` |

**使用示例：**

```bash
# 默认参数：自动发现，50 并发，5 分钟
python3 auto_monitor.py

# 10 个并发，录制 2 分钟
python3 auto_monitor.py -c 10 -d 120
```

**适用场景**：BTC 5 分钟市场的高频微观结构研究、订单流分析

**特别说明**：
- 自动发现机制：利用 `btc-updown-5m-{unix_ts}` 的确定性 slug 模式计算当前窗口
- 使用 `asyncio` + `aiohttp` 异步并发，25 个 worker 采集 YES 盘、25 个采集 NO 盘
- 数据带毫秒时间戳和 API 延迟（`latency_ms`），适合微观分析
- 导出时自动降采样至 1 fps

---

### 三种模式对比

| | 模式 1: record | 模式 2: monitor | 模式 3: auto_monitor |
|---|---|---|---|
| 适用场景 | 任意市场、低频 | 多市场全量盘口 | BTC 5min 高频 |
| 采样频率 | 可配置（默认 5s） | 固定 1s | ~150/s |
| 订单簿范围 | Top-N 档（默认 5） | 全部价位 | 全部价位 |
| 并发模型 | 单线程轮询 | 多线程 ThreadPoolExecutor | asyncio 50 并发 |
| 成交数据 | WebSocket | WebSocket | WebSocket |
| 自动发现 | 否（需手动 URL） | 否（需手动 URL） | 是（自动发现） |
| 运行结束条件 | 固定时长 | 结算或超时 | 固定时长 |

---

## 数据导出与回放

### 导出 replay.json

每次录制的数据存储在 `data/{slug}_{timestamp}/` 目录下，可以通过以下命令导出为回放文件：

```bash
# 方式一：通过 main.py 导出
python3 main.py export data/btc-updown-5m-1770965100_20260213T063946

# 方式二：直接调用 export_json.py
python3 export_json.py data/btc-updown-5m-1770965100_20260213T063946
```

导出命令**自动检测**数据库 schema 类型（recorder / monitor / auto），统一生成标准格式的 `replay.json`：
- **recorder** schema → 直接导出
- **monitor** schema → 导出全量价位级别
- **auto** schema → 自动降采样至 1 fps

### 盘口回放 Player

1. 用浏览器打开项目中的 `player.html`
2. 将 `replay.json` 拖拽到页面中（或点击选择文件）
3. 开始回放

**播放器功能**：

| 功能 | 说明 |
|------|------|
| 逐帧播放 | 查看盘口每秒的变化 |
| 速度控制 | 0.25x / 0.5x / 1x / 2x / 5x |
| 时间轴 | 拖动跳转到任意时间点 |
| YES/NO 切换 | 分别查看两个代币的盘口 |
| 深度可视化 | ±1c / 5c / 10c / 15c 深度条形图 |
| 市场信息 | 展开查看市场详情（结算规则、流动性等） |

**键盘快捷键**：

| 按键 | 功能 |
|------|------|
| `Space` | 播放 / 暂停 |
| `←` / `→` | 前一帧 / 后一帧 |
| `Home` / `End` | 跳到开头 / 结尾 |

---

## 数据存储说明

### 目录结构

每次录制会在 `data/` 下创建一个独立目录：

```
data/
  btc-updown-5m-1770965100_20260213T063946/
    ob_data.db        ← SQLite 数据库（核心数据）
    notes.md          ← 市场信息、录制参数摘要
    replay.json       ← 导出的回放文件（需手动导出或加 -e 参数）
```

### 数据库 Schema

#### recorder 模式（模式 1）

**`pm_sports_market_1s`** — 每采样间隔的聚合行情

| 字段 | 类型 | 说明 |
|------|------|------|
| `ts_utc` | TEXT | UTC 时间戳 |
| `market_id` | TEXT | 市场 condition ID |
| `best_bid_p` / `best_ask_p` | REAL | 买一 / 卖一价格 |
| `spread` | REAL | 买卖价差 |
| `depth_bid_Nc` / `depth_ask_Nc` | REAL | ±N 分深度 (N = 1, 5, 10, 15) |
| `volume_1s` / `trades_1s` | REAL / INT | 区间成交量 / 成交笔数 |

**`pm_sports_orderbook_top5_1s`** — Top-N 档位挂单明细

| 字段 | 类型 | 说明 |
|------|------|------|
| `token_side` | TEXT | `yes` / `no` |
| `side` | TEXT | `bid` / `ask` |
| `level` | INT | 档位（1 = 最优） |
| `price` / `size` | REAL | 价格 / 数量 |

#### monitor 模式（模式 2）

- **`pm_sports_market_1s`** — 同上
- **`pm_orderbook_full_1s`** — 每秒全量订单簿（所有价位级别）
- **`pm_trades_raw`** — WebSocket 原始成交事件
- **`pm_market_meta`** — 市场元数据（问题、slug、token ID 等）

#### auto 模式（模式 3）

**`pm_snapshot`** — 每次 HTTP 请求的快照

| 字段 | 类型 | 说明 |
|------|------|------|
| `snap_id` | INTEGER | 自增主键 |
| `ts_ms` | INTEGER | 毫秒级时间戳 |
| `best_bid_p` / `best_ask_p` | REAL | 买一 / 卖一价格 |
| `spread` | REAL | 价差 |
| `bid_levels` / `ask_levels` | INT | 买 / 卖盘层数 |
| `latency_ms` | REAL | API 请求延迟 |

- **`pm_ob_level`** — 全量订单簿级别（按 `snap_id` 关联，WITHOUT ROWID 优化性能）
- **`pm_trades_raw`** — WebSocket 成交事件（毫秒时间戳）
- **`pm_market_meta`** — 市场元数据

---

## 项目文件说明

| 文件 | 说明 |
|------|------|
| `main.py` | CLI 入口：`record` 录制 + `export` 导出 |
| `recorder.py` | 模式 1：通用单市场录制逻辑 |
| `monitor.py` | 模式 2：多市场全量盘口监听 |
| `auto_monitor.py` | 模式 3：BTC 5min 自动发现 + 高频采集 |
| `resolve.py` | Polymarket URL 解析 + Gamma API 市场查询 |
| `db.py` | SQLite 建表与写入（recorder 模式使用） |
| `config.py` | API 地址与默认参数配置 |
| `export_json.py` | SQLite → `replay.json` 导出（支持全部三种 schema） |
| `player.html` | 浏览器盘口回放播放器（纯前端，无需构建） |
| `requirements.txt` | Python 依赖清单 |

---

## 技术架构

```
Polymarket 市场 URL
       │
       ▼
  resolve.py ──── Gamma API (/events, /markets)
       │           解析 slug → condition_id, token_id_yes, token_id_no
       │
       ├───────────────────────────────────┐
       ▼                                   ▼
  REST 轮询                          WebSocket 订阅
  GET /book?token_id=xxx             wss://ws-subscriptions-clob...
  (订单簿快照)                        (last_trade_price 实时成交)
       │                                   │
       ▼                                   ▼
  ┌─────────────────────────────────────────────┐
  │              每个 Tick 合并写入               │
  │    订单簿快照 + 成交量统计 → SQLite           │
  └─────────────────────────────────────────────┘
       │
       ▼
  export_json.py ──── 自动检测 schema，导出 replay.json
       │
       ▼
  player.html ──── 浏览器拖入回放
```

**核心设计**：
- **混合数据采集**：REST 轮询获取完整订单簿深度，WebSocket 低延迟捕获成交事件
- **双代币追踪**：每个 Polymarket 二元市场有 YES 和 NO 两个代币，各自独立的订单簿
- **SQLite WAL 模式**：写入性能优良，轻量无需额外服务
- **Schema 自适应导出**：`export_json.py` 通过检查表名自动判断数据来源，统一输出格式

---

## 常见问题

**Q: 无法解析市场 URL？**

确保 URL 格式正确，例如 `https://polymarket.com/event/your-event-slug`。工具会自动通过 Gamma API 查询市场信息。如果 API 返回错误，可能是市场尚未创建或已下线。

**Q: WebSocket 连接断开？**

程序内置了自动重连机制。如果网络不稳定，WebSocket 会在断开后尝试重新连接。成交数据在断开期间可能丢失，但订单簿快照不受影响（通过 REST 独立采集）。

**Q: 模式 3 的 API 请求频率太高？**

可以通过 `-c` 参数降低并发数。例如 `-c 10` 将并发从默认 50 降到 10，相应降低请求频率。

**Q: 如何查看已录制的数据？**

可以直接用 SQLite 工具查询数据库：

```bash
sqlite3 data/your-session/ob_data.db "SELECT * FROM pm_sports_market_1s LIMIT 10;"
```

或者导出为 `replay.json` 后用 `player.html` 可视化回放。

**Q: `replay.json` 文件太大？**

高频采集（模式 3）的原始数据量很大，但导出时会自动降采样至 1 fps。如果仍然较大，可以在录制时缩短录制时长（`-d` 参数）。
