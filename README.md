# 跨机房镜像层核对

发版前，值班员在网页发起一次核对：两个机房各有一份**同长镜像层**（同一镜像的
两个副本），需要找出两者在哪里没对上。本应用起两个本地 HTTP 源站，分别代表 A、
B 两端，在网页上一次核对给出**全部差异字节区间**；核对只向源站取元数据，不下载
层正文。

演示覆盖三种情况：

- **层一致**：两端逐块摘要相同，结论 `identical`；
- **单块差异**：B 端换成"同长、只替换一个 64 KiB 块"的 `v2`，结论 `different`，
  并列出精确到字节的差异区间；
- **中途换版**：两端各自锁定首次应答的强 ETag 之后，B 端源站切回 `v1`；核对仍按
  锁定时的两版进行，结论与"单块差异"一致，页面同时展示 B 端当前版本已变。

另有**源站故障**场景：B 端在锁定后开始返回 503，核对标记为**未核完**（`incomplete`），
不会把失败当成"一致"。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

- 网页：<http://localhost:8000/>
- 源站 A：<http://localhost:8001/origin/state>
- 源站 B：<http://localhost:8002/origin/state>

compose 起三个服务：`origin-a`、`origin-b`（各自 `app.origin_app:app`）与
`gateway`（`app.gateway_app:app`，网页 + 核对 API）。网关等两个源站健康检查通过后
才启动，并分别通过 `DRILL_ORIGIN_A` / `DRILL_ORIGIN_B` 回源。

### 本地三进程（与 compose 拓扑一致）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DRILL_LAYER_BYTES=8388608 uvicorn app.origin_app:app --port 8001 &
DRILL_LAYER_BYTES=8388608 uvicorn app.origin_app:app --port 8002 &
DRILL_ORIGIN_A=http://127.0.0.1:8001 DRILL_ORIGIN_B=http://127.0.0.1:8002 \
  uvicorn app.gateway_app:app --port 8000
```

打开 <http://localhost:8000/>。

## 核对怎么做

### 两端锁定

核对开始先重置两端为等长 `v1`，再对每端发**一次** `HEAD /origin/layer`。应答带
强 ETag（形如 `"g1.20261124.base-<sha256 前 32 位>"`，无 `W/` 前缀）、长度、
分块数与整层 SHA-256。两端分别把**这一版的 ETag 锁定**，之后所有元数据请求都带
`If-Match: <锁定 ETag>`；源站即使已换版，也按锁定的那一版应答，并在响应头
`X-Version-Locked: true` 中说明。因此"中途换版"不会让核对改比新版本，页面同时列出
锁定 ETag 与源站当前版本。

### 只取元数据

源站的 `GET /origin/probe?windows=s:n,...` 返回请求窗口的摘要，正文是十六进制文本，
**每个窗口 64 个字符**：

```text
block_digest(i)     = sha256(第 i 块的真实字节)          # 末块按实际长度
window_digest(s, n) = sha256(block_digest(s) || ... || block_digest(s+n-1))
```

核对不会请求 `/origin/layer`（整个核对过程源站的 `layer_body_bytes` 为 0）。

### 分块定位

层按 64 KiB 分块。第 1 轮把整层切成 8 个粗窗口；对摘要不同的窗口继续按 8 路细分，
直到单个块；单个块的差异换算成字节区间 `[block*64KiB, min(size,(block+1)*64KiB)-1]`，
相邻块合并。末块长度不是 64 KiB 时按实际长度计算。

- 层一致：1 轮探针、约 1 KiB 正文；
- 单块差异：3–4 轮、约 2–4 KiB 正文；
- 128 MiB、单块差异：每端 5 个核对请求（1 次 HEAD + 4 次探针），两端正文合计
  3.5 KiB，远低于 16 KiB 上限。

### 用量上限

核对开始前会检查额度：**每端最多 16 个核对请求**（HEAD + 探针），**两端应答正文
合计最多 16 KiB**。若继续细分会超限，则停止并标记为**未核完**，不猜结果。页面
"实际用量"卡片显示两端请求数、两端正文合计、上限与剩余额度。

## 参数与 API

| 参数 | 含义 |
| --- | --- |
| `size_bytes` | 层大小（64 KiB–128 MiB，默认 8 MiB） |
| `scenario` | `match` / `single_block_diff` / `swap_midway` / `origin_fault` |
| `diff_block` | 可选，指定 B 端 `v2` 替换第几块（负数从末尾数；留空用场景默认值） |
| `max_requests_per_side` | 每端核对请求上限（默认 16，最大 16） |
| `max_combined_body_bytes` | 两端正文合计上限（默认 16384，最大 16384） |

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 网页 |
| `POST` | `/api/check` | 发起核对，body 同上 |
| `GET` | `/api/check` | 当前核对状态（锁定的 ETag、差异区间、实际用量、事件） |
| `POST` | `/api/check/cancel` | 取消 |
| `GET` | `/api/health` | 网关与两个源站可达性 |
| `GET` | `/api/origins` | 两个源站的版本与计数器 |
| `HEAD` | `/origin/layer` | 源站元数据（强 ETag / 长度 / 分块数 / 整层摘要） |
| `GET` | `/origin/probe?windows=s:n,...` | 窗口摘要，支持 `If-Match` 锁定版本 |
| `GET` | `/origin/layer` | 源站正文（支持单段 Range / If-Range），核对不使用 |
| `POST` | `/origin/{switch,reset,reset-stats,fault}` | 场景与测试用 |

## 验证

```bash
pip install -r requirements-dev.txt
python -m pytest -q          # 12 项：源站单测 + 真实 HTTP 端到端核对
python scripts/selftest.py   # 三进程起 A/B/网关，跑 5 个场景并打印用量表
```

`scripts/selftest.py` 覆盖：层一致、单块差异、中途换版、源站故障、128 MiB 单块差异；
并断言每端请求 ≤ 16、两端正文合计 ≤ 16 KiB。

## 已知限制与未完成事项

- 两个源站的层字节由种子确定性生成，不落盘；`v1` 基线两端同种子所以逐字节相同。
  若需要核对真实文件，需要另加"文件→分块"的读取路径（本次未做）。
- 差异定位按 8 路细分，单次核对假定"差异块数量不多"（≤ 数十块）。如果差异极多，
  额度会在核完前耗尽，结果标为未核完——这是有意的保守行为。
- 核对请求串行按轮推进，未做多轮并发合并；128 MiB 约 0.1–0.3 s。
- 源站是进程内状态（版本、计数器、故障注入），重启即清零；多网关共享同一对源站时
  计数器会叠加，核对开始时会重置，因此同时只应跑一个核对。
- 未在带真实网络的跨机房链路上验证；本次两个源站都在本地。

## 自测记录（2026-11-24）

- `python -m pytest -q`：12 passed（源站强 ETag / If-Match 锁定 / 末块真实长度 /
  区间换算 / 中途换版 / 源站故障 / 不可达源站 / 额度上限 / 128 MiB 用例）。
- `python scripts/selftest.py`：5 个场景全部通过，输出见下表。
- 128 MiB 单块差异：A 端 5 个请求、B 端 5 个请求，两端正文合计 3584 B，差异区间
  `83886080-83951615`（65536 B）。
- 本机无 Docker，`docker compose up --build` 未实际构建；compose 文件与 Dockerfile
  已按同拓扑改写，并以三进程方式跑通上述 selftest。
