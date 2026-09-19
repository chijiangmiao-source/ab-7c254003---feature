# 轮轴超声探伤 · 分片续传上传服务

面向车间不稳定网络的大文件上传后端：检测设备把扫描文件切成固定大小的分片乱序上传，
任意中断后只需补传缺片；服务重启不丢已确认进度；全部到齐且整文件 SHA-256 校验通过后，
成品文件被**原子发布**，否则保留现场并返回结构化错误。

封版（finalize）是**可跨进程崩溃续算、可抵御并发接管**的协议：流式组装、持久化检查点、
数据库租约 + 严格递增栅栏代次，任意崩溃窗口都能在下次调用时确定性地收敛。

- Python 3.13 + FastAPI，纯后端，无前端
- SQLite（WAL + `synchronous=FULL`）持久化：会话、每片 SHA-256、接收位图（BLOB）、封版检查点
- 分片正文落盘：先写 `.tmp` 并 fsync，`os.replace` 就位后才在同一事务里入账
- 封版组装：流式拼接分片 → 按检查点落盘并持久化 SHA-256 状态 → 校验整文件摘要 →
  记录发布意图 → `os.replace` 原子改名 → 单事务提交完成态

## 快速开始（Docker Compose）

```bash
docker compose up --build api                 # 宿主端口默认 8000
API_PORT=9000 docker compose up --build api   # API_PORT 覆盖宿主端口
```

一次性验收服务 `verify`（等待 API 健康后跑完整续传 + 封版恢复流程，退出码 0 = 通过）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
docker compose down
```

`verify` 挂载了 `/var/run/docker.sock`，用于把 `api` 容器 **SIGKILL 在封版中途**再重启，
验证跨进程崩溃续算；没有该套接字时这一段会明示跳过（同样的链路由 pytest 用真实
子进程 SIGKILL 覆盖）。

## 本地开发

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                        # 续传 + 封版恢复边界测试
DATA_DIR=./data uvicorn app.asgi:app --port 8000
```

可调环境变量（均有默认值，Compose 里为验收改小）：

| 变量 | 默认 | 含义 |
|---|---|---|
| `DATA_DIR` | `./data` | 数据目录 |
| `FINALIZE_CHECKPOINT_BYTES` | `1048576` | 封版检查点间隔（字节）：每组装这么多字节就 fsync + 提交检查点 |
| `FINALIZE_LEASE_TTL_SECONDS` | `10` | 封版租约时长（数据库时钟）；持有者停滞超过该时长后可被接管 |

## 数据布局与重启语义

`DATA_DIR`（容器内 `/data`，本地默认 `./data`）：

| 路径 | 内容 |
|---|---|
| `db.sqlite3` | `sessions`（位图、整文件 SHA-256、过期时间）、`chunks`（每片摘要与落盘路径）、`finalization`（封版状态机：代次、检查点、租约、发布意图、最近错误） |
| `chunks/<session_id>/00000000.chunk` | 分片正文，序号从 0 开始、8 位零填充命名 |
| `artifacts/.<session_id>.finalizing` | 封版临时文件：路径由会话决定，跨重启**同一个文件**续写 |
| `artifacts/<session_id>.bin` | 校验通过后原子发布的成品 |

进程启动时执行 `reconcile`：丢弃文件缺失或尺寸不符的 chunks 行、删除未入账的孤儿分片与
残留 `.tmp`、按存活行重建位图 —— **已确认分片绝不误判为缺失，未确认字节绝不误判为已收**。
封版状态不在启动时改写：它的崩溃窗口在下一次 `finalize` 调用时确定性地收敛（见下），
因此**已确认进度在重启后绝不倒退**。

## 封版协议（finalize）

### 检查点与崩溃续算

组装全程流式（每次最多 1 MiB 读写，绝不把整文件或整分片读入内存）。每组装
`FINALIZE_CHECKPOINT_BYTES` 字节：

1. 把新输出写入封版临时文件并 **fsync**；
2. 才在同一个数据库写里推进检查点：`confirmed_bytes` + 可序列化的**标准 SHA-256 状态**
   （`app/resumable_hash.py`，与 `hashlib.sha256` 摘要完全一致）+ 租约续期。

检查点只在对应输出落盘后推进，因此崩溃后临时文件可能比检查点**多**出一段
"已写入但未确认"的尾部。恢复时：

- 从 `confirmed_bytes` 精确继续：截断未确认尾部（**绝不截断有效前缀**）、从该偏移继续
  追加同一临时文件、从持久化的哈希状态继续 SHA-256 —— **不从第 0 字节重组，
  也不重读任何已检查过的源字节**；
- 临时文件丢失、长度短于检查点、检查点版本未知或哈希状态损坏 → 结构化恢复错误
  `409 FINALIZATION_RECOVERY_ERROR`，状态置为 `failed`，原分片与已发布成品保持不变，
  绝不猜测进度或静默发布。

### 并发、租约与栅栏代次

同一会话的并发封版由**持久化租约 + 严格递增且永不复用的栅栏代次**协调，不依赖任何
进程内锁（多进程/多容器同样成立）：

- 取得租约是单条原子 `UPDATE`：仅当无有效租约（从未租出或已按**数据库时钟**过期）时，
  `generation + 1` 并成为持有者；持有期间每次检查点都按数据库时钟续租；
- 未取得执行权的并发调用返回 `409 FINALIZATION_IN_PROGRESS`，`details` 含当前
  `generation` 与 `retry_after` 秒数；正常的单请求调用仍返回同步 `200`；
- 持有者停滞（崩溃、暂停）超过租约 TTL 后，其他请求**接管同一检查点**继续，
  代次再 +1；
- 所有推进类写入（检查点、发布意图、完成事务、失败落账）都带
  `WHERE generation = ? AND lease_owner = ?` 栅栏：**旧代次即使在暂停后恢复，
  也无法再推进检查点、替换成品或把会话标为完成**（命中 0 行即放弃，返回 409）。

完成、完整性失败与接管结果对所有后续调用保持确定且幂等：`completed` 后重复调用返回
同一 `200` 回执；`failed`（如 `INTEGRITY_MISMATCH`）后重复调用重放同一结构化错误。

### 发布阶段的崩溃窗口

| 窗口 | 现场 | 恢复动作 |
|---|---|---|
| 意图已记录、改名未发生 | `state=publishing` + 临时文件在 | 直接对临时文件重做原子改名并完成 |
| 改名已发生、完成事务未提交 | 临时文件不在、成品在 | 流式校验成品**长度 + SHA-256 均与建档值一致**才补记 `completed`，绝不重新组装 |
| 两者都不在 / 成品摘要不符 | 状态损坏 | `409 FINALIZATION_RECOVERY_ERROR`，现场保持不变 |

完成事务在**同一个数据库事务**里更新 `sessions` 与 `finalization` 两行，两表不会分叉。
半成品永远不会出现在下载路径上：`GET /artifact` 只认 `completed` 状态。

### 封版进度查询 `GET /sessions/{session_id}/finalization`

只读接口，返回持久化的封版状态（重启后不倒退）：

```json
{
  "session_id": "029c773e…",
  "state": "assembling",
  "confirmed_bytes": 2097152,
  "total_bytes": 4206649,
  "generation": 3,
  "last_error": null,
  "lease_expires_at": "2026-09-18T03:31:31.684000+00:00",
  "updated_at": "2026-09-18T03:31:21.684930+00:00"
}
```

`state` ∈ `idle`（未开始）/ `assembling`（组装中）/ `publishing`（发布中）/
`completed`（已完成）/ `failed`（确定性失败）；`last_error` 为
`{"code", "message", "details"}` 或 `null`。

### 就地迁移

老库启动时自动建 `finalization` 表并回填：历史 `completed` 会话生成
`state=completed, confirmed=total, generation=0` 的行；`active`/`expired` 会话首次
`finalize` 时惰性建行。**不清库、不需要重新上传**，历史会话与已发布成品继续可查询、
可下载。

## 接口约定

- 分片总数 = `ceil(file_size / chunk_size)`，序号从 `0` 开始
- 除最后一片外，每片长度必须等于 `chunk_size`；最后一片为剩余字节（`file_size - chunk_size × (total-1)`）
- 所有错误均为结构化 JSON：`{"error": {"code": "...", "message": "...", "details": {...}}}`

### 1. 建档 `POST /sessions`

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{
        "file_size": 4206649,
        "chunk_size": 1048576,
        "file_sha256": "b5fa3134…(整文件 SHA-256，64 位十六进制)",
        "expires_at": "2026-09-18T12:00:00+00:00"
      }'
```

`201` 响应（`total_chunks = ceil(4206649 / 1048576) = 5`）：

```json
{
  "session_id": "029c773e…",
  "status": "active",
  "file_size": 4206649,
  "chunk_size": 1048576,
  "total_chunks": 5,
  "file_sha256": "b5fa3134…",
  "received_count": 0,
  "missing_chunks": [0, 1, 2, 3, 4],
  "expires_at": "2026-09-18T12:00:00+00:00",
  "created_at": "2026-09-18T03:31:21.684930+00:00",
  "completed_at": null,
  "final_sha256": null,
  "artifact_url": null
}
```

`expires_at` 必须带时区且晚于当前时间，否则 `422 SESSION_EXPIRES_IN_PAST` / `422 VALIDATION_ERROR`。

### 2. 上传分片 `PUT /sessions/{session_id}/chunks/{index}`

请求体为分片原始字节，请求头 `X-Chunk-SHA256` 携带该片摘要：

```bash
CHUNK_SHA=$(sha256sum part0.bin | cut -d' ' -f1)
curl -s -X PUT "http://localhost:8000/sessions/$SID/chunks/0" \
  -H "X-Chunk-SHA256: $CHUNK_SHA" \
  --data-binary @part0.bin
```

- 新片入库：`201`，响应含 `"duplicate": false`
- **同序号同内容重传：幂等成功** `200`，`"duplicate": true`，状态不变
- **同序号不同内容：`409 CHUNK_CONFLICT`**，已确认分片不被覆盖
- 摘要与正文不符 `400 CHUNK_DIGEST_MISMATCH`、长度不符 `400 CHUNK_SIZE_MISMATCH`、
  序号越界 `400 CHUNK_INDEX_OUT_OF_RANGE` —— 均**不记入**任何状态
- 会话过期后新分片一律 `410 SESSION_EXPIRED`（已确认分片的幂等重放仍是 `200`）

```json
{
  "session_id": "029c773e…",
  "chunk_index": 0,
  "size": 1048576,
  "sha256": "9f86d081…",
  "duplicate": false,
  "received_count": 1,
  "total_chunks": 5
}
```

### 3. 状态 `GET /sessions/{session_id}`

```bash
curl -s http://localhost:8000/sessions/$SID
```

`missing_chunks` 为**升序**缺片序号，检测设备据此补传；`status` 为
`active` / `expired` / `completed`。

### 4. 发布 `POST /sessions/{session_id}/finalize`

```bash
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
```

- 缺片：`409 CHUNKS_INCOMPLETE`（`details.missing_chunks` 列出缺片）；若已过期则 `410 SESSION_EXPIRED`
- 到齐后流式组装（可崩溃续算，见上文"封版协议"）并校验整文件 SHA-256：
  - 一致 → 原子发布，`200` 返回 `final_sha256` 与 `artifact_url`；重复调用幂等
  - 不一致 → `422 INTEGRITY_MISMATCH`（`details` 含双方摘要与尺寸），**已上传分片全部保留**，
    该失败被持久化并对后续调用确定性重放
- 另一调用正在封版：`409 FINALIZATION_IN_PROGRESS`（`details` 含 `generation` 与
  `retry_after`），租约失效后重试即可接管同一检查点
- 封版状态不可恢复：`409 FINALIZATION_RECOVERY_ERROR`，现场保留

```json
{
  "session_id": "029c773e…",
  "status": "completed",
  "file_size": 4206649,
  "final_sha256": "b5fa3134…",
  "artifact_size": 4206649,
  "artifact_url": "/sessions/029c773e…/artifact",
  "completed_at": "2026-09-18T03:31:21.731585+00:00"
}
```

### 5. 封版进度 `GET /sessions/{session_id}/finalization`

只读返回封版状态机（见上文"封版协议"）：`idle` / `assembling` / `publishing` /
`completed` / `failed`、已确认字节数、总字节数、当前代次与最近错误。

### 6. 下载成品 `GET /sessions/{session_id}/artifact`

```bash
curl -s -OJ http://localhost:8000/sessions/$SID/artifact
sha256sum 029c773e….bin   # 与建档 file_sha256 比对即可复核
```

响应头 `X-File-SHA256` 附带成品摘要；未发布时 `409 ARTIFACT_NOT_READY`。

### 7. 健康检查 `GET /healthz`

返回 `{"status": "ok"}`，供 Compose healthcheck 与验收客户端使用。

## 错误码一览

| HTTP | code | 含义 |
|---|---|---|
| 400 | `CHUNK_INDEX_OUT_OF_RANGE` | 序号非整数或超出 `0..total-1` |
| 400 | `INVALID_CHUNK_DIGEST` | `X-Chunk-SHA256` 不是 64 位十六进制 |
| 400 | `CHUNK_SIZE_MISMATCH` | 分片长度不等于该片应有长度 |
| 400 | `CHUNK_DIGEST_MISMATCH` | 正文 SHA-256 与声明摘要不符 |
| 404 | `SESSION_NOT_FOUND` | 会话不存在 |
| 404 | `NOT_FOUND` | 路由不存在 |
| 409 | `CHUNK_CONFLICT` | 同序号不同内容，已确认分片不变 |
| 409 | `CHUNKS_INCOMPLETE` | 尚有缺片，不能发布 |
| 409 | `SESSION_ALREADY_COMPLETED` | 会话已完成，不可再写新分片 |
| 409 | `ARTIFACT_NOT_READY` | 成品尚未发布 |
| 409 | `FINALIZATION_IN_PROGRESS` | 封版被他租约持有，`details` 含 `generation` 与 `retry_after` |
| 409 | `FINALIZATION_RECOVERY_ERROR` | 封版状态不可恢复（版本未知/状态损坏/临时文件丢失或过短），现场保留 |
| 410 | `SESSION_EXPIRED` | 会话已过期，拒绝新分片 |
| 422 | `VALIDATION_ERROR` | 请求体/请求头校验失败 |
| 422 | `SESSION_EXPIRES_IN_PAST` | 建档过期时间不晚于当前时间 |
| 422 | `INTEGRITY_MISMATCH` | 组装结果与建档摘要不符，现场保留 |
| 500 | `INTERNAL_ERROR` | 未预期错误 |

## 测试

`pytest` 覆盖续传边界（建档校验、乱序上传、摘要/尺寸/越界拒绝且不入账、幂等重传、
异内容 409、缺片状态、过期拒绝、重启 reconcile、结构化错误）与封版协议：

- **真实 SIGKILL 恢复**：子进程在第二个检查点提交后被 `SIGKILL`，新进程从
  `confirmed_bytes` 精确续算（不重读已确认源前缀、不截断有效前缀、丢弃未确认尾部）；
- **并发与接管**：活租约下并发 `finalize` 得 `409 FINALIZATION_IN_PROGRESS`（含代次与
  重试时间）、租约过期后接管、暂停的旧代次所有栅栏写全部失效；
- **发布崩溃窗口**：意图已记录未改名、已改名未提交、成品摘要损坏三种现场分别收敛或
  报结构化恢复错误；
- **确定性失败**：`INTEGRITY_MISMATCH` 持久化重放、分片全保留、无可下载成品；
- **就地迁移**：老结构数据库（无封版表）迁移后历史 active/expired/completed 会话与
  成品全部可用；
- 进度端点在重启前后不倒退。

`verify`（Compose 验收）在真实 HTTP 链路上跑完整续传流程，并额外验证：封版进度端点、
并发 `finalize` 的 409 栅栏与确定性收敛，以及（挂载 docker 套接字时）把 `api` 容器
SIGKILL 在组装中途、重启后从检查点续算直到成品字节一致。
