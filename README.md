# 轮轴超声探伤 · 分片续传上传服务

面向车间不稳定网络的大文件上传后端：检测设备把扫描文件切成固定大小的分片乱序上传，
任意中断后只需补传缺片；服务重启不丢已确认进度；全部到齐且整文件 SHA-256 校验通过后，
成品文件被**原子发布**，否则保留现场并返回结构化错误。

- Python 3.13 + FastAPI，纯后端，无前端
- SQLite（WAL + `synchronous=FULL`）持久化：会话、每片 SHA-256、接收位图（BLOB）、封版协议元数据
- 分片正文落盘：先写 `.tmp` 并 fsync，`os.replace` 就位后才在同一事务里入账
- **封版协议**：流式组装、检查点只在输出 fsync 落盘后推进；可续算的标准 SHA-256
  状态随检查点持久化，崩溃后从最后一个已确认字节续算，不重读已校验前缀
- 封版并发由**持久化租约 + 严格递增、永不复用的栅栏代次（fence generation）**协调；
  发布经“代次候选硬链接 → 原子改名 → 单事务完成”，每个崩溃窗口重启后都可收敛

## 快速开始（Docker Compose）

```bash
docker compose up --build api                 # 宿主端口默认 8000
API_PORT=9000 docker compose up --build api   # API_PORT 覆盖宿主端口
```

一次性验收服务 `verify`（等待 API 健康后跑完整续传流程，退出码 0 = 通过）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
docker compose down
```

## 本地开发

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                        # 续传边界测试
DATA_DIR=./data uvicorn app.asgi:app --port 8000
```

## 数据布局与重启语义

`DATA_DIR`（容器内 `/data`，本地默认 `./data`）：

| 路径 | 内容 |
|---|---|
| `db.sqlite3` | `sessions` 表（含接收位图 BLOB、整文件 SHA-256、过期时间）、`chunks` 表（每片 SHA-256、大小、落盘路径）、`finalizations` 表（栅栏代次、阶段、已确认字节、可续算 SHA-256 状态、租约、发布意图、最近错误） |
| `chunks/<session_id>/00000000.chunk` | 分片正文，序号从 0 开始、8 位零填充命名 |
| `.finalize/<session_id>.g<gen>.tmp` | 封版组装临时文件，按代次命名；恢复时截断到已确认字节 |
| `artifacts/.<session_id>.g<gen>.cand` | 发布候选（临时文件的硬链接），原子改名后成为成品 |
| `artifacts/<session_id>.bin` | 长度与 SHA-256 均与建档值一致后原子发布的成品 |

进程启动时执行 `reconcile`：丢弃文件缺失或尺寸不符的 chunks 行、删除未入账的孤儿分片与
残留 `.tmp`、按存活行重建位图 —— **已确认分片绝不误判为缺失，未确认字节绝不误判为已收**。
随后对每个封版协议行做崩溃窗口收敛（见下节）。SQLite 用 `PRAGMA user_version`
**就地迁移**到新元数据：不清库、不要求重新上传，历史 active / expired / completed
会话与已发布成品继续可查询、可下载（迁移前已完成的会话会回填一条 `completed` 协议行）。

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
- 到齐后流式组装并校验整文件 SHA-256：
  - 一致 → 原子发布，`200` 返回 `final_sha256` 与 `artifact_url`；重复调用幂等
  - 不一致 → `422 INTEGRITY_MISMATCH`（`details` 含双方摘要与尺寸），**已上传分片全部保留**，无成品可下载
- 已有另一个请求持租约封版中：`409 FINALIZATION_IN_PROGRESS`（见下节），可在 `retry_at` 后重试

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

#### 封版协议：崩溃续算、并发接管与发布收敛

封版不是一次性从头组装，而是一个可跨进程崩溃续算的协议，状态全部落在
`finalizations` 表与 `.finalize/` 临时文件中，不依赖任何单进程锁：

- **流式 + 检查点**：按分片顺序、以有限缓冲（默认 1 MiB）流式追加到
  `.finalize/<sid>.g<gen>.tmp`，整文件与完整分片从不进入内存。每累计
  `FINALIZE_CHECKPOINT_BYTES`（默认 8 MiB）先 `fsync` 输出，再在一个事务里推进
  `confirmed_bytes` 与可续算 SHA-256 状态。**检查点只可能在对应输出落盘之后推进**。
- **可续算的标准 SHA-256**：检查点持久化 8 个 32 位链变量、已处理字节数与不足 64
  字节的尾块；恢复后续算出的摘要与一次性 `hashlib.sha256` 逐位一致，因此恢复时
  **不重读、不重算已经检查过的源前缀**，只从已确认字节继续读源分片。
- **未确认尾部只丢弃**：若进程在“字节已 fsync 但检查点事务未提交”之间死亡，临时文件
  会长于检查点；恢复打开时先 `truncate` 回 `confirmed_bytes`（并 fsync），绝不截断有效
  前缀，也绝不把未确认字节当作进度。临时文件短于检查点、丢失、或检查点版本未知/状态
  损坏时，返回结构化 `500 FINALIZATION_RECOVERY_ERROR`，分片与已发布成品保持不变，
  不猜测进度、不静默发布。
- **租约与栅栏代次**：首次封版以代次 0 建协议行；取得执行者把
  `fence_generation` 严格 +1（永不复用）并按**数据库时钟**记录 `lease_owner/lease_until`，
  组装途中周期性续租（`FINALIZE_LEASE_SECONDS` / `FINALIZE_LEASE_RENEW_SECONDS`）。
  租约未过期时其他 `finalize` 直接得到结构化 `409`；进程崩溃后租约留在数据库里，
  到期后另一个请求即可在**同一检查点**接管并取得更高代次。所有检查点/阶段/完成写库
  都带 `(fence_generation, lease_owner)` 条件：**旧代次即使暂停后恢复，也不能再推进
  检查点、替换成品或把会话标为完成**（其更新返回 0 行，调用随即停止）。接管时只为
  已确认前缀克隆一份新代次自己的临时 inode，旧持有者继续写也只会写到自己的孤儿文件。
- **发布各崩溃窗口均可收敛**：组装在全长检查点落库后进入 `publishing`：
  1. 把临时文件硬链接为代次候选 `artifacts/.<sid>.g<gen>.cand`（临时文件仍保留自己的链接）；
  2. 事务写入“发布意图” `publish_intent=1`；
  3. `os.replace` 候选为成品并 fsync 目录；
  4. 在**一个事务**里把封版与会话同时标为 `completed`（消除“已改名但完成事务未提交”窗口）。

  重启收敛规则：成品路径已存在且**长度和 SHA-256 都与建档值一致** → 直接收敛为
  `completed`，**绝不重新组装**；成品不存在但候选校验一致 → 重新改名；候选缺失/损坏
  且无合格成品 → 回到 `assembling` 从检查点续算；成品路径上是不符内容 → 绝不暴露下载，
  移除后从检查点续算。任何半成品都不会被 `/artifact` 下载。
- **确定性与幂等**：完成、完整性失败、接管结果对所有后续调用都确定且幂等；进度在重启后
  只增不减。

### 5. 封版进度 `GET /sessions/{session_id}/finalization`（只读）

```bash
curl -s http://localhost:8000/sessions/$SID/finalization
```

`state` 为 `idle` / `assembling` / `publishing` / `completed` / `failed`，并含
`confirmed_bytes`（已确认字节）、`total_bytes`（总代码字节数）、`generation`（当前栅栏
代次）与 `last_error`（失败时含 `code/message/details`）。从未封版返回 `idle`；该进度
持久化，重启后不倒退。封版进行中并发调用 `POST .../finalize` 得到：

```json
{
  "error": {
    "code": "FINALIZATION_IN_PROGRESS",
    "message": "another finalization attempt holds the lease for this session",
    "details": {
      "generation": 1,
      "phase": "assembling",
      "retry_at": "2026-09-19T03:31:21.731585+00:00",
      "retry_after_seconds": 14.2
    }
  }
}
```

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
| 409 | `FINALIZATION_IN_PROGRESS` | 另一请求持租约封版中；`details` 含当前代次与可重试时间 |
| 410 | `SESSION_EXPIRED` | 会话已过期，拒绝新分片 |
| 422 | `VALIDATION_ERROR` | 请求体/请求头校验失败 |
| 422 | `SESSION_EXPIRES_IN_PAST` | 建档过期时间不晚于当前时间 |
| 422 | `INTEGRITY_MISMATCH` | 组装结果与建档摘要不符，现场保留，无成品可下载 |
| 500 | `FINALIZATION_RECOVERY_ERROR` | 检查点版本未知/状态损坏、临时文件丢失或短于检查点；分片与已发布成品不变 |
| 500 | `INTERNAL_ERROR` | 未预期错误 |

## 测试

- `pytest`（`tests/test_upload_api.py`）覆盖续传边界：建档参数校验、乱序上传、
  摘要/尺寸/越界拒绝且不入账、同内容幂等重传、异内容 409 不污染进度、缺片状态升序、
  发布成功与幂等、完整性失败保留现场、过期拒绝新分片、重启后从已确认位置续作
  （含孤儿分片与残留临时文件清理）、结构化错误形态。
- `tests/test_finalization_recovery.py` 覆盖**真实恢复链路**：封版在独立 OS 进程中
  执行（`app.recovery_worker`），在检查点前/后、候选硬链接后、发布意图后、原子改名后
  被真实 `SIGKILL`，再以全新进程对同一 `DATA_DIR` 重启收敛；并验证未确认尾部只丢弃、
  续算不重读已确认源前缀（按实际读字节计数断言）、租约到期后更高代次跨进程接管且
  旧代次解封后无法完成、409 携带代次与重试时间、摘要不符崩溃后仍 `422` 且无成品、
  检查点/临时文件损坏的结构化恢复错误，以及旧库就地迁移后历史会话与成品继续可用。
  测试不含固定延时假设、假进度或仅内存状态。
- `python -m app.verify` 为一次性验收客户端；在 Compose 中以 `RECOVERY_SCENARIOS=1`
  与 API 共享卷，会真实让 API 进程在各封版窗口自杀并由 Docker 重启，确认跨进程收敛。
