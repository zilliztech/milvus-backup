# Restore 持久化实现计划

**目标：** 为普通、2PC 和独立 secondary restore 提供内存优先的终态持久化状态及跨进程 Resume / Cleanup。

**架构：** Store 持有运行时状态，版本化 DTO 保存完整计划和检查点。失败保存及成功删除 best effort；Resume 和 Cleanup 在远端操作前必须写入 stain。内存命中使用当前进度；冷读取 stain 时查询和 Resume 返回进度不可用，不注册到内存。Cleanup 可读取资源记录，以 FailedCleanupPending 注册后清理。

**技术栈：** Go、JSON、protobuf 二进制、S3 / 本地原子替换、testify。

---

### 任务 1：持久化状态及存储
- 创建 `internal/jobstate/restore_persisted_state.go`、`internal/jobstate/store_persistence.go` 及对应测试。
- 修改 `internal/client/milvus/stream_checkpoint.go`，导出独立持久化状态 DTO，pending 消息用 protobuf bytes 保持完整消息和顺序。
- 写普通与 secondary round-trip 测试，断言 `assert.Equal(t, before, after)`，protobuf 使用 `proto.Equal`。
- 实现版本检查、独立运行时重建、本地临时文件 + fsync + rename、S3 完整对象替换及精确 NotFound。
- 运行 `go test ./internal/jobstate ./internal/client/milvus`，预期 PASS。

### 任务 2：生命周期
- 修改 `internal/jobstate/store.go`、`restore.go`、`restore_resume.go`。
- 增加失败保存、内存优先冷加载、Resume stain 门禁、Cleanup pending 和删除重试。
- 写故障注入测试：写 stain 失败不得进入 Executing；冷 stain 不得 Resume；终态写失败保留内存；Cleanup 可重试。
- 运行 `go test -race ./internal/jobstate ./core/restore/...`，预期 PASS。

### 任务 3：配置及入口
- 修改 `internal/cfg/cfg.go`，新增 `internal/cfg/persistence.go`；接入 server / CLI 统一启动配置，不提供请求级位置覆盖。
- 修改 `core/apiv1/types.go`、`core/server/restore_resume.go` 及创建 / 查询入口；固定任务存储位置。
- 修改 `cmd/restore/restore.go`、`secondary.go`，创建 `cmd/restore/resume.go`。
- 增加 enable_resume、resume / cleanup 命令，打印任务 ID，等待终态持久化完成。
- 测试配置加载、CLI 参数、HTTP 错误与跨 Store 恢复。

### 任务 4：验证与文档
- 更新 `docs/user_guide/restore_resume.md` 和设计 HTML，写明配置、CLI、best effort、stain、重复导入和不支持运行中 crash 恢复的边界。
- 运行 `make test`、`PATH="/tmp/restore-resume-tools:$PATH" make lint`，预期 PASS；检查 diff。
- 本轮实现与测试，不自动提交。

### 调整：启动配置与任务流程隔离
- `configs/backup.yaml` 与 `internal/cfg/persistence.go` 默认使用 `local` / `restore-state`；增加 `restore.persistence.enabled`，默认 true，false 时仅保留内存 Resume。
- 配置 schema 仅声明参数；后端创建移至 `internal/jobstate/store_persistence.go`。Store 在启动时接收配置，`AddRestoreTask(id, resumable)` 只注册内存，后续持久化操作使用 Store 的系统配置，任务不再传持久化参数。
- 保存、删除及 stain 写入继续通过 Finish / StartResume / Cleanup 生命周期入口触发，保持与状态转换串行，防止分开的终态保存与下一次执行竞争。
- 补充禁用后不读写持久化状态、可同进程 Resume、成功不删除旧持久化状态的测试，以及启动配置驱动普通/secondary 任务的集成测试。
- 验证：`go test -race ./internal/jobstate ./internal/cfg/... ./core/restore/... ./core/server ./app ./cmd/...` 和 `make lint`，预期 PASS。

### 调整：通用 backend 与启动检查
- backend 接口和 Local/S3 实现合入 jobstate，只处理安全的相对 key 与原始 bytes；restoreStateKey 在 restore 层保留原有持久化状态文件名。
- Store.InitializePersistence 在 Server/CLI 启动时执行实际读写删检查；相同配置只执行一次，禁用时跳过，失败只警告，不关闭后续持久化能力。
- 检查 deadline 为 15 秒；失败/取消后的探测记录清理使用独立 5 秒 deadline。配置项保持 restore.persistence，当前仍仅为 restore 接入持久化状态策略。
- 详细实现及验证步骤见 `2026-11-16-store-persistence-backend.md`。
