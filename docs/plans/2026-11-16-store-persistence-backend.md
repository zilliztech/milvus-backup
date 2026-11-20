# 通用持久化 backend 与启动检查实现计划

**目标：** 将 restorestore 合入 jobstate，提供任务无关的 key/value backend，并在 Server、CLI 启动时检查读写删能力，失败只记日志。

**架构：** Store 管理系统配置、共享 backend 和一次启动检查；restore 层负责持久化状态编码、文件 key、stain 和任务状态。Local/S3 backend 仅保存 bytes，并用随机探测 key 检查实际读写删；保持已有 restore 文件名、持久化状态格式和配置项兼容。

**技术栈：** Go、minio SDK、testify、httptest、本地原子替换。

---

### 任务 1：通用 backend
- 新建 `internal/jobstate/store_persistence.go`、`store_persistence_local.go`、`store_persistence_s3.go`，迁移并删除 `internal/restorestore`。
- 接口定义 `Check(context.Context) error`、`Load(context.Context,string)([]byte,error)`、`Save(context.Context,string,[]byte)error`、`Delete(context.Context,string)error`。
- restore 层生成原有 `sha256(id)+".json"` key；backend 不编码 JSON，校验 key 为安全相对路径，支持按任务类型划分子目录。
- 迁移 Local/S3 测试，增加任意 bytes、命名空间、非法 key、Check 读写删失败与内容不一致、探测清理、超时测试。
- 运行 `go test -race ./internal/jobstate`，预期 PASS。

### 任务 2：启动检查
- Store 提供 `CheckPersistence(ctx) error` 和启动初始化入口，使用有限超时检查，日志记录错误，不影响正常启动。禁用时不初始化、不访问 backend。
- CLI 的 `cmd/root/root.go` 与 Server 的 `core/server/server.go` 通过 Store 统一初始化；同一配置的重复初始化不重复检查，避免 server CLI 路径检查两次。
- backend 使用独立随机探测 key，实际 Save/Load/校验/Delete，失败也尝试清理，使用独立有限清理超时。
- 补充初始化单次执行、失败日志、禁用无 I/O、启动失败仍可注册任务，以及 CLI/Server 调用链测试。

### 任务 3：回归和文档
- 修改 `internal/jobstate/restore_persistence.go`、`restore_persisted_state.go`、相关测试，全部转为共享通用 backend，保留 mandatory stain 与 best-effort 终态语义。
- 更新 `docs/user_guide/restore_resume.md` 与 `configs/backup.yaml` 的启动检查说明。
- 执行 `make test`、`PATH="/tmp/restore-resume-tools:$PATH" make lint`、`git diff --check`，预期全部通过。
- 按用户要求，所有改动留在工作区，不创建 commit。
