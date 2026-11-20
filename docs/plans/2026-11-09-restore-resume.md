# Restore resume 实现计划

**目标**　在同一个 backup 进程中按原 collection 计划恢复失败的普通 restore。

**架构**　内存 store 在进入 collection 调度后持有原计划。task 锁保护当前状态和完成通知。每个 import 只使用当前远端 ID 和自己的临时目录。

**技术栈**　Go、Echo、testify、Go race detector。

1. 在 internal/jobstate/restore.go 和 store.go 实现失败任务的 Resume 和 Cleanup。两个请求竞争同一个 Fail 状态，清理失败可重试。用 go test -race ./internal/jobstate 验证并发和 Wait。
2. 在 core/restore/task.go 的公共检查之后保留 selectPlan 已拆好的任务。复用原调度器和 L0 顺序。在 coll_ddl_task.go 测试成功 DDL 跳过、索引重试、目标身份改变。
3. 在 core/restore/import_task.go 统一按完成状态和 Milvus ID 选择跳过、接续、提交。在 copy_task.go 复用每个子任务的目录。用本地存储和模拟 Milvus 验证查询失败、远端失败、响应丢失和临时文件清理。
4. 在 coll_import_snap_task.go 复用普通 REST import。在 coll_snapshot_task.go 区分远端完成和本地后处理完成。用 go test -race ./core/restore 验证。
5. 在 core/server 增加仅接受任务标识的 Resume 和 Cleanup 接口并说明未知提交结果的重复导入风险。运行 make test、make lint 和 make build。
