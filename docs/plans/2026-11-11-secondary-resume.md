# Secondary restore Resume 实现计划

**目标**　在当前 backup server 进程内按原 task ID 继续失败的独立 secondary restore。

**架构**　沿用 Store 的 Resume/Cleanup 互斥和 Wait 结果。首次预检后保存不可变的备份配置和批次；复制流在首次入队前登记逻辑操作、原始消息和 tick。重建 client 时按通道顺序重放待确认消息。

**技术栈**　Go、Milvus replicate gRPC、现有内存 Store、testify。

### 复制流

在 internal/client/milvus/stream.go 接入按操作名去重的发送入口。内部 replay state 记录每通道 ACK 和待确认消息、已提交操作和已分配 tick，重连与重新创建 client 都复用原消息。测试半广播确认、乱序 checkpoint、恢复后下一条消息的 tick。

### Secondary plan 和执行

在 internal/restoreplan 和 core/restore/secondary 固定数据库、collection、binlog 批次、snapshot bundle 和目标映射。为每个 import 保存 job ID、结果和 commit 发送意图。发送、提交、清理发生错误时保留可恢复的当前状态。返回最终结果前关闭 stream、等待所有后台检查。

### 入口与验证

通过 core/restore/resume.go 接入现有 HTTP Resume 和 Cleanup，app/restore_secondary.go 固定每次 Wait 的结果。测试 snapshot 与 binlog 批次的失败恢复、未完成 commit、FlushAll 以及 Cleanup。运行 make test、make lint、make build。
