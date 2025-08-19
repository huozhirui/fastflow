# Fastflow MySQL 实现

这个目录包含了 Fastflow 使用 MySQL 作为存储和分布式协调的完整实现示例。

## 特性

- **分布式存储**: 使用 MySQL 替代 MongoDB 作为数据存储
- **分布式锁**: 基于 MySQL 实现的分布式锁机制
- **Leader 选举**: 基于 MySQL 的分布式 Leader 选举
- **节点健康检查**: 基于 MySQL 的心跳机制
- **高性能**: 使用连接池和优化的 SQL 查询
- **ACID 事务**: 利用 MySQL 的事务特性保证数据一致性

## 架构优势

### 相比 MongoDB 实现的优势

1. **ACID 事务支持**: MySQL 提供完整的 ACID 事务支持，确保数据一致性
2. **更好的锁机制**: 利用 MySQL 的行锁和表锁实现更可靠的分布式锁
3. **SQL 标准**: 使用标准 SQL 查询，更容易维护和优化
4. **更广泛的运维支持**: MySQL 在企业环境中有更成熟的运维工具链
5. **更好的性能监控**: 丰富的性能监控和调优工具

### 分布式特性

1. **多节点部署**: 支持多个 Fastflow 实例同时运行
2. **自动故障转移**: Leader 节点故障时自动选举新的 Leader
3. **负载均衡**: 任务自动分发到健康的 Worker 节点
4. **水平扩展**: 可以随时添加新的 Worker 节点

## 快速开始

### 1. 启动 MySQL 服务

使用 Docker Compose 快速启动 MySQL 服务：

```bash
cd examples/mysql
docker-compose up -d
```

这将启动：
- MySQL 8.0 数据库服务器（端口 3306）
- phpMyAdmin 管理界面（端口 8080）

### 2. 验证数据库连接

访问 phpMyAdmin: http://localhost:8080
- 用户名: root
- 密码: password

你应该能看到 `fastflow` 数据库和相关表已经自动创建。

### 3. 运行示例程序

```bash
cd examples/mysql
go run main.go
```

### 4. 观察执行结果

程序将：
1. 注册自定义 Action
2. 连接到 MySQL 数据库
3. 启动 Fastflow 引擎
4. 创建并执行多个 DAG 实例
5. 显示任务执行过程

## 配置说明

### MySQL Keeper 配置

```go
keeper := mysqlKeeper.NewKeeper(&mysqlKeeper.KeeperOption{
    Key: "worker-1", // 工作节点标识，格式: "prefix-number"
    DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
    Prefix: "test", // 表名前缀
    UnhealthyTime: 10 * time.Second, // 节点不健康判断时间
    Timeout: 5 * time.Second,        // 操作超时时间
    MaxIdleConns: 10,               // 最大空闲连接数
    MaxOpenConns: 100,              // 最大打开连接数
    ConnMaxLifetime: time.Hour,     // 连接最大生存时间
})
```

### MySQL Store 配置

```go
store := mysqlStore.NewStore(&mysqlStore.StoreOption{
    DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
    Prefix: "test", // 表名前缀，应与 Keeper 保持一致
    Timeout: 5 * time.Second,
    MaxIdleConns: 10,
    MaxOpenConns: 100,
    ConnMaxLifetime: time.Hour,
})
```

## 数据库表结构

MySQL 实现使用以下表：

1. **ff_dag**: 存储工作流定义
2. **ff_dag_instance**: 存储工作流执行实例
3. **ff_task_instance**: 存储任务执行实例
4. **ff_election**: Leader 选举表
5. **ff_heartbeat**: 节点心跳表
6. **ff_mutex**: 分布式锁表

所有表都支持前缀，通过 `Prefix` 配置项设置。

## 分布式部署

### 多节点部署示例

节点 1:
```go
keeper := mysqlKeeper.NewKeeper(&mysqlKeeper.KeeperOption{
    Key: "worker-1",
    DSN: "root:password@tcp(mysql-server:3306)/fastflow?...",
    // ... 其他配置
})
```

节点 2:
```go
keeper := mysqlKeeper.NewKeeper(&mysqlKeeper.KeeperOption{
    Key: "worker-2",  // 不同的节点 ID
    DSN: "root:password@tcp(mysql-server:3306)/fastflow?...", // 相同的数据库
    // ... 其他配置
})
```

### 高可用部署

1. **MySQL 集群**: 使用 MySQL 主从复制或 MySQL Group Replication
2. **负载均衡**: 使用 MySQL Router 或 ProxySQL
3. **监控**: 使用 Prometheus + Grafana 监控 Fastflow 和 MySQL
4. **备份**: 定期备份 MySQL 数据

## 性能优化

### 数据库优化

1. **索引优化**: 已为常用查询字段添加索引
2. **连接池**: 合理配置连接池参数
3. **事务大小**: 控制批量操作的事务大小
4. **分区表**: 对于大量数据可考虑表分区

### 应用优化

1. **批量操作**: 使用批量插入和更新
2. **缓存**: 合理使用缓存减少数据库访问
3. **监控**: 监控数据库性能指标
4. **资源限制**: 合理设置 goroutine 数量

## 监控和运维

### 关键指标

- 数据库连接数
- 查询响应时间
- 锁等待时间
- 心跳延迟
- 任务执行成功率

### 日志配置

```go
import "github.com/shiningrush/fastflow/pkg/log"

log.SetLevel(log.InfoLevel)
```

### 故障排查

1. **检查数据库连接**: 确保 DSN 配置正确
2. **检查表权限**: 确保用户有足够的表操作权限
3. **检查网络**: 确保网络连通性
4. **检查日志**: 查看 Fastflow 和 MySQL 日志

## 迁移指南

### 从 MongoDB 迁移到 MySQL

1. **数据导出**: 从 MongoDB 导出现有数据
2. **数据转换**: 将 BSON 数据转换为 JSON
3. **数据导入**: 导入到 MySQL 相应表中
4. **配置修改**: 更新应用配置使用 MySQL
5. **测试验证**: 全面测试功能正确性

### 注意事项

- JSON 字段在 MySQL 5.7+ 中得到原生支持
- 确保字符集设置为 utf8mb4
- 合理设置 SQL 模式
- 注意时区设置

## 故障处理

### 常见问题

1. **连接超时**: 检查网络和防火墙设置
2. **权限错误**: 确保数据库用户权限正确
3. **锁超时**: 检查事务大小和锁竞争
4. **内存不足**: 调整连接池和缓存设置

### 恢复流程

1. **节点故障**: 其他节点自动接管工作
2. **Leader 故障**: 自动选举新的 Leader
3. **数据库故障**: 使用备份恢复数据
4. **网络分区**: 等待网络恢复后自动重连

## 参考资料

- [Fastflow 官方文档](https://github.com/ShiningRush/fastflow)
- [MySQL 8.0 文档](https://dev.mysql.com/doc/refman/8.0/en/)
- [Go MySQL Driver 文档](https://github.com/go-sql-driver/mysql)
- [SQLX 文档](https://jmoiron.github.io/sqlx/)