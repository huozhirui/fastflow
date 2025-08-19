-- Fastflow MySQL Schema
-- 用于存储工作流相关数据的MySQL表结构

-- DAG表：存储工作流定义
CREATE TABLE IF NOT EXISTS ff_dag (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    vars JSON,
    tasks JSON NOT NULL,
    INDEX idx_name (name),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- DAG实例表：存储工作流执行实例
CREATE TABLE IF NOT EXISTS ff_dag_instance (
    id VARCHAR(255) PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'init',
    worker VARCHAR(255) DEFAULT '',
    reason TEXT,
    share_data JSON,
    vars JSON,
    cmd JSON,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    INDEX idx_dag_id (dag_id),
    INDEX idx_status (status),
    INDEX idx_worker (worker),
    INDEX idx_created_at (created_at),
    INDEX idx_updated_at (updated_at),
    FOREIGN KEY (dag_id) REFERENCES ff_dag(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 任务实例表：存储具体任务执行实例
CREATE TABLE IF NOT EXISTS ff_task_instance (
    id VARCHAR(255) PRIMARY KEY,
    dag_ins_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    action_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'init',
    reason TEXT,
    traces JSON,
    timeout_secs INT DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    INDEX idx_dag_ins_id (dag_ins_id),
    INDEX idx_task_id (task_id),
    INDEX idx_status (status),
    INDEX idx_action_name (action_name),
    INDEX idx_created_at (created_at),
    INDEX idx_updated_at (updated_at),
    FOREIGN KEY (dag_ins_id) REFERENCES ff_dag_instance(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Leader选举表：用于分布式环境下的Leader选举
CREATE TABLE IF NOT EXISTS ff_election (
    id VARCHAR(255) PRIMARY KEY DEFAULT 'leader',
    worker_key VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_worker_key (worker_key),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 心跳表：用于节点健康检查
CREATE TABLE IF NOT EXISTS ff_heartbeat (
    worker_key VARCHAR(255) PRIMARY KEY,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 分布式锁表：用于分布式锁实现
CREATE TABLE IF NOT EXISTS ff_mutex (
    mutex_key VARCHAR(255) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    expired_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_owner (owner),
    INDEX idx_expired_at (expired_at),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建清理过期记录的事件（需要MySQL事件调度器支持）
-- DELIMITER $$
-- 
-- CREATE EVENT IF NOT EXISTS cleanup_expired_heartbeat
-- ON SCHEDULE EVERY 60 SECOND
-- DO
-- BEGIN
--     DELETE FROM ff_heartbeat WHERE updated_at < DATE_SUB(NOW(), INTERVAL 10 SECOND);
-- END$$
-- 
-- CREATE EVENT IF NOT EXISTS cleanup_expired_election
-- ON SCHEDULE EVERY 60 SECOND  
-- DO
-- BEGIN
--     DELETE FROM ff_election WHERE updated_at < DATE_SUB(NOW(), INTERVAL 10 SECOND);
-- END$$
-- 
-- CREATE EVENT IF NOT EXISTS cleanup_expired_mutex
-- ON SCHEDULE EVERY 30 SECOND
-- DO
-- BEGIN
--     DELETE FROM ff_mutex WHERE expired_at < NOW();
-- END$$
-- 
-- DELIMITER ;

-- 启用事件调度器（可选，需要管理员权限）
-- SET GLOBAL event_scheduler = ON;