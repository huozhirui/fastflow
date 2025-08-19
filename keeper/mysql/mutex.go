package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/shiningrush/fastflow/pkg/mod"
)

// MySQLMutex MySQL分布式锁实现
type MySQLMutex struct {
	key       string
	tableName string
	db        *sqlx.DB
	timeout   time.Duration
	owner     string
	locked    bool
}

// Lock 获取锁
func (m *MySQLMutex) Lock(ctx context.Context, opts ...mod.LockOption) error {
	lockOpts := &mod.LockOptions{
		TTL: 30 * time.Second, // 默认TTL 30秒
	}
	
	for _, opt := range opts {
		opt(lockOpts)
	}

	if lockOpts.Owner != "" {
		m.owner = lockOpts.Owner
	} else {
		m.owner = fmt.Sprintf("lock-%d", time.Now().UnixNano())
	}

	expiredAt := time.Now().Add(lockOpts.TTL)

	// 如果是可重入锁，先检查是否已经持有锁
	if lockOpts.Owner != "" {
		if owned, err := m.isOwnedByMe(); err == nil && owned {
			// 更新锁的过期时间
			return m.renewLock(expiredAt)
		}
	}

	// 尝试获取锁
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := m.tryLock(expiredAt); err == nil {
			m.locked = true
			return nil
		}

		// 等待一小段时间后重试
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TryLock 尝试获取锁，不阻塞
func (m *MySQLMutex) TryLock(ctx context.Context, opts ...mod.LockOption) error {
	lockOpts := &mod.LockOptions{
		TTL: 30 * time.Second,
	}
	
	for _, opt := range opts {
		opt(lockOpts)
	}

	if lockOpts.Owner != "" {
		m.owner = lockOpts.Owner
	} else {
		m.owner = fmt.Sprintf("lock-%d", time.Now().UnixNano())
	}

	expiredAt := time.Now().Add(lockOpts.TTL)

	// 如果是可重入锁，先检查是否已经持有锁
	if lockOpts.Owner != "" {
		if owned, err := m.isOwnedByMe(); err == nil && owned {
			// 更新锁的过期时间
			return m.renewLock(expiredAt)
		}
	}

	if err := m.tryLock(expiredAt); err != nil {
		return err
	}

	m.locked = true
	return nil
}

// Unlock 释放锁
func (m *MySQLMutex) Unlock(ctx context.Context) error {
	if !m.locked {
		return nil
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE mutex_key = ? AND owner = ?", m.tableName)
	result, err := m.db.ExecContext(ctx, query, m.key, m.owner)
	if err != nil {
		return fmt.Errorf("unlock failed: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("unlock failed: lock not owned by current owner")
	}

	m.locked = false
	return nil
}

// tryLock 尝试获取锁的内部实现
func (m *MySQLMutex) tryLock(expiredAt time.Time) error {
	tx, err := m.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	// 清理过期的锁
	cleanupQuery := fmt.Sprintf("DELETE FROM %s WHERE mutex_key = ? AND expired_at < ?", m.tableName)
	_, err = tx.Exec(cleanupQuery, m.key, time.Now())
	if err != nil {
		return fmt.Errorf("cleanup expired locks failed: %w", err)
	}

	// 尝试插入锁记录
	insertQuery := fmt.Sprintf("INSERT INTO %s (mutex_key, owner, expired_at, created_at) VALUES (?, ?, ?, ?)", m.tableName)
	_, err = tx.Exec(insertQuery, m.key, m.owner, expiredAt, time.Now())
	if err != nil {
		// 锁已被其他进程持有
		return fmt.Errorf("lock is held by another process")
	}

	return tx.Commit()
}

// isOwnedByMe 检查锁是否由当前owner持有
func (m *MySQLMutex) isOwnedByMe() (bool, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE mutex_key = ? AND owner = ? AND expired_at > ?", m.tableName)
	
	var count int
	err := m.db.Get(&count, query, m.key, m.owner, time.Now())
	if err != nil {
		return false, fmt.Errorf("check lock ownership failed: %w", err)
	}

	return count > 0, nil
}

// renewLock 续约锁
func (m *MySQLMutex) renewLock(expiredAt time.Time) error {
	query := fmt.Sprintf("UPDATE %s SET expired_at = ? WHERE mutex_key = ? AND owner = ?", m.tableName)
	result, err := m.db.Exec(query, expiredAt, m.key, m.owner)
	if err != nil {
		return fmt.Errorf("renew lock failed: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("renew lock failed: lock not found or not owned")
	}

	m.locked = true
	return nil
}