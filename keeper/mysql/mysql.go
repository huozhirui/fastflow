package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/shiningrush/fastflow/keeper"
	"github.com/shiningrush/fastflow/pkg/event"
	"github.com/shiningrush/fastflow/pkg/log"
	"github.com/shiningrush/fastflow/pkg/mod"
	"github.com/shiningrush/fastflow/store"
	"github.com/shiningrush/goevent"
)

const LeaderKey = "leader"

// Keeper MySQL实现的分布式协调器
type Keeper struct {
	opt              *KeeperOption
	leaderTable      string
	heartbeatTable   string
	mutexTable       string

	leaderFlag    atomic.Value
	keyNumber     int
	db            *sqlx.DB

	wg            sync.WaitGroup
	firstInitWg   sync.WaitGroup
	initCompleted atomic.Value
	closeCh       chan struct{}
}

// KeeperOption MySQL Keeper配置选项
type KeeperOption struct {
	// Key 工作节点键，必须是格式 "xxxx-{{number}}"，number是工作节点编号
	Key string
	// DSN MySQL数据源名称
	DSN string
	// 表名前缀
	Prefix string
	// UnhealthyTime 不健康时间，默认5秒，选举和心跳时间将是其一半
	UnhealthyTime time.Duration
	// Timeout 操作超时时间，默认2秒
	Timeout time.Duration
	// 最大空闲连接数
	MaxIdleConns int
	// 最大打开连接数
	MaxOpenConns int
	// 连接最大生存时间
	ConnMaxLifetime time.Duration
}

// NewKeeper 创建新的MySQL Keeper实例
func NewKeeper(opt *KeeperOption) *Keeper {
	k := &Keeper{
		opt:     opt,
		closeCh: make(chan struct{}),
	}
	k.leaderFlag.Store(false)
	k.initCompleted.Store(false)
	return k
}

// Init 初始化Keeper
func (k *Keeper) Init() error {
	if err := k.readOpt(); err != nil {
		return err
	}
	store.InitFlakeGenerator(uint16(k.WorkerNumber()))

	db, err := sqlx.Connect("mysql", k.opt.DSN)
	if err != nil {
		return fmt.Errorf("connect to MySQL failed: %w", err)
	}

	// 设置连接池参数
	db.SetMaxIdleConns(k.opt.MaxIdleConns)
	db.SetMaxOpenConns(k.opt.MaxOpenConns)
	db.SetConnMaxLifetime(k.opt.ConnMaxLifetime)

	// 测试连接
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping MySQL failed: %w", err)
	}

	k.db = db

	// 创建清理过期记录的定时任务
	go k.cleanupExpiredRecords()

	k.firstInitWg.Add(2)

	k.wg.Add(1)
	go k.goElect()
	k.wg.Add(1)
	go k.goHeartBeat()

	k.firstInitWg.Wait()
	k.initCompleted.Store(true)
	return nil
}

func (k *Keeper) setLeaderFlag(isLeader bool) {
	k.leaderFlag.Store(isLeader)
	goevent.Publish(&event.LeaderChanged{
		IsLeader:  isLeader,
		WorkerKey: k.WorkerKey(),
	})
}

func (k *Keeper) readOpt() error {
	if k.opt.Key == "" || k.opt.DSN == "" {
		return fmt.Errorf("worker key or DSN cannot be empty")
	}

	if k.opt.UnhealthyTime == 0 {
		k.opt.UnhealthyTime = time.Second * 5
	}
	if k.opt.Timeout == 0 {
		k.opt.Timeout = time.Second * 2
	}
	if k.opt.MaxIdleConns == 0 {
		k.opt.MaxIdleConns = 10
	}
	if k.opt.MaxOpenConns == 0 {
		k.opt.MaxOpenConns = 100
	}
	if k.opt.ConnMaxLifetime == 0 {
		k.opt.ConnMaxLifetime = time.Hour
	}

	number, err := keeper.CheckWorkerKey(k.opt.Key)
	if err != nil {
		return err
	}
	k.keyNumber = number

	k.leaderTable = "ff_election"
	k.heartbeatTable = "ff_heartbeat"
	k.mutexTable = "ff_mutex"
	if k.opt.Prefix != "" {
		k.leaderTable = fmt.Sprintf("%s_%s", k.opt.Prefix, k.leaderTable)
		k.heartbeatTable = fmt.Sprintf("%s_%s", k.opt.Prefix, k.heartbeatTable)
		k.mutexTable = fmt.Sprintf("%s_%s", k.opt.Prefix, k.mutexTable)
	}

	return nil
}

// IsLeader 指示当前节点是否为Leader
func (k *Keeper) IsLeader() bool {
	return k.leaderFlag.Load().(bool)
}

// AliveNodes 获取所有存活的节点
func (k *Keeper) AliveNodes() ([]string, error) {
	query := fmt.Sprintf("SELECT worker_key FROM %s WHERE updated_at > ?", k.heartbeatTable)
	cutoff := time.Now().Add(-k.opt.UnhealthyTime)

	rows, err := k.db.Query(query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("query alive nodes failed: %w", err)
	}
	defer rows.Close()

	var aliveNodes []string
	for rows.Next() {
		var workerKey string
		if err := rows.Scan(&workerKey); err != nil {
			return nil, fmt.Errorf("scan worker key failed: %w", err)
		}
		aliveNodes = append(aliveNodes, workerKey)
	}

	return aliveNodes, nil
}

// IsAlive 检查指定工作节点是否存活
func (k *Keeper) IsAlive(workerKey string) (bool, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE worker_key = ? AND updated_at > ?", k.heartbeatTable)
	cutoff := time.Now().Add(-k.opt.UnhealthyTime)

	var count int
	err := k.db.Get(&count, query, workerKey, cutoff)
	if err != nil {
		return false, fmt.Errorf("check worker alive status failed: %w", err)
	}

	return count > 0, nil
}

// WorkerKey 获取工作节点键
func (k *Keeper) WorkerKey() string {
	return k.opt.Key
}

// WorkerNumber 获取工作节点编号
func (k *Keeper) WorkerNumber() int {
	return k.keyNumber
}

// NewMutex 创建新的分布式锁
func (k *Keeper) NewMutex(key string) mod.DistributedMutex {
	return &MySQLMutex{
		key:        key,
		tableName:  k.mutexTable,
		db:         k.db,
		timeout:    k.opt.Timeout,
	}
}

// Close 关闭Keeper
func (k *Keeper) Close() {
	close(k.closeCh)
	k.wg.Wait()

	if k.leaderFlag.Load().(bool) {
		query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", k.leaderTable)
		_, err := k.db.Exec(query, LeaderKey)
		if err != nil {
			log.Errorf("deregister leader failed: %s", err)
		}
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE worker_key = ?", k.heartbeatTable)
	_, err := k.db.Exec(query, k.opt.Key)
	if err != nil {
		log.Errorf("deregister heartbeat failed: %s", err)
	}

	if k.db != nil {
		err := k.db.Close()
		if err != nil {
			log.Errorf("close keeper database connection failed: %s", err)
		}
	}
}

// forceClose 强制关闭（仅用于测试）
func (k *Keeper) forceClose() {
	close(k.closeCh)
	k.wg.Wait()
}

// LeaderPayload Leader选举数据结构
type LeaderPayload struct {
	ID        string    `db:"id"`
	WorkerKey string    `db:"worker_key"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (k *Keeper) goElect() {
	defer k.wg.Done()
	
	timerCh := time.Tick(k.opt.UnhealthyTime / 2)
	closed := false
	
	for !closed {
		select {
		case <-k.closeCh:
			closed = true
		case <-timerCh:
			k.elect()
		}
	}
}

func (k *Keeper) elect() {
	if k.leaderFlag.Load().(bool) {
		if err := k.continueLeader(); err != nil {
			log.Errorf("continue leader failed: %s", err)
			k.setLeaderFlag(false)
			return
		}
	} else {
		if err := k.campaign(); err != nil {
			log.Errorf("campaign failed: %s", err)
			return
		}
	}

	if !k.initCompleted.Load().(bool) {
		k.firstInitWg.Done()
	}
}

func (k *Keeper) campaign() error {
	tx, err := k.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	// 查询当前Leader
	query := fmt.Sprintf("SELECT id, worker_key, updated_at FROM %s WHERE id = ? FOR UPDATE", k.leaderTable)
	var leader LeaderPayload
	err = tx.Get(&leader, query, LeaderKey)
	
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("query leader failed: %w", err)
	}

	now := time.Now()
	
	if errors.Is(err, sql.ErrNoRows) {
		// 没有Leader，尝试成为Leader
		insertQuery := fmt.Sprintf("INSERT INTO %s (id, worker_key, updated_at) VALUES (?, ?, ?)", k.leaderTable)
		_, err = tx.Exec(insertQuery, LeaderKey, k.opt.Key, now)
		if err != nil {
			// 可能有其他节点同时插入，忽略错误
			log.Infof("campaign failed, another node may have become leader")
			return nil
		}
		
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("commit campaign transaction failed: %w", err)
		}
		
		k.setLeaderFlag(true)
		log.Infof("successfully became leader")
		return nil
	}

	// 如果当前节点已经是Leader
	if leader.WorkerKey == k.opt.Key {
		k.setLeaderFlag(true)
		return nil
	}

	// 检查当前Leader是否过期
	if leader.UpdatedAt.Before(now.Add(-k.opt.UnhealthyTime)) {
		updateQuery := fmt.Sprintf("UPDATE %s SET worker_key = ?, updated_at = ? WHERE id = ? AND worker_key = ?", k.leaderTable)
		result, err := tx.Exec(updateQuery, k.opt.Key, now, LeaderKey, leader.WorkerKey)
		if err != nil {
			return fmt.Errorf("update leader failed: %w", err)
		}
		
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			if err = tx.Commit(); err != nil {
				return fmt.Errorf("commit leader takeover transaction failed: %w", err)
			}
			k.setLeaderFlag(true)
			log.Infof("successfully took over leadership from expired leader")
		}
	}

	return nil
}

func (k *Keeper) continueLeader() error {
	query := fmt.Sprintf("UPDATE %s SET updated_at = ? WHERE id = ? AND worker_key = ?", k.leaderTable)
	result, err := k.db.Exec(query, time.Now(), LeaderKey, k.opt.Key)
	if err != nil {
		return fmt.Errorf("update leader heartbeat failed: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("lost leadership, no rows updated")
	}
	
	return nil
}

func (k *Keeper) goHeartBeat() {
	defer k.wg.Done()
	
	timerCh := time.Tick(k.opt.UnhealthyTime / 2)
	closed := false
	
	for !closed {
		select {
		case <-k.closeCh:
			closed = true
		case <-timerCh:
			if err := k.heartBeat(); err != nil {
				log.Errorf("heartbeat failed: %s", err)
				continue
			}
		}
		
		if !k.initCompleted.Load().(bool) {
			k.firstInitWg.Done()
		}
	}
}

func (k *Keeper) heartBeat() error {
	query := fmt.Sprintf(`
		INSERT INTO %s (worker_key, updated_at) VALUES (?, ?) 
		ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)
	`, k.heartbeatTable)
	
	_, err := k.db.Exec(query, k.opt.Key, time.Now())
	if err != nil {
		return fmt.Errorf("update heartbeat failed: %w", err)
	}
	
	return nil
}

// cleanupExpiredRecords 清理过期记录
func (k *Keeper) cleanupExpiredRecords() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-k.closeCh:
			return
		case <-ticker.C:
			k.performCleanup()
		}
	}
}

func (k *Keeper) performCleanup() {
	// 清理过期的心跳记录
	heartbeatQuery := fmt.Sprintf("DELETE FROM %s WHERE updated_at < ?", k.heartbeatTable)
	cutoff := time.Now().Add(-k.opt.UnhealthyTime)
	if _, err := k.db.Exec(heartbeatQuery, cutoff); err != nil {
		log.Errorf("cleanup expired heartbeat records failed: %s", err)
	}

	// 清理过期的选举记录
	electionQuery := fmt.Sprintf("DELETE FROM %s WHERE updated_at < ?", k.leaderTable)
	if _, err := k.db.Exec(electionQuery, cutoff); err != nil {
		log.Errorf("cleanup expired election records failed: %s", err)
	}

	// 清理过期的锁记录
	mutexQuery := fmt.Sprintf("DELETE FROM %s WHERE expired_at < ?", k.mutexTable)
	if _, err := k.db.Exec(mutexQuery, time.Now()); err != nil {
		log.Errorf("cleanup expired mutex records failed: %s", err)
	}
}