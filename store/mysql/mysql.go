package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/shiningrush/fastflow/pkg/entity"
	"github.com/shiningrush/fastflow/pkg/event"
	"github.com/shiningrush/fastflow/pkg/log"
	"github.com/shiningrush/fastflow/pkg/mod"
	"github.com/shiningrush/fastflow/pkg/utils"
	"github.com/shiningrush/fastflow/pkg/utils/data"
	"github.com/shiningrush/goevent"
)

// StoreOption MySQL存储配置选项
type StoreOption struct {
	// MySQL数据源名称 (DSN)
	// 格式: username:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local
	DSN string
	// 连接超时时间，默认5秒
	Timeout time.Duration
	// 表名前缀
	Prefix string
	// 最大空闲连接数
	MaxIdleConns int
	// 最大打开连接数
	MaxOpenConns int
	// 连接最大生存时间
	ConnMaxLifetime time.Duration
}

// Store MySQL存储实现
type Store struct {
	opt            *StoreOption
	dagTable       string
	dagInsTable    string
	taskInsTable   string
	
	db *sqlx.DB
}

// NewStore 创建新的MySQL存储实例
func NewStore(option *StoreOption) *Store {
	return &Store{
		opt: option,
	}
}

// Init 初始化存储
func (s *Store) Init() error {
	if err := s.readOpt(); err != nil {
		return err
	}

	db, err := sqlx.Connect("mysql", s.opt.DSN)
	if err != nil {
		return fmt.Errorf("connect to MySQL failed: %w", err)
	}

	// 设置连接池参数
	db.SetMaxIdleConns(s.opt.MaxIdleConns)
	db.SetMaxOpenConns(s.opt.MaxOpenConns)
	db.SetConnMaxLifetime(s.opt.ConnMaxLifetime)

	// 测试连接
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping MySQL failed: %w", err)
	}

	s.db = db
	return nil
}

func (s *Store) readOpt() error {
	if s.opt.DSN == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	if s.opt.Timeout == 0 {
		s.opt.Timeout = 5 * time.Second
	}
	if s.opt.MaxIdleConns == 0 {
		s.opt.MaxIdleConns = 10
	}
	if s.opt.MaxOpenConns == 0 {
		s.opt.MaxOpenConns = 100
	}
	if s.opt.ConnMaxLifetime == 0 {
		s.opt.ConnMaxLifetime = time.Hour
	}

	s.dagTable = "ff_dag"
	s.dagInsTable = "ff_dag_instance"
	s.taskInsTable = "ff_task_instance"
	if s.opt.Prefix != "" {
		s.dagTable = fmt.Sprintf("%s_%s", s.opt.Prefix, s.dagTable)
		s.dagInsTable = fmt.Sprintf("%s_%s", s.opt.Prefix, s.dagInsTable)
		s.taskInsTable = fmt.Sprintf("%s_%s", s.opt.Prefix, s.taskInsTable)
	}

	return nil
}

// Close 关闭存储连接
func (s *Store) Close() {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			log.Errorf("close MySQL connection failed: %s", err)
		}
	}
}

// CreateDag 创建DAG
func (s *Store) CreateDag(dag *entity.Dag) error {
	// 检查任务连接
	_, err := mod.BuildRootNode(mod.MapTasksToGetter(dag.Tasks))
	if err != nil {
		return err
	}
	return s.genericCreate(dag, s.dagTable)
}

// CreateDagIns 创建DAG实例
func (s *Store) CreateDagIns(dagIns *entity.DagInstance) error {
	return s.genericCreate(dagIns, s.dagInsTable)
}

// CreateTaskIns 创建任务实例
func (s *Store) CreateTaskIns(taskIns *entity.TaskInstance) error {
	return s.genericCreate(taskIns, s.taskInsTable)
}

func (s *Store) genericCreate(input entity.BaseInfoGetter, tableName string) error {
	baseInfo := input.GetBaseInfo()
	baseInfo.Initial()

	var query string
	var args []interface{}

	switch v := input.(type) {
	case *entity.Dag:
		varsBytes, _ := json.Marshal(v.Vars)
		tasksBytes, _ := json.Marshal(v.Tasks)
		query = fmt.Sprintf("INSERT INTO %s (id, name, status, created_at, updated_at, vars, tasks) VALUES (?, ?, ?, ?, ?, ?, ?)", tableName)
		args = []interface{}{v.ID, v.Name, "active", baseInfo.CreatedAt, baseInfo.UpdatedAt, varsBytes, tasksBytes}
	case *entity.DagInstance:
		shareDataBytes, _ := json.Marshal(v.ShareData)
		varsBytes, _ := json.Marshal(v.Vars)
		cmdBytes, _ := json.Marshal(v.Cmd)
		query = fmt.Sprintf("INSERT INTO %s (id, dag_id, status, worker, reason, share_data, vars, cmd, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tableName)
		args = []interface{}{v.ID, v.DagID, v.Status, v.Worker, v.Reason, shareDataBytes, varsBytes, cmdBytes, baseInfo.CreatedAt, baseInfo.UpdatedAt}
	case *entity.TaskInstance:
		tracesBytes, _ := json.Marshal(v.Traces)
		query = fmt.Sprintf("INSERT INTO %s (id, dag_ins_id, task_id, action_name, status, reason, traces, timeout_secs, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tableName)
		args = []interface{}{v.ID, v.DagInsID, v.TaskID, v.ActionName, v.Status, v.Reason, tracesBytes, v.TimeoutSecs, baseInfo.CreatedAt, baseInfo.UpdatedAt}
	default:
		return fmt.Errorf("unsupported entity type")
	}

	_, err := s.db.Exec(query, args...)
	if err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%s key[ %s ] already existed: %w", tableName, baseInfo.ID, data.ErrDataConflicted)
		}
		return fmt.Errorf("insert into %s failed: %w", tableName, err)
	}
	return nil
}

// BatchCreatTaskIns 批量创建任务实例
func (s *Store) BatchCreatTaskIns(taskIns []*entity.TaskInstance) error {
	if len(taskIns) == 0 {
		return nil
	}

	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf("INSERT INTO %s (id, dag_ins_id, task_id, action_name, status, reason, traces, timeout_secs, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", s.taskInsTable)
	
	for _, task := range taskIns {
		task.Initial()
		tracesBytes, _ := json.Marshal(task.Traces)
		_, err := tx.Exec(query, task.ID, task.DagInsID, task.TaskID, task.ActionName, task.Status, task.Reason, tracesBytes, task.TimeoutSecs, task.CreatedAt, task.UpdatedAt)
		if err != nil {
			return fmt.Errorf("batch insert task instance failed: %w", err)
		}
	}

	return tx.Commit()
}

// PatchTaskIns 更新任务实例部分字段
func (s *Store) PatchTaskIns(taskIns *entity.TaskInstance) error {
	if taskIns.ID == "" {
		return fmt.Errorf("id cannot be empty")
	}

	setParts := []string{"updated_at = ?"}
	args := []interface{}{time.Now().Unix()}

	if taskIns.Status != "" {
		setParts = append(setParts, "status = ?")
		args = append(args, taskIns.Status)
	}
	if taskIns.Reason != "" {
		setParts = append(setParts, "reason = ?")
		args = append(args, taskIns.Reason)
	}
	if len(taskIns.Traces) > 0 {
		tracesBytes, _ := json.Marshal(taskIns.Traces)
		setParts = append(setParts, "traces = ?")
		args = append(args, tracesBytes)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", s.taskInsTable, 
		fmt.Sprintf("%s", setParts[0]))
	for i := 1; i < len(setParts); i++ {
		query = fmt.Sprintf("UPDATE %s SET %s, %s WHERE id = ?", s.taskInsTable, 
			query[len(fmt.Sprintf("UPDATE %s SET ", s.taskInsTable)):len(query)-len(" WHERE id = ?")], setParts[i])
	}
	args = append(args, taskIns.ID)

	result, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("patch task instance failed: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("task instance not found: %w", data.ErrDataNotFound)
	}

	return nil
}

// PatchDagIns 更新DAG实例部分字段
func (s *Store) PatchDagIns(dagIns *entity.DagInstance, mustsPatchFields ...string) error {
	setParts := []string{"updated_at = ?"}
	args := []interface{}{time.Now().Unix()}

	if dagIns.ShareData != nil {
		shareDataBytes, _ := json.Marshal(dagIns.ShareData)
		setParts = append(setParts, "share_data = ?")
		args = append(args, shareDataBytes)
	}
	if dagIns.Status != "" {
		setParts = append(setParts, "status = ?")
		args = append(args, dagIns.Status)
	}
	if utils.StringsContain(mustsPatchFields, "Cmd") || dagIns.Cmd != nil {
		cmdBytes, _ := json.Marshal(dagIns.Cmd)
		setParts = append(setParts, "cmd = ?")
		args = append(args, cmdBytes)
	}
	if dagIns.Worker != "" {
		setParts = append(setParts, "worker = ?")
		args = append(args, dagIns.Worker)
	}
	if utils.StringsContain(mustsPatchFields, "Reason") || dagIns.Reason != "" {
		setParts = append(setParts, "reason = ?")
		args = append(args, dagIns.Reason)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", s.dagInsTable, 
		fmt.Sprintf("%s", setParts[0]))
	for i := 1; i < len(setParts); i++ {
		query = fmt.Sprintf("UPDATE %s SET %s, %s WHERE id = ?", s.dagInsTable, 
			query[len(fmt.Sprintf("UPDATE %s SET ", s.dagInsTable)):len(query)-len(" WHERE id = ?")], setParts[i])
	}
	args = append(args, dagIns.ID)

	result, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("patch dag instance failed: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("dag instance not found: %w", data.ErrDataNotFound)
	}

	goevent.Publish(&event.DagInstancePatched{
		Payload:         dagIns,
		MustPatchFields: mustsPatchFields,
	})
	return nil
}

// UpdateDag 更新DAG
func (s *Store) UpdateDag(dag *entity.Dag) error {
	// 检查任务连接
	_, err := mod.BuildRootNode(mod.MapTasksToGetter(dag.Tasks))
	if err != nil {
		return err
	}
	return s.genericUpdate(dag, s.dagTable)
}

// UpdateDagIns 更新DAG实例
func (s *Store) UpdateDagIns(dagIns *entity.DagInstance) error {
	if err := s.genericUpdate(dagIns, s.dagInsTable); err != nil {
		return err
	}

	goevent.Publish(&event.DagInstanceUpdated{Payload: dagIns})
	return nil
}

// UpdateTaskIns 更新任务实例
func (s *Store) UpdateTaskIns(taskIns *entity.TaskInstance) error {
	return s.genericUpdate(taskIns, s.taskInsTable)
}

// genericUpdate 通用更新方法
func (s *Store) genericUpdate(input entity.BaseInfoGetter, tableName string) error {
	baseInfo := input.GetBaseInfo()
	baseInfo.Update()

	var query string
	var args []interface{}

	switch v := input.(type) {
	case *entity.Dag:
		varsBytes, _ := json.Marshal(v.Vars)
		tasksBytes, _ := json.Marshal(v.Tasks)
		query = fmt.Sprintf("UPDATE %s SET name = ?, vars = ?, tasks = ?, updated_at = ? WHERE id = ?", tableName)
		args = []interface{}{v.Name, varsBytes, tasksBytes, baseInfo.UpdatedAt, v.ID}
	case *entity.DagInstance:
		shareDataBytes, _ := json.Marshal(v.ShareData)
		varsBytes, _ := json.Marshal(v.Vars)
		cmdBytes, _ := json.Marshal(v.Cmd)
		query = fmt.Sprintf("UPDATE %s SET dag_id = ?, status = ?, worker = ?, reason = ?, share_data = ?, vars = ?, cmd = ?, updated_at = ? WHERE id = ?", tableName)
		args = []interface{}{v.DagID, v.Status, v.Worker, v.Reason, shareDataBytes, varsBytes, cmdBytes, baseInfo.UpdatedAt, v.ID}
	case *entity.TaskInstance:
		tracesBytes, _ := json.Marshal(v.Traces)
		query = fmt.Sprintf("UPDATE %s SET dag_ins_id = ?, task_id = ?, action_name = ?, status = ?, reason = ?, traces = ?, timeout_secs = ?, updated_at = ? WHERE id = ?", tableName)
		args = []interface{}{v.DagInsID, v.TaskID, v.ActionName, v.Status, v.Reason, tracesBytes, v.TimeoutSecs, baseInfo.UpdatedAt, v.ID}
	default:
		return fmt.Errorf("unsupported entity type")
	}

	result, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("update %s failed: %w", tableName, err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("%s has no key[ %s ] to update: %w", tableName, baseInfo.ID, data.ErrDataNotFound)
	}
	return nil
}

// BatchUpdateDagIns 批量更新DAG实例
func (s *Store) BatchUpdateDagIns(dagIns []*entity.DagInstance) error {
	if len(dagIns) == 0 {
		return nil
	}

	errChan := make(chan error, len(dagIns))
	defer close(errChan)

	errs := &data.Errors{}
	go func() {
		for err := range errChan {
			errs.Append(err)
		}
	}()

	wg := sync.WaitGroup{}
	for i := range dagIns {
		wg.Add(1)
		go func(dag *entity.DagInstance, ch chan error) {
			defer wg.Done()
			if err := s.UpdateDagIns(dag); err != nil {
				errChan <- fmt.Errorf("batch update dag instance failed: %w", err)
			}
		}(dagIns[i], errChan)
	}
	wg.Wait()

	if errs.HasError() {
		return errs
	}
	return nil
}

// BatchUpdateTaskIns 批量更新任务实例
func (s *Store) BatchUpdateTaskIns(taskIns []*entity.TaskInstance) error {
	if len(taskIns) == 0 {
		return nil
	}

	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf("UPDATE %s SET dag_ins_id = ?, task_id = ?, action_name = ?, status = ?, reason = ?, traces = ?, timeout_secs = ?, updated_at = ? WHERE id = ?", s.taskInsTable)
	
	for _, task := range taskIns {
		task.Update()
		tracesBytes, _ := json.Marshal(task.Traces)
		_, err := tx.Exec(query, task.DagInsID, task.TaskID, task.ActionName, task.Status, task.Reason, tracesBytes, task.TimeoutSecs, task.UpdatedAt, task.ID)
		if err != nil {
			return fmt.Errorf("batch update task instance failed: %w", err)
		}
	}

	return tx.Commit()
}

// GetTaskIns 获取任务实例
func (s *Store) GetTaskIns(taskInsId string) (*entity.TaskInstance, error) {
	ret := new(entity.TaskInstance)
	if err := s.genericGet(s.taskInsTable, taskInsId, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// GetDag 获取DAG
func (s *Store) GetDag(dagId string) (*entity.Dag, error) {
	ret := new(entity.Dag)
	if err := s.genericGet(s.dagTable, dagId, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// GetDagInstance 获取DAG实例
func (s *Store) GetDagInstance(dagInsId string) (*entity.DagInstance, error) {
	ret := new(entity.DagInstance)
	if err := s.genericGet(s.dagInsTable, dagInsId, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) genericGet(tableName, id string, ret interface{}) error {
	var query string
	switch ret.(type) {
	case *entity.Dag:
		query = fmt.Sprintf("SELECT id, name, created_at, updated_at, vars, tasks FROM %s WHERE id = ?", tableName)
	case *entity.DagInstance:
		query = fmt.Sprintf("SELECT id, dag_id, status, worker, reason, share_data, vars, cmd, created_at, updated_at FROM %s WHERE id = ?", tableName)
	case *entity.TaskInstance:
		query = fmt.Sprintf("SELECT id, dag_ins_id, task_id, action_name, status, reason, traces, timeout_secs, created_at, updated_at FROM %s WHERE id = ?", tableName)
	default:
		return fmt.Errorf("unsupported entity type")
	}

	row := s.db.QueryRowx(query, id)
	if err := s.scanRow(row, ret); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%s key[ %s ] not found: %w", tableName, id, data.ErrDataNotFound)
		}
		return fmt.Errorf("get from %s failed: %w", tableName, err)
	}

	return nil
}

func (s *Store) scanRow(row *sqlx.Row, ret interface{}) error {
	switch v := ret.(type) {
	case *entity.Dag:
		var varsBytes, tasksBytes []byte
		err := row.Scan(&v.ID, &v.Name, &v.CreatedAt, &v.UpdatedAt, &varsBytes, &tasksBytes)
		if err != nil {
			return err
		}
		if len(varsBytes) > 0 {
			json.Unmarshal(varsBytes, &v.Vars)
		}
		if len(tasksBytes) > 0 {
			json.Unmarshal(tasksBytes, &v.Tasks)
		}
	case *entity.DagInstance:
		var shareDataBytes, varsBytes, cmdBytes []byte
		err := row.Scan(&v.ID, &v.DagID, &v.Status, &v.Worker, &v.Reason, &shareDataBytes, &varsBytes, &cmdBytes, &v.CreatedAt, &v.UpdatedAt)
		if err != nil {
			return err
		}
		if len(shareDataBytes) > 0 {
			json.Unmarshal(shareDataBytes, &v.ShareData)
		}
		if len(varsBytes) > 0 {
			json.Unmarshal(varsBytes, &v.Vars)
		}
		if len(cmdBytes) > 0 {
			json.Unmarshal(cmdBytes, &v.Cmd)
		}
	case *entity.TaskInstance:
		var tracesBytes []byte
		err := row.Scan(&v.ID, &v.DagInsID, &v.TaskID, &v.ActionName, &v.Status, &v.Reason, &tracesBytes, &v.TimeoutSecs, &v.CreatedAt, &v.UpdatedAt)
		if err != nil {
			return err
		}
		if len(tracesBytes) > 0 {
			json.Unmarshal(tracesBytes, &v.Traces)
		}
	default:
		return fmt.Errorf("unsupported entity type")
	}
	return nil
}

// ListDag 列出DAG
func (s *Store) ListDag(input *mod.ListDagInput) ([]*entity.Dag, error) {
	query := fmt.Sprintf("SELECT id, name, created_at, updated_at, vars, tasks FROM %s", s.dagTable)
	
	rows, err := s.db.Queryx(query)
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", s.dagTable, err)
	}
	defer rows.Close()

	var ret []*entity.Dag
	for rows.Next() {
		dag := &entity.Dag{}
		if err := s.scanRow(rows, dag); err != nil {
			return nil, err
		}
		ret = append(ret, dag)
	}

	return ret, nil
}

// ListDagInstance 列出DAG实例
func (s *Store) ListDagInstance(input *mod.ListDagInstanceInput) ([]*entity.DagInstance, error) {
	query := fmt.Sprintf("SELECT id, dag_id, status, worker, reason, share_data, vars, cmd, created_at, updated_at FROM %s WHERE 1=1", s.dagInsTable)
	var args []interface{}

	if len(input.Status) > 0 {
		placeholders := make([]string, len(input.Status))
		for i, status := range input.Status {
			placeholders[i] = "?"
			args = append(args, status)
		}
		query += fmt.Sprintf(" AND status IN (%s)", fmt.Sprintf("%s", placeholders[0]))
		for i := 1; i < len(placeholders); i++ {
			query += fmt.Sprintf(", %s", placeholders[i])
		}
		query += ")"
	}

	if input.Worker != "" {
		query += " AND worker = ?"
		args = append(args, input.Worker)
	}

	if input.UpdatedEnd > 0 {
		query += " AND updated_at <= ?"
		args = append(args, input.UpdatedEnd)
	}

	if input.HasCmd {
		query += " AND cmd IS NOT NULL"
	}

	if input.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, input.Limit)
	}

	rows, err := s.db.Queryx(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", s.dagInsTable, err)
	}
	defer rows.Close()

	var ret []*entity.DagInstance
	for rows.Next() {
		dagIns := &entity.DagInstance{}
		if err := s.scanRow(rows, dagIns); err != nil {
			return nil, err
		}
		ret = append(ret, dagIns)
	}

	return ret, nil
}

// ListTaskInstance 列出任务实例
func (s *Store) ListTaskInstance(input *mod.ListTaskInstanceInput) ([]*entity.TaskInstance, error) {
	selectFields := "id, dag_ins_id, task_id, action_name, status, reason, traces, timeout_secs, created_at, updated_at"
	if len(input.SelectField) > 0 {
		selectFields = fmt.Sprintf("%s", input.SelectField[0])
		for i := 1; i < len(input.SelectField); i++ {
			selectFields += fmt.Sprintf(", %s", input.SelectField[i])
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE 1=1", selectFields, s.taskInsTable)
	var args []interface{}

	if len(input.IDs) > 0 {
		placeholders := make([]string, len(input.IDs))
		for i, id := range input.IDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += fmt.Sprintf(" AND id IN (%s)", fmt.Sprintf("%s", placeholders[0]))
		for i := 1; i < len(placeholders); i++ {
			query += fmt.Sprintf(", %s", placeholders[i])
		}
		query += ")"
	}

	if len(input.Status) > 0 {
		placeholders := make([]string, len(input.Status))
		for i, status := range input.Status {
			placeholders[i] = "?"
			args = append(args, status)
		}
		query += fmt.Sprintf(" AND status IN (%s)", fmt.Sprintf("%s", placeholders[0]))
		for i := 1; i < len(placeholders); i++ {
			query += fmt.Sprintf(", %s", placeholders[i])
		}
		query += ")"
	}

	if input.Expired {
		query += " AND updated_at <= (UNIX_TIMESTAMP() - 5 - timeout_secs)"
	}

	if input.DagInsID != "" {
		query += " AND dag_ins_id = ?"
		args = append(args, input.DagInsID)
	}

	rows, err := s.db.Queryx(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", s.taskInsTable, err)
	}
	defer rows.Close()

	var ret []*entity.TaskInstance
	for rows.Next() {
		taskIns := &entity.TaskInstance{}
		if err := s.scanRow(rows, taskIns); err != nil {
			return nil, err
		}
		ret = append(ret, taskIns)
	}

	return ret, nil
}

// BatchDeleteDag 批量删除DAG
func (s *Store) BatchDeleteDag(ids []string) error {
	return s.genericBatchDelete(ids, s.dagTable)
}

// BatchDeleteDagIns 批量删除DAG实例
func (s *Store) BatchDeleteDagIns(ids []string) error {
	return s.genericBatchDelete(ids, s.dagInsTable)
}

// BatchDeleteTaskIns 批量删除任务实例
func (s *Store) BatchDeleteTaskIns(ids []string) error {
	return s.genericBatchDelete(ids, s.taskInsTable)
}

func (s *Store) genericBatchDelete(ids []string, tableName string) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", tableName, fmt.Sprintf("%s", placeholders[0]))
	for i := 1; i < len(placeholders); i++ {
		query += fmt.Sprintf(", %s", placeholders[i])
	}
	query += ")"

	_, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("delete from %s failed: %w", tableName, err)
	}

	return nil
}

// Marshal 序列化对象
func (s *Store) Marshal(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// Unmarshal 反序列化对象
func (s *Store) Unmarshal(bytes []byte, ptr interface{}) error {
	return json.Unmarshal(bytes, ptr)
}

// isDuplicateKeyError 检查是否为重复键错误
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	// MySQL的重复键错误代码是1062
	return fmt.Sprintf("%v", err) == "Error 1062"
}