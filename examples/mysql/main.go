package main

import (
	"fmt"
	"log"
	"time"

	"github.com/shiningrush/fastflow"
	mysqlKeeper "github.com/shiningrush/fastflow/keeper/mysql"
	"github.com/shiningrush/fastflow/pkg/entity/run"
	"github.com/shiningrush/fastflow/pkg/mod"
	mysqlStore "github.com/shiningrush/fastflow/store/mysql"
)

// PrintAction 示例Action实现
type PrintAction struct{}

// Name 定义唯一的Action标识符，将被Task使用
func (a *PrintAction) Name() string {
	return "PrintAction"
}

// Run 执行Action的主要逻辑
func (a *PrintAction) Run(ctx run.ExecuteContext, params interface{}) error {
	fmt.Printf("Action执行开始: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("Task ID: %s\n", ctx.TaskInstance().TaskID)
	fmt.Printf("DAG Instance ID: %s\n", ctx.DagInstance().ID)
	
	// 模拟一些工作
	time.Sleep(time.Second * 2)
	
	fmt.Printf("Action执行完成: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	return nil
}

// ParameterNew 返回参数的新实例
func (a *PrintAction) ParameterNew() interface{} {
	return nil // 这个示例不使用参数
}

func main() {
	// 注册Action
	fastflow.RegisterAction([]run.Action{
		&PrintAction{},
	})

	// 初始化MySQL Keeper
	// DSN格式: username:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local
	keeper := mysqlKeeper.NewKeeper(&mysqlKeeper.KeeperOption{
		Key: "worker-1", // 工作节点标识，格式必须为 "xxxx-数字"
		DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
		Prefix: "test", // 表名前缀
		UnhealthyTime: 10 * time.Second, // 节点不健康判断时间
		Timeout: 5 * time.Second,        // 操作超时时间
	})

	if err := keeper.Init(); err != nil {
		log.Fatal(fmt.Errorf("初始化keeper失败: %w", err))
	}

	// 初始化MySQL Store
	store := mysqlStore.NewStore(&mysqlStore.StoreOption{
		DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
		Prefix: "test", // 表名前缀，应与keeper保持一致
		Timeout: 5 * time.Second,
	})

	if err := store.Init(); err != nil {
		log.Fatal(fmt.Errorf("初始化store失败: %w", err))
	}

	// 启动DAG创建和执行协程
	go createDagAndInstance()

	// 启动fastflow
	fmt.Println("启动Fastflow (基于MySQL)...")
	if err := fastflow.Start(&fastflow.InitialOption{
		Keeper: keeper,
		Store:  store,
		// 从当前目录读取YAML定义的DAG
		ReadDagFromDir: "./",
	}); err != nil {
		panic(fmt.Sprintf("启动fastflow失败: %s", err))
	}
}

func createDagAndInstance() {
	// 等待fastflow启动完成
	time.Sleep(time.Second * 3)

	fmt.Println("开始创建和执行DAG实例...")

	// 执行一些DAG实例
	for i := 0; i < 5; i++ {
		fmt.Printf("创建DAG实例 #%d\n", i+1)
		
		dagInsId, err := mod.GetCommander().RunDag("test-dag", map[string]string{
			"message": fmt.Sprintf("这是第%d个实例", i+1),
		})
		if err != nil {
			log.Printf("创建DAG实例失败: %v", err)
			continue
		}

		fmt.Printf("DAG实例创建成功，ID: %s\n", dagInsId)
		
		// 等待一段时间再创建下一个实例
		time.Sleep(time.Second * 15)
	}
	
	fmt.Println("DAG实例创建完成")
}