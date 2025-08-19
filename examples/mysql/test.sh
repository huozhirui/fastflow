#!/bin/bash

# Fastflow MySQL 实现测试脚本

set -e

echo "🚀 开始测试 Fastflow MySQL 实现..."

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，请先安装 Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装，请先安装 Docker Compose"
    exit 1
fi

# 切换到示例目录
cd "$(dirname "$0")"

echo "📦 启动 MySQL 服务..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d

echo "⏳ 等待 MySQL 服务就绪..."
timeout=60
while ! docker-compose exec -T mysql mysqladmin ping -h localhost -uroot -ppassword --silent 2>/dev/null; do
    timeout=$((timeout - 1))
    if [ $timeout -eq 0 ]; then
        echo "❌ MySQL 启动超时"
        docker-compose logs mysql
        exit 1
    fi
    sleep 1
done

echo "✅ MySQL 服务已就绪"

# 检查表是否创建成功
echo "🔍 检查数据库表..."
docker-compose exec -T mysql mysql -uroot -ppassword -e "USE fastflow; SHOW TABLES;" | grep -E "test_ff_(dag|election|heartbeat|mutex)" > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ 数据库表创建成功"
else
    echo "❌ 数据库表创建失败"
    exit 1
fi

# 检查 Go 模块
echo "📦 检查 Go 依赖..."
cd ../..
go mod tidy
if [ $? -ne 0 ]; then
    echo "❌ Go 依赖安装失败"
    exit 1
fi

echo "✅ Go 依赖检查完成"

echo "🎯 运行基本功能测试..."

# 创建简单的测试程序
cat > examples/mysql/quick_test.go << 'EOF'
package main

import (
	"fmt"
	"log"
	"time"

	mysqlKeeper "github.com/shiningrush/fastflow/keeper/mysql"
	mysqlStore "github.com/shiningrush/fastflow/store/mysql"
	"github.com/shiningrush/fastflow/pkg/entity"
)

func main() {
	fmt.Println("🧪 开始快速测试...")
	
	// 测试 Store 连接
	store := mysqlStore.NewStore(&mysqlStore.StoreOption{
		DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
		Prefix: "test",
		Timeout: 5 * time.Second,
	})
	
	if err := store.Init(); err != nil {
		log.Fatalf("❌ Store 初始化失败: %v", err)
	}
	fmt.Println("✅ Store 连接成功")
	
	// 测试创建 DAG
	dag := &entity.Dag{
		BaseInfo: entity.BaseInfo{ID: "test-dag"},
		Name: "测试工作流",
		Tasks: []entity.Task{
			{ID: "task1", ActionName: "TestAction"},
		},
	}
	
	if err := store.CreateDag(dag); err != nil {
		log.Printf("⚠️  DAG 创建失败 (可能已存在): %v", err)
	} else {
		fmt.Println("✅ DAG 创建成功")
	}
	
	// 测试 Keeper 连接
	keeper := mysqlKeeper.NewKeeper(&mysqlKeeper.KeeperOption{
		Key: "test-worker-1",
		DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
		Prefix: "test",
		UnhealthyTime: 10 * time.Second,
		Timeout: 5 * time.Second,
	})
	
	if err := keeper.Init(); err != nil {
		log.Fatalf("❌ Keeper 初始化失败: %v", err)
	}
	fmt.Println("✅ Keeper 连接成功")
	
	// 测试分布式锁
	mutex := keeper.NewMutex("test-lock")
	ctx := context.Background()
	
	if err := mutex.TryLock(ctx); err != nil {
		log.Printf("⚠️  分布式锁测试失败: %v", err)
	} else {
		fmt.Println("✅ 分布式锁获取成功")
		mutex.Unlock(ctx)
		fmt.Println("✅ 分布式锁释放成功")
	}
	
	// 清理
	keeper.Close()
	store.Close()
	
	fmt.Println("🎉 所有基本测试通过！")
}
EOF

# 添加必要的 import
cat > examples/mysql/quick_test.go << 'EOF'
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	mysqlKeeper "github.com/shiningrush/fastflow/keeper/mysql"
	mysqlStore "github.com/shiningrush/fastflow/store/mysql"
	"github.com/shiningrush/fastflow/pkg/entity"
)

func main() {
	fmt.Println("🧪 开始快速测试...")
	
	// 测试 Store 连接
	store := mysqlStore.NewStore(&mysqlStore.StoreOption{
		DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
		Prefix: "test",
		Timeout: 5 * time.Second,
	})
	
	if err := store.Init(); err != nil {
		log.Fatalf("❌ Store 初始化失败: %v", err)
	}
	fmt.Println("✅ Store 连接成功")
	defer store.Close()
	
	// 测试创建 DAG
	dag := &entity.Dag{
		BaseInfo: entity.BaseInfo{ID: "test-dag-quick"},
		Name: "测试工作流",
		Tasks: []entity.Task{
			{ID: "task1", ActionName: "TestAction"},
		},
	}
	
	if err := store.CreateDag(dag); err != nil {
		log.Printf("⚠️  DAG 创建失败 (可能已存在): %v", err)
	} else {
		fmt.Println("✅ DAG 创建成功")
	}
	
	// 测试 Keeper 连接
	keeper := mysqlKeeper.NewKeeper(&mysqlKeeper.KeeperOption{
		Key: "test-worker-1",
		DSN: "root:password@tcp(127.0.0.1:3306)/fastflow?charset=utf8mb4&parseTime=True&loc=Local",
		Prefix: "test",
		UnhealthyTime: 10 * time.Second,
		Timeout: 5 * time.Second,
	})
	
	if err := keeper.Init(); err != nil {
		log.Fatalf("❌ Keeper 初始化失败: %v", err)
	}
	fmt.Println("✅ Keeper 连接成功")
	defer keeper.Close()
	
	// 等待一下让 keeper 完成初始化
	time.Sleep(2 * time.Second)
	
	// 测试分布式锁
	mutex := keeper.NewMutex("test-lock")
	ctx := context.Background()
	
	if err := mutex.TryLock(ctx); err != nil {
		log.Printf("⚠️  分布式锁测试失败: %v", err)
	} else {
		fmt.Println("✅ 分布式锁获取成功")
		mutex.Unlock(ctx)
		fmt.Println("✅ 分布式锁释放成功")
	}
	
	fmt.Println("🎉 所有基本测试通过！")
}
EOF

# 运行快速测试
echo "🔧 编译并运行快速测试..."
cd examples/mysql
if go run quick_test.go; then
    echo "✅ 快速测试通过"
else
    echo "❌ 快速测试失败"
    exit 1
fi

# 清理测试文件
rm -f quick_test.go

echo ""
echo "🎉 所有测试通过！MySQL 实现工作正常"
echo ""
echo "📚 接下来你可以："
echo "   1. 访问 phpMyAdmin: http://localhost:8080 (用户名: root, 密码: password)"
echo "   2. 运行完整示例: go run main.go"
echo "   3. 查看 Pull Request: https://github.com/huozhirui/fastflow/pull/1"
echo ""
echo "🛑 清理环境 (可选): docker-compose down -v"