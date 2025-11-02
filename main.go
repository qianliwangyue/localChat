package main

import (
	"fmt"
	"localchat/types"
	"math/rand"

	"time"
)

func main() {
	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	fmt.Println("=== Go聊天客户端 ===")
	fmt.Println("正在启动...")

	client := types.NewChatClient()
	client.Start()
}
