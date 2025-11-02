package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"localchat/pkg/common"
	"net"
	"os"
	"sort"
	"strings"
	"sync"

	"time"
)

const (
	BroadcastPort = 9999  // 广播端口
	TCPPortStart  = 10000 // TCP端口起始
)

type ChatClient struct {
	name           string
	udpConn        *net.UDPConn
	tcpListener    net.Listener
	tcpPort        int
	udpPort        int
	peers          map[string]*common.Peer // IP:Port -> Peer
	peersMutex     sync.RWMutex
	online         bool
	localIP        string
	inChatSession  bool
	currentChat    string // 当前聊天对象
	chatConn       net.Conn
	chatMutex      sync.Mutex
	lastPeersHash  string          // 上次在线用户列表的哈希值
	announcedPeers map[string]bool // 已经公告过的用户
}

func NewChatClient() *ChatClient {
	client := &ChatClient{
		peers:          make(map[string]*common.Peer),
		online:         true,
		localIP:        getLocalIP(),
		announcedPeers: make(map[string]bool),
	}
	return client
}

func (c *ChatClient) Start() {
	// 获取用户名
	c.getName()

	// 启动UDP广播监听
	if err := c.startUDPListener(); err != nil {
		fmt.Printf("启动UDP监听失败: %v\n", err)
		fmt.Println("使用回环地址模式...")
		c.localIP = "127.0.0.1"
		// 重新尝试UDP监听
		c.startUDPListener()
	}

	// 启动TCP监听
	if err := c.startTCPListener(); err != nil {
		fmt.Printf("启动TCP监听失败: %v\n", err)
		return
	}

	fmt.Printf("客户端启动成功! IP: %s, TCP端口: %d, UDP端口: %d\n", c.localIP, c.tcpPort, c.udpPort)

	// 广播上线通知
	go c.broadcastOnline()

	// 主动发现已在线用户
	go c.activeDiscovery()

	// 定期发送发现消息
	go c.periodicDiscover()

	// 处理用户输入
	c.handleUserInput()
}

// 获取用户名称
func (c *ChatClient) getName() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("请输入您的中文代号: ")
		name, _ := reader.ReadString('\n')
		name = strings.TrimSpace(name)
		if name != "" {
			c.name = name
			break
		}
		fmt.Println("代号不能为空，请重新输入!")
	}
}

// 启动UDP广播监听
func (c *ChatClient) startUDPListener() error {
	// 尝试绑定可用的UDP端口
	for port := BroadcastPort; port < BroadcastPort+100; port++ {
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
		if err != nil {
			continue
		}

		conn, err := net.ListenUDP("udp", addr)
		if err == nil {
			c.udpConn = conn
			c.udpPort = port
			go c.handleUDPMessages()
			return nil
		}
	}
	return fmt.Errorf("无法绑定UDP端口范围 %d-%d", BroadcastPort, BroadcastPort+100)
}

// 处理UDP消息
func (c *ChatClient) handleUDPMessages() {
	defer func() {
		if c.udpConn != nil {
			c.udpConn.Close()
		}
	}()

	buffer := make([]byte, 1024)
	for c.online {
		if c.udpConn == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		c.udpConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, srcAddr, err := c.udpConn.ReadFromUDP(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if c.online {
				fmt.Printf("读取UDP消息失败: %v\n", err)
			}
			continue
		}

		// 不处理自己发送的消息
		if srcAddr.IP.String() == c.localIP && srcAddr.Port == c.udpPort {
			continue
		}

		var msg common.Message
		if err := json.Unmarshal(buffer[:n], &msg); err != nil {
			continue
		}

		c.handleUDPMessage(&msg, srcAddr.IP.String())
	}
}

// 处理UDP消息
func (c *ChatClient) handleUDPMessage(msg *common.Message, srcIP string) {
	switch msg.Type {
	case common.MsgTypeDiscover:
		// 响应发现请求 - 立即回复在线通知
		c.sendUDPResponse(srcIP)
	case common.MsgTypeOnline:
		// 处理上线通知
		peer := &common.Peer{
			Name: msg.From,
			IP:   srcIP,
			Port: msg.Port,
		}
		changed, isNew := c.addPeer(peer)
		if !c.inChatSession {
			if isNew {
				// 只有新用户才公告上线
				fmt.Printf("🎉 [系统] %s 上线了!\n", msg.From)
			}
			if changed {
				c.printOnlinePeers()
			}
		}
	case common.MsgTypeOffline:
		// 处理下线通知
		changed := c.removePeerByIPAndPort(srcIP, msg.Port)
		if !c.inChatSession {
			if changed {
				fmt.Printf("👋 [系统] %s 下线了\n", msg.From)
				c.printOnlinePeers()
			}
		} else if msg.From == c.currentChat {
			fmt.Printf("\n⚠️  %s 已下线，聊天自动结束\n", msg.From)
			c.endChatSession()
		}
	}
}

// 发送UDP响应
func (c *ChatClient) sendUDPResponse(destIP string) {
	msg := common.Message{
		Type:    common.MsgTypeOnline,
		From:    c.name,
		IP:      c.localIP,
		Port:    c.tcpPort,
		Content: "online",
	}

	data, _ := msg.Serialize()
	destAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", destIP, BroadcastPort))
	if c.udpConn != nil {
		c.udpConn.WriteToUDP(data, destAddr)
	}
}

// 启动TCP监听
func (c *ChatClient) startTCPListener() error {
	// 尝试绑定可用的TCP端口
	for port := TCPPortStart; port < TCPPortStart+100; port++ {
		// 使用 0.0.0.0 绑定所有接口
		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
		if err == nil {
			c.tcpListener = listener
			c.tcpPort = port
			go c.acceptTCPConnections()
			fmt.Printf("TCP监听已启动在 0.0.0.0:%d\n", port)
			return nil
		}
	}
	return fmt.Errorf("无法绑定TCP端口范围 %d-%d", TCPPortStart, TCPPortStart+100)
}

// 接受TCP连接
func (c *ChatClient) acceptTCPConnections() {
	for c.online {
		conn, err := c.tcpListener.Accept()
		if err != nil {
			if c.online {
				fmt.Printf("TCP接受连接失败: %v\n", err)
			}
			continue
		}

		go c.handleIncomingConnection(conn)
	}
}

// 处理传入连接
func (c *ChatClient) handleIncomingConnection(conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().(*net.TCPAddr)
	fmt.Printf("📞 接收到来自 %s 的TCP连接\n", remoteAddr.IP)

	reader := bufio.NewReader(conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			// 连接断开，只有在聊天会话中才提示
			if c.inChatSession && c.chatConn == conn {
				fmt.Printf("\n⚠️  连接断开，聊天结束\n")
				c.endChatSession()
			}
			return
		}

		message = strings.TrimSpace(message)
		if message == "" {
			continue
		}

		var msg common.Message
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			continue
		}

		if msg.Type == common.MsgTypeChat {
			if c.inChatSession {
				// 已经在聊天会话中，直接显示消息
				if msg.Content == "/quit" {
					fmt.Printf("\n%s 结束了聊天\n", msg.From)
					c.endChatSession()
				} else {
					fmt.Printf("[%s]: %s\n", msg.From, msg.Content)
					if c.inChatSession {
						fmt.Print("你: ")
					}
				}
			} else {
				// 不在聊天会话中，自动开始聊天
				fmt.Printf("\n💬 %s 想与你聊天:\n", msg.From)
				fmt.Printf("[%s]: %s\n", msg.From, msg.Content)

				// 保存连接并开始聊天会话
				c.chatMutex.Lock()
				c.chatConn = conn
				c.currentChat = msg.From
				c.inChatSession = true
				c.chatMutex.Unlock()

				fmt.Printf("\n🎯 开始与 %s 聊天 (输入 /quit 结束聊天)\n", msg.From)
				fmt.Println("=====================================")
				fmt.Print("你: ")

				// 继续接收消息
				go c.receiveChatMessages(conn)
			}
		}
	}
}

// 开始聊天会话
func (c *ChatClient) startChatSession(targetName string) bool {
	c.peersMutex.RLock()
	defer c.peersMutex.RUnlock()

	var targetPeer *common.Peer
	for _, peer := range c.peers {
		if peer.Name == targetName {
			targetPeer = peer
			break
		}
	}

	if targetPeer == nil {
		fmt.Printf("用户 %s 不在线或不存在\n", targetName)
		c.forcePrintOnlinePeers()
		return false
	}

	// 尝试多种连接方式
	addresses := c.generateConnectionAddresses(targetPeer.IP, targetPeer.Port)

	var conn net.Conn
	var err error
	var usedAddress string

	for i, address := range addresses {
		fmt.Printf("连接尝试 %d/%d 到 %s...\n", i+1, len(addresses), address)

		conn, err = net.DialTimeout("tcp", address, 5*time.Second)
		if err == nil {
			usedAddress = address
			break
		}
		fmt.Printf("连接尝试 %d/%d 失败: %v\n", i+1, len(addresses), err)

		if i < len(addresses)-1 {
			time.Sleep(1 * time.Second)
		}
	}

	if err != nil {
		fmt.Printf("连接用户 %s 失败，已尝试所有地址\n", targetName)
		fmt.Printf("可能的原因:\n")
		fmt.Printf("1. 目标用户可能已下线\n")
		fmt.Printf("2. 防火墙可能阻止了TCP连接\n")
		fmt.Printf("3. 网络配置问题\n")
		fmt.Printf("4. 目标用户的TCP端口未正确监听\n")

		// 从对等列表中移除，因为无法连接
		c.removePeerByIPAndPort(targetPeer.IP, targetPeer.Port)
		return false
	}

	fmt.Printf("成功连接到 %s 使用地址 %s\n", targetName, usedAddress)

	c.chatMutex.Lock()
	c.chatConn = conn
	c.currentChat = targetName
	c.inChatSession = true
	c.chatMutex.Unlock()

	fmt.Printf("\n🎯 开始与 %s 聊天 (输入 /quit 结束聊天)\n", targetName)
	fmt.Println("=====================================")

	// 启动消息接收goroutine
	go c.receiveChatMessages(conn)

	// 发送初始消息
	initMsg := common.Message{
		Type:    common.MsgTypeChat,
		From:    c.name,
		Content: "你好！",
	}
	data, _ := json.Marshal(initMsg)
	_, err = conn.Write(append(data, '\n'))
	if err != nil {
		fmt.Printf("发送初始消息失败: %v\n", err)
		c.endChatSession()
		return false
	}

	fmt.Print("你: ")
	return true
}

// 生成多种连接地址尝试
func (c *ChatClient) generateConnectionAddresses(ip string, port int) []string {
	addresses := []string{
		fmt.Sprintf("%s:%d", ip, port), // 原始地址
	}

	// 如果是本地IP，尝试回环地址
	if ip == c.localIP || isPrivateIP(ip) {
		addresses = append(addresses, fmt.Sprintf("127.0.0.1:%d", port))
	}

	// 如果是IPv6本地地址，尝试IPv4回环
	if strings.Contains(ip, ":") {
		addresses = append(addresses, fmt.Sprintf("127.0.0.1:%d", port))
	}

	return addresses
}

// 检查是否为私有IP
func isPrivateIP(ip string) bool {
	privateIPBlocks := []string{
		"10.",
		"172.16.", "172.17.", "172.18.", "172.19.",
		"172.20.", "172.21.", "172.22.", "172.23.",
		"172.24.", "172.25.", "172.26.", "172.27.",
		"172.28.", "172.29.", "172.30.", "172.31.",
		"192.168.",
		"169.254.", // 链路本地
	}

	for _, block := range privateIPBlocks {
		if strings.HasPrefix(ip, block) {
			return true
		}
	}

	return false
}

// 接收聊天消息
func (c *ChatClient) receiveChatMessages(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			// 只有在聊天会话中才处理断开
			if c.inChatSession && c.chatConn == conn {
				fmt.Printf("\n⚠️  连接断开: %v\n", err)
				c.endChatSession()
			}
			return
		}

		message = strings.TrimSpace(message)
		if message == "" {
			continue
		}

		var msg common.Message
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			continue
		}

		if msg.Type == common.MsgTypeChat {
			if msg.Content == "/quit" {
				fmt.Printf("\n%s 结束了聊天\n", msg.From)
				c.endChatSession()
			} else {
				fmt.Printf("\n[%s]: %s\n", msg.From, msg.Content)
				if c.inChatSession {
					fmt.Print("你: ")
				}
			}
		}
	}
}

// 发送聊天消息
func (c *ChatClient) sendChatMessage(message string) bool {
	c.chatMutex.Lock()
	defer c.chatMutex.Unlock()

	if c.chatConn == nil {
		return false
	}

	msg := common.Message{
		Type:    common.MsgTypeChat,
		From:    c.name,
		Content: message,
	}

	data, _ := json.Marshal(msg)
	_, err := c.chatConn.Write(append(data, '\n'))
	if err != nil {
		fmt.Printf("发送消息失败: %v\n", err)
		return false
	}
	return true
}

// 结束聊天会话
func (c *ChatClient) endChatSession() {
	c.chatMutex.Lock()
	defer c.chatMutex.Unlock()

	if c.inChatSession {
		// 发送退出消息
		if c.chatConn != nil {
			quitMsg := common.Message{
				Type:    common.MsgTypeChat,
				From:    c.name,
				Content: "/quit",
			}
			data, _ := json.Marshal(quitMsg)
			c.chatConn.Write(append(data, '\n'))
		}

		c.inChatSession = false
		c.currentChat = ""
		if c.chatConn != nil {
			c.chatConn.Close()
			c.chatConn = nil
		}

		fmt.Println("\n=====================================")
		fmt.Println("已退出聊天模式")
		c.printOnlinePeers()
		fmt.Print("> ")
	}
}

// 计算在线用户列表的哈希值
func (c *ChatClient) calculatePeersHash() string {
	c.peersMutex.RLock()
	defer c.peersMutex.RUnlock()

	// 创建一个包含所有用户名的切片并排序
	var names []string
	for _, peer := range c.peers {
		names = append(names, peer.Name)
	}
	sort.Strings(names)

	// 返回排序后的用户名连接作为哈希
	return strings.Join(names, "|")
}

// 检查在线用户列表是否发生变化
func (c *ChatClient) hasPeersChanged() bool {
	currentHash := c.calculatePeersHash()
	if currentHash != c.lastPeersHash {
		c.lastPeersHash = currentHash
		return true
	}
	return false
}

// 添加对等节点，返回是否发生变化和是否是新用户
func (c *ChatClient) addPeer(peer *common.Peer) (bool, bool) {
	key := fmt.Sprintf("%s:%d", peer.IP, peer.Port)
	c.peersMutex.Lock()
	defer c.peersMutex.Unlock()

	// 不添加自己
	if peer.IP == c.localIP && peer.Port == c.tcpPort {
		return false, false
	}

	// 检查是否已存在
	if existing, exists := c.peers[key]; exists {
		// 如果用户名相同，没有变化
		if existing.Name == peer.Name {
			return false, false
		}
		// 用户名变化，更新
		existing.Name = peer.Name
		return true, false
	} else {
		// 新用户
		c.peers[key] = peer

		// 检查是否已经公告过这个用户
		peerID := fmt.Sprintf("%s@%s:%d", peer.Name, peer.IP, peer.Port)
		if c.announcedPeers[peerID] {
			return true, false // 已经公告过，不重复公告
		} else {
			c.announcedPeers[peerID] = true // 标记为已公告
			return true, true               // 是新用户，需要公告
		}
	}
}

// 通过IP和端口移除对等节点，返回是否发生变化
func (c *ChatClient) removePeerByIPAndPort(ip string, port int) bool {
	key := fmt.Sprintf("%s:%d", ip, port)
	c.peersMutex.Lock()
	defer c.peersMutex.Unlock()

	if peer, exists := c.peers[key]; exists {
		// 从已公告列表中移除对应的用户
		peerID := fmt.Sprintf("%s@%s:%d", peer.Name, ip, port)
		delete(c.announcedPeers, peerID)

		delete(c.peers, key)
		return true
	}
	return false
}

// 打印在线用户（仅在发生变化时刷新）
func (c *ChatClient) printOnlinePeers() {
	if !c.hasPeersChanged() {
		return
	}

	c.peersMutex.RLock()
	defer c.peersMutex.RUnlock()

	fmt.Println("\n=== 在线用户列表 ===")
	if len(c.peers) == 0 {
		fmt.Println("   暂无其他在线用户")
	} else {
		for _, peer := range c.peers {
			fmt.Printf("   👤 %s (%s:%d)\n", peer.Name, peer.IP, peer.Port)
		}
	}
	fmt.Printf("   当前用户: %s\n", c.name)
	fmt.Println("===================")
}

// 强制刷新在线用户列表（用于/list命令）
func (c *ChatClient) forcePrintOnlinePeers() {
	c.peersMutex.RLock()
	defer c.peersMutex.RUnlock()

	fmt.Println("\n=== 在线用户列表 ===")
	if len(c.peers) == 0 {
		fmt.Println("   暂无其他在线用户")
	} else {
		for _, peer := range c.peers {
			fmt.Printf("   👤 %s (%s:%d)\n", peer.Name, peer.IP, peer.Port)
		}
	}
	fmt.Printf("   当前用户: %s\n", c.name)
	fmt.Println("===================")
}

// 广播上线通知
func (c *ChatClient) broadcastOnline() {
	// 等待一下确保监听器都启动了
	time.Sleep(1 * time.Second)

	msg := common.Message{
		Type:    common.MsgTypeOnline,
		From:    c.name,
		IP:      c.localIP,
		Port:    c.tcpPort,
		Content: "online",
	}

	c.sendBroadcast(&msg)
	fmt.Println("已广播上线通知")
}

// 主动发现已在线用户
func (c *ChatClient) activeDiscovery() {
	// 等待UDP监听器完全启动
	time.Sleep(2 * time.Second)

	fmt.Println("正在发现已在线用户...")

	// 发送多次发现请求，确保能发现所有在线用户
	for i := 0; i < 5; i++ {
		msg := common.Message{
			Type:    common.MsgTypeDiscover,
			From:    c.name,
			IP:      c.localIP,
			Port:    c.tcpPort,
			Content: "active_discover",
		}
		c.sendBroadcast(&msg)
		time.Sleep(500 * time.Millisecond)
	}

	// 等待响应
	time.Sleep(3 * time.Second)

	// 显示初始在线用户列表
	fmt.Println("发现完成!")
	c.forcePrintOnlinePeers()
}

// 定期发送发现消息
func (c *ChatClient) periodicDiscover() {
	time.Sleep(10 * time.Second) // 等待初始发现完成

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for c.online {
		msg := common.Message{
			Type:    common.MsgTypeDiscover,
			From:    c.name,
			IP:      c.localIP,
			Port:    c.tcpPort,
			Content: "periodic_discover",
		}
		c.sendBroadcast(&msg)
		<-ticker.C
	}
}

// 发送广播消息
func (c *ChatClient) sendBroadcast(msg *common.Message) {
	if c.udpConn == nil {
		return
	}

	data, _ := msg.Serialize()

	// 广播到所有可能的端口
	for port := BroadcastPort; port < BroadcastPort+100; port++ {
		addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("255.255.255.255:%d", port))
		c.udpConn.WriteTo(data, addr)
	}

	// 也发送到本地网络广播地址
	if c.localIP != "127.0.0.1" {
		ipParts := strings.Split(c.localIP, ".")
		if len(ipParts) == 4 {
			broadcastIP := fmt.Sprintf("%s.%s.%s.255", ipParts[0], ipParts[1], ipParts[2])
			for port := BroadcastPort; port < BroadcastPort+100; port++ {
				addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", broadcastIP, port))
				c.udpConn.WriteTo(data, addr)
			}
		}
	}
}

// 处理用户输入
func (c *ChatClient) handleUserInput() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("\n可用命令:")
	fmt.Println("/list - 显示在线用户")
	fmt.Println("/chat <用户名> - 开始与用户聊天")
	fmt.Println("/quit - 退出程序")
	fmt.Println()

	for c.online {
		if !c.inChatSession {
			fmt.Print("> ")
		}

		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		if c.inChatSession {
			// 聊天会话中的输入处理
			if input == "/quit" {
				fmt.Println("结束聊天...")
				c.endChatSession()
			} else {
				// 发送聊天消息
				if c.sendChatMessage(input) {
					fmt.Print("你: ")
				} else {
					fmt.Println("发送失败，聊天连接已断开")
					c.endChatSession()
				}
			}
		} else {
			// 主菜单输入处理
			switch {
			case input == "/quit" || input == "/exit":
				c.Stop()
				return
			case input == "/list":
				c.forcePrintOnlinePeers()
			case strings.HasPrefix(input, "/chat "):
				parts := strings.SplitN(input, " ", 2)
				if len(parts) >= 2 {
					targetName := strings.TrimSpace(parts[1])
					if targetName == c.name {
						fmt.Println("不能与自己聊天")
					} else {
						c.startChatSession(targetName)
					}
				} else {
					fmt.Println("用法: /chat <用户名>")
					fmt.Println("示例: /chat 张三")
				}
			case input == "/help":
				fmt.Println("\n可用命令:")
				fmt.Println("/list - 显示在线用户")
				fmt.Println("/chat <用户名> - 开始与用户聊天")
				fmt.Println("/quit - 退出程序")
				fmt.Println()
			default:
				fmt.Println("未知命令，输入 /help 查看帮助")
			}
		}
	}
}

// 停止客户端
func (c *ChatClient) Stop() {
	if !c.online {
		return
	}

	c.online = false

	// 发送下线通知
	msg := common.Message{
		Type:    common.MsgTypeOffline,
		From:    c.name,
		IP:      c.localIP,
		Port:    c.tcpPort,
		Content: "offline",
	}
	c.sendBroadcast(&msg)

	if c.udpConn != nil {
		c.udpConn.Close()
	}
	if c.tcpListener != nil {
		c.tcpListener.Close()
	}

	c.endChatSession()

	fmt.Println("聊天客户端已退出")
}

// 获取本地IP
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

func main() {
	fmt.Println("=== Go聊天客户端 ===")
	fmt.Println("正在启动...")

	client := NewChatClient()
	client.Start()
}
