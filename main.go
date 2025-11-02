package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"localchat/pkg/common"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"time"
)

const (
	BroadcastPort = 9999  // 广播端口
	TCPPortStart  = 10000 // TCP端口起始
)

// 传输中的文件信息
type TransferringFile struct {
	FilePath   string
	FileName   string
	FileSize   int64
	FileID     string
	File       *os.File
	TotalChunks int
	CurrentChunk int
	Buffer     []byte
}

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
	// 文件传输相关字段
	fileMutex      sync.Mutex
	transmitting   bool
	receiving      bool
	fileDataDir    string
	// 传输中的文件映射
	sendingFiles   map[string]*TransferringFile    // fileID -> 文件信息
	receivingFiles map[string]*TransferringFile    // fileID -> 文件信息
}

func NewChatClient() *ChatClient {
	// 创建文件保存目录
	dataDir := "received_files"
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		os.Mkdir(dataDir, 0755)
	}

	client := &ChatClient{
		peers:          make(map[string]*common.Peer),
		online:         true,
		localIP:        getLocalIP(),
		announcedPeers: make(map[string]bool),
		fileDataDir:    dataDir,
		transmitting:   false,
		receiving:      false,
		sendingFiles:   make(map[string]*TransferringFile),
		receivingFiles: make(map[string]*TransferringFile),
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

		switch msg.Type {
				case common.MsgTypeChat:
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
				case common.MsgTypeFileOffer:
					// 处理文件发送请求
					go c.handleFileOffer(&msg, conn)
				case common.MsgTypeFileAccept, common.MsgTypeFileReject, common.MsgTypeFileData, common.MsgTypeFileDone, common.MsgTypeFileCancel:
					// 处理其他文件传输相关消息
					go c.handleFileMessage(&msg, conn)
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

		switch msg.Type {
				case common.MsgTypeChat:
					if msg.Content == "/quit" {
						fmt.Printf("\n%s 结束了聊天\n", msg.From)
						c.endChatSession()
					} else {
						fmt.Printf("\n[%s]: %s\n", msg.From, msg.Content)
						if c.inChatSession {
							fmt.Print("你: ")
						}
					}
				case common.MsgTypeFileOffer:
					// 处理文件发送请求
					go c.handleFileOffer(&msg, conn)
				case common.MsgTypeFileAccept, common.MsgTypeFileReject, common.MsgTypeFileData, common.MsgTypeFileDone, common.MsgTypeFileCancel:
					// 处理其他文件传输相关消息
					go c.handleFileMessage(&msg, conn)
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
	fmt.Println("/sendfile <文件路径> - 在聊天中发送文件")
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
			} else if strings.HasPrefix(input, "/sendfile ") {
				parts := strings.SplitN(input, " ", 2)
				if len(parts) >= 2 {
					filePath := strings.TrimSpace(parts[1])
					c.sendFile(filePath)
				} else {
					fmt.Println("用法: /sendfile <文件路径>")
					fmt.Println("示例: /sendfile C:\\Users\\Documents\\example.txt")
					fmt.Print("你: ")
				}
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
			fmt.Println("/sendfile <文件路径> - 在聊天中发送文件")
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

// 生成文件唯一ID
func generateFileID() string {
	return fmt.Sprintf("file_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}

// 发送文件
func (c *ChatClient) sendFile(filePath string) {
	// 检查是否正在传输文件
	c.fileMutex.Lock()
	if c.transmitting {
		c.fileMutex.Unlock()
		fmt.Println("正在传输其他文件，请稍后再试")
		fmt.Print("你: ")
		return
	}
	c.transmitting = true
	c.fileMutex.Unlock()

	// 检查文件是否存在
	file, err := os.Open(filePath)
	if err != nil {
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		fmt.Printf("无法访问文件: %v\n", err)
		fmt.Print("你: ")
		return
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		fmt.Printf("获取文件信息失败: %v\n", err)
		fmt.Print("你: ")
		return
	}

	// 检查是否为文件
	if fileInfo.IsDir() {
		file.Close()
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		fmt.Println("不能发送目录，请选择文件")
		fmt.Print("你: ")
		return
	}

	// 获取文件名和大小
	fileName := fileInfo.Name()
	fileSize := fileInfo.Size()

	// 生成文件ID
	fileID := generateFileID()

	// 计算总块数（每块16KB）
	const chunkSize = 16 * 1024
	totalChunks := int((fileSize + chunkSize - 1) / chunkSize)

	// 保存文件信息
	transferFile := &TransferringFile{
		FilePath:     filePath,
		FileName:     fileName,
		FileSize:     fileSize,
		FileID:       fileID,
		File:         file,
		TotalChunks:  totalChunks,
		CurrentChunk: 0,
		Buffer:       make([]byte, chunkSize),
	}

	c.fileMutex.Lock()
	c.sendingFiles[fileID] = transferFile
	c.fileMutex.Unlock()

	// 发送文件发送请求
	fileOfferMsg := common.Message{
		Type:        common.MsgTypeFileOffer,
		From:        c.name,
		FileName:    fileName,
		FileSize:    fileSize,
		FileID:      fileID,
		TotalChunks: totalChunks,
	}

	data, _ := json.Marshal(fileOfferMsg)
	_, err = c.chatConn.Write(append(data, '\n'))
	if err != nil {
		c.cleanupSendingFile(fileID)
		fmt.Printf("发送文件请求失败: %v\n", err)
		fmt.Print("你: ")
		return
	}

	fmt.Printf("📤 发送文件请求: %s (%s)\n", fileName, formatFileSize(fileSize))
	fmt.Println("等待对方接受...")

	// 等待响应（在handleFileMessage中处理）
	fmt.Print("你: ")
}

// 清理接收中的文件资源
func (c *ChatClient) cleanupReceivingFile(fileID string) {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	
	if file, exists := c.receivingFiles[fileID]; exists {
		if file.File != nil {
			file.File.Close()
		}
		delete(c.receivingFiles, fileID)
	}
	c.receiving = false
}

// 处理文件发送请求
func (c *ChatClient) handleFileOffer(msg *common.Message, conn net.Conn) {
	c.fileMutex.Lock()
	if c.receiving {
		c.fileMutex.Unlock()
		// 发送拒绝消息
		rejectMsg := common.Message{
			Type:   common.MsgTypeFileReject,
			From:   c.name,
			FileID: msg.FileID,
		}
		data, _ := json.Marshal(rejectMsg)
		conn.Write(append(data, '\n'))
		return
	}
	c.receiving = true
	c.fileMutex.Unlock()

	// 显示文件信息并询问是否接受
	fmt.Printf("\n📥 %s 向你发送文件:\n", msg.From)
	fmt.Printf("文件名: %s\n", msg.FileName)
	fmt.Printf("文件大小: %s\n", formatFileSize(msg.FileSize))
	fmt.Print("是否接受文件？(y/n): ")

	reader := bufio.NewReader(os.Stdin)
	response, _ := reader.ReadString('\n')
	response = strings.TrimSpace(response)

	if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
		// 拒绝文件
		rejectMsg := common.Message{
			Type:   common.MsgTypeFileReject,
			From:   c.name,
			FileID: msg.FileID,
		}
		data, _ := json.Marshal(rejectMsg)
		conn.Write(append(data, '\n'))
		fmt.Println("文件传输已取消")
		if c.inChatSession {
			fmt.Print("你: ")
		}
		c.fileMutex.Lock()
		c.receiving = false
		c.fileMutex.Unlock()
		return
	}

	// 创建保存文件的完整路径
	savePath := filepath.Join(c.fileDataDir, msg.FileName)
	
	// 检查文件是否已存在，如果存在则添加时间戳
	if _, err := os.Stat(savePath); err == nil {
		timestamp := time.Now().Format("20060102_150405")
		ext := filepath.Ext(msg.FileName)
		baseName := strings.TrimSuffix(msg.FileName, ext)
		savePath = filepath.Join(c.fileDataDir, fmt.Sprintf("%s_%s%s", baseName, timestamp, ext))
	}

	// 创建文件用于保存
	file, err := os.Create(savePath)
	if err != nil {
		fmt.Printf("创建文件失败: %v\n", err)
		// 发送拒绝消息
		rejectMsg := common.Message{
			Type:   common.MsgTypeFileReject,
			From:   c.name,
			FileID: msg.FileID,
		}
		data, _ := json.Marshal(rejectMsg)
		conn.Write(append(data, '\n'))
		c.fileMutex.Lock()
		c.receiving = false
		c.fileMutex.Unlock()
		if c.inChatSession {
			fmt.Print("你: ")
		}
		return
	}

	// 保存文件传输信息
	transferFile := &TransferringFile{
		FilePath:     savePath,
		FileName:     msg.FileName,
		FileSize:     msg.FileSize,
		FileID:       msg.FileID,
		File:         file,
		TotalChunks:  msg.TotalChunks,
		CurrentChunk: 0,
	}

	c.fileMutex.Lock()
	c.receivingFiles[msg.FileID] = transferFile
	c.fileMutex.Unlock()

	// 接受文件
	acceptMsg := common.Message{
		Type:   common.MsgTypeFileAccept,
		From:   c.name,
		FileID: msg.FileID,
	}
	data, _ := json.Marshal(acceptMsg)
	conn.Write(append(data, '\n'))

	// 准备接收文件数据
	fmt.Printf("开始接收文件: %s\n", msg.FileName)
	fmt.Printf("保存路径: %s\n", savePath)

	if c.inChatSession {
		fmt.Print("你: ")
	}
}

// 处理文件传输相关消息
func (c *ChatClient) handleFileMessage(msg *common.Message, conn net.Conn) {
	switch msg.Type {
	case common.MsgTypeFileAccept:
		// 对方接受了文件，开始发送文件数据
		go c.sendFileData(msg.FileID)
	case common.MsgTypeFileReject:
		// 对方拒绝了文件
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		fmt.Printf("\n❌ %s 拒绝了你的文件\n", msg.From)
		if c.inChatSession {
			fmt.Print("你: ")
		}
	case common.MsgTypeFileData:
		// 接收文件数据
		go c.receiveFileData(msg)
	case common.MsgTypeFileDone:
		// 文件传输完成
		c.fileMutex.Lock()
		c.receiving = false
		c.fileMutex.Unlock()
		fmt.Printf("\n✅ 文件 '%s' 接收完成，已保存到: %s\n", msg.FileName, c.fileDataDir)
		if c.inChatSession {
			fmt.Print("你: ")
		}
	case common.MsgTypeFileCancel:
		// 文件传输取消
		c.fileMutex.Lock()
		c.transmitting = false
		c.receiving = false
		c.fileMutex.Unlock()
		fmt.Printf("\n❌ 文件传输已取消\n")
		if c.inChatSession {
			fmt.Print("你: ")
		}
	}
}

// 清理发送中的文件资源
func (c *ChatClient) cleanupSendingFile(fileID string) {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	
	if file, exists := c.sendingFiles[fileID]; exists {
		if file.File != nil {
			file.File.Close()
		}
		delete(c.sendingFiles, fileID)
	}
	c.transmitting = false
}

// 发送文件数据
func (c *ChatClient) sendFileData(fileID string) {
	c.fileMutex.Lock()
	transferFile, exists := c.sendingFiles[fileID]
	if !exists {
		c.fileMutex.Unlock()
		fmt.Println("找不到文件传输信息")
		return
	}
	c.fileMutex.Unlock()

	fmt.Printf("开始发送文件: %s\n", transferFile.FileName)
	const chunkSize = 16 * 1024

	for transferFile.CurrentChunk < transferFile.TotalChunks {
		// 读取文件数据
		n, err := transferFile.File.Read(transferFile.Buffer)
		if err != nil && err.Error() != "EOF" {
			fmt.Printf("读取文件失败: %v\n", err)
			// 发送取消消息
			cancelMsg := common.Message{
				Type:   common.MsgTypeFileCancel,
				From:   c.name,
				FileID: fileID,
			}
			data, _ := json.Marshal(cancelMsg)
			c.chatConn.Write(append(data, '\n'))
			c.cleanupSendingFile(fileID)
			return
		}

		if n == 0 {
			break
		}

		// 编码文件数据为base64
		fileData := string(transferFile.Buffer[:n])

		// 计算进度
		progress := int(float64(transferFile.CurrentChunk+1) / float64(transferFile.TotalChunks) * 100)

		// 发送数据块
		dataMsg := common.Message{
			Type:        common.MsgTypeFileData,
			From:        c.name,
			FileID:      fileID,
			FileData:    fileData,
			ChunkIndex:  transferFile.CurrentChunk,
			TotalChunks: transferFile.TotalChunks,
			Progress:    progress,
		}

		data, _ := json.Marshal(dataMsg)
		_, err = c.chatConn.Write(append(data, '\n'))
		if err != nil {
			fmt.Printf("发送文件数据失败: %v\n", err)
			c.cleanupSendingFile(fileID)
			return
		}

		// 显示进度
		fmt.Printf("发送进度: %d%% (块 %d/%d)\r", progress, transferFile.CurrentChunk+1, transferFile.TotalChunks)

		transferFile.CurrentChunk++

		// 小延迟避免网络拥塞
		time.Sleep(10 * time.Millisecond)
	}

	// 发送完成消息
	doneMsg := common.Message{
		Type:     common.MsgTypeFileDone,
		From:     c.name,
		FileID:   fileID,
		FileName: transferFile.FileName,
	}
	data, _ := json.Marshal(doneMsg)
	c.chatConn.Write(append(data, '\n'))

	fmt.Printf("\n✅ 文件 '%s' 发送完成\n", transferFile.FileName)
	c.cleanupSendingFile(fileID)
}

// 接收文件数据
func (c *ChatClient) receiveFileData(msg *common.Message) {
	c.fileMutex.Lock()
	transferFile, exists := c.receivingFiles[msg.FileID]
	if !exists {
		c.fileMutex.Unlock()
		fmt.Printf("找不到接收文件信息: %s\n", msg.FileID)
		return
	}
	c.fileMutex.Unlock()

	// 检查数据块索引是否正确
	if msg.ChunkIndex != transferFile.CurrentChunk {
		fmt.Printf("数据块顺序错误，期望: %d, 收到: %d\n", transferFile.CurrentChunk, msg.ChunkIndex)
		// 可以选择请求重新发送该块
		return
	}

	// 写入文件数据
	data := []byte(msg.FileData)
	_, err := transferFile.File.Write(data)
	if err != nil {
		fmt.Printf("写入文件失败: %v\n", err)
		// 发送取消消息
		cancelMsg := common.Message{
			Type:   common.MsgTypeFileCancel,
			From:   c.name,
			FileID: msg.FileID,
		}
		data, _ := json.Marshal(cancelMsg)
		c.chatConn.Write(append(data, '\n'))
		c.cleanupReceivingFile(msg.FileID)
		return
	}

	// 更新当前块索引
	transferFile.CurrentChunk++

	// 显示进度
	fmt.Printf("接收进度: %d%% (块 %d/%d)\r", msg.Progress, transferFile.CurrentChunk, transferFile.TotalChunks)

	// 如果所有数据块都已接收，不需要做特殊处理，因为发送方会发送FileDone消息
}

// 格式化文件大小
func formatFileSize(size int64) string {
	const (
		_ = iota
		KB int64 = 1 << (10 * iota)
		MB
		GB
		TB
	)

	switch {
	case size >= TB:
		return fmt.Sprintf("%.2f TB", float64(size)/float64(TB))
	case size >= GB:
		return fmt.Sprintf("%.2f GB", float64(size)/float64(GB))
	case size >= MB:
		return fmt.Sprintf("%.2f MB", float64(size)/float64(MB))
	case size >= KB:
		return fmt.Sprintf("%.2f KB", float64(size)/float64(KB))
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func main() {
	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())
	
	fmt.Println("=== Go聊天客户端 ===")
	fmt.Println("正在启动...")

	client := NewChatClient()
	client.Start()
}
