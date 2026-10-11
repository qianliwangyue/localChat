package types

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"localchat/pkg/common"
	"sync/atomic"

	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"net"
	"sync"
)

// fileOfferRequest 待用户确认的文件/文件夹请求
type fileOfferRequest struct {
	msg  *common.Message
	conn net.Conn
}

type ChatClient struct {
	name           string
	udpConn        *net.UDPConn
	tcpListener    net.Listener
	tcpPort        int
	udpPort        int
	peers          map[string]*common.Peer // IP:Port -> Peer
	peersMutex     sync.RWMutex
	online         atomic.Bool
	localIP        string
	inChatSession  bool
	currentChat    string // 当前聊天对象
	chatConn       net.Conn
	chatMutex      sync.Mutex
	lastPeersHash  string          // 上次在线用户列表的哈希值
	announcedPeers map[string]bool // 已经公告过的用户
	// 文件传输相关字段
	fileMutex    sync.Mutex
	transmitting bool
	receiving    bool
	fileDataDir  string
	// 传输中的文件映射
	sendingFiles   map[string]*TransferringFile // fileID -> 文件信息
	receivingFiles map[string]*TransferringFile // fileID -> 文件信息
	// 文件夹传输相关字段
	sendingFolders   map[string]*TransferringFolder // folderID -> 文件夹信息
	receivingFolders map[string]*TransferringFolder // folderID -> 文件夹信息

	// 事件回调（供GUI使用，均为可选，需在Start之前设置）
	OnLog           func(text string)                                                         // 系统日志/提示信息
	OnPeersChanged  func(peers []common.Peer)                                                 // 在线用户列表变化
	OnChatStarted   func(peerName string)                                                     // 聊天会话开始
	OnChatMessage   func(from, content string)                                                // 收到聊天消息
	OnChatEnded     func()                                                                    // 聊天会话结束
	OnFileOffer     func(from, fileName string, size int64, fileID string)                    // 收到文件请求
	OnFolderOffer   func(from, folderName string, size int64, fileCount int, folderID string) // 收到文件夹请求
	OnFileProgress  func(pct int, sending bool, label string)                                 // 传输进度
	OnTransferEnded func(sending bool, text string)                                           // 传输结束（完成/拒绝/取消）
	OnFileSaved     func(fileName, savePath string)                                           // 单文件接收完成（含保存路径）

	// 待用户确认的传输请求
	pendingFileOffers   map[string]*fileOfferRequest // fileID -> 请求
	pendingFolderOffers map[string]*fileOfferRequest // folderID -> 请求
}

func NewChatClient() *ChatClient {
	// 创建文件保存目录
	dataDir := "received_files"
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		os.Mkdir(dataDir, 0755)
	}

	client := &ChatClient{
		peers:               make(map[string]*common.Peer),
		localIP:             getLocalIP(),
		announcedPeers:      make(map[string]bool),
		fileDataDir:         dataDir,
		transmitting:        false,
		receiving:           false,
		sendingFiles:        make(map[string]*TransferringFile),
		receivingFiles:      make(map[string]*TransferringFile),
		sendingFolders:      make(map[string]*TransferringFolder),
		receivingFolders:    make(map[string]*TransferringFolder),
		pendingFileOffers:   make(map[string]*fileOfferRequest),
		pendingFolderOffers: make(map[string]*fileOfferRequest),
	}
	client.online.Store(true)
	return client
}

// logf 输出系统日志（GUI通过OnLog回调接收）
func (c *ChatClient) logf(format string, args ...any) {
	if c.OnLog != nil {
		c.OnLog(fmt.Sprintf(format, args...))
	}
}

// emitPeersChanged 通知GUI在线用户列表已变化
func (c *ChatClient) emitPeersChanged() {
	if c.OnPeersChanged == nil {
		return
	}
	c.peersMutex.RLock()
	peers := make([]common.Peer, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, *p)
	}
	c.peersMutex.RUnlock()
	c.OnPeersChanged(peers)
}

func (c *ChatClient) Start() {
	// 启动UDP广播监听
	if err := c.startUDPListener(); err != nil {
		c.logf("启动UDP监听失败: %v，使用回环地址模式...", err)
		c.localIP = "127.0.0.1"
		// 重新尝试UDP监听
		c.startUDPListener()
	}

	// 启动TCP监听
	if err := c.startTCPListener(); err != nil {
		c.logf("启动TCP监听失败: %v", err)
		return
	}

	c.logf("客户端启动成功! IP: %s, TCP端口: %d, UDP端口: %d", c.localIP, c.tcpPort, c.udpPort)

	// 广播上线通知
	go c.broadcastOnline()

	// 主动发现已在线用户
	go c.activeDiscovery()

	// 定期发送发现消息
	go c.periodicDiscover()
}

// SetName 设置用户名
func (c *ChatClient) SetName(name string) {
	if name != "" {
		c.name = name
	}
}

// GetName 获取用户名
func (c *ChatClient) GetName() string {
	return c.name
}

// 启动UDP广播监听
func (c *ChatClient) startUDPListener() error {
	// 尝试绑定可用的UDP端口
	for port := common.BroadcastPort; port < common.BroadcastPort+100; port++ {
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
	return fmt.Errorf("无法绑定UDP端口范围 %d-%d", common.BroadcastPort, common.BroadcastPort+100)
}

// 处理UDP消息
func (c *ChatClient) handleUDPMessages() {
	defer func() {
		if c.udpConn != nil {
			c.udpConn.Close()
		}
	}()

	buffer := make([]byte, 1024)
	for c.online.Load() {
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
			if c.online.Load() {
				c.logf("读取UDP消息失败: %v", err)
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
		if isNew {
			// 只有新用户才公告上线（聊天中不打断会话，仅静默刷新列表）
			if !c.inChatSession {
				c.logf("[系统] %s 上线了!", msg.From)
			}
			// 发现新用户时，再次广播自己的上线通知（无论是否正在聊天）
			go c.broadcastOnline()
		}
		if changed {
			// 无论是否在聊天会话中，在线用户列表都必须同步刷新
			c.printOnlinePeers()
		}
	case common.MsgTypeOffline:
		// 处理下线通知
		changed := c.removePeerByIPAndPort(srcIP, msg.Port)
		// 下线的是当前聊天对象：结束会话
		if c.inChatSession && msg.From == c.currentChat {
			c.logf("%s 已下线，聊天自动结束", msg.From)
			c.endChatSession()
		} else if changed && !c.inChatSession {
			c.logf("[系统] %s 下线了", msg.From)
		}
		if changed {
			// 无论是否在聊天会话中，在线用户列表都必须同步刷新
			c.printOnlinePeers()
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
	destAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", destIP, common.BroadcastPort))
	if c.udpConn != nil {
		c.udpConn.WriteToUDP(data, destAddr)
	}
}

// 启动TCP监听
func (c *ChatClient) startTCPListener() error {
	// 尝试绑定可用的TCP端口
	for port := common.TCPPortStart; port < common.TCPPortStart+100; port++ {
		// 使用 0.0.0.0 绑定所有接口
		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
		if err == nil {
			c.tcpListener = listener
			c.tcpPort = port
			go c.acceptTCPConnections()
			return nil
		}
	}
	return fmt.Errorf("无法绑定TCP端口范围 %d-%d", common.TCPPortStart, common.TCPPortStart+100)
}

// 接受TCP连接
func (c *ChatClient) acceptTCPConnections() {
	for c.online.Load() {
		conn, err := c.tcpListener.Accept()
		if err != nil {
			if c.online.Load() {
				c.logf("TCP接受连接失败: %v", err)
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
	c.logf("接收到来自 %s 的TCP连接", remoteAddr.IP)

	reader := bufio.NewReader(conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			// 连接断开，只有在聊天会话中才提示
			if c.inChatSession && c.chatConn == conn {
				c.logf("连接断开，聊天结束")
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
					c.logf("%s 结束了聊天", msg.From)
					c.endChatSession()
				} else {
					if c.OnChatMessage != nil {
						c.OnChatMessage(msg.From, msg.Content)
					}
				}
			} else {
				// 不在聊天会话中，自动开始聊天
				// 保存连接并开始聊天会话，之后由receiveChatMessages独占读取该连接
				c.chatMutex.Lock()
				c.chatConn = conn
				c.currentChat = msg.From
				c.inChatSession = true
				c.chatMutex.Unlock()

				if c.OnChatStarted != nil {
					c.OnChatStarted(msg.From)
				}
				if c.OnChatMessage != nil {
					c.OnChatMessage(msg.From, msg.Content)
				}

				// 继续接收消息（同步调用保持单一读取者，返回后由defer关闭连接）
				c.receiveChatMessages(conn)
				return
			}
		case common.MsgTypeFileOffer:
			// 处理文件发送请求
			go c.handleFileOffer(&msg, conn)
		case common.MsgTypeFileData:
			// 同步处理文件数据：消息按TCP顺序到达，同步写入保证数据块严格按序落盘，
			// 也避免每个数据块新开goroutine并发访问接收状态
			c.receiveFileData(&msg)
		case common.MsgTypeFileAccept, common.MsgTypeFileReject, common.MsgTypeFileDone, common.MsgTypeFileCancel:
			// 处理其他文件传输相关消息
			go c.handleFileMessage(&msg, conn)
		case common.MsgTypeFolderOffer:
			// 处理文件夹发送请求
			go c.handleFolderOffer(&msg, conn)
		case common.MsgTypeFolderAccept, common.MsgTypeFolderReject, common.MsgTypeFolderDone:
			// 处理文件夹传输相关消息
			go c.handleFolderMessage(&msg, conn)
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
		c.logf("用户 %s 不在线或不存在", targetName)
		c.emitPeersChanged()
		return false
	}

	// 尝试多种连接方式
	addresses := c.generateConnectionAddresses(targetPeer.IP, targetPeer.Port)

	var conn net.Conn
	var err error

	for i, address := range addresses {
		c.logf("连接尝试 %d/%d 到 %s...", i+1, len(addresses), address)

		conn, err = net.DialTimeout("tcp", address, 5*time.Second)
		if err == nil {
			break
		}
		c.logf("连接尝试 %d/%d 失败: %v", i+1, len(addresses), err)

		if i < len(addresses)-1 {
			time.Sleep(1 * time.Second)
		}
	}

	if err != nil {
		c.logf("连接用户 %s 失败，已尝试所有地址（可能已下线、被防火墙拦截或网络异常）", targetName)

		// 从对等列表中移除，因为无法连接
		c.removePeerByIPAndPort(targetPeer.IP, targetPeer.Port)
		return false
	}

	c.logf("成功连接到 %s", targetName)

	c.chatMutex.Lock()
	c.chatConn = conn
	c.currentChat = targetName
	c.inChatSession = true
	c.chatMutex.Unlock()

	if c.OnChatStarted != nil {
		c.OnChatStarted(targetName)
	}

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
		c.logf("发送初始消息失败: %v", err)
		c.endChatSession()
		return false
	}

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
				c.logf("连接断开: %v", err)
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
				c.logf("%s 结束了聊天", msg.From)
				c.endChatSession()
			} else if c.OnChatMessage != nil {
				c.OnChatMessage(msg.From, msg.Content)
			}
		case common.MsgTypeFileOffer:
			// 处理文件发送请求
			go c.handleFileOffer(&msg, conn)
		case common.MsgTypeFileData:
			// 同步处理文件数据：消息按TCP顺序到达，同步写入保证数据块严格按序落盘，
			// 也避免每个数据块新开goroutine并发访问接收状态
			c.receiveFileData(&msg)
		case common.MsgTypeFileAccept, common.MsgTypeFileReject, common.MsgTypeFileDone, common.MsgTypeFileCancel:
			// 处理其他文件传输相关消息
			go c.handleFileMessage(&msg, conn)
		case common.MsgTypeFolderOffer:
			// 处理文件夹发送请求
			go c.handleFolderOffer(&msg, conn)
		case common.MsgTypeFolderAccept, common.MsgTypeFolderReject, common.MsgTypeFolderDone:
			// 处理文件夹传输相关消息
			go c.handleFolderMessage(&msg, conn)
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
		c.logf("发送消息失败: %v", err)
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

		// 清理未完成的传输：关闭文件句柄、复位传输状态，
		// 否则结束聊天后 transmitting 卡死导致无法再次发送文件
		c.fileMutex.Lock()
		for id, tf := range c.sendingFiles {
			if tf.File != nil {
				tf.File.Close()
			}
			delete(c.sendingFiles, id)
		}
		for id, tf := range c.receivingFiles {
			if tf.File != nil {
				tf.File.Close()
			}
			delete(c.receivingFiles, id)
		}
		c.sendingFolders = make(map[string]*TransferringFolder)
		c.receivingFolders = make(map[string]*TransferringFolder)
		c.pendingFileOffers = make(map[string]*fileOfferRequest)
		c.pendingFolderOffers = make(map[string]*fileOfferRequest)
		c.transmitting = false
		c.receiving = false
		c.fileMutex.Unlock()

		if c.OnChatEnded != nil {
			c.OnChatEnded()
		}
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
	c.emitPeersChanged()
}

// 强制刷新在线用户列表（用于/list命令）
func (c *ChatClient) forcePrintOnlinePeers() {
	c.emitPeersChanged()
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
}

// 主动发现已在线用户
func (c *ChatClient) activeDiscovery() {
	// 等待UDP监听器完全启动
	time.Sleep(2 * time.Second)

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
}

// 定期发送发现消息
func (c *ChatClient) periodicDiscover() {
	time.Sleep(10 * time.Second) // 等待初始发现完成

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for c.online.Load() {
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
	for port := common.BroadcastPort; port < common.BroadcastPort+100; port++ {
		addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("255.255.255.255:%d", port))
		c.udpConn.WriteTo(data, addr)
	}

	// 也发送到本地网络广播地址
	if c.localIP != "127.0.0.1" {
		ipParts := strings.Split(c.localIP, ".")
		if len(ipParts) == 4 {
			broadcastIP := fmt.Sprintf("%s.%s.%s.255", ipParts[0], ipParts[1], ipParts[2])
			for port := common.BroadcastPort; port < common.BroadcastPort+100; port++ {
				addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", broadcastIP, port))
				c.udpConn.WriteTo(data, addr)
			}
		}
	}
}

// ListPeers 显示在线用户
func (c *ChatClient) ListPeers() {
	c.forcePrintOnlinePeers()
}

// StartChat 开始与指定用户聊天
func (c *ChatClient) StartChat(targetName string) bool {
	if targetName == c.name {
		c.logf("不能与自己聊天")
		return false
	}
	return c.startChatSession(targetName)
}

// EndChat 结束当前聊天
func (c *ChatClient) EndChat() {
	c.endChatSession()
}

// IsInChat 检查是否在聊天会话中
func (c *ChatClient) IsInChat() bool {
	return c.inChatSession
}

// GetCurrentChat 获取当前聊天对象
func (c *ChatClient) GetCurrentChat() string {
	return c.currentChat
}

// SendFile 发送文件
func (c *ChatClient) SendFile(filePath string) bool {
	if !c.inChatSession {
		c.logf("请先开始一个聊天会话")
		return false
	}

	c.sendFile(filePath)
	return true
}

// SendFolder 发送文件夹
func (c *ChatClient) SendFolder(folderPath string) bool {
	if !c.inChatSession {
		c.logf("请先开始一个聊天会话")
		return false
	}

	c.sendFolder(folderPath)
	return true
}

// IsTransmitting 检查是否正在传输文件
func (c *ChatClient) IsTransmitting() bool {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	return c.transmitting
}

// IsReceiving 检查是否正在接收文件
func (c *ChatClient) IsReceiving() bool {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	return c.receiving
}

// IsOnline 检查客户端是否在线
func (c *ChatClient) IsOnline() bool {
	return c.online.Load()
}

// 停止客户端
func (c *ChatClient) Stop() {
	if !c.online.Load() {
		return
	}

	c.online.Store(false)

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

	c.logf("聊天客户端已退出")
}

// 获取本地IP
func getLocalIP() string {
	// 通过UDP拨号探测默认路由出口地址（UDP dial不会真正发送数据）
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
			ip := addr.IP.To4()
			conn.Close()
			if ip != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				return ip.String()
			}
		} else {
			conn.Close()
		}
	}

	// 兜底：扫描网卡，优先取私有网段IPv4，跳过链路本地地址
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	fallback := ""
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			s := ip4.String()
			if strings.HasPrefix(s, "169.254.") {
				continue // 链路本地地址，未连接网络
			}
			if isPrivateIP(s) {
				return s
			}
			if fallback == "" {
				fallback = s
			}
		}
	}
	if fallback != "" {
		return fallback
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
		c.logf("正在传输其他文件，请稍后再试")
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
		c.logf("无法访问文件: %v", err)
		return
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		c.logf("获取文件信息失败: %v", err)
		return
	}

	// 检查是否为文件
	if fileInfo.IsDir() {
		file.Close()
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		c.logf("不能发送目录，请选择文件")
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

	if !c.writeProtocolMsg(&fileOfferMsg) {
		c.cleanupSendingFile(fileID)
		c.logf("发送文件请求失败：会话已结束")
		return
	}

	c.logf("发送文件请求: %s (%s)，等待对方接受...", fileName, FormatFileSize(fileSize))
}

// SendMessage 发送聊天消息（GUI使用）
func (c *ChatClient) SendMessage(content string) bool {
	if !c.inChatSession {
		return false
	}
	return c.sendChatMessage(content)
}

// writeProtocolMsg 线程安全地向聊天连接写入协议消息，会话已结束时返回false
func (c *ChatClient) writeProtocolMsg(msg *common.Message) bool {
	c.chatMutex.Lock()
	defer c.chatMutex.Unlock()
	if c.chatConn == nil {
		return false
	}
	data, _ := json.Marshal(msg)
	_, err := c.chatConn.Write(append(data, '\n'))
	return err == nil
}

// LocalIP 获取本地IP
func (c *ChatClient) LocalIP() string {
	return c.localIP
}

// TCPPort 获取TCP监听端口
func (c *ChatClient) TCPPort() int {
	return c.tcpPort
}

// UDPPort 获取UDP监听端口
func (c *ChatClient) UDPPort() int {
	return c.udpPort
}

// FileDataDir 获取接收文件的保存目录
func (c *ChatClient) FileDataDir() string {
	return c.fileDataDir
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

		// 如果是普通文件传输，设置receiving为false
		if file.FolderID == "" {
			c.receiving = false
		}
	}
}

// 处理文件发送请求
func (c *ChatClient) handleFileOffer(msg *common.Message, conn net.Conn) {
	c.fileMutex.Lock()
	isFolderFile := msg.FolderID != ""
	if c.receiving && !isFolderFile {
		c.fileMutex.Unlock()
		// 正在接收其他文件，自动拒绝并提示，避免对方莫名收到"拒绝"
		c.logf("正在接收其他文件，已自动拒绝 %s", msg.FileName)
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
	if !isFolderFile {
		c.receiving = true
	}
	c.fileMutex.Unlock()

	if isFolderFile {
		// 是文件夹中的文件，直接接受并使用文件夹保存路径
		c.fileMutex.Lock()
		folderInfo, exists := c.receivingFolders[msg.FolderID]
		c.fileMutex.Unlock()
		if !exists {
			c.logf("找不到对应的文件夹信息")
			return
		}

		var savePath string
		// 根据相对路径创建目录结构
		if msg.RelativePath != "" {
			savePath = filepath.Join(folderInfo.FolderPath, msg.RelativePath)
		} else {
			savePath = filepath.Join(folderInfo.FolderPath, msg.FileName)
		}

		// 创建必要的目录结构
		dir := filepath.Dir(savePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			c.logf("创建目录失败: %v", err)
			return
		}

		c.acceptIncomingFile(msg, conn, savePath)
		return
	}

	// 独立文件：记录请求并通知GUI等待用户决定
	c.fileMutex.Lock()
	c.pendingFileOffers[msg.FileID] = &fileOfferRequest{msg: msg, conn: conn}
	c.fileMutex.Unlock()

	if c.OnFileOffer != nil {
		c.OnFileOffer(msg.From, msg.FileName, msg.FileSize, msg.FileID)
	}
}

// AcceptFile 用户同意接收文件（GUI调用）
func (c *ChatClient) AcceptFile(fileID string) {
	c.fileMutex.Lock()
	req, exists := c.pendingFileOffers[fileID]
	if exists {
		delete(c.pendingFileOffers, fileID)
	}
	c.fileMutex.Unlock()
	if !exists {
		return
	}

	// 创建保存文件的完整路径
	savePath := filepath.Join(c.fileDataDir, req.msg.FileName)

	// 检查文件是否已存在，如果存在则添加时间戳
	if _, err := os.Stat(savePath); err == nil {
		timestamp := time.Now().Format("20060102_150405")
		ext := filepath.Ext(req.msg.FileName)
		baseName := strings.TrimSuffix(req.msg.FileName, ext)
		savePath = filepath.Join(c.fileDataDir, fmt.Sprintf("%s_%s%s", baseName, timestamp, ext))
	}

	c.acceptIncomingFile(req.msg, req.conn, savePath)
}

// RejectFile 用户拒绝接收文件（GUI调用）
func (c *ChatClient) RejectFile(fileID string) {
	c.fileMutex.Lock()
	req, exists := c.pendingFileOffers[fileID]
	if exists {
		delete(c.pendingFileOffers, fileID)
	}
	c.fileMutex.Unlock()
	if !exists {
		return
	}

	// 发送拒绝消息
	rejectMsg := common.Message{
		Type:   common.MsgTypeFileReject,
		From:   c.name,
		FileID: fileID,
	}
	data, _ := json.Marshal(rejectMsg)
	req.conn.Write(append(data, '\n'))

	c.fileMutex.Lock()
	c.receiving = false
	c.fileMutex.Unlock()

	c.logf("已拒绝文件传输")
}

// acceptIncomingFile 创建文件并回复接受，开始接收数据
func (c *ChatClient) acceptIncomingFile(msg *common.Message, conn net.Conn, savePath string) {
	// 创建文件用于保存
	file, err := os.Create(savePath)
	if err != nil {
		c.logf("创建文件失败: %v", err)
		// 发送拒绝消息
		rejectMsg := common.Message{
			Type:   common.MsgTypeFileReject,
			From:   c.name,
			FileID: msg.FileID,
		}
		data, _ := json.Marshal(rejectMsg)
		conn.Write(append(data, '\n'))
		if msg.FolderID == "" {
			c.fileMutex.Lock()
			c.receiving = false
			c.fileMutex.Unlock()
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
		RelativePath: msg.RelativePath,
		FolderID:     msg.FolderID,
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

	if msg.FolderID == "" {
		c.logf("开始接收文件: %s，保存到: %s", msg.FileName, savePath)
	}
}

// 处理文件传输相关消息
func (c *ChatClient) handleFileMessage(msg *common.Message, _ net.Conn) {
	switch msg.Type {
	case common.MsgTypeFileAccept:
		// 对方接受了文件，开始发送文件数据
		go c.sendFileData(msg.FileID)
	case common.MsgTypeFileReject:
		// 对方拒绝了文件，清理发送资源（关闭文件句柄并复位传输状态）
		c.cleanupSendingFile(msg.FileID)
		if c.OnTransferEnded != nil {
			c.OnTransferEnded(true, fmt.Sprintf("%s 拒绝了你的文件", msg.From))
		}
	case common.MsgTypeFileDone:
		// 文件传输完成
		c.fileMutex.Lock()
		var savePath string
		isFolderFile := false
		if tf, exists := c.receivingFiles[msg.FileID]; exists {
			savePath = tf.FilePath
			isFolderFile = tf.FolderID != ""
			tf.File.Close()
			delete(c.receivingFiles, msg.FileID)
		}
		if !isFolderFile {
			c.receiving = false
		}
		c.fileMutex.Unlock()

		if c.OnTransferEnded != nil && !isFolderFile {
			c.OnTransferEnded(false, fmt.Sprintf("文件 '%s' 接收完成", msg.FileName))
		}
		if c.OnFileSaved != nil && !isFolderFile {
			c.OnFileSaved(msg.FileName, savePath)
		}
	case common.MsgTypeFileCancel:
		// 文件传输取消（接收方视角），清理接收资源
		c.cleanupReceivingFile(msg.FileID)
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		if c.OnTransferEnded != nil {
			c.OnTransferEnded(false, "对方取消了文件传输")
		}
	}
}

// 处理文件夹传输相关消息
func (c *ChatClient) handleFolderMessage(msg *common.Message, _ net.Conn) {
	switch msg.Type {
	case common.MsgTypeFolderAccept:
		// 对方接受了文件夹，开始发送文件夹内容
		go c.sendFilesInFolder(msg.FolderID)
	case common.MsgTypeFolderReject:
		// 对方拒绝了文件夹，清理发送资源
		c.cleanupSendingFolder(msg.FolderID)
		if c.OnTransferEnded != nil {
			c.OnTransferEnded(true, fmt.Sprintf("%s 拒绝了你的文件夹", msg.From))
		}
	case common.MsgTypeFolderDone:
		// 文件夹传输完成
		c.fileMutex.Lock()
		folderPath := ""
		if _, exists := c.receivingFolders[msg.FolderID]; exists {
			folderPath = filepath.Join(c.fileDataDir, msg.FolderName)
			delete(c.receivingFolders, msg.FolderID)
			c.receiving = false
		}
		c.fileMutex.Unlock()
		if folderPath != "" && c.OnTransferEnded != nil {
			c.OnTransferEnded(false, fmt.Sprintf("文件夹 '%s' 接收完成，已保存到: %s", msg.FolderName, folderPath))
		}
	}
}

// 扫描文件夹内容
func scanFolder(folderPath string) ([]string, int64, error) {
	var files []string
	var totalSize int64

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
			totalSize += info.Size()
		}
		return nil
	})

	return files, totalSize, err
}

// 发送文件夹
func (c *ChatClient) sendFolder(folderPath string) {
	if !c.inChatSession {
		c.logf("请先开始一个聊天会话")
		return
	}

	// 检查文件夹是否存在
	folderInfo, err := os.Stat(folderPath)
	if err != nil || !folderInfo.IsDir() {
		c.logf("文件夹不存在或无效: %v", err)
		return
	}

	c.fileMutex.Lock()
	if c.transmitting {
		c.fileMutex.Unlock()
		c.logf("正在进行文件传输，请等待完成")
		return
	}
	c.transmitting = true
	c.fileMutex.Unlock()

	// 扫描文件夹内容
	files, totalSize, err := scanFolder(folderPath)
	if err != nil {
		c.logf("扫描文件夹失败: %v", err)
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		return
	}

	if len(files) == 0 {
		c.logf("文件夹为空")
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		return
	}

	// 生成文件夹ID
	folderID := generateFileID()

	// 创建文件夹传输信息
	folderName := folderInfo.Name()
	transferFolder := &TransferringFolder{
		FolderPath:  folderPath,
		FolderName:  folderName,
		FolderSize:  totalSize,
		FileCount:   len(files),
		FolderID:    folderID,
		CurrentFile: 0,
		TotalFiles:  len(files),
		Files:       files,
	}

	c.fileMutex.Lock()
	c.sendingFolders[folderID] = transferFolder
	c.fileMutex.Unlock()

	// 发送文件夹请求
	folderOfferMsg := common.Message{
		Type:       common.MsgTypeFolderOffer,
		From:       c.name,
		FolderName: folderName,
		FolderSize: totalSize,
		FileCount:  len(files),
		FolderID:   folderID,
	}

	if !c.writeProtocolMsg(&folderOfferMsg) {
		c.cleanupSendingFolder(folderID)
		c.logf("发送文件夹请求失败：会话已结束")
		return
	}

	c.logf("发送文件夹请求: %s (%s, %d 个文件)，等待对方接受...", folderName, FormatFileSize(totalSize), len(files))
}

// AcceptFolder 用户同意接收文件夹（GUI调用）
func (c *ChatClient) AcceptFolder(folderID string) {
	c.fileMutex.Lock()
	req, exists := c.pendingFolderOffers[folderID]
	if exists {
		delete(c.pendingFolderOffers, folderID)
	}
	c.fileMutex.Unlock()
	if !exists {
		return
	}

	// 创建保存文件夹的完整路径
	folderSavePath := filepath.Join(c.fileDataDir, req.msg.FolderName)

	// 检查文件夹是否已存在，如果存在则添加时间戳
	if _, err := os.Stat(folderSavePath); err == nil {
		timestamp := time.Now().Format("20060102_150405")
		folderSavePath = filepath.Join(c.fileDataDir, fmt.Sprintf("%s_%s", req.msg.FolderName, timestamp))
	}

	// 创建文件夹
	if err := os.MkdirAll(folderSavePath, 0755); err != nil {
		c.logf("创建文件夹失败: %v", err)
		// 发送拒绝消息
		rejectMsg := common.Message{
			Type:     common.MsgTypeFolderReject,
			From:     c.name,
			FolderID: folderID,
		}
		data, _ := json.Marshal(rejectMsg)
		req.conn.Write(append(data, '\n'))
		c.fileMutex.Lock()
		c.receiving = false
		c.fileMutex.Unlock()
		return
	}

	// 保存文件夹传输信息
	transferFolder := &TransferringFolder{
		FolderPath:  folderSavePath,
		FolderName:  req.msg.FolderName,
		FolderSize:  req.msg.FolderSize,
		FileCount:   req.msg.FileCount,
		FolderID:    folderID,
		CurrentFile: 0,
		TotalFiles:  req.msg.FileCount,
		Files:       []string{},
	}

	c.fileMutex.Lock()
	c.receivingFolders[folderID] = transferFolder
	c.fileMutex.Unlock()

	// 接受文件夹
	acceptMsg := common.Message{
		Type:     common.MsgTypeFolderAccept,
		From:     c.name,
		FolderID: folderID,
	}
	data, _ := json.Marshal(acceptMsg)
	req.conn.Write(append(data, '\n'))

	c.logf("开始接收文件夹: %s，保存到: %s", req.msg.FolderName, folderSavePath)
}

// RejectFolder 用户拒绝接收文件夹（GUI调用）
func (c *ChatClient) RejectFolder(folderID string) {
	c.fileMutex.Lock()
	req, exists := c.pendingFolderOffers[folderID]
	if exists {
		delete(c.pendingFolderOffers, folderID)
	}
	c.fileMutex.Unlock()
	if !exists {
		return
	}

	// 发送拒绝消息
	rejectMsg := common.Message{
		Type:     common.MsgTypeFolderReject,
		From:     c.name,
		FolderID: folderID,
	}
	data, _ := json.Marshal(rejectMsg)
	req.conn.Write(append(data, '\n'))

	c.fileMutex.Lock()
	c.receiving = false
	c.fileMutex.Unlock()

	c.logf("已拒绝文件夹传输")
}

// 处理文件夹发送请求
func (c *ChatClient) handleFolderOffer(msg *common.Message, conn net.Conn) {
	c.fileMutex.Lock()
	if c.receiving {
		c.fileMutex.Unlock()
		// 发送拒绝消息
		rejectMsg := common.Message{
			Type:     common.MsgTypeFolderReject,
			From:     c.name,
			FolderID: msg.FolderID,
		}
		data, _ := json.Marshal(rejectMsg)
		conn.Write(append(data, '\n'))
		return
	}
	c.receiving = true
	c.fileMutex.Unlock()

	// 记录请求并通知GUI等待用户决定
	c.fileMutex.Lock()
	c.pendingFolderOffers[msg.FolderID] = &fileOfferRequest{msg: msg, conn: conn}
	c.fileMutex.Unlock()

	if c.OnFolderOffer != nil {
		c.OnFolderOffer(msg.From, msg.FolderName, msg.FolderSize, msg.FileCount, msg.FolderID)
	}
}

// 发送文件夹中的文件
func (c *ChatClient) sendFilesInFolder(folderID string) {
	c.fileMutex.Lock()
	transferFolder, exists := c.sendingFolders[folderID]
	c.fileMutex.Unlock()
	if !exists {
		c.logf("找不到文件夹传输信息")
		return
	}

	// 发送每个文件
	for transferFolder.CurrentFile < transferFolder.TotalFiles {
		filePath := transferFolder.Files[transferFolder.CurrentFile]

		// 计算相对路径
		relativePath, err := filepath.Rel(transferFolder.FolderPath, filePath)
		if err != nil {
			relativePath = filepath.Base(filePath)
		}

		// 获取文件信息
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			c.logf("获取文件信息失败: %v", err)
			transferFolder.CurrentFile++
			continue
		}

		// 打开文件
		file, err := os.Open(filePath)
		if err != nil {
			c.logf("打开文件失败: %v", err)
			transferFolder.CurrentFile++
			continue
		}

		fileSize := fileInfo.Size()
		fileName := filepath.Base(filePath)
		fileID := generateFileID()
		const chunkSize = 16 * 1024
		totalChunks := int((fileSize + int64(chunkSize) - 1) / int64(chunkSize))

		// 创建传输文件信息
		transferFile := &TransferringFile{
			FilePath:     filePath,
			FileName:     fileName,
			FileSize:     fileSize,
			FileID:       fileID,
			File:         file,
			TotalChunks:  totalChunks,
			CurrentChunk: 0,
			Buffer:       make([]byte, chunkSize),
			RelativePath: relativePath,
			FolderID:     folderID,
		}

		c.fileMutex.Lock()
		c.sendingFiles[fileID] = transferFile
		c.fileMutex.Unlock()

		// 发送文件信息
		fileOfferMsg := common.Message{
			Type:         common.MsgTypeFileOffer,
			From:         c.name,
			FileName:     fileName,
			FileSize:     fileSize,
			FileID:       fileID,
			TotalChunks:  totalChunks,
			FolderID:     folderID,
			RelativePath: relativePath,
		}

		if !c.writeProtocolMsg(&fileOfferMsg) {
			file.Close()
			c.cleanupSendingFile(fileID)
			c.logf("发送文件请求失败：会话已结束")
			return
		}

		// 等待文件接受（在handleFileMessage中处理）
		// 我们需要等待当前文件传输完成
		for {
			c.fileMutex.Lock()
			_, fileExists := c.sendingFiles[fileID]
			c.fileMutex.Unlock()
			if !fileExists {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		transferFolder.CurrentFile++
		fileProgress := int(float64(transferFolder.CurrentFile) / float64(transferFolder.TotalFiles) * 100)
		if c.OnFileProgress != nil {
			c.OnFileProgress(fileProgress, true, fmt.Sprintf("发送文件夹 %s (%d/%d 个文件)", transferFolder.FolderName, transferFolder.CurrentFile, transferFolder.TotalFiles))
		}
	}

	// 发送文件夹完成消息
	doneMsg := common.Message{
		Type:       common.MsgTypeFolderDone,
		From:       c.name,
		FolderID:   folderID,
		FolderName: transferFolder.FolderName,
	}
	c.writeProtocolMsg(&doneMsg)

	c.cleanupSendingFolder(folderID)
	if c.OnTransferEnded != nil {
		c.OnTransferEnded(true, fmt.Sprintf("文件夹 '%s' 发送完成", transferFolder.FolderName))
	}
}

// 清理发送中的文件夹资源
func (c *ChatClient) cleanupSendingFolder(folderID string) {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()

	delete(c.sendingFolders, folderID)

	c.transmitting = false
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
		// 文件夹内的文件传输完成不代表整个文件夹传输结束，
		// transmitting 由 cleanupSendingFolder 在 FolderDone 发出后统一复位，
		// 否则文件夹传输中途 transmitting 会被误复位，允许并发发起新传输
		if file.FolderID == "" {
			c.transmitting = false
		}
	} else {
		c.transmitting = false
	}
}

// 发送文件数据
func (c *ChatClient) sendFileData(fileID string) {
	c.fileMutex.Lock()
	transferFile, exists := c.sendingFiles[fileID]
	c.fileMutex.Unlock()
	if !exists {
		c.logf("找不到文件传输信息")
		return
	}

	const chunkSize = 16 * 1024

	for transferFile.CurrentChunk < transferFile.TotalChunks {
		// 读取文件数据
		n, err := transferFile.File.Read(transferFile.Buffer)
		if err != nil && !errors.Is(err, io.EOF) {
			c.logf("读取文件失败: %v", err)
			// 发送取消消息
			cancelMsg := common.Message{
				Type:   common.MsgTypeFileCancel,
				From:   c.name,
				FileID: fileID,
			}
			c.writeProtocolMsg(&cancelMsg)
			c.cleanupSendingFile(fileID)
			if c.OnTransferEnded != nil {
				c.OnTransferEnded(true, fmt.Sprintf("文件 '%s' 读取失败，传输已取消", transferFile.FileName))
			}
			return
		}

		if n == 0 {
			break
		}

		// 编码文件数据为base64（保证二进制数据能安全通过JSON传输）
		fileData := base64.StdEncoding.EncodeToString(transferFile.Buffer[:n])

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

		if !c.writeProtocolMsg(&dataMsg) {
			c.logf("发送文件数据失败：会话已结束")
			c.cleanupSendingFile(fileID)
			if c.OnTransferEnded != nil {
				c.OnTransferEnded(true, fmt.Sprintf("文件 '%s' 发送失败", transferFile.FileName))
			}
			return
		}

		// 通知进度
		if c.OnFileProgress != nil {
			c.OnFileProgress(progress, true, fmt.Sprintf("发送文件 %s", transferFile.FileName))
		}

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
	c.writeProtocolMsg(&doneMsg)

	c.cleanupSendingFile(fileID)
	if c.OnTransferEnded != nil {
		c.OnTransferEnded(true, fmt.Sprintf("文件 '%s' 发送完成", transferFile.FileName))
	}
}

// 接收文件数据
func (c *ChatClient) receiveFileData(msg *common.Message) {
	c.fileMutex.Lock()
	transferFile, exists := c.receivingFiles[msg.FileID]
	c.fileMutex.Unlock()
	if !exists {
		c.logf("找不到接收文件信息: %s", msg.FileID)
		return
	}

	// 检查数据块索引是否正确
	if msg.ChunkIndex != transferFile.CurrentChunk {
		c.logf("数据块顺序错误，期望: %d, 收到: %d", transferFile.CurrentChunk, msg.ChunkIndex)
		// 可以选择请求重新发送该块
		return
	}

	// 解码base64文件数据
	data, err := base64.StdEncoding.DecodeString(msg.FileData)
	if err != nil {
		c.logf("解码文件数据失败: %v", err)
		return
	}

	// 写入文件数据
	_, err = transferFile.File.Write(data)
	if err != nil {
		c.logf("写入文件失败: %v", err)
		// 发送取消消息
		cancelMsg := common.Message{
			Type:   common.MsgTypeFileCancel,
			From:   c.name,
			FileID: msg.FileID,
		}
		c.writeProtocolMsg(&cancelMsg)
		c.cleanupReceivingFile(msg.FileID)
		if c.OnTransferEnded != nil {
			c.OnTransferEnded(false, fmt.Sprintf("文件 '%s' 保存失败，传输已取消", transferFile.FileName))
		}
		return
	}

	// 更新当前块索引
	transferFile.CurrentChunk++

	// 通知进度
	if c.OnFileProgress != nil {
		c.OnFileProgress(msg.Progress, false, fmt.Sprintf("接收文件 %s", transferFile.FileName))
	}

	// 如果所有数据块都已接收，不需要做特殊处理，因为发送方会发送FileDone消息
}

// FormatFileSize 格式化文件大小
func FormatFileSize(size int64) string {
	const (
		_        = iota
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
