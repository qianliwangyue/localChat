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

	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"net"
	"sync"
)

const (
	// fileChunkSize 单个文件数据块大小。
	// 原实现 16KB + 每块 sleep 10ms，吞吐被硬性锁死在约 1.6MB/s，该 sleep 已删除。
	// 改为二进制帧之后（数据块 = JSON 头 + 换行 + 原始字节，一次 Write 发出），
	// 回环上 128KB 实测约 300MB/s，256KB/512KB 抖动较大且收益不稳（多在大文件时更快），
	// 综合考虑内存占用与续传粒度（续传点按块对齐）仍取 128KB。
	// 接收方按 ChunkIndex 顺序拼接，不依赖块大小，因此与旧版本对端保持互通。
	fileChunkSize = 128 * 1024

	// progressInterval 进度回调的最小间隔。
	// 高速传输时每块都回调会向 UI 线程灌入上千次/秒的刷新请求，反而拖慢传输。
	progressInterval = 100 * time.Millisecond

	// recvReadBufferSize 接收端 bufio 读缓冲大小，需大于单条最大消息（块 + JSON 头）
	recvReadBufferSize = 512 * 1024
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
	currentChat    string // 当前聊天对象代号（仅用于显示）
	currentChatKey string // 当前聊天对象唯一身份 "IP:TCP端口"，代号相同的不同用户以此区分
	chatConn       net.Conn
	chatMutex      sync.RWMutex
	lastPeersHash  string          // 上次在线用户列表的哈希值
	announcedPeers map[string]bool // 已经公告过的用户
	peerTimeout    time.Duration   // 超过该时长无任何存活消息则判定离线
	reapInterval   time.Duration   // 离线收割扫描间隔
	portBase       int             // UDP/TCP端口段偏移（生产为0；测试用不同段隔离SO_REUSEADDR残留抢包）
	// 文件传输相关字段
	fileMutex    sync.Mutex
	fileDoneCond *sync.Cond // 与 fileMutex 绑定：某个发送任务结束时唤醒等待者
	transmitting bool
	receiving    bool
	fileDataDir  string
	dirMutex     sync.RWMutex // 保护 fileDataDir（运行期可由设置页修改）
	// 传输中的文件映射
	sendingFiles   map[string]*TransferringFile // fileID -> 文件信息
	receivingFiles map[string]*TransferringFile // fileID -> 文件信息
	// 文件夹传输相关字段
	sendingFolders   map[string]*TransferringFolder // folderID -> 文件夹信息
	receivingFolders map[string]*TransferringFolder // folderID -> 文件夹信息

	// 事件回调（供GUI使用，均为可选，需在Start之前设置）
	OnLog             func(text string)                                                                   // 系统日志/提示信息
	OnPeersChanged    func(peers []common.Peer)                                                           // 在线用户列表变化
	OnChatStarted     func(peerName string)                                                               // 聊天会话开始
	OnChatMessage     func(from, content string)                                                          // 收到聊天消息
	OnChatEnded       func()                                                                              // 聊天会话结束
	OnFileOffer       func(from, fromAddr, fileName string, size int64, fileID string)                    // 收到文件请求（fromAddr为发送者IP，用于同名用户区分）
	OnFolderOffer     func(from, fromAddr, folderName string, size int64, fileCount int, folderID string) // 收到文件夹请求
	OnFileProgress    func(pct int, sending bool, label string)                                           // 传输进度
	OnTransferEnded   func(sending bool, text string)                                                     // 传输结束（完成/拒绝/取消）
	OnFileSaved       func(fileName, savePath string)                                                     // 单文件接收完成（含保存路径）
	OnTransferResumed func(fileName string, offset, total int64)                                          // 断点续传开始（已续传偏移/文件总大小）

	// 待用户确认的传输请求
	pendingFileOffers   map[string]*fileOfferRequest // fileID -> 请求
	pendingFolderOffers map[string]*fileOfferRequest // folderID -> 请求
}

func NewChatClient() *ChatClient {
	// 创建文件保存目录
	// 打包为 MSIX（商店应用）时安装目录是只读的，resolveDataDir 会自动回退到用户文档目录
	dataDir := resolveDataDir()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Printf("创建接收目录失败 %s: %v", dataDir, err)
	}

	client := &ChatClient{
		peers:               make(map[string]*common.Peer),
		localIP:             getLocalIP(),
		announcedPeers:      make(map[string]bool),
		peerTimeout:         60 * time.Second,
		reapInterval:        10 * time.Second,
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
	// 文件夹批量发送时用条件变量等待「当前文件结束」，替代 100ms 轮询
	client.fileDoneCond = sync.NewCond(&client.fileMutex)
	return client
}

// resolveDataDir 决定接收文件的保存目录。
// 免安装/开发运行时优先用当前目录下的 received_files（保持原有行为）；
// MSIX 打包后安装目录只读，写探测失败时回退到“我的文档\LoacalCP\received_files”。
func resolveDataDir() string {
	const rel = "received_files"
	if err := os.MkdirAll(rel, 0755); err == nil {
		probe := filepath.Join(rel, ".write_probe")
		if f, err := os.Create(probe); err == nil {
			f.Close()
			os.Remove(probe)
			return rel
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		p := filepath.Join(home, "Documents", "LoacalCP", "received_files")
		if err := os.MkdirAll(p, 0755); err == nil {
			return p
		}
	}
	if cfg, err := os.UserConfigDir(); err == nil && cfg != "" {
		p := filepath.Join(cfg, "LoacalCP", "received_files")
		if err := os.MkdirAll(p, 0755); err == nil {
			return p
		}
	}
	return rel
}

// peerKeyOf 生成用户唯一身份键（IP:TCP端口）。代号可以重复，网络端点才是唯一身份
func peerKeyOf(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

// isSelfPeer 判断给定端点是否就是本机客户端自己（含回环地址与本机网卡IP）
func (c *ChatClient) isSelfPeer(ip string, port int) bool {
	return port == c.tcpPort && isLocalAddress(ip, c.localIP)
}

// remoteIP 安全获取TCP连接对端IP
func remoteIP(conn net.Conn) string {
	if conn == nil {
		return ""
	}
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		return addr.IP.String()
	}
	return conn.RemoteAddr().String()
}

// isLocalAddress 判断IP是否指向本机（回环或本机网卡IP）
func isLocalAddress(ip, localIP string) bool {
	if ip == "127.0.0.1" || ip == "::1" || ip == "localhost" {
		return true
	}
	return ip == localIP
}

// connTargetsSelf 判断已建立的TCP连接对端是否就是本机自己的监听端口（TCP自连）。
// TCP允许connect到自身监听的端口，会形成自发自收的回环连接，必须在握手阶段拦截。
func (c *ChatClient) connTargetsSelf(conn net.Conn) bool {
	addr, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return false
	}
	// 对端端口必须与本机TCP监听端口一致；跨机器时IP不同也不构成自连
	return addr.Port == c.tcpPort && isLocalAddress(addr.IP.String(), c.localIP)
}

// isCurrentChatPeer 判断给定端点是否为当前聊天对象（按端点而非代号判断）
func (c *ChatClient) isCurrentChatPeer(ip string, port int) bool {
	c.chatMutex.RLock()
	defer c.chatMutex.RUnlock()
	return c.inChatSession && c.currentChatKey == peerKeyOf(ip, port)
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

	// 确保Windows防火墙放行入站连接（跨机聊天依赖对端连入；异步执行不阻塞启动）
	go c.ensureFirewallRule()

	// 广播上线通知
	go c.broadcastOnline()

	// 主动发现已在线用户
	go c.activeDiscovery()

	// 定期发送发现消息
	go c.periodicDiscover()

	// 定期收割超时无响应的用户（崩溃/断网/Offline丢失的兜底）
	go c.reapStalePeersLoop()
}

// SetName 设置用户名
func (c *ChatClient) SetName(name string) {
	if name != "" {
		c.name = name
	}
}

// SetPortBase 设置UDP/TCP监听端口段偏移（必须在Start之前调用）。
// 生产使用默认0；测试通过互不重叠的端口段隔离，避免Windows下UDP SO_REUSEADDR
// 导致刚关闭的socket随机吸走新实例广播包
func (c *ChatClient) SetPortBase(base int) {
	c.portBase = base
}

// udpPortStart 本段UDP端口范围起点
func (c *ChatClient) udpPortStart() int { return common.BroadcastPort + c.portBase }

// tcpPortStart 本段TCP端口范围起点
func (c *ChatClient) tcpPortStart() int { return common.TCPPortStart + c.portBase }

// GetName 获取用户名
func (c *ChatClient) GetName() string {
	return c.name
}

// 启动UDP广播监听
func (c *ChatClient) startUDPListener() error {
	// 尝试绑定本段内可用的UDP端口
	start := c.udpPortStart()
	for port := start; port < start+100; port++ {
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
	return fmt.Errorf("无法绑定UDP端口范围 %d-%d", start, start+100)
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

		c.handleUDPMessage(&msg, srcAddr.IP.String(), srcAddr.Port)
	}
}

// 处理UDP消息
func (c *ChatClient) handleUDPMessage(msg *common.Message, srcIP string, srcUDPPort int) {
	switch msg.Type {
	case common.MsgTypeDiscover:
		// Discover本身就是存活证据：刷新已知用户的最后可见时间
		c.touchPeer(srcIP, msg.Port)
		// 响应发现请求 - 直接回复发送者的UDP来源端口（不能写死9999，同机实例端口不同）
		c.sendUDPResponse(srcIP, srcUDPPort)
	case common.MsgTypeOnline:
		// 处理上线通知
		peer := &common.Peer{
			Name: msg.From,
			IP:   srcIP,
			Port: msg.Port,
		}
		changed, isNew := c.addPeer(peer)
		if isNew {
			c.chatMutex.RLock()
			inChat := c.inChatSession
			c.chatMutex.RUnlock()
			// 只有新用户才公告上线（聊天中不打断会话，仅静默刷新列表）
			if !inChat {
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
		// 处理下线通知（按端点识别，避免同名用户下线误伤当前会话）
		isChatPeer := c.isCurrentChatPeer(srcIP, msg.Port)
		changed := c.removePeerByIPAndPort(srcIP, msg.Port)
		c.chatMutex.RLock()
		inChat := c.inChatSession
		c.chatMutex.RUnlock()
		// 下线的是当前聊天对象：结束会话
		if isChatPeer {
			c.logf("%s 已下线，聊天自动结束", msg.From)
			c.endChatSession()
		} else if changed && !inChat {
			c.logf("[系统] %s 下线了", msg.From)
		}
		if changed {
			// 无论是否在聊天会话中，在线用户列表都必须同步刷新
			c.printOnlinePeers()
		}
	}
}

// 发送UDP响应
func (c *ChatClient) sendUDPResponse(destIP string, destPort int) {
	msg := common.Message{
		Type:    common.MsgTypeOnline,
		From:    c.name,
		IP:      c.localIP,
		Port:    c.tcpPort,
		Content: "online",
	}

	data, _ := msg.Serialize()
	destAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", destIP, destPort))
	if c.udpConn != nil && destAddr != nil {
		c.udpConn.WriteToUDP(data, destAddr)
	}
}

// 启动TCP监听
func (c *ChatClient) startTCPListener() error {
	// 尝试绑定本段内可用的TCP端口
	start := c.tcpPortStart()
	for port := start; port < start+100; port++ {
		// 使用 0.0.0.0 绑定所有接口
		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
		if err == nil {
			c.tcpListener = listener
			c.tcpPort = port
			go c.acceptTCPConnections()
			return nil
		}
	}
	return fmt.Errorf("无法绑定TCP端口范围 %d-%d", start, start+100)
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

	reader := bufio.NewReaderSize(conn, recvReadBufferSize)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			// 连接断开，只有在聊天会话中才提示
			c.chatMutex.RLock()
			owned := c.inChatSession && c.chatConn == conn
			c.chatMutex.RUnlock()
			if owned {
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

		// 深度防御：拒绝TCP自连。聊天握手消息携带发送方TCP监听端口，
		// 来源为本机且端口与本机监听口相同，说明有人（或错误的fallback）连到了自己
		if msg.Type == common.MsgTypeChat && msg.Port == c.tcpPort &&
			isLocalAddress(remoteAddr.IP.String(), c.localIP) {
			c.logf("拒绝来自本机自身的聊天连接（不能与自己聊天）")
			return
		}

		switch msg.Type {
		case common.MsgTypeChat:
			c.chatMutex.RLock()
			inChat := c.inChatSession
			c.chatMutex.RUnlock()
			if inChat {
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
				// 用对端TCP监听端点作为唯一身份（初始消息携带Port），同名用户也能区分
				c.currentChatKey = peerKeyOf(remoteAddr.IP.String(), msg.Port)
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
		default:
			c.handleTransferMessage(&msg, reader, conn)
		}
	}
}

// handleTransferMessage 分派文件/文件夹相关消息。
// reader 是连接上唯一的 bufio 读取器，收到二进制数据块头后由它继续读取原始负载，
// 保证「头 + 负载」不会被别的 goroutine 抢读。
func (c *ChatClient) handleTransferMessage(msg *common.Message, reader *bufio.Reader, conn net.Conn) {
	switch msg.Type {
	case common.MsgTypeFileOffer:
		// 文件发送请求
		go c.handleFileOffer(msg, conn)
	case common.MsgTypeFileData:
		// 同步处理 base64 数据块：消息按TCP顺序到达，同步写入保证数据块严格按序落盘，
		// 也避免每个数据块新开goroutine并发访问接收状态
		c.receiveFileData(msg)
	case common.MsgTypeFileDataBin:
		// 同步读取并写入二进制数据块；必须在当前读取协程内完成，随后才继续读下一条消息
		c.receiveBinChunk(msg, reader, conn)
	case common.MsgTypeFileAck:
		// 对端确认已落盘到 AckedOffset，记账后用于断点续传
		c.handleFileAck(msg)
	case common.MsgTypeFileAccept, common.MsgTypeFileReject, common.MsgTypeFileDone, common.MsgTypeFileCancel:
		go c.handleFileMessage(msg, conn)
	case common.MsgTypeFolderOffer:
		go c.handleFolderOffer(msg, conn)
	case common.MsgTypeFolderAccept, common.MsgTypeFolderReject, common.MsgTypeFolderDone:
		go c.handleFolderMessage(msg, conn)
	}
}

// 开始聊天会话（按代号查找；同名时排除自己的端点，取第一个匹配项。
// 需要精确定位同名用户时应使用 StartChatWith）
func (c *ChatClient) startChatSession(targetName string) bool {
	c.peersMutex.RLock()
	var target *common.Peer
	for _, peer := range c.peers {
		if peer.Name == targetName && !c.isSelfPeer(peer.IP, peer.Port) {
			p := *peer
			target = &p
			break
		}
	}
	c.peersMutex.RUnlock()

	if target == nil {
		c.logf("用户 %s 不在线或不存在", targetName)
		c.emitPeersChanged()
		return false
	}
	return c.connectToPeer(target)
}

// connectToPeer 连接指定端点的用户并建立聊天会话
func (c *ChatClient) connectToPeer(targetPeer *common.Peer) bool {
	targetName := targetPeer.Name

	// 尝试多种连接方式
	addresses := c.generateConnectionAddresses(targetPeer.IP, targetPeer.Port)

	var conn net.Conn
	var err error

	for i, address := range addresses {
		c.logf("连接尝试 %d/%d 到 %s...", i+1, len(addresses), address)

		conn, err = net.DialTimeout("tcp", address, 5*time.Second)
		if err == nil {
			// 防止TCP自连：对端实际指向本机自己的监听端口时立即断开，
			// 绝不建立"自己跟自己聊天"的会话（即使错误的fallback地址也无法绕过）
			if c.connTargetsSelf(conn) {
				c.logf("连接尝试 %d/%d 指向本机自己的端口，已拒绝自连", i+1, len(addresses))
				conn.Close()
				conn = nil
				err = fmt.Errorf("目标 %s 是本机自己，不能与自己聊天", address)
			} else {
				break
			}
		}
		if err != nil {
			c.logf("连接尝试 %d/%d 失败: %v", i+1, len(addresses), err)
		}

		if i < len(addresses)-1 {
			time.Sleep(1 * time.Second)
		}
	}

	if err != nil {
		// 区分失败原因给出精准提示：
		// - i/o timeout：对端主机在线但入站SYN被防火墙/安全软件DROP（对端UDP广播仍可达，
		//   典型场景是对端防火墙规则的网络配置文件Public/Private与当前网络不匹配）
		// - connection refused：对端主机可达但程序未监听该端口（已退出或端口被占满）
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			c.logf("连接用户 %s 失败：连接超时，对方(%s:%d)的防火墙很可能拦截了入站连接（可让对方以管理员身份运行一次本程序自动放行防火墙）",
				targetName, targetPeer.IP, targetPeer.Port)
		} else {
			c.logf("连接用户 %s 失败: %v（对方可能已退出）", targetName, err)
		}

		// 不从在线列表移除：UDP发现机制证明对方仍在线（超过peerTimeout无广播才会被收割），
		// TCP连不上多为防火墙拦截，误删会导致对方下次广播时反复提示"上线了"刷屏、列表抖动
		return false
	}

	c.logf("成功连接到 %s", targetName)

	c.chatMutex.Lock()
	c.chatConn = conn
	c.currentChat = targetName
	c.currentChatKey = peerKeyOf(targetPeer.IP, targetPeer.Port)
	c.inChatSession = true
	c.chatMutex.Unlock()

	if c.OnChatStarted != nil {
		c.OnChatStarted(targetName)
	}

	// 启动消息接收goroutine
	go c.receiveChatMessages(conn)

	// 发送初始消息（携带TCP端口，供对端建立唯一身份键，区分同名用户）
	initMsg := common.Message{
		Type:    common.MsgTypeChat,
		From:    c.name,
		Port:    c.tcpPort,
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

	// 只有目标确实在本机（同机运行的另一个实例）时，才追加回环地址兜底。
	// 绝不能对局域网内其他机器（192.168.x.x等私有IP）回退到127.0.0.1：
	// 一旦对方连不上，fallback会连到本机同端口，若该端口正是自己的监听口就变成自己跟自己聊天。
	if ip != "127.0.0.1" && isLocalAddress(ip, c.localIP) {
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
	reader := bufio.NewReaderSize(conn, recvReadBufferSize)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			// 只有在聊天会话中才处理断开
			c.chatMutex.RLock()
			owned := c.inChatSession && c.chatConn == conn
			c.chatMutex.RUnlock()
			if owned {
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
		default:
			c.handleTransferMessage(&msg, reader, conn)
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
		c.currentChatKey = ""
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
			// 会话中断不清删临时文件：把当前断点写进元数据，下次同一个文件可续传
			if tf.PartPath != "" {
				writePartMeta(tf.FilePath, partMeta{
					Key: tf.FileKey, FileName: tf.FileName, Size: tf.FileSize, Written: tf.Written,
				})
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
		// 收到Online即证明对方存活，刷新最后可见时间
		existing.LastSeen = time.Now()
		// 如果用户名相同，没有变化
		if existing.Name == peer.Name {
			return false, false
		}
		// 用户名变化，更新
		existing.Name = peer.Name
		return true, false
	} else {
		// 新用户
		peer.LastSeen = time.Now()
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

// touchPeer 刷新已知用户的最后可见时间（收到该用户的Discover等存活消息时调用）
func (c *ChatClient) touchPeer(ip string, port int) {
	key := fmt.Sprintf("%s:%d", ip, port)
	c.peersMutex.Lock()
	defer c.peersMutex.Unlock()
	if p, exists := c.peers[key]; exists {
		p.LastSeen = time.Now()
	}
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

// reapStalePeersLoop 定期移除超时无任何存活消息的用户。
// 对端正常退出会立即收到Offline；该循环兜底处理崩溃、杀进程、断网或Offline丢包。
func (c *ChatClient) reapStalePeersLoop() {
	interval := c.reapInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	timeout := c.peerTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for c.online.Load() {
		<-ticker.C
		if !c.online.Load() {
			return
		}
		c.reapStalePeers(timeout)
	}
}

// reapStalePeers 移除最后可见时间超过timeout的用户，并通知GUI刷新列表
func (c *ChatClient) reapStalePeers(timeout time.Duration) {
	now := time.Now()

	c.peersMutex.Lock()
	var expired []common.Peer
	for key, p := range c.peers {
		if now.Sub(p.LastSeen) > timeout {
			expired = append(expired, *p)
			delete(c.announcedPeers, fmt.Sprintf("%s@%s:%d", p.Name, p.IP, p.Port))
			delete(c.peers, key)
		}
	}
	c.peersMutex.Unlock()

	if len(expired) == 0 {
		return
	}

	for _, p := range expired {
		c.logf("[系统] %s 已离线（长时间无响应）", p.Name)
	}
	// 注意：此处仅把超时用户移出在线列表，绝不因此自动结束聊天（不自动/quit）。
	// UDP广播可能因WiFi切换、休眠唤醒、网卡抖动等短暂丢失，而TCP聊天连接仍然完好；
	// 聊天会话只由以下真实事件结束：用户手动结束、对方显式/quit、对方正常退出的Offline、
	// TCP连接实际断开（读取失败，含对方进程崩溃/断网）。
	c.printOnlinePeers()
}

// 发送广播消息
func (c *ChatClient) sendBroadcast(msg *common.Message) {
	if c.udpConn == nil {
		return
	}

	data, _ := msg.Serialize()

	// 广播到本段所有可能的端口
	udpStart := c.udpPortStart()
	for port := udpStart; port < udpStart+100; port++ {
		addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("255.255.255.255:%d", port))
		c.udpConn.WriteTo(data, addr)
	}

	// 也发送到本地网络广播地址
	if c.localIP != "127.0.0.1" {
		ipParts := strings.Split(c.localIP, ".")
		if len(ipParts) == 4 {
			broadcastIP := fmt.Sprintf("%s.%s.%s.255", ipParts[0], ipParts[1], ipParts[2])
			for port := udpStart; port < udpStart+100; port++ {
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

// StartChat 按代号开始聊天。代号允许与自己相同（同名不同机），
// 查找时会自动排除本机端点；同名在线时取第一个，精确选择请用 StartChatWith。
func (c *ChatClient) StartChat(targetName string) bool {
	return c.startChatSession(targetName)
}

// StartChatWith 与指定IP:TCP端口的用户开始聊天（同名用户的精确发起入口）。
// 只有端点与本机完全一致时才判定为"自己"。
func (c *ChatClient) StartChatWith(ip string, port int) bool {
	if c.isSelfPeer(ip, port) {
		c.logf("不能与自己聊天")
		return false
	}

	c.peersMutex.RLock()
	var target *common.Peer
	if p, ok := c.peers[peerKeyOf(ip, port)]; ok {
		cp := *p
		target = &cp
	}
	c.peersMutex.RUnlock()

	if target == nil {
		c.logf("用户 %s:%d 不在线或不存在", ip, port)
		c.emitPeersChanged()
		return false
	}
	return c.connectToPeer(target)
}

// IsCurrentChatPeer 判断指定端点是否为当前聊天对象（供GUI同名用户去重用）
func (c *ChatClient) IsCurrentChatPeer(ip string, port int) bool {
	return c.isCurrentChatPeer(ip, port)
}

// EndChat 结束当前聊天
func (c *ChatClient) EndChat() {
	c.endChatSession()
}

// IsInChat 检查是否在聊天会话中
func (c *ChatClient) IsInChat() bool {
	c.chatMutex.RLock()
	defer c.chatMutex.RUnlock()
	return c.inChatSession
}

// GetCurrentChat 获取当前聊天对象
func (c *ChatClient) GetCurrentChat() string {
	c.chatMutex.RLock()
	defer c.chatMutex.RUnlock()
	return c.currentChat
}

// SendFile 发送文件
func (c *ChatClient) SendFile(filePath string) bool {
	c.chatMutex.RLock()
	inChat := c.inChatSession
	c.chatMutex.RUnlock()
	if !inChat {
		c.logf("请先开始一个聊天会话")
		return false
	}

	c.sendFile(filePath)
	return true
}

// SendFolder 发送文件夹
func (c *ChatClient) SendFolder(folderPath string) bool {
	c.chatMutex.RLock()
	inChat := c.inChatSession
	c.chatMutex.RUnlock()
	if !inChat {
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

	// 计算总块数
	totalChunks := int((fileSize + fileChunkSize - 1) / fileChunkSize)

	// 保存文件信息
	transferFile := &TransferringFile{
		FilePath:     filePath,
		FileName:     fileName,
		FileSize:     fileSize,
		FileID:       fileID,
		File:         file,
		TotalChunks:  totalChunks,
		CurrentChunk: 0,
		Buffer:       make([]byte, fileChunkSize),
		FileKey:      stableFileKey(fileName, fileSize, fileInfo.ModTime()),
	}

	c.fileMutex.Lock()
	c.sendingFiles[fileID] = transferFile
	c.fileMutex.Unlock()

	// 发送文件发送请求（带 FileKey 与 Proto，供接收端判断能否续传）
	fileOfferMsg := common.Message{
		Type:        common.MsgTypeFileOffer,
		From:        c.name,
		FileName:    fileName,
		FileSize:    fileSize,
		FileID:      fileID,
		TotalChunks: totalChunks,
		FileKey:     transferFile.FileKey,
		Proto:       protoV2,
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
	c.chatMutex.RLock()
	inChat := c.inChatSession
	c.chatMutex.RUnlock()
	if !inChat {
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

// writeRaw 线程安全地写入一段已拼好的原始字节（二进制数据块整帧一次写出）
func (c *ChatClient) writeRaw(data []byte) bool {
	c.chatMutex.Lock()
	defer c.chatMutex.Unlock()
	if c.chatConn == nil {
		return false
	}
	_, err := c.chatConn.Write(data)
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
	c.dirMutex.RLock()
	defer c.dirMutex.RUnlock()
	return c.fileDataDir
}

// SetFileDataDir 修改接收文件的保存目录。
// 目录不存在时自动创建，并做一次写探测，确保后续接收文件不会因权限问题失败。
// 修改立即生效，不影响正在进行的传输（已打开的写入路径保持不变）。
func (c *ChatClient) SetFileDataDir(dir string) error {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return fmt.Errorf("保存位置不能为空")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("无法创建目录: %w", err)
	}
	probe := filepath.Join(dir, ".write_probe")
	f, err := os.Create(probe)
	if err != nil {
		return fmt.Errorf("目录不可写: %w", err)
	}
	f.Close()
	os.Remove(probe)

	c.dirMutex.Lock()
	c.fileDataDir = dir
	c.dirMutex.Unlock()
	return nil
}

// cleanupReceivingFile 关闭接收句柄并移除状态。
// discardPart=true 表示这次传输彻底作废（用户/对端明确取消），删除临时文件；
// false 表示只是中断（断网、进程退出），保留临时文件与元数据供下次续传。
func (c *ChatClient) cleanupReceivingFile(fileID string, discardPart bool) {
	c.fileMutex.Lock()
	file, exists := c.receivingFiles[fileID]
	written := int64(0)
	if exists {
		delete(c.receivingFiles, fileID)
		written = file.Written
		// 如果是普通文件传输，设置receiving为false
		if file.FolderID == "" {
			c.receiving = false
		}
	}
	c.fileMutex.Unlock()

	if !exists {
		return
	}
	if file.File != nil {
		file.File.Close()
	}
	if file.PartPath == "" {
		return
	}
	if discardPart {
		removePartFiles(file.FilePath)
		return
	}
	// 记录断点：以真实写入字节为准，下次同一文件从块边界处续传
	if err := writePartMeta(file.FilePath, partMeta{
		Key: file.FileKey, FileName: file.FileName, Size: file.FileSize, Written: written,
	}); err != nil {
		c.logf("保存续传断点失败: %v", err)
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
		c.OnFileOffer(msg.From, remoteIP(conn), msg.FileName, msg.FileSize, msg.FileID)
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
	dir := c.FileDataDir() // 一次性取值，避免改名过程中路径分裂到两个目录
	savePath := filepath.Join(dir, req.msg.FileName)

	// 检查文件是否已存在，如果存在则添加时间戳
	if _, err := os.Stat(savePath); err == nil {
		timestamp := time.Now().Format("20060102_150405")
		ext := filepath.Ext(req.msg.FileName)
		baseName := strings.TrimSuffix(req.msg.FileName, ext)
		savePath = filepath.Join(dir, fmt.Sprintf("%s_%s%s", baseName, timestamp, ext))
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

// acceptIncomingFile 创建文件并回复接受，开始接收数据。
// 协议 v2 下数据先写入 xxx.lcpart，收到 FileDone 且长度校验通过后才改名为正式文件名，
// 这样中断的传输不会留下看起来「完整」的半截文件，而且下次可以接着传。
func (c *ChatClient) acceptIncomingFile(msg *common.Message, conn net.Conn, savePath string) {
	// 逐文件协商：只有发送端也声明 Proto>=2 才启用二进制块与续传
	wantV2 := msg.Proto >= protoV2

	var (
		file     *os.File
		err      error
		partPath string
		resume   int64
	)

	if wantV2 {
		resume = detectResumeOffset(savePath, msg)
		partPath = partPathFor(savePath)
		file, err = openPartFile(partPath, resume)
		if err != nil {
			// 临时文件打不开（权限/磁盘满等）：退回直接写目标文件，至少不中断传输
			c.logf("无法创建临时文件 %s: %v，改为直接写入目标文件", partPath, err)
			wantV2 = false
			partPath = ""
			resume = 0
			file, err = os.Create(savePath)
		}
	} else {
		file, err = os.Create(savePath)
	}

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
		CurrentChunk: int(resume / int64(fileChunkSize)),
		Buffer:       make([]byte, fileChunkSize),
		RelativePath: msg.RelativePath,
		FolderID:     msg.FolderID,
		// 续传：字节数从续传点起算，块序号也必须对齐到同一位置
		Written:   resume,
		Resume:    resume,
		Proto2:    wantV2,
		FileKey:   msg.FileKey,
		PartPath:  partPath,
		LastAckAt: resume,
	}

	// 记录续传元数据：进程崩溃/断网后，下次同一个文件就能靠它匹配到底稿。
	// 只对够大的文件预先落盘，小文件改为中断时补写（见 cleanupReceivingFile）。
	if wantV2 && msg.FileSize >= resumeMetaMinSize {
		if err := writePartMeta(savePath, partMeta{
			Key: msg.FileKey, FileName: msg.FileName, Size: msg.FileSize, Written: resume,
		}); err != nil {
			c.logf("写入续传元数据失败: %v", err)
		}
	}

	c.fileMutex.Lock()
	c.receivingFiles[msg.FileID] = transferFile
	c.fileMutex.Unlock()

	// 接受文件：回显 Proto（表示双方都支持 v2）以及续传起点
	proto := 0
	if wantV2 {
		proto = protoV2
	}
	acceptMsg := common.Message{
		Type:         common.MsgTypeFileAccept,
		From:         c.name,
		FileID:       msg.FileID,
		Proto:        proto,
		ResumeOffset: resume,
	}
	data, _ := json.Marshal(acceptMsg)
	conn.Write(append(data, '\n'))

	if msg.FolderID == "" {
		if resume > 0 {
			c.logf("继续接收文件: %s，从 %s 处续传，保存到: %s",
				msg.FileName, FormatFileSize(resume), savePath)
			if c.OnTransferResumed != nil {
				c.OnTransferResumed(msg.FileName, resume, msg.FileSize)
			}
		} else {
			c.logf("开始接收文件: %s，保存到: %s", msg.FileName, savePath)
		}
	}
}

// detectResumeOffset 判断能否对本次 offer 续传，返回对齐到块边界的起始偏移。
// 只有「元数据里的文件标识与大小都完全一致」时才续传，
// 否则说明同名文件已被改动（或根本是另一个文件），旧临时文件必须作废。
func detectResumeOffset(savePath string, msg *common.Message) int64 {
	partPath := partPathFor(savePath)

	m, ok := readPartMeta(savePath)
	if !ok || msg.FileKey == "" || m.Key != msg.FileKey || m.Size != msg.FileSize {
		// 没有元数据或身份不符：残留的 .lcpart 无从确认内容，直接丢弃
		if _, err := os.Stat(partPath); err == nil {
			removePartFiles(savePath)
		}
		return 0
	}

	st, err := os.Stat(partPath)
	if err != nil {
		removePartFiles(savePath)
		return 0
	}
	// 以实际文件长度为准（元数据里的 Written 只是同一时刻的快照，
	// 进程被强杀时文件长度可能更准），并对齐到块边界
	size := st.Size()
	if size > msg.FileSize {
		size = msg.FileSize
	}
	return alignDownToChunk(size)
}

// finalizePart 把临时文件改名为正式文件名并清理续传元数据。
// 目标名被占用时（极少见）追加时间戳，绝不覆盖用户已有文件。
func finalizePart(tf *TransferringFile) (string, error) {
	savePath := tf.FilePath
	if err := os.Rename(tf.PartPath, savePath); err != nil {
		stamp := time.Now().Format("20060102_150405")
		ext := filepath.Ext(savePath)
		base := strings.TrimSuffix(savePath, ext)
		alt := fmt.Sprintf("%s_%s%s", base, stamp, ext)
		if err2 := os.Rename(tf.PartPath, alt); err2 != nil {
			return "", err
		}
		savePath = alt
	}
	removePartFiles(tf.FilePath)
	return savePath, nil
}

// 处理文件传输相关消息
func (c *ChatClient) handleFileMessage(msg *common.Message, _ net.Conn) {
	switch msg.Type {
	case common.MsgTypeFileAccept:
		// 对方接受了文件。accept 里带回 Proto 与 ResumeOffset：
		// Proto>=2 表示双方可以走二进制块 + 续传，ResumeOffset 是对方要我们从哪里接着发
		c.fileMutex.Lock()
		if tf, ok := c.sendingFiles[msg.FileID]; ok {
			tf.Proto2 = msg.Proto >= protoV2
		}
		c.fileMutex.Unlock()
		go c.sendFileData(msg.FileID, msg.ResumeOffset, msg.Proto >= protoV2)
	case common.MsgTypeFileReject:
		// 对方拒绝了文件，清理发送资源（关闭文件句柄并复位传输状态）
		c.cleanupSendingFile(msg.FileID)
		if c.OnTransferEnded != nil {
			c.OnTransferEnded(true, fmt.Sprintf("%s 拒绝了你的文件", msg.From))
		}
	case common.MsgTypeFileDone:
		c.finalizeReceivingFile(msg)
	case common.MsgTypeFileCancel:
		// 先看这个文件是不是本端正在发送：是则说明对方放弃了接收
		c.fileMutex.Lock()
		tf, sendingNow := c.sendingFiles[msg.FileID]
		if sendingNow {
			tf.Canceled = true
		}
		c.fileMutex.Unlock()
		if sendingNow {
			// 打完取消标记后由发送循环自己退出（FileDone 不再发出）
			c.cleanupSendingFile(msg.FileID)
			if c.OnTransferEnded != nil {
				c.OnTransferEnded(true, "对方放弃了接收，传输已取消")
			}
			return
		}

		// 接收端视角：明确取消（对方中止或用户取消），临时文件不再有用，直接删除
		c.fileMutex.Lock()
		folderID := ""
		if tf, ok := c.receivingFiles[msg.FileID]; ok {
			folderID = tf.FolderID
		}
		c.fileMutex.Unlock()
		c.cleanupReceivingFile(msg.FileID, true)

		folderPath := ""
		if folderID != "" {
			// 文件夹内的文件被取消时，整个文件夹接收一并放弃，
			// 否则 receiving 状态会卡死，后续文件请求都被拒
			c.fileMutex.Lock()
			if f, ok := c.receivingFolders[folderID]; ok {
				folderPath = f.FolderPath
				delete(c.receivingFolders, folderID)
			}
			c.receiving = false
			c.fileMutex.Unlock()
			if folderPath != "" {
				removeFolderMarker(folderPath)
			}
		}
		c.fileMutex.Lock()
		c.transmitting = false
		c.fileMutex.Unlock()
		if c.OnTransferEnded != nil {
			c.OnTransferEnded(false, "对方放弃了发送，传输已取消")
		}
	}
}

// finalizeReceivingFile 处理 FileDone：校验长度、把临时文件转正、回调界面
func (c *ChatClient) finalizeReceivingFile(msg *common.Message) {
	c.fileMutex.Lock()
	tf, exists := c.receivingFiles[msg.FileID]
	if exists {
		delete(c.receivingFiles, msg.FileID)
		if tf.FolderID == "" {
			c.receiving = false
		}
	}
	written, expected := int64(0), int64(0)
	if exists {
		written, expected = tf.Written, tf.FileSize
	}
	c.fileMutex.Unlock()
	if !exists {
		return
	}

	if tf.File != nil {
		tf.File.Close()
	}

	// 长度校验：块序号错位/丢块时不能让残缺文件伪装成“接收完成”。
	// 缺失的部分（written < expected）保留临时文件与元数据，用户重发同一文件即可续传。
	if expected > 0 && written != expected {
		if written > expected {
			// 多出来的内容无法解释，元数据一并作废
			removePartFiles(tf.FilePath)
		} else if tf.PartPath != "" {
			writePartMeta(tf.FilePath, partMeta{
				Key: tf.FileKey, FileName: tf.FileName, Size: tf.FileSize, Written: written,
			})
		}
		c.logf("文件 '%s' 长度不符（收到 %d 字节，应为 %d 字节），判定传输失败",
			msg.FileName, written, expected)
		if c.OnTransferEnded != nil {
			c.OnTransferEnded(false, fmt.Sprintf("文件 '%s' 接收不完整（%s / %s），重新发送可继续传输",
				msg.FileName, FormatFileSize(written), FormatFileSize(expected)))
		}
		return
	}

	savePath := tf.FilePath
	if tf.PartPath != "" {
		final, err := finalizePart(tf)
		if err != nil {
			c.logf("保存文件 %s 失败: %v", tf.FileName, err)
			if c.OnTransferEnded != nil && tf.FolderID == "" {
				c.OnTransferEnded(false, fmt.Sprintf("文件 '%s' 保存失败: %v", tf.FileName, err))
			}
			return
		}
		savePath = final
	}

	if c.OnTransferEnded != nil && tf.FolderID == "" {
		c.OnTransferEnded(false, fmt.Sprintf("文件 '%s' 接收完成", msg.FileName))
	}
	if c.OnFileSaved != nil && tf.FolderID == "" {
		c.OnFileSaved(msg.FileName, savePath)
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
		if tf, exists := c.receivingFolders[msg.FolderID]; exists {
			// 用接受时确定下来的真实保存路径（可能带时间戳后缀），
			// 不能再用 FileDataDir()/FolderName 重新拼一遍
			folderPath = tf.FolderPath
			delete(c.receivingFolders, msg.FolderID)
			c.receiving = false
		}
		c.fileMutex.Unlock()
		if folderPath != "" {
			// 整个文件夹收完，续传标记作废，下次同名文件夹会作为新副本接收
			removeFolderMarker(folderPath)
			if c.OnTransferEnded != nil {
				c.OnTransferEnded(false, fmt.Sprintf("文件夹 '%s' 接收完成，已保存到: %s", msg.FolderName, folderPath))
			}
		}
	}
}

// scanFolder 扫描文件夹内容
func scanFolder(folderPath string) ([]string, int64, error) {
	var files []string
	var totalSize int64

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			// 跳过本程序自己的中间文件：直接发送「上次没传完的接收目录」时，
			// 临时文件与续传标记不应该被当成用户数据传给对方
			if isInternalTransferFile(info.Name()) {
				return nil
			}
			files = append(files, path)
			totalSize += info.Size()
		}
		return nil
	})

	return files, totalSize, err
}

// 发送文件夹
func (c *ChatClient) sendFolder(folderPath string) {
	c.chatMutex.RLock()
	inChat := c.inChatSession
	c.chatMutex.RUnlock()
	if !inChat {
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

	// 发送文件夹请求（FileKey 作为文件夹的稳定标识，供接收端匹配未完成的目录）
	folderOfferMsg := common.Message{
		Type:       common.MsgTypeFolderOffer,
		From:       c.name,
		FolderName: folderName,
		FolderSize: totalSize,
		FileCount:  len(files),
		FolderID:   folderID,
		FileKey:    stableFolderKey(folderName, totalSize, len(files)),
		Proto:      protoV2,
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
	dir := c.FileDataDir()
	folderSavePath := ""

	// 协议 v2：如果同名目录里留着「上次没传完」的标记，且文件夹标识一致，
	// 就继续往这个目录里写，里面的 .lcpart 会被逐个文件续传复用
	if req.msg.Proto >= protoV2 && req.msg.FileKey != "" {
		candidate := filepath.Join(dir, req.msg.FolderName)
		if key, ok := readFolderMarker(candidate); ok && key == req.msg.FileKey {
			folderSavePath = candidate
		}
	}

	if folderSavePath == "" {
		folderSavePath = filepath.Join(dir, req.msg.FolderName)
		// 检查文件夹是否已存在，如果存在则添加时间戳
		if _, err := os.Stat(folderSavePath); err == nil {
			timestamp := time.Now().Format("20060102_150405")
			folderSavePath = filepath.Join(dir, fmt.Sprintf("%s_%s", req.msg.FolderName, timestamp))
		}
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

	// 留下续传标记：整个文件夹收完后由 FolderDone 删除
	if req.msg.Proto >= protoV2 {
		if err := writeFolderMarker(folderSavePath, req.msg.FileKey); err != nil {
			c.logf("写入文件夹续传标记失败: %v", err)
		}
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
		c.OnFolderOffer(msg.From, remoteIP(conn), msg.FolderName, msg.FolderSize, msg.FileCount, msg.FolderID)
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
		// 用户放弃发送或对方放弃接收：立即终止整个文件夹
		if c.isFolderCanceled(transferFolder) {
			c.cleanupSendingFolder(folderID)
			return
		}

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
		const chunkSize = fileChunkSize
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
			Buffer:       make([]byte, fileChunkSize),
			RelativePath: relativePath,
			FolderID:     folderID,
			FileKey:      stableFileKey(relativePath, fileSize, fileInfo.ModTime()),
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
			FileKey:      transferFile.FileKey,
			Proto:        protoV2,
		}

		if !c.writeProtocolMsg(&fileOfferMsg) {
			file.Close()
			c.cleanupSendingFile(fileID)
			c.logf("发送文件请求失败：会话已结束")
			return
		}

		// 等待当前文件传输结束（接受后被 sendFileData 消费，拒绝/失败时由 cleanup 唤醒）
		c.waitSendingFileDone(fileID)

		// 当前文件被取消（用户放弃发送/对方放弃接收）时终止整个文件夹，
		// 而不是继续把剩余文件一个一个发给已经不想收的对端
		if c.isSendCanceled(transferFile) || c.isFolderCanceled(transferFolder) {
			c.cleanupSendingFolder(folderID)
			return
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
	// 唤醒正在等待该文件发送结束的文件夹发送循环
	if c.fileDoneCond != nil {
		c.fileDoneCond.Broadcast()
	}
}

// waitSendingFileDone 阻塞等待某个发送任务结束（完成/拒绝/取消/失败）。
// 取代原先每 100ms 轮询一次的实现：那会让文件夹传输每个文件固定多等约 100ms，
// 上千个小文件就要多花几分钟。
func (c *ChatClient) waitSendingFileDone(fileID string) {
	if c.fileDoneCond == nil {
		return
	}
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	for {
		if _, exists := c.sendingFiles[fileID]; !exists {
			return
		}
		c.fileDoneCond.Wait()
	}
}

// isSendCanceled 读取发送取消标记（跨协程访问，必须在锁内读）。
func (c *ChatClient) isSendCanceled(tf *TransferringFile) bool {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	return tf.Canceled
}

// isFolderCanceled 读取文件夹发送取消标记（跨协程访问，必须在锁内读）。
func (c *ChatClient) isFolderCanceled(f *TransferringFolder) bool {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	return f.Canceled
}

// CancelSending 用户主动放弃发送（GUI 调用）。
// 给对端发 FileCancel 让接收方立即停止，并给本端所有发送任务打上取消标记，
// 发送循环检测到后安静退出。传输中没有任何任务时是空操作。
func (c *ChatClient) CancelSending() {
	c.fileMutex.Lock()
	var fileIDs []string
	for id, tf := range c.sendingFiles {
		tf.Canceled = true
		fileIDs = append(fileIDs, id)
	}
	var folderIDs []string
	for id, f := range c.sendingFolders {
		f.Canceled = true
		folderIDs = append(folderIDs, id)
	}
	c.fileMutex.Unlock()
	if len(fileIDs) == 0 && len(folderIDs) == 0 {
		return
	}

	// 通知接收方停止：接收端收到后清理临时文件并复位状态
	for _, id := range fileIDs {
		c.writeProtocolMsg(&common.Message{
			Type:   common.MsgTypeFileCancel,
			From:   c.name,
			FileID: id,
		})
		c.cleanupSendingFile(id)
	}
	for _, id := range folderIDs {
		c.cleanupSendingFolder(id)
	}
	//c.logf("已放弃发送")
	if c.OnTransferEnded != nil {
		c.OnTransferEnded(true, "已放弃发送")
	}
}

// CancelReceiving 用户主动放弃接收（GUI 调用）。
// 通知发送方停止，丢弃已收到的临时文件与续传断点，并复位接收状态。
// 传输中没有任何任务时是空操作。
func (c *ChatClient) CancelReceiving() {
	c.fileMutex.Lock()
	var fileIDs []string
	var folderPaths []string
	for id := range c.receivingFiles {
		fileIDs = append(fileIDs, id)
	}
	for id, f := range c.receivingFolders {
		folderPaths = append(folderPaths, f.FolderPath)
		delete(c.receivingFolders, id)
	}
	c.fileMutex.Unlock()
	if len(fileIDs) == 0 && len(folderPaths) == 0 {
		return
	}

	// 通知发送方停止：发送循环检测到取消标记后安静退出
	for _, id := range fileIDs {
		c.writeProtocolMsg(&common.Message{
			Type:   common.MsgTypeFileCancel,
			From:   c.name,
			FileID: id,
		})
		// 明确放弃：临时文件与续传断点一并丢弃
		c.cleanupReceivingFile(id, true)
	}
	// 文件夹续传标记也清掉，下次重发同名文件夹从全新目录开始
	for _, p := range folderPaths {
		removeFolderMarker(p)
	}
	c.fileMutex.Lock()
	c.receiving = false
	c.fileMutex.Unlock()
	//c.logf("已放弃接收")
	if c.OnTransferEnded != nil {
		c.OnTransferEnded(false, "已放弃接收")
	}
}

// 发送文件数据。
// resume 为接收端要求的续传起点（0 表示从头发），proto2 表示双方都支持协议 v2。
func (c *ChatClient) sendFileData(fileID string, resume int64, proto2 bool) {
	c.fileMutex.Lock()
	transferFile, exists := c.sendingFiles[fileID]
	c.fileMutex.Unlock()
	if !exists {
		c.logf("找不到文件传输信息")
		return
	}

	// 接收端要求从某个偏移续传时，先把文件位置与块序号一起对齐过去
	if proto2 && resume > 0 {
		c.applyResume(transferFile, resume)
	}
	transferFile.Sent = transferFile.Resume

	buf := transferFile.Buffer
	lastPct := -1
	var lastReport time.Time

	// 二进制模式下把「JSON 头 + 换行 + 原始字节」拼进同一块缓冲，一块只有一次 Write。
	// 预分配一次后反复复用，避免每块都向 GC 申请 128KB。
	var frameBuf []byte
	if proto2 {
		frameBuf = make([]byte, 0, chunkFrameSlack+fileChunkSize)
	}

	for transferFile.CurrentChunk < transferFile.TotalChunks {
		// 用户放弃发送（本端取消）或对方放弃接收（对端 FileCancel）时安静退出：
		// 取消方已经负责发送 FileCancel、清理资源并回调 UI，这里不再重复
		if c.isSendCanceled(transferFile) {
			c.cleanupSendingFile(fileID)
			return
		}

		// 一次尽量读满一个数据块：os.File.Read 可能提前返回，
		// 按实际字节数推进块序号会造成块大小与总块数不匹配，因此这里补齐到整块
		n, err := io.ReadFull(transferFile.File, buf)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
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

		transferFile.Sent += int64(n)
		progress := 0
		if transferFile.FileSize > 0 {
			progress = int(float64(transferFile.Sent) / float64(transferFile.FileSize) * 100)
		}
		if progress > 100 {
			progress = 100
		}

		var ok bool
		if proto2 {
			// 协议 v2：JSON 头声明本块字节数，随后跟原始二进制，省掉 base64 的 33% 膨胀
			hdr := common.Message{
				Type:       common.MsgTypeFileDataBin,
				From:       c.name,
				FileID:     fileID,
				ChunkIndex: transferFile.CurrentChunk,
				ChunkBytes: n,
				Progress:   progress,
			}
			header, err := json.Marshal(&hdr)
			if err != nil {
				c.logf("编码数据块头失败: %v", err)
				ok = false
			} else {
				frameBuf = appendChunkFrame(frameBuf[:0], header, buf[:n])
				ok = c.writeRaw(frameBuf)
			}
		} else {
			// 兼容通道：base64 塞进 JSON，能被旧版本对端解析
			dataMsg := common.Message{
				Type:        common.MsgTypeFileData,
				From:        c.name,
				FileID:      fileID,
				FileData:    base64.StdEncoding.EncodeToString(buf[:n]),
				ChunkIndex:  transferFile.CurrentChunk,
				TotalChunks: transferFile.TotalChunks,
				Progress:    progress,
			}
			ok = c.writeProtocolMsg(&dataMsg)
		}

		if !ok {
			c.logf("发送文件数据失败：会话已结束")
			c.cleanupSendingFile(fileID)
			if c.OnTransferEnded != nil {
				c.OnTransferEnded(true, fmt.Sprintf("文件 '%s' 发送失败", transferFile.FileName))
			}
			return
		}

		// 进度节流：仅在百分比变化且距上次回调超过间隔时通知，避免刷爆UI队列。
		// 回调必须在持有 fileMutex 时发出：FileCancel 处理协程在同一临界区内
		// 设置取消标记，锁的互斥保证「进度」与「结束」两组回调不会乱序——
		// 否则取消时一个迟到的进度回调会在 UI 上重建 endProgress 刚关闭的
		// 进度对话框，留下一个永远没人关闭的传输窗口（小概率但可复现）。
		c.fileMutex.Lock()
		fire := !transferFile.Canceled && c.OnFileProgress != nil && progress != lastPct &&
			(progress >= 100 || time.Since(lastReport) >= progressInterval)
		if fire {
			c.OnFileProgress(progress, true, fmt.Sprintf("发送文件 %s", transferFile.FileName))
			lastPct = progress
			lastReport = time.Now()
		}
		c.fileMutex.Unlock()

		transferFile.CurrentChunk++

		// 分块之间不再人为 sleep：TCP 发送缓冲区本身就是背压，
		// 原来的 10ms 延迟把吞吐硬性锁死在约 1.6MB/s
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

// applyResume 把文件读写位置与块序号对齐到接收端要求的续传点，返回实际生效的偏移。
// 偏移被强制对齐到块边界：接收端按 ChunkIndex 顺序拼接，起点错开一个字节就会全盘错位。
func (c *ChatClient) applyResume(tf *TransferringFile, resume int64) int64 {
	if resume > tf.FileSize {
		resume = tf.FileSize
	}
	resume = alignDownToChunk(resume)
	if resume <= 0 {
		return 0
	}
	if _, err := tf.File.Seek(resume, io.SeekStart); err != nil {
		c.logf("定位续传起点失败（%v），改为从头发送", err)
		tf.File.Seek(0, io.SeekStart)
		return 0
	}
	tf.CurrentChunk = int(resume / int64(fileChunkSize))
	tf.Resume = resume
	pct := 0
	if tf.FileSize > 0 {
		pct = int(resume * 100 / tf.FileSize)
	}
	c.logf("从 %s 处继续发送 %s（已完成 %d%%）", FormatFileSize(resume), tf.FileName, pct)
	if c.OnTransferResumed != nil {
		c.OnTransferResumed(tf.FileName, resume, tf.FileSize)
	}
	return resume
}

// receiveFileData 处理旧协议的 base64 数据块（与 v1 对端互通时使用）
func (c *ChatClient) receiveFileData(msg *common.Message) {
	data, err := base64.StdEncoding.DecodeString(msg.FileData)
	if err != nil {
		c.fileMutex.Lock()
		tf := c.receivingFiles[msg.FileID]
		c.fileMutex.Unlock()
		name := msg.FileID
		if tf != nil {
			name = tf.FileName
		}
		c.logf("解码文件数据失败: %v", err)
		c.abortReceivingFile(msg.FileID, fmt.Sprintf("文件 '%s' 传输中断：数据解码失败", name))
		return
	}
	c.writeReceivedChunk(msg, data)
}

// receiveBinChunk 处理协议 v2 的二进制数据块：紧跟 JSON 头之后的 ChunkBytes 个原始字节。
// 必须在本连接的唯一读取协程里同步读完负载，否则后续消息会被当成文件数据吞掉。
func (c *ChatClient) receiveBinChunk(msg *common.Message, reader *bufio.Reader, conn net.Conn) {
	n := msg.ChunkBytes
	if n <= 0 || n > fileChunkSize {
		// 长度非法说明帧已经不可信，继续读下去只会越错越远，直接断开连接
		c.logf("数据块长度非法（%d，上限 %d），断开连接以避免协议错乱", n, fileChunkSize)
		conn.Close()
		return
	}
	// 复用该次接收自带的缓冲（接收端原本用不到 Buffer 字段），
	// 避免每块都申请一次 128KB：高速传输时这是成百 MB/s 的垃圾产出
	c.fileMutex.Lock()
	var scratch []byte
	if tf, ok := c.receivingFiles[msg.FileID]; ok {
		scratch = tf.Buffer
	}
	c.fileMutex.Unlock()

	payload, err := readChunkPayload(reader, n, scratch)
	if err != nil {
		c.logf("读取二进制数据块失败: %v", err)
		conn.Close()
		return
	}
	c.writeReceivedChunk(msg, payload)
}

// writeReceivedChunk 把一个数据块写入文件，并负责序号校验、ACK 与进度回调。
// JSON 与二进制两条通道共用这段逻辑，保证行为一致。
// 接收状态（Written/CurrentChunk）可能在别的协程被读取（FileDone、界面统计），
// 因此统一在 fileMutex 保护下更新；但文件写入之外的回调一律放到锁外调用，避免嵌套加锁。
func (c *ChatClient) writeReceivedChunk(msg *common.Message, data []byte) {
	c.fileMutex.Lock()
	transferFile, exists := c.receivingFiles[msg.FileID]
	if !exists {
		c.fileMutex.Unlock()
		//c.logf("找不到接收文件信息: %s", msg.FileID)
		return
	}

	// 检查数据块索引是否正确。
	// 旧实现只记录日志就丢包，后续所有块都会“序号不匹配”被丢弃，
	// 最终得到一个静默截断的坏文件；这里改为立即终止本次传输并明确告知双方。
	if msg.ChunkIndex != transferFile.CurrentChunk {
		name, expected, got := transferFile.FileName, transferFile.CurrentChunk, msg.ChunkIndex
		c.fileMutex.Unlock()
		c.logf("数据块顺序错误（期望 %d，收到 %d），终止本次传输以避免生成损坏文件", expected, got)
		c.abortReceivingFile(msg.FileID, fmt.Sprintf("文件 '%s' 传输中断：数据块错位", name))
		return
	}

	// 写入文件数据
	n, err := transferFile.File.Write(data)
	if err != nil {
		name := transferFile.FileName
		c.fileMutex.Unlock()
		c.logf("写入文件失败: %v", err)
		c.abortReceivingFile(msg.FileID, fmt.Sprintf("文件 '%s' 保存失败，传输已取消", name))
		return
	}
	transferFile.Written += int64(n)

	// 更新当前块索引
	transferFile.CurrentChunk++

	// 每 ackInterval 字节回一次 ACK，并顺手把断点写进元数据。
	// ACK 只是让对端知道「落盘到哪了」，不参与逐块流控，所以频率很低。
	var ackOffset int64
	if transferFile.Proto2 && transferFile.Written-transferFile.LastAckAt >= ackInterval {
		transferFile.LastAckAt = transferFile.Written
		ackOffset = transferFile.Written
	}

	// 通知进度（节流规则与发送端一致，避免高速传输时刷爆UI队列）。
	// 与发送端同理：回调必须在持锁时发出，否则用户 CancelReceiving 删除
	// 接收项并触发「已放弃接收」后，一个迟到的进度回调会在 UI 上重建
	// 刚关闭的进度对话框，卡在传输页面。
	notify := false
	pct := msg.Progress
	if c.OnFileProgress != nil && pct != transferFile.LastPct &&
		(pct >= 100 || time.Since(transferFile.LastReport) >= progressInterval) {
		transferFile.LastPct = pct
		transferFile.LastReport = time.Now()
		notify = true
	}
	fileName := transferFile.FileName
	savePath, key, size := transferFile.FilePath, transferFile.FileKey, transferFile.FileSize
	if notify {
		c.OnFileProgress(pct, false, fmt.Sprintf("接收文件 %s", fileName))
	}
	c.fileMutex.Unlock()

	if ackOffset > 0 {
		ack := common.Message{
			Type:        common.MsgTypeFileAck,
			From:        c.name,
			FileID:      msg.FileID,
			AckedOffset: ackOffset,
		}
		c.writeProtocolMsg(&ack)
		if err := writePartMeta(savePath, partMeta{
			Key: key, FileName: fileName, Size: size, Written: ackOffset,
		}); err != nil {
			c.logf("更新续传断点失败: %v", err)
		}
	}

	// 如果所有数据块都已接收，不需要做特殊处理，因为发送方会发送FileDone消息
}

// handleFileAck 记录对端确认的落盘偏移（发送端使用）。
// 传输若在此时中断，下次可从该偏移续传，不必从头再来。
func (c *ChatClient) handleFileAck(msg *common.Message) {
	c.fileMutex.Lock()
	if tf, ok := c.sendingFiles[msg.FileID]; ok && msg.AckedOffset > tf.Acked {
		tf.Acked = msg.AckedOffset
	}
	c.fileMutex.Unlock()
}

// abortReceivingFile 终止一次接收：通知对方取消、清理资源并回调UI。
// 已收到的部分保留为临时文件（可以续传），只有在收到明确的取消消息时才删除。
func (c *ChatClient) abortReceivingFile(fileID, text string) {
	cancelMsg := common.Message{
		Type:   common.MsgTypeFileCancel,
		From:   c.name,
		FileID: fileID,
	}
	c.writeProtocolMsg(&cancelMsg)
	c.cleanupReceivingFile(fileID, false)
	if c.OnTransferEnded != nil {
		c.OnTransferEnded(false, text)
	}
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
