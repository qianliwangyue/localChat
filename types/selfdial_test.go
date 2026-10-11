package types

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"localchat/pkg/common"
)

// TestGenerateConnectionAddresses 验证只有目标在本机时才回退127.0.0.1，
// 局域网其他机器连不上时绝不允许偷偷连localhost（否则会连到本机自己的监听端口）
func TestGenerateConnectionAddresses(t *testing.T) {
	c := NewChatClient()
	local := c.LocalIP()

	cases := []struct {
		name     string
		ip       string
		port     int
		wantLen  int
		wantLoop bool // 是否【额外】追加127.0.0.1备选
	}{
		{"本机网卡IP", local, c.TCPPort(), 2, true},
		{"回环地址不重复追加", "127.0.0.1", c.TCPPort(), 1, false},
		{"局域网其他机器", "192.168.0.200", 10005, 1, false},
		{"10网段其他机器", "10.0.0.25", 10006, 1, false},
		{"172.16网段其他机器", "172.16.5.5", 10007, 1, false},
		{"公网地址", "8.8.8.8", 10008, 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addrs := c.generateConnectionAddresses(tc.ip, tc.port)
			if len(addrs) != tc.wantLen {
				t.Fatalf("%s: 地址数=%d, 期望%d (%v)", tc.name, len(addrs), tc.wantLen, addrs)
			}
			if tc.wantLen == 1 {
				return
			}
			// 追加的备选必须是回环地址，且首地址必须是原始地址
			if addrs[0] != fmt.Sprintf("%s:%d", tc.ip, tc.port) {
				t.Fatalf("%s: 首地址被篡改: %s", tc.name, addrs[0])
			}
			if addrs[1] != fmt.Sprintf("127.0.0.1:%d", tc.port) {
				t.Fatalf("%s: 备选地址错误: %s", tc.name, addrs[1])
			}
		})
	}
}

// TestRejectTCPDialToSelf 直接向本机自己的监听端口发起TCP聊天连接，必须被拒绝：
// 不触发聊天开始事件、不进入聊天会话、连接被关闭（不能自己跟自己聊天）
func TestRejectTCPDialToSelf(t *testing.T) {
	os.RemoveAll("received_files")
	defer os.RemoveAll("received_files")

	started := make(chan struct{}, 2)
	c := NewChatClient()
	c.SetName("SelfMe")
	c.SetPortBase(6000)
	c.OnChatStarted = func(string) { started <- struct{}{} }
	c.OnLog = func(s string) { t.Logf("[C] %s", s) }
	c.Start()
	defer c.Stop()

	// 等待监听就绪
	waitFor(t, 10*time.Second, func() bool { return c.TCPPort() != 0 }, "TCP监听应就绪")

	// 直接连接自己（模拟错误fallback/自连），并发送携带自己端口的握手消息
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", c.TCPPort()), 5*time.Second)
	if err != nil {
		t.Fatalf("自连dial失败: %v", err)
	}
	defer conn.Close()

	handshake := common.Message{
		Type:    common.MsgTypeChat,
		From:    "SelfMe",
		Port:    c.TCPPort(), // 发送方端口与本机监听口相同 => 自连
		Content: "你好！",
	}
	data, _ := json.Marshal(handshake)
	if _, err := conn.Write(append(data, '\n')); err != nil {
		t.Fatalf("发送握手失败: %v", err)
	}

	// 服务端应立即关闭连接
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("自连握手后连接应被服务端关闭，却仍可读取")
	}

	// 不得触发聊天开始、不得进入会话
	select {
	case <-started:
		t.Fatal("自连不得触发OnChatStarted")
	case <-time.After(500 * time.Millisecond):
	}
	if c.IsInChat() {
		t.Fatal("自连后客户端不应进入聊天会话")
	}
}

// TestStartChatWithSelfEndpoint 通过公开API与自己端点聊天必须返回false
func TestStartChatWithSelfEndpoint(t *testing.T) {
	c := NewChatClient()
	c.SetName("SelfAPI")
	c.SetPortBase(7000)
	// 不Start，直接用已确定的localIP和默认0端口之外的场景：先Start拿端口
	c.OnLog = func(string) {}
	c.Start()
	defer c.Stop()
	waitFor(t, 10*time.Second, func() bool { return c.TCPPort() != 0 }, "监听应就绪")

	if c.StartChatWith(c.LocalIP(), c.TCPPort()) {
		t.Fatal("StartChatWith自己的网卡IP+端口必须被拒绝")
	}
	if c.StartChatWith("127.0.0.1", c.TCPPort()) {
		t.Fatal("StartChatWith回环IP+自己端口必须被拒绝")
	}
	if c.IsInChat() {
		t.Fatal("自我聊天被拒绝后不应进入会话")
	}
}
