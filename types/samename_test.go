package types

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"localchat/pkg/common"
)

// TestSameNameDifferentEndpoint 两个代号完全相同但端点不同的用户可以正常聊天和传文件
func TestSameNameDifferentEndpoint(t *testing.T) {
	os.RemoveAll("received_files")
	defer os.RemoveAll("received_files")

	var mu sync.Mutex
	aPeers := map[string]int{} // 同名用户按端点计数
	gotFromB := false

	// A、B使用完全相同的代号（本机测试IP相同、端口不同，比"不同IP"场景更严格）
	a := NewChatClient()
	a.SetName("Twin")
	a.SetPortBase(2000)
	a.OnPeersChanged = func(peers []common.Peer) {
		mu.Lock()
		defer mu.Unlock()
		aPeers = map[string]int{}
		for _, p := range peers {
			aPeers[peerKeyOf(p.IP, p.Port)]++
		}
	}
	a.OnChatMessage = func(from, content string) {
		mu.Lock()
		defer mu.Unlock()
		if from == "Twin" && content == "reply from twin B" {
			gotFromB = true
		}
	}
	a.OnLog = func(s string) { t.Logf("[A] %s", s) }

	chatStarted := make(chan string, 2)
	msgCh := make(chan string, 4)
	b := NewChatClient()
	b.SetName("Twin")
	b.SetPortBase(2000)
	b.OnChatStarted = func(name string) { chatStarted <- name }
	b.OnChatMessage = func(from, content string) { msgCh <- from + "|" + content }
	b.OnFileOffer = func(from, fromAddr, fileName string, size int64, fileID string) {
		t.Logf("[B] 收到同名用户 %s(%s) 的文件 %s，自动接受", from, fromAddr, fileName)
		go b.AcceptFile(fileID)
	}
	b.OnLog = func(s string) { t.Logf("[B] %s", s) }

	a.Start()
	b.Start()
	defer func() {
		a.Stop()
		b.Stop()
	}()

	bAddr := peerKeyOf(b.LocalIP(), b.TCPPort())

	// A应能发现同名的B（按端点存在，而不是被当作自己过滤）
	waitFor(t, 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return aPeers[bAddr] == 1
	}, "A应发现同名但端点不同的B")
	t.Log("同名用户发现通过：B未被当作A自己过滤")

	// 与自己的端点聊天必须被拒绝
	if a.StartChatWith(a.LocalIP(), a.TCPPort()) {
		t.Fatal("与自己端点聊天应被拒绝")
	}
	t.Log("自我拦截通过：IP+端口完全一致才判定为自己")

	// 按端点精确发起聊天（不能按名字，因为名字与自己相同）
	if !a.StartChatWith(b.LocalIP(), b.TCPPort()) {
		t.Fatal("与同名不同端点的B发起聊天失败")
	}
	select {
	case name := <-chatStarted:
		if name != "Twin" {
			t.Fatalf("B收到的聊天对象代号应为Twin，实际: %s", name)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("B未收到聊天开始事件")
	}

	// B收到A的初始消息
	select {
	case m := <-msgCh:
		if m != "Twin|你好！" {
			t.Fatalf("B应收到A的初始消息，实际: %s", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("B未收到初始消息")
	}

	// B回复A
	if !b.SendMessage("reply from twin B") {
		t.Fatal("B回复消息失败")
	}
	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotFromB
	}, "A应收到同名B的回复")
	t.Log("同名用户双向聊天通过")

	// 准备100KB二进制文件并发送
	tmp := t.TempDir()
	binFile := filepath.Join(tmp, "twin_bin.dat")
	binData := make([]byte, 100*1024)
	for i := range binData {
		binData[i] = byte(i*7 + 3)
	}
	if err := os.WriteFile(binFile, binData, 0644); err != nil {
		t.Fatal(err)
	}
	if !a.SendFile(binFile) {
		t.Fatal("同名用户间发送文件失败")
	}
	waitFor(t, 30*time.Second, func() bool {
		got, err := os.ReadFile(filepath.Join("received_files", "twin_bin.dat"))
		return err == nil && bytes.Equal(got, binData) && !a.IsTransmitting()
	}, "B应完整接收同名A发送的文件")
	t.Log("同名用户文件传输通过：100KB二进制内容校验一致")

	// 会话对象身份正确（代号相同也能记录端点）
	if !b.IsCurrentChatPeer(a.LocalIP(), a.TCPPort()) {
		t.Fatal("B的当前聊天对象端点应指向A")
	}
	if b.IsCurrentChatPeer(b.LocalIP(), b.TCPPort()) {
		t.Fatal("B不应把自己识别为当前聊天对象")
	}

	// 第三个同名用户C上线后正常退出：不能误伤A-B之间的会话
	c := NewChatClient()
	c.SetName("Twin")
	c.SetPortBase(2000)
	c.OnLog = func(s string) { t.Logf("[C] %s", s) }
	c.Start()
	defer c.Stop()

	cAddr := peerKeyOf(c.LocalIP(), c.TCPPort())
	waitFor(t, 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return aPeers[cAddr] == 1
	}, "A应发现第三个同名用户C")

	// C正常退出（发送Offline广播）
	c.Stop()
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return aPeers[cAddr] == 0
	}, "C下线后A的列表应移除C")

	// A与同名B的会话必须仍然保持（端点匹配，不能因代号相同误结束）
	if !a.IsInChat() || !b.IsInChat() {
		t.Fatal("同名第三方C下线后，A与B的聊天会话不应被误结束")
	}
	if a.SendMessage("still alive?") != true {
		t.Fatal("C下线后A应仍能向B发消息")
	}
	select {
	case m := <-msgCh:
		if m != "Twin|still alive?" {
			t.Fatalf("B应在C下线后继续收到A的消息，实际: %s", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("C下线后B未再收到A的消息")
	}
	t.Log("同名第三方下线不误伤会话通过")
}
