package types

import (
	"os"
	"sync"
	"testing"
	"time"

	"localchat/pkg/common"
)

// simulateCrash 模拟进程崩溃/被杀死：直接关闭网络资源，不发送Offline广播
func simulateCrash(c *ChatClient) {
	c.online.Store(false)
	if c.udpConn != nil {
		c.udpConn.Close()
	}
	if c.tcpListener != nil {
		c.tcpListener.Close()
	}
}

// peerSnapshot 用OnPeersChanged维护线程安全的在线名单快照
func peerSnapshot(c *ChatClient) func() map[string]bool {
	var mu sync.Mutex
	names := map[string]bool{}
	c.OnPeersChanged = func(peers []common.Peer) {
		mu.Lock()
		defer mu.Unlock()
		names = map[string]bool{}
		for _, p := range peers {
			names[p.Name] = true
		}
	}
	return func() map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		cp := map[string]bool{}
		for k, v := range names {
			cp[k] = v
		}
		return cp
	}
}

// TestStalePeerReapedAfterCrash 对端崩溃（无Offline广播）时，超时收割应将其移出在线列表
func TestStalePeerReapedAfterCrash(t *testing.T) {
	os.RemoveAll("received_files")
	defer os.RemoveAll("received_files")

	// A、B使用短超时便于测试：1.5秒无响应即判定离线，300毫秒扫描一次
	a := NewChatClient()
	a.SetName("CrashA")
	a.SetPortBase(1000)
	a.peerTimeout = 1500 * time.Millisecond
	a.reapInterval = 300 * time.Millisecond
	aPeers := peerSnapshot(a)
	a.OnLog = func(s string) { t.Logf("[A] %s", s) }

	b := NewChatClient()
	b.SetName("CrashB")
	b.SetPortBase(1000)
	b.peerTimeout = 1500 * time.Millisecond
	b.reapInterval = 300 * time.Millisecond
	bPeers := peerSnapshot(b)

	// C为正常配置的第三方
	c := NewChatClient()
	c.SetName("CrashC")
	c.SetPortBase(1000)

	a.Start()
	b.Start()
	c.Start()
	defer func() {
		a.Stop()
		b.Stop()
		c.Stop() // 已崩溃时Stop为空操作
	}()

	// 等待三方互相发现
	waitFor(t, 20*time.Second, func() bool { return aPeers()["CrashC"] && aPeers()["CrashB"] }, "A应发现B和C")
	waitFor(t, 20*time.Second, func() bool { return bPeers()["CrashC"] && bPeers()["CrashA"] }, "B应发现A和C")
	t.Log("三方互见通过")

	// C崩溃：不发送任何Offline通知
	simulateCrash(c)
	t.Log("C已模拟崩溃（无Offline广播）")

	// A、B应在超时收割后自动将C从在线列表移除
	waitFor(t, 10*time.Second, func() bool { return !aPeers()["CrashC"] }, "A的在线列表应自动移除崩溃的C")
	waitFor(t, 10*time.Second, func() bool { return !bPeers()["CrashC"] }, "B的在线列表应自动移除崩溃的C")

	// 仍然存活的A、B不能被误删
	if !aPeers()["CrashB"] {
		t.Fatal("C崩溃后A不应丢失仍然在线的B")
	}
	if !bPeers()["CrashA"] {
		t.Fatal("C崩溃后B不应丢失仍然在线的A")
	}
	t.Log("超时离线收割通过：崩溃用户消失，存活用户保留")
}
