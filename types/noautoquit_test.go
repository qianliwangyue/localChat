package types

import (
	"os"
	"testing"
	"time"
)

// TestNoAutoQuitOnPeerTimeout 聊天对象仅UDP广播暂时中断（长时间无操作/网络抖动）、
// 但TCP聊天连接仍然完好时，不得自动结束聊天（不得自动执行/quit）。
// 超时只应把对方移出在线列表，会话保持且消息继续可达。
func TestNoAutoQuitOnPeerTimeout(t *testing.T) {
	os.RemoveAll("received_files")
	defer os.RemoveAll("received_files")

	// A使用极短超时快速复现收割；B保持默认配置（周期Discover为20秒，测试窗口内不会刷新）
	a := NewChatClient()
	a.SetName("KeepA")
	a.SetPortBase(3000)
	a.peerTimeout = 800 * time.Millisecond
	a.reapInterval = 200 * time.Millisecond
	aPeers := peerSnapshot(a)
	gotAfterReap := make(chan string, 2)
	a.OnChatMessage = func(from, content string) {
		if content == "reply after reap" {
			gotAfterReap <- from
		}
	}
	a.OnLog = func(s string) { t.Logf("[A] %s", s) }

	bMsgs := make(chan string, 4)
	b := NewChatClient()
	b.SetName("KeepB")
	b.SetPortBase(3000)
	b.OnChatStarted = func(string) {}
	b.OnChatMessage = func(from, content string) { bMsgs <- content }
	b.OnLog = func(s string) { t.Logf("[B] %s", s) }

	a.Start()
	b.Start()
	defer func() {
		a.Stop()
		b.Stop()
	}()

	// 建立A→B聊天
	waitFor(t, 20*time.Second, func() bool { return aPeers()["KeepB"] }, "A应发现B")
	if !a.StartChatWith(a.LocalIP(), b.TCPPort()) {
		t.Fatal("A发起聊天失败")
	}
	select {
	case m := <-bMsgs:
		if m != "你好！" {
			t.Fatalf("B应收到初始消息，实际: %s", m)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("B未收到初始消息")
	}
	if !a.IsInChat() || !b.IsInChat() {
		t.Fatal("聊天会话应已建立")
	}

	// 等待B因无UDP广播被A的收割逻辑移出在线列表（约1秒内发生）
	waitFor(t, 10*time.Second, func() bool { return !aPeers()["KeepB"] },
		"B应被超时收割移出A的在线列表")
	t.Log("B已被移出在线列表（模拟长时间无操作/广播中断）")

	// 核心断言：会话没有被自动/quit
	if !a.IsInChat() {
		t.Fatal("B仅UDP超时但TCP正常，A的聊天不应自动结束")
	}
	if !b.IsInChat() {
		t.Fatal("B仅UDP超时但TCP正常，B的聊天不应自动结束")
	}

	// TCP连接仍然完好：A能继续发消息，B能收到
	if !a.SendMessage("still connected") {
		t.Fatal("收割后A应仍能通过现有TCP连接发送消息")
	}
	select {
	case m := <-bMsgs:
		if m != "still connected" {
			t.Fatalf("B应收到A在收割后的消息，实际: %s", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("B未收到A在收割后的消息，TCP连接可能被误断")
	}

	// 反向消息同样可达
	if !b.SendMessage("reply after reap") {
		t.Fatal("B回复消息失败")
	}
	select {
	case from := <-gotAfterReap:
		if from != "KeepB" {
			t.Fatalf("A应收到B的回复，发送者: %s", from)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("A未收到B在收割后的回复")
	}
	t.Log("通过：UDP超时未触发自动quit，聊天会话与双向消息保持正常")
}
