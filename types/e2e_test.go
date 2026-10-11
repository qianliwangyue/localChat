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

// waitFor 轮询等待条件成立
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("超时等待: %s", msg)
}

// TestE2E 端到端测试：两个客户端实例在同一台机器上完成发现、聊天、文件与文件夹传输
func TestE2E(t *testing.T) {
	// 清理上次运行残留，确保文件校验针对本次传输
	os.RemoveAll("received_files")
	defer os.RemoveAll("received_files")

	// ===== 准备测试数据 =====
	tmp := t.TempDir()

	// 普通小文件
	smallFile := filepath.Join(tmp, "hello.txt")
	if err := os.WriteFile(smallFile, []byte("hello localchat"), 0644); err != nil {
		t.Fatal(err)
	}

	// 100KB二进制文件（跨多个16KB数据块，验证base64编解码）
	binFile := filepath.Join(tmp, "bin.dat")
	binData := make([]byte, 100*1024)
	for i := range binData {
		binData[i] = byte(i % 251)
	}
	if err := os.WriteFile(binFile, binData, 0644); err != nil {
		t.Fatal(err)
	}

	// 文件夹（含子目录）
	folder := filepath.Join(tmp, "testfolder")
	if err := os.MkdirAll(filepath.Join(folder, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "a.txt"), []byte("AAA"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "sub", "b.txt"), []byte("BBB"), 0644); err != nil {
		t.Fatal(err)
	}

	// ===== 客户端A（Alice，主动方） =====
	var mu sync.Mutex
	aPeers := map[string]bool{}
	aMsgs := []string{}
	a := NewChatClient()
	a.SetName("Alice")
	a.OnLog = func(s string) { t.Logf("[A] %s", s) }
	a.OnPeersChanged = func(peers []common.Peer) {
		mu.Lock()
		defer mu.Unlock()
		aPeers = map[string]bool{}
		for _, p := range peers {
			aPeers[p.Name] = true
		}
	}
	a.OnChatMessage = func(from, content string) {
		mu.Lock()
		defer mu.Unlock()
		aMsgs = append(aMsgs, from+":"+content)
	}
	a.OnFileProgress = func(pct int, sending bool, label string) { t.Logf("[A进度] %d%% %s", pct, label) }
	a.OnTransferEnded = func(sending bool, text string) { t.Logf("[A传输结束] %v %s", sending, text) }

	// ===== 客户端B（Bob，被动方） =====
	bChatStarted := make(chan string, 4)
	bMsgs := make(chan string, 8)
	bChatEnded := make(chan bool, 4)
	b := NewChatClient()
	b.SetName("Bob")
	b.OnLog = func(s string) { t.Logf("[B] %s", s) }
	b.OnChatStarted = func(peerName string) { bChatStarted <- peerName }
	b.OnChatMessage = func(from, content string) { bMsgs <- from + ":" + content }
	b.OnChatEnded = func() { bChatEnded <- true }
	b.OnFileProgress = func(pct int, sending bool, label string) { t.Logf("[B进度] %d%% %s", pct, label) }
	b.OnTransferEnded = func(sending bool, text string) { t.Logf("[B传输结束] %v %s", sending, text) }
	// 收到文件请求自动接受
	b.OnFileOffer = func(from, fileName string, size int64, fileID string) {
		t.Logf("[B] 收到文件请求 %s (%d字节)，自动接受", fileName, size)
		go b.AcceptFile(fileID)
	}
	// 收到文件夹请求自动接受
	b.OnFolderOffer = func(from, folderName string, size int64, fileCount int, folderID string) {
		t.Logf("[B] 收到文件夹请求 %s (%d个文件)，自动接受", folderName, fileCount)
		go b.AcceptFolder(folderID)
	}

	// ===== 启动 =====
	a.Start()
	b.Start()
	defer func() {
		a.Stop()
		b.Stop()
	}()

	// ===== 1. 服务发现 =====
	t.Log("--- 测试服务发现 ---")
	waitFor(t, 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return aPeers["Bob"]
	}, "A应发现B(Bob)在线")
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(b.peers) > 0
	}, "B应发现A(Alice)在线")
	t.Log("服务发现通过：双方互相发现")

	// ===== 2. 聊天 =====
	t.Log("--- 测试聊天 ---")
	go a.StartChat("Bob")

	select {
	case name := <-bChatStarted:
		if name != "Alice" {
			t.Fatalf("B应与Alice开始聊天，实际: %s", name)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("超时：B未收到聊天会话开始事件")
	}

	// B收到A的初始消息"你好！"
	select {
	case msg := <-bMsgs:
		if msg != "Alice:你好！" {
			t.Fatalf("B应收到'Alice:你好！'，实际: %s", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("超时：B未收到初始消息")
	}

	// B回复，A应收到
	if !b.SendMessage("hi alice") {
		t.Fatal("B发送消息失败")
	}
	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range aMsgs {
			if m == "Bob:hi alice" {
				return true
			}
		}
		return false
	}, "A应收到B的回复消息")
	t.Log("聊天通过：双向收发正常")

	// ===== 3. 文件传输（二进制多块） =====
	t.Log("--- 测试文件传输 ---")
	if !a.SendFile(binFile) {
		t.Fatal("A发起文件发送失败")
	}
	waitFor(t, 30*time.Second, func() bool {
		_, err := os.Stat(filepath.Join("received_files", "bin.dat"))
		return err == nil && !a.IsTransmitting()
	}, "bin.dat应接收完成且A结束传输")
	got, err := os.ReadFile(filepath.Join("received_files", "bin.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, binData) {
		t.Fatalf("文件内容不一致: 期望%d字节，实际%d字节", len(binData), len(got))
	}
	t.Log("文件传输通过：100KB二进制文件内容校验一致")

	// ===== 4. 小文件传输 =====
	t.Log("--- 测试小文件传输 ---")
	if !a.SendFile(smallFile) {
		t.Fatal("A发起小文件发送失败")
	}
	waitFor(t, 30*time.Second, func() bool {
		_, err := os.Stat(filepath.Join("received_files", "hello.txt"))
		return err == nil && !a.IsTransmitting()
	}, "hello.txt应接收完成且A结束传输")
	got, err = os.ReadFile(filepath.Join("received_files", "hello.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello localchat" {
		t.Fatalf("小文件内容不一致: %s", string(got))
	}
	t.Log("小文件传输通过")

	// ===== 5. 文件夹传输（含子目录） =====
	t.Log("--- 测试文件夹传输 ---")
	if !a.SendFolder(folder) {
		t.Fatal("A发起文件夹发送失败")
	}
	recvFolder := filepath.Join("received_files", "testfolder")
	waitFor(t, 30*time.Second, func() bool {
		data, err := os.ReadFile(filepath.Join(recvFolder, "sub", "b.txt"))
		return err == nil && string(data) == "BBB" && !a.IsTransmitting()
	}, "B应收到testfolder/sub/b.txt且A结束传输")
	data, err := os.ReadFile(filepath.Join(recvFolder, "a.txt"))
	if err != nil || string(data) != "AAA" {
		t.Fatalf("子文件a.txt内容校验失败: err=%v data=%s", err, data)
	}
	t.Log("文件夹传输通过：目录结构及内容校验一致")

	// ===== 6. 结束聊天 =====
	t.Log("--- 测试结束聊天 ---")
	a.EndChat()
	select {
	case <-bChatEnded:
		t.Log("结束聊天通过：B收到会话结束通知")
	case <-time.After(5 * time.Second):
		t.Fatal("超时：B未收到聊天结束通知")
	}
}
