package types

// 用户主动放弃传输的测试：发送方 CancelSending / 接收方 CancelReceiving。
// 验证：双方状态复位、临时文件清理、取消后还能正常发起新传输。

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"localchat/pkg/common"
)

// waitUntil 每 5ms 轮询 cond，直到为真或超时
func waitUntil(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// setupCancelPair 启动一对互为对端的客户端，B 自动接受文件
func setupCancelPair(t *testing.T, portBase int) (a, b *ChatClient, mu *sync.Mutex) {
	t.Helper()
	a = NewChatClient()
	a.SetName("CancelA")
	a.SetPortBase(portBase)
	b = NewChatClient()
	b.SetName("CancelB")
	b.SetPortBase(portBase)

	mu = &sync.Mutex{}
	var peersA, peersB []common.Peer
	a.OnPeersChanged = func(p []common.Peer) { mu.Lock(); peersA = p; mu.Unlock() }
	b.OnPeersChanged = func(p []common.Peer) { mu.Lock(); peersB = p; mu.Unlock() }
	b.OnFileOffer = func(from, fromAddr, fileName string, size int64, fileID string) {
		go b.AcceptFile(fileID)
	}

	dirA := filepath.Join(t.TempDir(), "a_recv")
	dirB := filepath.Join(t.TempDir(), "b_recv")
	if err := a.SetFileDataDir(dirA); err != nil {
		t.Fatal(err)
	}
	if err := b.SetFileDataDir(dirB); err != nil {
		t.Fatal(err)
	}

	a.Start()
	b.Start()
	t.Cleanup(func() { a.Stop(); b.Stop() })

	if !waitUntil(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(peersA) > 0 && len(peersB) > 0
	}, 20*time.Second) {
		t.Fatal("未互相发现")
	}
	mu.Lock()
	var ip string
	var port int
	for _, p := range peersA {
		if p.Name == "CancelB" {
			ip, port = p.IP, p.Port
		}
	}
	mu.Unlock()
	if !a.StartChatWith(ip, port) {
		t.Fatal("建立会话失败")
	}
	time.Sleep(300 * time.Millisecond)
	return a, b, mu
}

// bigTestFile 生成 sizeMB 大小的测试文件
func bigTestFile(t *testing.T, sizeMB int) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "big.bin")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 1024*1024)
	for i := range buf {
		buf[i] = byte(i * 7 % 251)
	}
	for i := 0; i < sizeMB; i++ {
		if _, err := f.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

// TestCancelSendingMidTransfer 发送方中途放弃发送：
// 双方都应收到结束回调、状态复位，且取消后还能再次正常传输。
func TestCancelSendingMidTransfer(t *testing.T) {
	a, b, mu := setupCancelPair(t, 4820)

	aEnded := make(chan string, 4)
	bEnded := make(chan string, 4)
	a.OnTransferEnded = func(sending bool, text string) {
		if sending {
			aEnded <- text
		}
	}
	b.OnTransferEnded = func(sending bool, text string) {
		if !sending {
			bEnded <- text
		}
	}

	src := bigTestFile(t, 256)
	go a.SendFile(src)

	// 等发送真正开始
	if !waitUntil(func() bool {
		a.fileMutex.Lock()
		defer a.fileMutex.Unlock()
		return len(a.sendingFiles) > 0
	}, 10*time.Second) {
		t.Fatal("发送未启动")
	}
	time.Sleep(50 * time.Millisecond) // 让若干块先发出去

	a.CancelSending()

	select {
	case text := <-aEnded:
		t.Logf("发送方回调: %s", text)
	case <-time.After(5 * time.Second):
		t.Fatal("发送方未收到放弃回调")
	}
	select {
	case text := <-bEnded:
		t.Logf("接收方回调: %s", text)
	case <-time.After(5 * time.Second):
		t.Fatal("接收方未收到取消回调")
	}

	// 状态复位
	if !waitUntil(func() bool {
		return !a.IsTransmitting() && !b.IsReceiving()
	}, 5*time.Second) {
		a.fileMutex.Lock()
		t.Fatalf("取消后状态未复位: a.sendingFiles=%d b.receivingFiles=%d",
			len(a.sendingFiles), len(b.receivingFiles))
	}
	// 接收端不应残留临时文件
	mu.Lock()
	b.fileMutex.Lock()
	_ = mu
	b.fileMutex.Unlock()
	mu.Unlock()
	leftovers, _ := filepath.Glob(filepath.Join(b.FileDataDir(), "*.lcpart*"))
	if len(leftovers) > 0 {
		t.Fatalf("接收端残留临时文件: %v", leftovers)
	}

	// 取消后还能再次正常传输
	small := filepath.Join(t.TempDir(), "small.txt")
	if err := os.WriteFile(small, []byte("after cancel"), 0644); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 1)
	b.OnFileSaved = func(name, path string) { done <- struct{}{} }
	if !a.SendFile(small) {
		t.Fatal("取消后无法再次发起发送")
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("取消后的再次传输未完成")
	}
}

// TestCancelReceivingMidTransfer 接收方中途放弃接收：
// 临时文件被丢弃、发送方收到通知并复位、之后还能再次正常传输。
func TestCancelReceivingMidTransfer(t *testing.T) {
	a, b, _ := setupCancelPair(t, 4860)

	aEnded := make(chan string, 4)
	bEnded := make(chan string, 4)
	a.OnTransferEnded = func(sending bool, text string) {
		if sending {
			aEnded <- text
		}
	}
	b.OnTransferEnded = func(sending bool, text string) {
		if !sending {
			bEnded <- text
		}
	}

	src := bigTestFile(t, 256)
	go a.SendFile(src)

	// 等接收真正开始
	if !waitUntil(func() bool {
		b.fileMutex.Lock()
		defer b.fileMutex.Unlock()
		return len(b.receivingFiles) > 0
	}, 10*time.Second) {
		t.Fatal("接收未启动")
	}
	time.Sleep(50 * time.Millisecond)

	b.CancelReceiving()

	select {
	case text := <-bEnded:
		t.Logf("接收方回调: %s", text)
	case <-time.After(5 * time.Second):
		t.Fatal("接收方未收到放弃回调")
	}
	select {
	case text := <-aEnded:
		t.Logf("发送方回调: %s", text)
	case <-time.After(5 * time.Second):
		t.Fatal("发送方未收到取消回调")
	}

	if !waitUntil(func() bool {
		return !a.IsTransmitting() && !b.IsReceiving()
	}, 5*time.Second) {
		a.fileMutex.Lock()
		b.fileMutex.Lock()
		t.Fatalf("取消后状态未复位: a.sendingFiles=%d b.receivingFiles=%d",
			len(a.sendingFiles), len(b.receivingFiles))
	}
	leftovers, _ := filepath.Glob(filepath.Join(b.FileDataDir(), "*.lcpart*"))
	if len(leftovers) > 0 {
		t.Fatalf("接收端残留临时文件: %v", leftovers)
	}

	small := filepath.Join(t.TempDir(), "small.txt")
	if err := os.WriteFile(small, []byte("after recv cancel"), 0644); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 1)
	b.OnFileSaved = func(name, path string) { done <- struct{}{} }
	if !a.SendFile(small) {
		t.Fatal("取消后无法再次发起发送")
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("取消后的再次传输未完成")
	}
}

// TestCancelProgressCallbackOrder 回归测试：取消传输时，进度回调绝不能晚于
// 结束回调到达。乱序会让 GUI 在 endProgress 之后又用迟到的进度回调重建
// 进度对话框，发送方卡在传输页面（约 30% 概率复现的 bug）。
func TestCancelProgressCallbackOrder(t *testing.T) {
	// run 是一次「发送→中途取消→检查回调顺序」的完整迭代
	run := func(t *testing.T, portBase int, senderCancels bool) {
		a, b, _ := setupCancelPair(t, portBase)

		var evMu sync.Mutex
		var order []string
		record := func(e string) { evMu.Lock(); order = append(order, e); evMu.Unlock() }
		a.OnFileProgress = func(pct int, sending bool, label string) { record("p") }
		a.OnTransferEnded = func(sending bool, text string) {
			if sending {
				record("e")
			}
		}
		b.OnFileProgress = func(pct int, sending bool, label string) { record("p") }
		b.OnTransferEnded = func(sending bool, text string) {
			if !sending {
				record("e")
			}
		}

		src := bigTestFile(t, 256)
		go a.SendFile(src)

		if senderCancels {
			if !waitUntil(func() bool {
				a.fileMutex.Lock()
				defer a.fileMutex.Unlock()
				return len(a.sendingFiles) > 0
			}, 10*time.Second) {
				t.Fatal("发送未启动")
			}
		} else {
			if !waitUntil(func() bool {
				b.fileMutex.Lock()
				defer b.fileMutex.Unlock()
				return len(b.receivingFiles) > 0
			}, 10*time.Second) {
				t.Fatal("接收未启动")
			}
		}
		time.Sleep(50 * time.Millisecond)

		if senderCancels {
			a.CancelSending()
		} else {
			b.CancelReceiving()
		}

		// 等结束回调到齐，再留出窗口观察是否有迟到的进度回调
		if !waitUntil(func() bool {
			evMu.Lock()
			defer evMu.Unlock()
			return len(order) > 0 && order[len(order)-1] == "e"
		}, 10*time.Second) {
			t.Fatalf("未观察到结束回调，事件序列: %v", order)
		}
		time.Sleep(300 * time.Millisecond)
		evMu.Lock()
		defer evMu.Unlock()
		for i, e := range order {
			if e == "p" && i > 0 && order[i-1] == "e" {
				t.Fatalf("进度回调出现在结束回调之后（乱序），事件序列: %v", order)
			}
		}
	}

	for i := 0; i < 6; i++ {
		t.Run(fmt.Sprintf("发送方取消/%d", i), func(t *testing.T) {
			run(t, 4900+i*20, true)
		})
	}
	for i := 0; i < 6; i++ {
		t.Run(fmt.Sprintf("接收方取消/%d", i), func(t *testing.T) {
			run(t, 5020+i*20, false)
		})
	}
}
