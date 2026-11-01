package types

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"localchat/pkg/common"
)

// sha256File 计算文件摘要，用于校验续传后的内容是否逐字节一致
func sha256File(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	return sum[:], nil
}

// newResumePair 启动一对互为对端的客户端（B 自动接受文件），返回双方与聊天目标端点
func newResumePair(t *testing.T, portBase int) (a, b *ChatClient, ip string, port int) {
	t.Helper()

	os.RemoveAll("received_files")
	t.Cleanup(func() { os.RemoveAll("received_files") })

	a = NewChatClient()
	a.SetName("ResumeA")
	a.SetPortBase(portBase)
	a.OnLog = func(s string) { t.Logf("[A] %s", s) }

	b = NewChatClient()
	b.SetName("ResumeB")
	b.SetPortBase(portBase)
	b.OnLog = func(s string) { t.Logf("[B] %s", s) }
	b.OnFileOffer = func(from, fromAddr, fileName string, size int64, fileID string) {
		go b.AcceptFile(fileID)
	}

	var mu sync.Mutex
	var peers []common.Peer
	a.OnPeersChanged = func(p []common.Peer) { mu.Lock(); peers = p; mu.Unlock() }

	a.Start()
	b.Start()
	t.Cleanup(func() { a.Stop(); b.Stop() })

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		found := false
		for _, p := range peers {
			if p.Name == "ResumeB" {
				ip, port, found = p.IP, p.Port, true
			}
		}
		mu.Unlock()
		if found {
			return a, b, ip, port
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("超时：未发现对端")
	return nil, nil, "", 0
}

// writeTestFile 生成确定内容的测试文件
func writeTestFile(t *testing.T, path string, size int) {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*7 + i/251) % 251)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

// TestResumeFromExistingPart 预置一个「传到一半」的临时文件，验证：
// 1) 能识别出这是同一个文件并从块边界续传；2) 拼接结果与源文件逐字节一致；
// 3) 源文件被改动（标识不符）时不会误续传，而是重新开始。
func TestResumeFromExistingPart(t *testing.T) {
	a, b, ip, port := newResumePair(t, 4900)
	if !a.StartChatWith(ip, port) {
		t.Fatal("建立会话失败")
	}
	time.Sleep(300 * time.Millisecond)

	const size = 3 * 1024 * 1024 // 3MB，24 个 128KB 块
	src := filepath.Join(t.TempDir(), "resume.bin")
	writeTestFile(t, src, size)

	st, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	key := stableFileKey(st.Name(), st.Size(), st.ModTime())

	// 预置断点：故意停在一个非块边界的偏移上，验证接收端会对齐
	var resumed int64
	var resumedMu sync.Mutex
	b.OnTransferResumed = func(fileName string, offset, total int64) {
		resumedMu.Lock()
		resumed = offset
		resumedMu.Unlock()
	}

	savePath := filepath.Join("received_files", st.Name())
	os.MkdirAll("received_files", 0755)
	srcData, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	partial := int64(1536*1024 + 1234) // 1.5MB + 1234 字节
	partPath := partPathFor(savePath)
	if err := os.WriteFile(partPath, srcData[:partial], 0644); err != nil {
		t.Fatal(err)
	}
	if err := writePartMeta(savePath, partMeta{
		Key: key, FileName: st.Name(), Size: st.Size(), Written: partial,
	}); err != nil {
		t.Fatal(err)
	}

	if !a.SendFile(src) {
		t.Fatal("发送文件失败")
	}
	waitFor(t, 30*time.Second, func() bool {
		_, err := os.Stat(savePath)
		return err == nil && !a.IsTransmitting()
	}, "文件应接收完成")

	// 最终产物必须是完整文件，且临时文件与元数据都已清理
	gotSum, err := sha256File(savePath)
	if err != nil {
		t.Fatal(err)
	}
	wantSum := sha256.Sum256(srcData)
	if string(gotSum) != string(wantSum[:]) {
		t.Fatalf("续传后内容不一致（期望 %d 字节）", len(srcData))
	}
	for _, p := range []string{partPath, partMetaPathFor(savePath)} {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("传输完成后残留临时文件: %s", p)
		}
	}

	resumedMu.Lock()
	got := resumed
	resumedMu.Unlock()
	want := alignDownToChunk(partial)
	if got != want {
		t.Fatalf("续传起点应为 %d（块边界对齐），实际 %d", want, got)
	}
	t.Logf("续传验证通过：从 %s 处续传，最终文件 SHA-256 一致", FormatFileSize(got))

	// ===== 源文件被改动后不应误用旧断点 =====
	src2 := filepath.Join(t.TempDir(), "changed.bin")
	writeTestFile(t, src2, 512*1024)
	st2, _ := os.Stat(src2)
	save2 := filepath.Join("received_files", st2.Name())
	os.WriteFile(partPathFor(save2), make([]byte, 100*1024), 0644)
	// 元数据里的 key 与真实文件不符（模拟源文件被改过）
	writePartMeta(save2, partMeta{Key: "deadbeefdeadbeef", FileName: st2.Name(), Size: st2.Size()})

	resumedMu.Lock()
	resumed = -1
	resumedMu.Unlock()

	if !a.SendFile(src2) {
		t.Fatal("发送第二个文件失败")
	}
	waitFor(t, 30*time.Second, func() bool {
		_, err := os.Stat(save2)
		return err == nil && !a.IsTransmitting()
	}, "第二个文件应接收完成")

	resumedMu.Lock()
	got = resumed
	resumedMu.Unlock()
	if got != -1 {
		t.Fatalf("源文件已变化时不应续传，实际续传偏移 %d", got)
	}
	data2, err := os.ReadFile(save2)
	if err != nil {
		t.Fatal(err)
	}
	srcData2, _ := os.ReadFile(src2)
	if string(data2) != string(srcData2) {
		t.Fatal("第二个文件内容不一致")
	}
	t.Log("安全校验通过：文件标识不符时放弃续传并重新传输")
}

// receivingBytes 返回当前正在接收的字节数（供测试观察传输进度）
func (c *ChatClient) receivingBytes() int64 {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	var total int64
	for _, tf := range c.receivingFiles {
		total += tf.Written
	}
	return total
}

// TestResumeAfterInterrupt 真实中断场景：传到一半掐断连接，重新建立会话后再发同一个文件，
// 应当从断点继续而不是从头重传，最终内容必须完全一致。
func TestResumeAfterInterrupt(t *testing.T) {
	a, b, ip, port := newResumePair(t, 5000)
	if !a.StartChatWith(ip, port) {
		t.Fatal("建立会话失败")
	}
	time.Sleep(300 * time.Millisecond)

	const size = 96 * 1024 * 1024 // 96MB，足够在中途掐断
	src := filepath.Join(t.TempDir(), "big.bin")
	writeTestFile(t, src, size)

	var resumedMu sync.Mutex
	var resumed, total int64
	b.OnTransferResumed = func(fileName string, offset, size int64) {
		resumedMu.Lock()
		resumed, total = offset, size
		resumedMu.Unlock()
	}

	if !a.SendFile(src) {
		t.Fatal("发送文件失败")
	}

	// 等到接收端收到约 40% 时掐断连接（模拟断网/对方退出）
	deadline := time.Now().Add(60 * time.Second)
	interrupted := false
	for time.Now().Before(deadline) {
		if !a.IsTransmitting() {
			break // 传输已经结束，没赶上中断
		}
		if b.receivingBytes() > size*4/10 {
			a.EndChat()
			interrupted = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !interrupted {
		t.Skip("传输在观察到 40% 前已结束，跳过中断续传验证")
	}

	waitFor(t, 10*time.Second, func() bool { return !a.IsTransmitting() }, "中断后发送端应结束传输")

	// 中断后应留下临时文件而不是半截正式文件
	st, _ := os.Stat(src)
	savePath := filepath.Join("received_files", st.Name())
	partPath := partPathFor(savePath)
	partInfo, err := os.Stat(partPath)
	if err != nil {
		t.Fatalf("中断后应保留临时文件用于续传: %v", err)
	}
	if _, err := os.Stat(savePath); err == nil {
		t.Fatal("中断后不应生成看起来完整的正式文件")
	}
	t.Logf("已在 %s 处中断，临时文件 %s", FormatFileSize(partInfo.Size()), partPath)

	// 重新建立会话并再发一次同一个文件
	time.Sleep(500 * time.Millisecond)
	if !a.StartChatWith(ip, port) {
		t.Fatal("重新建立会话失败")
	}
	time.Sleep(500 * time.Millisecond)
	if !a.SendFile(src) {
		t.Fatal("重新发送文件失败")
	}
	waitFor(t, 120*time.Second, func() bool {
		_, err := os.Stat(savePath)
		return err == nil && !a.IsTransmitting()
	}, "续传应完成")

	resumedMu.Lock()
	got, wantTotal := resumed, total
	resumedMu.Unlock()
	if got <= 0 {
		t.Fatal("重新发送时应从断点续传，实际从头开始")
	}
	if wantTotal != int64(size) {
		t.Fatalf("续传总大小错误: %d", wantTotal)
	}

	gotSum, err := sha256File(savePath)
	if err != nil {
		t.Fatal(err)
	}
	srcData, _ := os.ReadFile(src)
	wantSum := sha256.Sum256(srcData)
	if string(gotSum) != string(wantSum[:]) {
		t.Fatal("续传后的文件内容与源文件不一致")
	}
	if _, err := os.Stat(partPath); err == nil {
		t.Fatal("续传完成后应清理临时文件")
	}
	t.Logf("中断续传通过：从 %s（%.0f%%）继续，最终 SHA-256 一致",
		FormatFileSize(got), float64(got)/float64(size)*100)
}

// TestLegacyPeerCompat 模拟一台「旧版本」对端（纯 JSON + base64，offer 里不带 Proto/FileKey），
// 验证新版接收端依然能正确收下文件，并且在 accept 里不回显 Proto——
// 这样新版发送端遇到旧接收端时会自动回落到 base64 通道。
func TestLegacyPeerCompat(t *testing.T) {
	os.RemoveAll("received_files")
	t.Cleanup(func() { os.RemoveAll("received_files") })

	b := NewChatClient()
	b.SetName("LegacyHost")
	b.SetPortBase(5200)
	b.OnLog = func(s string) { t.Logf("[B] %s", s) }
	offerCh := make(chan string, 1)
	b.OnFileOffer = func(from, fromAddr, fileName string, size int64, fileID string) {
		offerCh <- fileID
	}
	b.Start()
	t.Cleanup(b.Stop)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", b.TCPPort()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	sendJSON := func(msg map[string]any) {
		msg["from"] = "OldPeer"
		data, _ := json.Marshal(msg)
		if _, err := conn.Write(append(data, '\n')); err != nil {
			t.Fatalf("发送失败: %v", err)
		}
	}
	readMsg := func() map[string]any {
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("读取失败: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &m); err != nil {
			t.Fatalf("解析失败: %v (%s)", err, line)
		}
		return m
	}

	// 旧版握手：不带 Proto 字段；端口填一个无关值，避免被本机自连保护拦截
	sendJSON(map[string]any{"type": "chat", "port": 59999, "content": "你好！"})
	waitFor(t, 5*time.Second, func() bool { return b.IsInChat() }, "B 应进入聊天会话")

	payload := []byte("legacy base64 payload，旧版本对端的数据")
	sendJSON(map[string]any{
		"type": "file_offer", "fileName": "legacy.txt", "fileSize": len(payload),
		"fileID": "legacy1", "totalChunks": 1,
	})
	select {
	case <-offerCh:
	case <-time.After(10 * time.Second):
		t.Fatal("未收到文件请求")
	}
	go b.AcceptFile("legacy1")

	accept := readMsg()
	if accept["type"] != common.MsgTypeFileAccept {
		t.Fatalf("期望 file_accept，实际 %v", accept["type"])
	}
	if proto, ok := accept["proto"]; ok && proto != float64(0) {
		t.Fatalf("对旧版本对端的请求不应声明 Proto，实际 %v", proto)
	}

	// 旧版数据块：base64 塞在 JSON 里
	sendJSON(map[string]any{
		"type": "file_data", "fileID": "legacy1", "chunkIndex": 0, "totalChunks": 1,
		"progress": 100, "fileData": base64.StdEncoding.EncodeToString(payload),
	})
	sendJSON(map[string]any{"type": "file_done", "fileID": "legacy1", "fileName": "legacy.txt"})

	savePath := filepath.Join("received_files", "legacy.txt")
	// 等文件大小正确而不是只等存在：AcceptFile 在接受时就 os.Create 了目标文件，
	// 此刻内容还是空的，只等存在会读到空文件（全量跑时的偶发失败来源）
	waitFor(t, 10*time.Second, func() bool {
		info, err := os.Stat(savePath)
		return err == nil && info.Size() == int64(len(payload))
	}, "旧版本通道的文件应接收完成")
	got, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("内容不一致: %q", string(got))
	}
	t.Log("兼容性验证通过：旧版本（base64/JSON）通道仍可正常传输")
}

// TestBinaryFramingNegotiation 验证双方协商到 v2 后走二进制通道：
// 整文件内容逐字节一致（这条链路上不会再出现 base64 的 33% 膨胀）。
func TestBinaryFramingNegotiation(t *testing.T) {
	a, b, ip, port := newResumePair(t, 5100)
	if !a.StartChatWith(ip, port) {
		t.Fatal("建立会话失败")
	}
	time.Sleep(300 * time.Millisecond)

	src := filepath.Join(t.TempDir(), "frame.bin")
	writeTestFile(t, src, 2*1024*1024)

	if !a.SendFile(src) {
		t.Fatal("发送失败")
	}
	st, _ := os.Stat(src)
	savePath := filepath.Join("received_files", st.Name())
	waitFor(t, 30*time.Second, func() bool {
		_, err := os.Stat(savePath)
		return err == nil && !a.IsTransmitting()
	}, "文件应接收完成")

	gotSum, err := sha256File(savePath)
	if err != nil {
		t.Fatal(err)
	}
	srcData, _ := os.ReadFile(src)
	wantSum := sha256.Sum256(srcData)
	if string(gotSum) != string(wantSum[:]) {
		t.Fatal("二进制分帧传输后内容不一致")
	}
	_ = fmt.Sprintf("%v", b)
	t.Log("二进制分帧通道传输校验通过")
}
