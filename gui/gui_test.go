package gui

import (
	"bytes"
	"localchat/pkg/common"
	"localchat/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/test"
)

// waitCond 轮询等待条件成立，失败时可选输出诊断信息
func waitCond(t *testing.T, timeout time.Duration, cond func() bool, msg string, dump ...func()) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, d := range dump {
		d()
	}
	t.Fatalf("超时等待: %s", msg)
}

// newTestUI 使用Fyne测试驱动器创建真实的ChatUI实例
func newTestUI(t *testing.T, name string) *ChatUI {
	ui := &ChatUI{win: test.NewWindow(nil)}
	ui.win.Resize(fyne.NewSize(960, 640))

	client := types.NewChatClient()
	client.SetName(name)
	ui.client = client
	ui.bindCallbacks()
	client.Start()
	ui.showMainUI()
	return ui
}

// TestGUITransferFlow 模拟真实GUI完整流程：发现→聊天→发送文件→发送文件夹→拒绝文件
// 注意：测试驱动器是单线程同步执行fyne.Do，因此本测试不开启-race（UI与网络goroutine
// 在真实应用中由事件循环串行化，测试驱动器下由uiDo内的锁串行化）
func TestGUITransferFlow(t *testing.T) {
	test.NewApp()
	os.RemoveAll("received_files")
	defer os.RemoveAll("received_files")

	uiA := newTestUI(t, "GuiA")
	uiB := newTestUI(t, "GuiB")
	defer func() {
		uiA.client.Stop()
		uiB.client.Stop()
	}()

	// B端自动接受文件/文件夹请求（模拟用户点击"是"对话框）
	uiB.client.OnFileOffer = func(from, fileName string, size int64, fileID string) {
		t.Logf("[B] 收到文件请求 %s(%d字节)，自动接受", fileName, size)
		go uiB.client.AcceptFile(fileID)
	}
	uiB.client.OnFolderOffer = func(from, folderName string, size int64, fileCount int, folderID string) {
		t.Logf("[B] 收到文件夹请求 %s(%d个文件)，自动接受", folderName, fileCount)
		go uiB.client.AcceptFolder(folderID)
	}

	// ===== 1. 服务发现 =====
	waitCond(t, 20*time.Second, func() bool {
		found := false
		uiA.uiDo(func() {
			for _, p := range uiA.peers {
				if p.Name == "GuiB" {
					found = true
				}
			}
		})
		return found
	}, "A的界面应显示B在线")
	t.Log("服务发现通过：A界面显示B在线")

	// ===== 2. 点击用户列表开始聊天 =====
	var target common.Peer
	uiA.uiDo(func() {
		for _, p := range uiA.peers {
			if p.Name == "GuiB" {
				target = p
			}
		}
	})
	uiA.uiDo(func() { uiA.tryStartChat(target) })
	waitCond(t, 15*time.Second, func() bool {
		started := false
		uiB.uiDo(func() { started = strings.Contains(uiB.chatLog.Text, "[GuiA]: 你好！") })
		return started
	}, "B应收到聊天会话与初始消息")
	t.Log("聊天开始通过：A→B会话建立，B收到'你好！'")

	// B通过输入框回复
	uiB.uiDo(func() {
		uiB.msgInput.SetText("hi from B")
		uiB.sendMsg()
	})
	waitCond(t, 5*time.Second, func() bool {
		has := false
		uiA.uiDo(func() { has = strings.Contains(uiA.chatLog.Text, "[GuiB]: hi from B") })
		return has
	}, "A的聊天记录应显示B的消息",
		func() {
			uiA.uiDo(func() { t.Logf("[诊断] A日志: %q", uiA.chatLog.Text) })
			uiB.uiDo(func() { t.Logf("[诊断] B日志: %q", uiB.chatLog.Text) })
			t.Logf("[诊断] B在会话中: %v, B当前聊天: %s", uiB.client.IsInChat(), uiB.client.GetCurrentChat())
		})
	t.Log("消息收发通过")

	// ===== 3. 发送100KB二进制文件 =====
	tmp := t.TempDir()
	binFile := filepath.Join(tmp, "gui_bin.dat")
	binData := make([]byte, 100*1024)
	for i := range binData {
		binData[i] = byte(i % 251)
	}
	if err := os.WriteFile(binFile, binData, 0644); err != nil {
		t.Fatal(err)
	}

	if !uiA.client.SendFile(binFile) {
		t.Fatal("A发送文件失败")
	}

	// 验证：B收到完整内容，A进度对话框已关闭（OnTransferEnded触发endProgress）
	waitCond(t, 30*time.Second, func() bool {
		got, err := os.ReadFile(filepath.Join("received_files", "gui_bin.dat"))
		return err == nil && bytes.Equal(got, binData) && !uiA.client.IsTransmitting() && uiA.progressDlg == nil
	}, "B应收到完整文件且A的进度对话框已关闭")
	t.Log("文件发送通过：内容校验一致，进度对话框正常关闭")

	// ===== 4. 发送文件夹 =====
	folder := filepath.Join(tmp, "gui_folder")
	if err := os.MkdirAll(filepath.Join(folder, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "a.txt"), []byte("AAA"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "sub", "b.txt"), []byte("BBB"), 0644); err != nil {
		t.Fatal(err)
	}

	if !uiA.client.SendFolder(folder) {
		t.Fatal("A发送文件夹失败")
	}
	waitCond(t, 30*time.Second, func() bool {
		d1, e1 := os.ReadFile(filepath.Join("received_files", "gui_folder", "a.txt"))
		d2, e2 := os.ReadFile(filepath.Join("received_files", "gui_folder", "sub", "b.txt"))
		return e1 == nil && string(d1) == "AAA" && e2 == nil && string(d2) == "BBB" &&
			!uiA.client.IsTransmitting() && uiA.progressDlg == nil
	}, "B应收到完整文件夹且A的进度对话框已关闭")
	t.Log("文件夹发送通过：目录结构与内容校验一致")

	// ===== 5. 拒绝文件后能继续发送（验证清理逻辑） =====
	rejected := false
	uiB.client.OnFileOffer = func(from, fileName string, size int64, fileID string) {
		t.Logf("[B] 收到文件请求 %s，自动拒绝", fileName)
		rejected = true
		go uiB.client.RejectFile(fileID)
	}
	if !uiA.client.SendFile(binFile) {
		t.Fatal("A发送文件失败（拒绝测试）")
	}
	waitCond(t, 15*time.Second, func() bool {
		return rejected && !uiA.client.IsTransmitting() && uiA.progressDlg == nil
	}, "拒绝后A应结束传输并关闭进度对话框",
		func() {
			uiA.uiDo(func() { t.Logf("[诊断] A日志: %q", uiA.chatLog.Text) })
			uiB.uiDo(func() { t.Logf("[诊断] B日志: %q", uiB.chatLog.Text) })
			t.Logf("[诊断] rejected=%v A传输中=%v A对话框=%v", rejected, uiA.client.IsTransmitting(), uiA.progressDlg != nil)
		})
	t.Log("拒绝流程通过：资源已清理")

	// ===== 6. 拒绝后再次发送应成功 =====
	uiB.client.OnFileOffer = func(from, fileName string, size int64, fileID string) {
		t.Logf("[B] 收到文件请求 %s，自动接受", fileName)
		go uiB.client.AcceptFile(fileID)
	}
	if !uiA.client.SendFile(binFile) {
		t.Fatal("拒绝后A再次发送文件失败（transmitting未正确复位？）")
	}
	waitCond(t, 30*time.Second, func() bool {
		got, err := os.ReadFile(filepath.Join("received_files", "gui_bin.dat"))
		return err == nil && bytes.Equal(got, binData) && !uiA.client.IsTransmitting() && uiA.progressDlg == nil
	}, "拒绝后重新发送的文件应完整接收")
	t.Log("拒绝后重发通过")
}

// peerNames 在uiDo锁内安全读取在线用户名单
func peerNames(ui *ChatUI) []string {
	var names []string
	ui.uiDo(func() {
		names = make([]string, 0, len(ui.peers))
		for _, p := range ui.peers {
			names = append(names, p.Name)
		}
	})
	return names
}

func containsName(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// TestGUIPeerListRefreshDuringChat 验证A与B聊天时C上线/下线，A、B的在线列表都同步更新
func TestGUIPeerListRefreshDuringChat(t *testing.T) {
	test.NewApp()
	os.RemoveAll("received_files")
	defer os.RemoveAll("received_files")

	uiA := newTestUI(t, "ListA")
	uiB := newTestUI(t, "ListB")
	defer func() {
		uiA.client.Stop()
		uiB.client.Stop()
	}()

	// A、B互相发现
	waitCond(t, 20*time.Second, func() bool {
		return containsName(peerNames(uiA), "ListB") && containsName(peerNames(uiB), "ListA")
	}, "A、B应互相发现")

	// A与B进入聊天会话
	var target common.Peer
	uiA.uiDo(func() {
		for _, p := range uiA.peers {
			if p.Name == "ListB" {
				target = p
			}
		}
		uiA.tryStartChat(target)
	})
	waitCond(t, 15*time.Second, func() bool {
		got := false
		uiB.uiDo(func() { got = strings.Contains(uiB.chatLog.Text, "[ListA]: 你好！") })
		return got
	}, "A、B应已建立聊天会话")
	if !uiA.client.IsInChat() || !uiB.client.IsInChat() {
		t.Fatal("双方应都处于聊天会话中")
	}

	// C在A、B聊天期间上线
	uiC := newTestUI(t, "ListC")
	defer uiC.client.Stop()

	// A、B的列表必须在聊天中也出现C
	waitCond(t, 20*time.Second, func() bool {
		return containsName(peerNames(uiA), "ListC")
	}, "A在聊天中也应看到C上线")
	waitCond(t, 20*time.Second, func() bool {
		return containsName(peerNames(uiB), "ListC")
	}, "B在聊天中也应看到C上线")

	// C自己也应发现正在聊天的A、B
	waitCond(t, 20*time.Second, func() bool {
		names := peerNames(uiC)
		return containsName(names, "ListA") && containsName(names, "ListB")
	}, "C上线后应看到A、B")
	t.Log("聊天中第三方上线：三方列表同步通过")

	// C下线后A、B的列表应移除C
	uiC.client.Stop()
	waitCond(t, 20*time.Second, func() bool {
		return !containsName(peerNames(uiA), "ListC")
	}, "A的列表应移除已下线的C")
	waitCond(t, 20*time.Second, func() bool {
		return !containsName(peerNames(uiB), "ListC")
	}, "B的列表应移除已下线的C")
	t.Log("聊天中第三方下线：双方列表同步通过")

	// A、B聊天会话不应受C上下线影响
	if !uiA.client.IsInChat() || !uiB.client.IsInChat() {
		t.Fatal("C上下线后A、B的聊天会话应保持")
	}
}
