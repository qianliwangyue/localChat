// Package gui LocalChat 的 Fyne 图形界面
package gui

import (
	"fmt"
	"strings"
	"sync"

	"localchat/pkg/common"
	"localchat/types"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

// ChatUI LocalChat 图形界面
type ChatUI struct {
	win    fyne.Window
	client *types.ChatClient

	peers []common.Peer // 在线用户快照

	peerList  *widget.List
	chatLog   *widget.Entry
	msgInput  *widget.Entry
	chatTitle *widget.Label

	sendFileBtn   *widget.Button
	sendFolderBtn *widget.Button
	endChatBtn    *widget.Button

	// 传输进度对话框
	progressDlg   *dialog.ProgressDialog
	progressLabel string
	lastPct       int

	ready bool // 主界面控件是否已创建
}

// uiMutex 包级全局锁：串行化所有UI操作。fyne的文本整形器(Harfbuzz)是全局单例且
// 非线程安全，而fyne.Do在测试驱动器下同步执行于调用者goroutine，多个网络goroutine
// （甚至多个窗口）并发测量文本会panic。真实应用中fyne.Do排队主线程本就串行，此锁无副作用。
var uiMutex sync.Mutex

// uiDo 在主线程执行UI更新；主界面未就绪时忽略，防止回调访问未创建的控件导致崩溃。
// 持锁串行化UI操作：真实应用中 fyne.Do 排队主线程本就串行，锁无副作用；
// 测试驱动器下 fyne.Do 同步执行在调用者goroutine上，需锁防止多个网络goroutine
// 并发测量/整形文本导致崩溃（Harfbuzz整形器非线程安全）。
func (ui *ChatUI) uiDo(f func()) {
	fyne.Do(func() {
		if !ui.ready {
			return
		}
		uiMutex.Lock()
		defer uiMutex.Unlock()
		f()
	})
}

// Run 启动图形界面应用
func Run() {
	a := app.New()
	w := a.NewWindow("LocalChat - 局域网聊天与文件传输")
	w.Resize(fyne.NewSize(960, 640))
	w.SetMaster()

	ui := &ChatUI{win: w}
	ui.showNameSetup()
	w.ShowAndRun()
}

// showNameSetup 显示用户名设置界面
func (ui *ChatUI) showNameSetup() {
	nameEntry := widget.NewEntry()
	nameEntry.SetPlaceHolder("例如: 小明")

	submit := func() {
		ui.startClient(strings.TrimSpace(nameEntry.Text))
	}
	// 回车直接提交（替代Form的OnSubmit行为）
	nameEntry.OnSubmitted = func(string) { submit() }

	form := widget.NewForm(
		widget.NewFormItem("你的代号", nameEntry),
	)

	confirmBtn := widget.NewButton("确定", submit)
	confirmBtn.Importance = widget.HighImportance
	cancelBtn := widget.NewButton("取消", func() {
		fyne.CurrentApp().Quit()
	})

	// 两侧Spacer平分剩余空间，使取消/确定按钮组水平居中
	buttons := container.NewHBox(
		layout.NewSpacer(),
		cancelBtn,
		confirmBtn,
		layout.NewSpacer(),
	)

	ui.win.SetContent(container.NewVBox(
		widget.NewLabelWithStyle("LocalChat", fyne.TextAlignCenter, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("局域网即时聊天与文件传输工具", fyne.TextAlignCenter, fyne.TextStyle{}),
		form,
		buttons,
	))
}

// startClient 创建并启动聊天客户端
func (ui *ChatUI) startClient(name string) {
	if name == "" {
		dialog.ShowError(fmt.Errorf("代号不能为空"), ui.win)
		return
	}

	client := types.NewChatClient()
	client.SetName(name)
	ui.client = client
	ui.bindCallbacks()
	client.Start()
	ui.showMainUI()
}

// bindCallbacks 绑定客户端事件回调（所有UI更新通过uiDo切换到主线程，未就绪时忽略）
func (ui *ChatUI) bindCallbacks() {
	c := ui.client

	c.OnLog = func(text string) {
		ui.uiDo(func() { ui.appendLog(text) })
	}

	c.OnPeersChanged = func(peers []common.Peer) {
		ui.uiDo(func() {
			ui.peers = peers
			ui.peerList.Refresh()
		})
	}

	c.OnChatStarted = func(peerName string) {
		ui.uiDo(func() {
			ui.chatTitle.SetText(fmt.Sprintf("正在与 %s 聊天", peerName))
			ui.setChatControls(true)
		})
	}

	c.OnChatMessage = func(from, content string) {
		ui.uiDo(func() {
			ui.appendLog(fmt.Sprintf("[%s]: %s", from, content))
		})
	}

	c.OnChatEnded = func() {
		ui.uiDo(func() {
			ui.chatTitle.SetText("未在聊天中 - 点击左侧用户开始聊天")
			ui.setChatControls(false)
		})
	}

	c.OnFileOffer = func(from, fileName string, size int64, fileID string) {
		ui.uiDo(func() {
			dialog.ShowConfirm("收到文件请求",
				fmt.Sprintf("%s 向你发送文件：\n\n%s (%s)\n\n是否接受？", from, fileName, types.FormatFileSize(size)),
				func(ok bool) {
					if ok {
						go ui.client.AcceptFile(fileID)
					} else {
						go ui.client.RejectFile(fileID)
					}
				}, ui.win)
		})
	}

	c.OnFolderOffer = func(from, folderName string, size int64, fileCount int, folderID string) {
		ui.uiDo(func() {
			dialog.ShowConfirm("收到文件夹请求",
				fmt.Sprintf("%s 向你发送文件夹：\n\n%s (%s, %d 个文件)\n\n是否接受？", from, folderName, types.FormatFileSize(size), fileCount),
				func(ok bool) {
					if ok {
						go ui.client.AcceptFolder(folderID)
					} else {
						go ui.client.RejectFolder(folderID)
					}
				}, ui.win)
		})
	}

	c.OnFileProgress = func(pct int, sending bool, label string) {
		ui.uiDo(func() { ui.updateProgress(pct, label) })
	}

	c.OnTransferEnded = func(sending bool, text string) {
		ui.uiDo(func() {
			ui.endProgress()
			ui.appendLog(text)
		})
	}

	c.OnFileSaved = func(fileName, savePath string) {
		ui.uiDo(func() {
			dialog.ShowInformation("接收完成",
				fmt.Sprintf("文件 '%s' 已保存到：\n%s", fileName, savePath), ui.win)
		})
	}
}

// showMainUI 显示主界面
func (ui *ChatUI) showMainUI() {
	// 顶部状态信息
	info := widget.NewLabel(fmt.Sprintf("代号: %s    IP: %s    TCP端口: %d    接收目录: %s",
		ui.client.GetName(), ui.client.LocalIP(), ui.client.TCPPort(), ui.client.FileDataDir()))

	// 左侧：在线用户列表
	ui.peerList = widget.NewList(
		func() int { return len(ui.peers) },
		func() fyne.CanvasObject {
			return container.NewVBox(
				widget.NewLabelWithStyle("用户名", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
				widget.NewLabel("ip:port"),
			)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			box := obj.(*fyne.Container)
			name := box.Objects[0].(*widget.Label)
			addr := box.Objects[1].(*widget.Label)
			if id < len(ui.peers) {
				p := ui.peers[id]
				name.SetText(p.Name)
				addr.SetText(fmt.Sprintf("%s:%d", p.IP, p.Port))
			}
		})
	ui.peerList.OnSelected = func(id widget.ListItemID) {
		if id < len(ui.peers) {
			ui.tryStartChat(ui.peers[id])
		}
	}

	peersPanel := container.NewBorder(
		container.NewVBox(widget.NewLabelWithStyle("在线用户", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})),
		nil, nil, nil,
		ui.peerList)

	// 右侧：聊天区
	ui.chatTitle = widget.NewLabelWithStyle("未在聊天中 - 点击左侧用户开始聊天", fyne.TextAlignLeading, fyne.TextStyle{Italic: true})

	ui.chatLog = widget.NewMultiLineEntry()
	ui.chatLog.Wrapping = fyne.TextWrapBreak
	ui.chatLog.Disable()

	ui.msgInput = widget.NewEntry()
	ui.msgInput.SetPlaceHolder("输入消息，回车发送...")
	ui.msgInput.OnSubmitted = func(string) { ui.sendMsg() }
	ui.msgInput.Disable()

	sendBtn := widget.NewButton("发送", ui.sendMsg)

	ui.sendFileBtn = widget.NewButton("发送文件...", ui.pickFile)
	ui.sendFileBtn.Disable()
	ui.sendFolderBtn = widget.NewButton("发送文件夹...", ui.pickFolder)
	ui.sendFolderBtn.Disable()
	ui.endChatBtn = widget.NewButton("结束聊天", func() { ui.client.EndChat() })
	ui.endChatBtn.Disable()

	chatPanel := container.NewBorder(
		ui.chatTitle,
		container.NewVBox(
			container.NewBorder(nil, nil, nil, sendBtn, ui.msgInput),
			container.NewHBox(ui.sendFileBtn, ui.sendFolderBtn, layout.NewSpacer(), ui.endChatBtn),
		),
		nil, nil,
		container.NewScroll(ui.chatLog),
	)

	split := container.NewHSplit(peersPanel, chatPanel)
	split.Offset = 0.28

	ui.win.SetContent(container.NewBorder(info, nil, nil, nil, split))

	// 关闭窗口时优雅退出
	ui.win.SetCloseIntercept(func() {
		dialog.ShowConfirm("退出", "确定退出 LocalChat？", func(ok bool) {
			if ok {
				go ui.client.Stop()
				fyne.CurrentApp().Quit()
			}
		}, ui.win)
	})

	// 控件全部创建完毕，允许回调更新UI
	ui.ready = true
}

// appendLog 向聊天日志追加一行
func (ui *ChatUI) appendLog(text string) {
	ui.chatLog.SetText(ui.chatLog.Text + text + "\n")
}

// setChatControls 启用/禁用聊天相关控件
func (ui *ChatUI) setChatControls(enabled bool) {
	if enabled {
		ui.sendFileBtn.Enable()
		ui.sendFolderBtn.Enable()
		ui.endChatBtn.Enable()
		ui.msgInput.Enable()
	} else {
		ui.sendFileBtn.Disable()
		ui.sendFolderBtn.Disable()
		ui.endChatBtn.Disable()
		ui.msgInput.Disable()
	}
}

// sendMsg 发送输入框中的消息
func (ui *ChatUI) sendMsg() {
	text := strings.TrimSpace(ui.msgInput.Text)
	if text == "" || ui.client == nil {
		return
	}
	if !ui.client.SendMessage(text) {
		ui.appendLog("[系统] 发送失败：未在聊天会话中")
		return
	}
	ui.appendLog(fmt.Sprintf("[我]: %s", text))
	ui.msgInput.SetText("")
}

// tryStartChat 点击用户列表项，开始或切换聊天
func (ui *ChatUI) tryStartChat(p common.Peer) {
	ui.peerList.UnselectAll()

	if ui.client.IsInChat() {
		current := ui.client.GetCurrentChat()
		if current == p.Name {
			return
		}
		dialog.ShowConfirm("切换聊天",
			fmt.Sprintf("当前正在与 %s 聊天，是否结束并切换到 %s？", current, p.Name),
			func(ok bool) {
				if ok {
					ui.client.EndChat()
					go ui.client.StartChat(p.Name)
				}
			}, ui.win)
		return
	}

	go ui.client.StartChat(p.Name)
}

// pickFile 选择并发送文件
func (ui *ChatUI) pickFile() {
	fd := dialog.NewFileOpen(func(rc fyne.URIReadCloser, err error) {
		if err != nil || rc == nil {
			return
		}
		path := windowsPath(rc.URI().Path())
		rc.Close()
		if !ui.client.IsInChat() {
			ui.appendLog("[系统] 请先开始一个聊天会话")
			return
		}
		go ui.client.SendFile(path)
	}, ui.win)
	// 必须先Show再Resize：FileDialog内部dialog在首次Show时才创建，
	// Show前调用Resize会触发MinSize解引用nil而panic（fyne 2.8.1）
	fd.Show()
	fd.Resize(fyne.NewSize(700, 500))
}

// pickFolder 选择并发送文件夹
func (ui *ChatUI) pickFolder() {
	fd := dialog.NewFolderOpen(func(lu fyne.ListableURI, err error) {
		if err != nil || lu == nil {
			return
		}
		path := windowsPath(lu.Path())
		if !ui.client.IsInChat() {
			ui.appendLog("[系统] 请先开始一个聊天会话")
			return
		}
		go ui.client.SendFolder(path)
	}, ui.win)
	// 同pickFile：先Show再Resize，避免内部dialog未创建时nil panic
	fd.Show()
	fd.Resize(fyne.NewSize(700, 500))
}

// windowsPath 修正Fyne在Windows下URI路径的斜杠前缀问题 (/C:/xx -> C:/xx)
func windowsPath(p string) string {
	if len(p) > 2 && p[0] == '/' && p[2] == ':' {
		return p[1:]
	}
	return p
}

// updateProgress 显示/更新传输进度对话框
func (ui *ChatUI) updateProgress(pct int, label string) {
	// 标题变化时重建对话框
	if ui.progressDlg == nil || label != ui.progressLabel {
		if ui.progressDlg != nil {
			ui.progressDlg.Hide()
		}
		ui.progressDlg = dialog.NewProgress("文件传输", label, ui.win)
		ui.progressDlg.Show()
		ui.progressLabel = label
		ui.lastPct = -1
	}
	if pct != ui.lastPct {
		ui.progressDlg.SetValue(float64(pct) / 100)
		ui.lastPct = pct
	}
}

// endProgress 关闭传输进度对话框
func (ui *ChatUI) endProgress() {
	if ui.progressDlg != nil {
		ui.progressDlg.Hide()
		ui.progressDlg = nil
		ui.progressLabel = ""
	}
}
