// Package gui LocalChat 的 Fyne 图形界面
package gui

import (
	"fmt"
	"strings"
	"sync"

	apptheme "localchat/gui/theme"
	"localchat/pkg/common"
	"localchat/types"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// ChatUI LocalChat 图形界面
type ChatUI struct {
	win    fyne.Window
	client *types.ChatClient

	peers []common.Peer // 在线用户快照

	peerList   *widget.List
	chatLog    *widget.Label
	chatScroll *container.Scroll
	msgInput   *widget.Entry
	chatTitle  *widget.Label

	sendFileBtn   *widget.Button
	sendFolderBtn *widget.Button
	endChatBtn    *widget.Button

	peerCount  *widget.Label    // 在线人数
	emptyHint  *widget.Label    // 用户列表为空时的占位提示
	statusDot  *canvas.Circle   // 聊天标题旁的状态点
	headerInfo *widget.RichText // 顶部栏本机信息（小字）

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

// maxWidthLayout 让子控件保持自身最小高度、但宽度固定为 w，并水平居中。
// 用于欢迎页等内容列在窗口中的居中排版。
type maxWidthLayout struct{ w float32 }

func (m *maxWidthLayout) Layout(objs []fyne.CanvasObject, size fyne.Size) {
	for _, o := range objs {
		min := o.MinSize()
		h := min.Height
		if h < size.Height {
			h = size.Height
		}
		o.Resize(fyne.NewSize(m.w, h))
		o.Move(fyne.NewPos((size.Width-m.w)/2, 0))
	}
}

func (m *maxWidthLayout) MinSize(objs []fyne.CanvasObject) fyne.Size {
	h := float32(0)
	for _, o := range objs {
		if mh := o.MinSize().Height; mh > h {
			h = mh
		}
	}
	return fyne.NewSize(m.w, h)
}

// brandLogo 生成品牌圆形图标（主题色圆底 + 居中的设备图标）
func brandLogo(size float32) fyne.CanvasObject {
	s := fyne.CurrentApp().Settings()
	c := s.Theme().Color(theme.ColorNamePrimary, s.ThemeVariant())
	bg := canvas.NewCircle(c)
	icon := widget.NewIcon(theme.NewColoredResource(theme.ComputerIcon(), theme.ColorNameForegroundOnPrimary))
	// MaxLayout 让圆底铺满整个格子，图标用 CenterLayout 保持原尺寸居中
	logo := container.New(layout.NewMaxLayout(), bg, container.New(layout.NewCenterLayout(), icon))
	return container.NewGridWrap(fyne.NewSize(size, size), logo)
}

// Run 启动图形界面应用
func Run() {
	a := app.New()
	a.Settings().SetTheme(&apptheme.MyTheme{})
	w := a.NewWindow("LocalChat - 局域网聊天与文件传输")
	w.Resize(fyne.NewSize(480, 540))
	w.SetMaster()

	ui := &ChatUI{win: w}
	ui.showNameSetup()
	w.ShowAndRun()
}

// showNameSetup 显示用户名设置界面（居中欢迎卡片）
func (ui *ChatUI) showNameSetup() {
	nameEntry := widget.NewEntry()
	nameEntry.SetPlaceHolder("例如: 小明")

	submit := func() {
		ui.startClient(strings.TrimSpace(nameEntry.Text))
	}
	// 回车直接提交（替代Form的OnSubmit行为）
	nameEntry.OnSubmitted = func(string) { submit() }

	subtitle := widget.NewRichText(&widget.TextSegment{
		Text: "局域网即时聊天与文件传输\n免登录 · 自动发现附近设备",
		Style: widget.RichTextStyle{
			Alignment: fyne.TextAlignCenter,
			ColorName: theme.ColorNamePlaceHolder,
			Inline:    false,
		},
	})

	form := widget.NewForm(
		widget.NewFormItem("你的代号", nameEntry),
	)

	confirmBtn := widget.NewButtonWithIcon("开始使用", theme.ConfirmIcon(), submit)
	confirmBtn.Importance = widget.HighImportance
	cancelBtn := widget.NewButtonWithIcon("退出", theme.CancelIcon(), func() {
		fyne.CurrentApp().Quit()
	})
	cancelBtn.Importance = widget.LowImportance

	// 取消/确定按钮组水平居中
	buttons := container.NewHBox(
		layout.NewSpacer(),
		cancelBtn,
		confirmBtn,
		layout.NewSpacer(),
	)

	card := container.NewVBox(
		container.NewCenter(brandLogo(72)),
		widget.NewRichText(&widget.TextSegment{
			Text: "LocalChat",
			Style: widget.RichTextStyle{
				Alignment: fyne.TextAlignCenter,
				SizeName:  theme.SizeNameHeadingText,
				Inline:    false,
				TextStyle: fyne.TextStyle{Bold: true},
			},
		}),
		subtitle,
		widget.NewSeparator(),
		form,
		buttons,
	)

	// 固定宽度内容列 + 垂直水平居中，呈现现代卡片式欢迎页
	content := container.New(&maxWidthLayout{w: 380}, card)
	ui.win.SetContent(container.NewCenter(content))
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
			ui.peerCount.SetText(fmt.Sprintf("%d 台设备", len(peers)))
			if len(peers) == 0 {
				ui.emptyHint.SetText("正在搜索附近的设备…")
			} else {
				ui.emptyHint.SetText(fmt.Sprintf("发现 %d 台设备，点击即可开始聊天", len(peers)))
			}
			ui.emptyHint.Hidden = len(peers) > 0
			ui.emptyHint.Refresh()
		})
	}

	c.OnChatStarted = func(peerName string) {
		ui.uiDo(func() {
			ui.chatTitle.TextStyle = fyne.TextStyle{Bold: true}
			ui.chatTitle.SetText(fmt.Sprintf("正在与 %s 聊天", peerName))
			ui.statusDot.Hidden = false
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
			ui.chatTitle.TextStyle = fyne.TextStyle{Italic: true}
			ui.chatTitle.SetText("未在聊天中 - 点击左侧用户开始聊天")
			ui.statusDot.Hidden = true
			ui.setChatControls(false)
		})
	}

	c.OnFileOffer = func(from, fromAddr, fileName string, size int64, fileID string) {
		ui.uiDo(func() {
			sender := from
			if fromAddr != "" {
				sender = fmt.Sprintf("%s (%s)", from, fromAddr)
			}
			dialog.ShowConfirm("收到文件请求",
				fmt.Sprintf("%s 向你发送文件：\n\n%s (%s)\n\n是否接受？", sender, fileName, types.FormatFileSize(size)),
				func(ok bool) {
					if ok {
						go ui.client.AcceptFile(fileID)
					} else {
						go ui.client.RejectFile(fileID)
					}
				}, ui.win)
		})
	}

	c.OnFolderOffer = func(from, fromAddr, folderName string, size int64, fileCount int, folderID string) {
		ui.uiDo(func() {
			sender := from
			if fromAddr != "" {
				sender = fmt.Sprintf("%s (%s)", from, fromAddr)
			}
			dialog.ShowConfirm("收到文件夹请求",
				fmt.Sprintf("%s 向你发送文件夹：\n\n%s (%s, %d 个文件)\n\n是否接受？", sender, folderName, types.FormatFileSize(size), fileCount),
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
	ui.win.Resize(fyne.NewSize(1000, 660))

	// ===== 顶部栏：品牌 + 本机信息 =====
	ui.headerInfo = widget.NewRichText(&widget.TextSegment{
		Text: fmt.Sprintf("代号 %s    IP %s    TCP端口 %d    接收目录 %s",
			ui.client.GetName(), ui.client.LocalIP(), ui.client.TCPPort(), ui.client.FileDataDir()),
		Style: widget.RichTextStyle{
			ColorName: theme.ColorNamePlaceHolder,
			SizeName:  theme.SizeNameCaptionText,
			Inline:    false,
		},
	})
	appName := widget.NewRichText(&widget.TextSegment{
		Text:  "LocalChat",
		Style: widget.RichTextStyle{Inline: true, TextStyle: fyne.TextStyle{Bold: true}},
	})
	// 品牌名靠左、本机信息靠右，一行排开
	headerRow := container.NewBorder(nil, nil, brandLogo(36), nil,
		container.NewHBox(appName, layout.NewSpacer(), ui.headerInfo))
	headerBar := container.NewVBox(
		container.NewPadded(headerRow),
		widget.NewSeparator(),
	)

	// ===== 左侧：在线用户列表 =====
	ui.peerCount = widget.NewLabel("0 台设备")
	ui.peerCount.TextStyle = fyne.TextStyle{Italic: true}

	peerTitle := widget.NewLabelWithStyle("在线用户", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})

	ui.peerList = widget.NewList(
		func() int { return len(ui.peers) },
		func() fyne.CanvasObject {
			dot := canvas.NewCircle(theme.SuccessColor())
			dotWrap := container.NewGridWrap(fyne.NewSize(9, 9), dot)
			dotAlign := container.New(layout.NewVBoxLayout(), dotWrap) // 顶部对齐小圆点
			name := widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
			addr := canvas.NewText("", theme.PlaceHolderColor())
			addr.TextSize = 11
			info := container.NewVBox(name, addr)
			return container.NewHBox(dotAlign, info)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			box := obj.(*fyne.Container).Objects[1].(*fyne.Container)
			name := box.Objects[0].(*widget.Label)
			addr := box.Objects[1].(*canvas.Text)
			if id < len(ui.peers) {
				p := ui.peers[id]
				name.SetText(p.Name)
				addr.Text = fmt.Sprintf("%s:%d", p.IP, p.Port)
				addr.Refresh()
			}
		})
	ui.peerList.OnSelected = func(id widget.ListItemID) {
		if id < len(ui.peers) {
			ui.tryStartChat(ui.peers[id])
		}
	}

	ui.emptyHint = widget.NewLabelWithStyle("正在搜索附近的设备…",
		fyne.TextAlignCenter, fyne.TextStyle{Italic: true})
	peersStack := container.NewStack(ui.peerList, container.NewCenter(ui.emptyHint))

	peersPanel := container.NewBorder(
		container.NewVBox(peerTitle, widget.NewSeparator()),
		nil, nil, nil,
		peersStack)

	// ===== 右侧：聊天区 =====
	ui.chatTitle = widget.NewLabelWithStyle("未在聊天中 - 点击左侧用户开始聊天",
		fyne.TextAlignLeading, fyne.TextStyle{Italic: true})

	ui.statusDot = canvas.NewCircle(theme.SuccessColor())
	ui.statusDot.Hidden = true
	dotWrap := container.NewGridWrap(fyne.NewSize(9, 9), ui.statusDot)

	// 聊天记录使用Label而非禁用的Entry：Entry被Disable后文字渲染成低对比度灰色，
	// 系统通知和聊天内容都看不清；Label使用正常前景色且天然只读
	ui.chatLog = widget.NewLabel("")
	ui.chatLog.Wrapping = fyne.TextWrapBreak
	ui.chatScroll = container.NewScroll(ui.chatLog)

	ui.msgInput = widget.NewEntry()
	ui.msgInput.SetPlaceHolder("输入消息，回车发送…")
	ui.msgInput.OnSubmitted = func(string) { ui.sendMsg() }
	ui.msgInput.Disable()

	sendBtn := widget.NewButtonWithIcon("发送", theme.MailSendIcon(), ui.sendMsg)
	sendBtn.Importance = widget.HighImportance

	ui.sendFileBtn = widget.NewButtonWithIcon("发送文件…", theme.FileIcon(), ui.pickFile)
	ui.sendFileBtn.Importance = widget.LowImportance
	ui.sendFileBtn.Disable()
	ui.sendFolderBtn = widget.NewButtonWithIcon("发送文件夹…", theme.FolderOpenIcon(), ui.pickFolder)
	ui.sendFolderBtn.Importance = widget.LowImportance
	ui.sendFolderBtn.Disable()
	ui.endChatBtn = widget.NewButtonWithIcon("结束聊天", theme.CancelIcon(), func() { ui.client.EndChat() })
	ui.endChatBtn.Importance = widget.DangerImportance
	ui.endChatBtn.Disable()

	inputRow := container.NewBorder(nil, nil, nil, sendBtn, ui.msgInput)
	actionRow := container.NewHBox(ui.sendFileBtn, ui.sendFolderBtn, layout.NewSpacer(), ui.endChatBtn)

	chatHeader := container.NewBorder(nil, nil, dotWrap, nil,
		container.NewVBox(ui.chatTitle, widget.NewSeparator()))

	chatPanel := container.NewBorder(
		chatHeader,
		container.NewVBox(inputRow, actionRow),
		nil, nil,
		container.NewPadded(ui.chatScroll),
	)

	split := container.NewHSplit(peersPanel, chatPanel)
	split.Offset = 0.26

	ui.win.SetContent(container.NewBorder(headerBar, nil, nil, nil, split))

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

// appendLog 向聊天日志追加一行，并自动滚动到底部保证新消息可见
func (ui *ChatUI) appendLog(text string) {
	ui.chatLog.SetText(ui.chatLog.Text + text + "\n")
	ui.chatScroll.ScrollToBottom()
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

// tryStartChat 点击用户列表项，开始或切换聊天。
// 按IP:端口定位用户，代号相同的不同用户也能精确发起聊天
func (ui *ChatUI) tryStartChat(p common.Peer) {
	ui.peerList.UnselectAll()

	if ui.client.IsInChat() {
		// 点击的正是当前聊天对象（按端点判断，同名用户不误判）
		if ui.client.IsCurrentChatPeer(p.IP, p.Port) {
			return
		}
		current := ui.client.GetCurrentChat()
		dialog.ShowConfirm("切换聊天",
			fmt.Sprintf("当前正在与 %s 聊天，是否结束并切换到 %s（%s:%d）？", current, p.Name, p.IP, p.Port),
			func(ok bool) {
				if ok {
					ui.client.EndChat()
					go ui.client.StartChatWith(p.IP, p.Port)
				}
			}, ui.win)
		return
	}

	go ui.client.StartChatWith(p.IP, p.Port)
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
