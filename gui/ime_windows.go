//go:build windows

package gui

// 输入法候选窗跟随修复。
//
// 根因：fyne 基于 GLFW，而 GLFW（及 fyne 的 glfw 驱动）从不调用 Win32 IMM32 的
// ImmSetCandidateWindow / ImmSetCompositionWindow，系统输入法拿不到光标位置，
// 候选词窗口只会出现在窗口左上角等默认位置，不跟随输入光标。
//
// 修复：不改动 fyne/glfw 依赖，而是在应用层补上缺失的桥接——
//  1. 用 fyne 公开 API 计算当前焦点 Entry 的光标位置（画布坐标）：
//     AbsolutePositionForObject(entry) + entry.CursorPosition()
//  2. 用 ClientToScreen 把画布坐标换算成屏幕像素坐标（考虑 DPI 缩放）
//  3. 通过 ImmSetCandidateWindow(CFS_CANDIDATEPOS) 与
//     ImmSetCompositionWindow(CFS_POINT) 把候选窗/组字窗钉到光标处
//
// IMM32 调用要求在拥有该窗口的线程上执行（fyne 主线程），因此通过 fyne.Do 派发。
// 用低频轮询（120ms）持续刷新位置：输入法在合成开始时才读取候选窗位置，
// 轮询保证用户每次开始打拼音时拿到的都是最新光标位置，开销可忽略。

import (
	"syscall"
	"time"
	"unsafe"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

var (
	imm32                       = syscall.NewLazyDLL("imm32.dll")
	procImmGetContext           = imm32.NewProc("ImmGetContext")
	procImmReleaseContext       = imm32.NewProc("ImmReleaseContext")
	procImmSetCandidateWindow   = imm32.NewProc("ImmSetCandidateWindow")
	procImmSetCompositionWindow = imm32.NewProc("ImmSetCompositionWindow")
	procGetForegroundWindow     = user32.NewProc("GetForegroundWindow")
)

// CANDIDATEFORM / COMPOSITIONFORM 的定位方式标志位
const (
	cfsPoint        = 0x0002 // pt 为窗口客户区坐标
	cfsCandidatePos = 0x0040 // pt 为屏幕坐标
)

// compositionForm 对应 Win32 COMPOSITIONFORM（组字窗位置）
type compositionForm struct {
	dwStyle      uint32
	ptCurrentPos winPoint
	rcArea       winRect
}

// candidateForm 对应 Win32 CANDIDATEFORM（候选词窗位置）
type candidateForm struct {
	dwIndex uint32
	dwStyle uint32
	pt      winPoint
	rcArea  winRect
}

// imeInterval 候选窗位置刷新间隔
const imeInterval = 120 * time.Millisecond

// startImeTracker 启动输入法候选窗跟随。仅处理主窗口，窗口销毁后自动失效。
func startImeTracker(win fyne.Window) {
	go func() {
		ticker := time.NewTicker(imeInterval)
		defer ticker.Stop()
		for range ticker.C {
			fyne.Do(func() {
				defer func() { _ = recover() }() // 应用退出时事件循环可能已关闭
				updateImeCandidatePos(win)
			})
		}
	}()
}

// updateImeCandidatePos 把当前焦点输入框的光标位置同步给输入法（必须在主线程调用）。
func updateImeCandidatePos(win fyne.Window) {
	if win == nil {
		return
	}
	c := win.Canvas()
	if c == nil || c.Focused() == nil {
		return
	}
	e, ok := c.Focused().(*widget.Entry)
	if !ok || e.Disabled() {
		return // readOnlyEntry 等不可编辑控件不需要候选窗
	}

	hwnd := mainWindowHandle()
	if hwnd == 0 {
		return
	}
	// 前台不是本窗口时输入法不活跃，跳过
	if fg, _, _ := procGetForegroundWindow.Call(); fg != hwnd {
		return
	}

	scale := windowClientScale(hwnd, c)
	abs := fyne.CurrentApp().Driver().AbsolutePositionForObject(e)
	cur := e.CursorPosition()

	// 光标底部（下一行行首）作为候选窗挂靠点
	th := e.Theme()
	textSize := th.Size(theme.SizeNameText)
	caretY := abs.Y + cur.Y + textSize

	clientPt := winPoint{
		X: int32((abs.X + cur.X) * scale),
		Y: int32(caretY * scale),
	}
	// 客户区左上角在屏幕坐标系中的位置
	var origin winPoint
	if r, _, _ := procClientToScreen.Call(hwnd, uintptr(unsafe.Pointer(&origin))); r == 0 {
		return
	}
	screenPt := winPoint{X: origin.X + clientPt.X, Y: origin.Y + clientPt.Y}

	himc, _, _ := procImmGetContext.Call(hwnd)
	if himc == 0 {
		return
	}
	defer procImmReleaseContext.Call(hwnd, himc)

	// 候选词窗：跟随光标（屏幕坐标）
	cf := candidateForm{dwIndex: 0, dwStyle: cfsCandidatePos, pt: screenPt}
	procImmSetCandidateWindow.Call(himc, uintptr(unsafe.Pointer(&cf)))

	// 组字窗（拼音串）：跟随光标（客户区坐标）
	comp := compositionForm{dwStyle: cfsPoint, ptCurrentPos: clientPt}
	procImmSetCompositionWindow.Call(himc, uintptr(unsafe.Pointer(&comp)))
}

// windowClientScale 返回窗口的像素/画布单位缩放比（DPI 缩放），与
// drop_windows.go 中 cursorCanvasPos 的还原方式一致。
func windowClientScale(hwnd uintptr, c fyne.Canvas) float32 {
	var rc winRect
	if r, _, _ := procGetClientRect.Call(hwnd, uintptr(unsafe.Pointer(&rc))); r == 0 || rc.Right <= 0 {
		return 1
	}
	w := c.Size().Width
	if w <= 0 {
		return 1
	}
	s := float32(rc.Right) / w
	if s <= 0 {
		return 1
	}
	return s
}
