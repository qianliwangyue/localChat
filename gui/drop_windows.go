//go:build windows

package gui

import (
	"syscall"
	"unsafe"

	"fyne.io/fyne/v2"
)

var (
	user32             = syscall.NewLazyDLL("user32.dll")
	procFindWindow     = user32.NewProc("FindWindowW")
	procIsIconic       = user32.NewProc("IsIconic")
	procMessageBeep    = user32.NewProc("MessageBeep")
	procGetCursorPos   = user32.NewProc("GetCursorPos")
	procClientToScreen = user32.NewProc("ClientToScreen")
	procGetClientRect  = user32.NewProc("GetClientRect")
)

type winPoint struct {
	X, Y int32
}

type winRect struct {
	Left, Top, Right, Bottom int32
}

// mainWindowHandle 按窗口标题查找主窗口句柄，失败返回 0。
func mainWindowHandle() uintptr {
	titlePtr, err := syscall.UTF16PtrFromString(windowTitle)
	if err != nil {
		return 0
	}
	hwnd, _, _ := procFindWindow.Call(0, uintptr(unsafe.Pointer(titlePtr)))
	return hwnd
}

// isMainWindowMinimized 通过 Win32 API 判断主窗口当前是否处于最小化状态。
// 找不到窗口（标题不匹配等）时按「未最小化」处理，避免误发通知。
func isMainWindowMinimized() bool {
	hwnd := mainWindowHandle()
	if hwnd == 0 {
		return false
	}
	iconic, _, _ := procIsIconic.Call(hwnd)
	return iconic != 0
}

// cursorCanvasPos 返回鼠标光标相对主窗口客户区左上角的坐标（Fyne 画布单位）。
// 拖放时系统走 OLE 拖拽，目标窗口收不到鼠标移动消息，Fyne 回调里的坐标会是
// 拖拽开始前的旧值，因此这里直接向系统取实时光标位置。
// canvasWidth 为画布宽度（画布单位），用于把像素换算回画布单位。
func cursorCanvasPos(canvasWidth float32) (fyne.Position, bool) {
	hwnd := mainWindowHandle()
	if hwnd == 0 {
		return fyne.Position{}, false
	}

	var cursor winPoint
	if r, _, _ := procGetCursorPos.Call(uintptr(unsafe.Pointer(&cursor))); r == 0 {
		return fyne.Position{}, false
	}

	// 客户区左上角在屏幕坐标系中的位置
	var origin winPoint
	if r, _, _ := procClientToScreen.Call(hwnd, uintptr(unsafe.Pointer(&origin))); r == 0 {
		return fyne.Position{}, false
	}

	// 客户区像素宽度，用于还原缩放比例（像素 / 画布单位）
	var rc winRect
	if r, _, _ := procGetClientRect.Call(hwnd, uintptr(unsafe.Pointer(&rc))); r == 0 {
		return fyne.Position{}, false
	}
	scale := float32(1)
	if canvasWidth > 0 && rc.Right > 0 {
		scale = float32(rc.Right) / canvasWidth
	}
	if scale <= 0 {
		scale = 1
	}

	return fyne.NewPos(
		float32(cursor.X-origin.X)/scale,
		float32(cursor.Y-origin.Y)/scale,
	), true
}
