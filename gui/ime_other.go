//go:build !windows

package gui

import "fyne.io/fyne/v2"

// startImeTracker 非 Windows 平台无此问题（macOS/Web 的输入法处理由各自系统层负责），
// 保留空实现以维持跨平台编译。
func startImeTracker(fyne.Window) {}
