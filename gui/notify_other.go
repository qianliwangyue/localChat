//go:build !windows

package gui

import "fyne.io/fyne/v2"

// 非 Windows 平台暂不支持系统通知与原生拖放定位，保持空实现以便编译通过。

func isMainWindowMinimized() bool { return false }

// cursorCanvasPos 非 Windows 平台直接退回 Fyne 回调给的坐标
func cursorCanvasPos(float32) (fyne.Position, bool) { return fyne.Position{}, false }

func notifyDesktop(string, string) {}
