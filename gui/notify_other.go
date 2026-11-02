//go:build !windows

package gui

// 非 Windows 平台暂不支持系统通知，保持空实现以便编译通过。

func isMainWindowMinimized() bool { return false }

func notifyDesktop(string, string) {}
