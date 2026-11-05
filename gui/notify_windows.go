//go:build windows

package gui

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/windows/registry"
)

//go:embed toast.ico
var toastIconData []byte

// toastAppID 是通知的 AUMID（AppUserModelID）。
// 通过注册表注册显示名后，通知顶部会显示「LocalCP」而不是一串 GUID。
const toastAppID = "LocalCP"

var registerAppIDOnce sync.Once

// ensureToastAppID 在 HKCU\Software\Classes\AppUserModelId\LocalCP 写入显示名与图标，
// Windows 弹 Toast 时用它把 AUMID 解析为「图标 + LocalCP」的友好头部（进程内只执行一次）。
func ensureToastAppID() {
	registerAppIDOnce.Do(func() {
		k, _, err := registry.CreateKey(registry.CURRENT_USER,
			`Software\Classes\AppUserModelId\`+toastAppID, registry.SET_VALUE)
		if err != nil {
			return
		}
		defer k.Close()
		_ = k.SetStringValue("DisplayName", "LocalCP")
		if p := extractToastIcon(); p != "" {
			_ = k.SetStringValue("IconUri", p)
		}
	})
}

// extractToastIcon 把内嵌的 toast.ico 释放到 %APPDATA%\LocalCP\toast.ico，
// 返回可用于 IconUri 的绝对路径；失败时返回空串（通知仍可正常弹出，只是无图标）。
func extractToastIcon() string {
	dir := filepath.Join(os.Getenv("APPDATA"), "LocalCP")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ""
	}
	p := filepath.Join(dir, "toast.ico")
	if err := os.WriteFile(p, toastIconData, 0o644); err != nil {
		return ""
	}
	return p
}

var notifyScriptNum = 0

// notifyDesktop 弹出 Windows 系统通知（右下角 Toast）并播放系统提示音。
// Toast 本身设为静音，响铃改由 MessageBeep 负责——这样提示音可预期、不会与
// 通知中心的声音叠加成两声。
func notifyDesktop(content string) {
	// 播放系统默认提示音（异步、立即返回）
	procMessageBeep.Call(0)

	ensureToastAppID()
	// 不显示标题，直接显示内容：正文用 Base64 传入，避免 %q 转义出的 \n、
	// 引号等被 PowerShell 当字面文本显示。
	// 首行之后的每行开头缩进 4 个全角空格（U+3000，与汉字等宽、不会被渲染裁剪），
	// 视觉上正好空 4 个字。首行不缩进——聊天通知里它是「发送方：」。
	lines := strings.Split(content, "\n")
	for i := 1; i < len(lines); i++ {
		lines[i] = strings.Repeat("\u3000", 4) + lines[i]
	}
	content = strings.Join(lines, "\n")
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	script := fmt.Sprintf(toastTemplate, b64(content), toastAppID)
	go runPowerShell("notify", script)
}

// toastTemplate 使用自适应 ToastGeneric 模板弹出通知：只有一个文本，直接显示内容。
// 不使用 group/占位文本——纯 group 通知 Windows 会补「应用名 + 新通知」标识块，
// 占位文本又会多出一行空白。
// 正文以 Base64 传入再解码，换行、引号等任意字符都能原样显示；
// 声音交给 MessageBeep 播放。
const toastTemplate = `$content = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String("%s"))
$appID = %q
[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null
[Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] > $null
$escC = [System.Security.SecurityElement]::Escape($content)
$xmlStr = '<toast><visual><binding template="ToastGeneric">' +
    '<text>' + $escC + '</text>' +
    '</binding></visual><audio silent="true"/></toast>'
$xml = New-Object Windows.Data.Xml.Dom.XmlDocument
$xml.LoadXml($xmlStr)
$toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($appID).Show($toast);`

// runPowerShell 后台静默执行通知脚本（与 Fyne 官方 Windows 通知实现相同的做法）
func runPowerShell(name, script string) {
	notifyScriptNum++
	tmp := filepath.Join(os.TempDir(), fmt.Sprintf("localchat-%s-%d.ps1", name, notifyScriptNum))
	if err := os.WriteFile(tmp, []byte(script), 0o600); err != nil {
		return
	}
	defer os.Remove(tmp)

	launch := "(Get-Content -Encoding UTF8 -Path " + tmp + " -Raw) | Invoke-Expression"
	cmd := exec.Command("PowerShell", "-ExecutionPolicy", "Bypass", launch)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	_ = cmd.Run()
}
