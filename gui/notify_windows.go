//go:build windows

package gui

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows/registry"
)

//go:embed toast.ico
var toastIconData []byte

var (
	user32          = syscall.NewLazyDLL("user32.dll")
	procFindWindow  = user32.NewProc("FindWindowW")
	procIsIconic    = user32.NewProc("IsIconic")
	procMessageBeep = user32.NewProc("MessageBeep")
)

// isMainWindowMinimized 通过 Win32 API 判断主窗口当前是否处于最小化状态。
// 找不到窗口（标题不匹配等）时按「未最小化」处理，避免误发通知。
func isMainWindowMinimized() bool {
	titlePtr, err := syscall.UTF16PtrFromString(windowTitle)
	if err != nil {
		return false
	}
	hwnd, _, _ := procFindWindow.Call(0, uintptr(unsafe.Pointer(titlePtr)))
	if hwnd == 0 {
		return false
	}
	iconic, _, _ := procIsIconic.Call(hwnd)
	return iconic != 0
}

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
func notifyDesktop(title, content string) {
	// 播放系统默认提示音（异步、立即返回）
	procMessageBeep.Call(0)

	ensureToastAppID()
	// 标题/正文用 Base64 传入：避免 %q 转义出的 \n、引号等被 PowerShell 当字面文本显示
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	script := fmt.Sprintf(toastTemplate, b64(title), b64(content), toastAppID)
	go runPowerShell("notify", script)
}

// toastTemplate 使用 WinRT ToastText02 模板（大标题+正文）弹出通知。
// 标题与正文以 Base64 传入再解码，换行、引号等任意字符都能原样显示；
// 模板自带的 <audio> 改为静音，声音交给 MessageBeep 播放。
const toastTemplate = `$title = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String("%s"))
$content = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String("%s"))
$appID = %q
[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null
$template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02)
$toastXml = [xml] $template.GetXml()
$toastXml.GetElementsByTagName("text")[0].AppendChild($toastXml.CreateTextNode($title)) > $null
$toastXml.GetElementsByTagName("text")[1].AppendChild($toastXml.CreateTextNode($content)) > $null
$toastEl = $toastXml.GetElementsByTagName("toast")[0]
$audios = $toastEl.GetElementsByTagName("audio")
if ($audios.Count -gt 0) {
    $audios[0].SetAttribute("silent", "true")
} else {
    $audio = $toastXml.CreateElement("audio")
    $audio.SetAttribute("silent", "true")
    $toastEl.AppendChild($audio) > $null
}
$xml = New-Object Windows.Data.Xml.Dom.XmlDocument
$xml.LoadXml($toastXml.OuterXml)
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
