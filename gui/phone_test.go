package gui

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fyne.io/fyne/v2/test"
	"localchat/types"
)

// TestZipFolder 验证文件夹打包：结构保留、内容一致、服务停止后临时 zip 被清理
func TestZipFolder(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "photos", "2026")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "a.jpg"), []byte("fake-jpg"), 0o644); err != nil {
		t.Fatal(err)
	}

	zipPath, err := zipFolder(dir)
	if err != nil {
		t.Fatalf("打包失败: %v", err)
	}
	defer os.Remove(zipPath)

	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatalf("zip 不可读: %v", err)
	}
	defer zr.Close()
	got := map[string]string{}
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()
		got[f.Name] = string(data)
	}
	_ = zr.Close() // Windows 下须先关闭句柄才能删除文件
	top := filepath.Base(dir)
	if got[top+"/readme.txt"] != "hello" || got[top+"/photos/2026/a.jpg"] != "fake-jpg" {
		t.Fatalf("zip 内容或结构异常: %v", got)
	}

	// Stop 应删除临时 zip
	test.NewApp()
	ui := &ChatUI{win: test.NewWindow(nil), ready: true}
	client := types.NewChatClient()
	client.SetName("Tester")
	ui.client = client
	if err := client.SetFileDataDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	pt, err := ui.startPhoneServer("send", []string{zipPath})
	if err != nil {
		t.Fatal(err)
	}
	pt.tempZip = []string{zipPath}
	pt.Stop()
	if _, err := os.Stat(zipPath); !os.IsNotExist(err) {
		t.Fatalf("服务停止后临时 zip 应被删除")
	}
}

// TestPhoneUploadAbortCleansPartialFile 手机上传中途断开（点取消）时，
// 服务端不能残留写了一半的文件
func TestPhoneUploadAbortCleansPartialFile(t *testing.T) {
	test.NewApp()
	ui := &ChatUI{win: test.NewWindow(nil), ready: true}
	client := types.NewChatClient()
	client.SetName("Tester")
	ui.client = client

	dir := t.TempDir()
	if err := client.SetFileDataDir(dir); err != nil {
		t.Fatalf("设置接收目录失败: %v", err)
	}
	pt, err := ui.startPhoneServer("recv", nil)
	if err != nil {
		t.Fatalf("启动接收服务失败: %v", err)
	}
	defer pt.Stop()

	// 构造一个 4MB 文件的上传体
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("f", "half.bin")
	fw.Write(bytes.Repeat([]byte("A"), 4<<20))
	mw.Close()
	full := body.Bytes()

	hostPort := strings.TrimPrefix(strings.TrimSuffix(pt.url, "/t/"+pt.token), "http://")
	conn, err := net.Dial("tcp", hostPort)
	if err != nil {
		t.Fatalf("连接服务失败: %v", err)
	}
	_, err = fmt.Fprintf(conn, "POST /t/%s/upload HTTP/1.1\r\nHost: %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n",
		pt.token, hostPort, mw.FormDataContentType(), len(full))
	if err != nil {
		t.Fatalf("写请求头失败: %v", err)
	}
	if _, err := conn.Write(full[:len(full)/4]); err != nil {
		t.Fatalf("写请求体失败: %v", err)
	}

	// 给服务端一点时间落盘，确认半截文件已经产生
	time.Sleep(300 * time.Millisecond)
	if entries, _ := os.ReadDir(dir); len(entries) == 0 {
		conn.Close()
		t.Fatalf("前置条件不成立：服务端未创建接收中的文件")
	}
	_ = conn.Close() // 模拟手机点「取消发送」：直接断开连接

	// 服务端应删除半截文件，目录最终为空
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, _ := os.ReadDir(dir)
		if len(entries) == 0 {
			return
		}
		if time.Now().After(deadline) {
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Fatalf("取消上传后仍有残留文件: %v", names)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestPhoneTransferHTTP 验证手机互传的下载与上传 HTTP 流程
func TestPhoneTransferHTTP(t *testing.T) {
	test.NewApp()
	ui := &ChatUI{win: test.NewWindow(nil), ready: true}
	client := types.NewChatClient()
	client.SetName("Tester")
	ui.client = client

	dir := t.TempDir()
	if err := client.SetFileDataDir(dir); err != nil {
		t.Fatalf("设置接收目录失败: %v", err)
	}

	// ===== 发送模式：电脑 -> 手机 =====
	src := filepath.Join(dir, "hello.txt")
	content := []byte("hello from LocalCP")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}
	pt, err := ui.startPhoneServer("send", []string{src})
	if err != nil {
		t.Fatalf("启动发送服务失败: %v", err)
	}
	if !strings.Contains(pt.url, "/t/") {
		t.Fatalf("二维码地址格式异常: %s", pt.url)
	}

	// 页面可访问且包含文件名
	resp, err := http.Get(pt.url)
	if err != nil {
		t.Fatalf("访问主页失败: %v", err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(page), "hello.txt") {
		t.Fatalf("发送模式页面异常: status=%d body=%s", resp.StatusCode, page)
	}

	// 手机浏览器可能请求带尾斜杠的地址，也应命中主页
	if resp, err = http.Get(pt.url + "/"); err == nil {
		page, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(page), "hello.txt") {
			t.Fatalf("尾斜杠地址应命中主页: status=%d", resp.StatusCode)
		}
	}

	// 下载文件内容一致
	resp, err = http.Get(pt.url + "/file/0")
	if err != nil {
		t.Fatalf("下载失败: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("下载内容不一致: got %q", got)
	}

	// 越界索引应 404
	if resp, err := http.Get(pt.url + "/file/9"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Fatalf("越界下载应404，实际 %d", resp.StatusCode)
		}
	}

	// 页面里的下载链接必须是绝对路径（含 token），否则浏览器解析相对路径会丢 token
	if !strings.Contains(string(page), "/t/"+pt.token+"/file/0") {
		t.Fatalf("下载链接应带完整 token 路径，页面内容: %s", page)
	}

	// favicon 可访问
	if resp, err := http.Get(strings.TrimSuffix(pt.url, "/t/"+pt.token) + "/favicon.ico"); err == nil {
		ico, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || len(ico) == 0 {
			t.Fatalf("favicon 应可访问: status=%d len=%d", resp.StatusCode, len(ico))
		}
	}

	// 旧页面漏掉 token 的链接（/t/file/0）应被重定向到正确地址
	base := strings.TrimSuffix(pt.url, "/t/"+pt.token)
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if resp, err := noRedirect.Get(base + "/t/file/0"); err == nil {
		loc := resp.Header.Get("Location")
		resp.Body.Close()
		if resp.StatusCode != http.StatusFound || loc != "/t/"+pt.token+"/file/0" {
			t.Fatalf("漏 token 链接应重定向: status=%d loc=%s", resp.StatusCode, loc)
		}
	}
	pt.Stop()

	// ===== 接收模式：手机 -> 电脑 =====
	pt2, err := ui.startPhoneServer("recv", nil)
	if err != nil {
		t.Fatalf("启动接收服务失败: %v", err)
	}
	defer pt2.Stop()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("f", "手机照片.png")
	fw.Write([]byte("fake png data"))
	mw.Close()

	resp, err = http.Post(pt2.url+"/upload", mw.FormDataContentType(), &body)
	if err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("上传返回 %d", resp.StatusCode)
	}

	saved := filepath.Join(dir, "手机照片.png")
	data, err := os.ReadFile(saved)
	if err != nil {
		t.Fatalf("上传文件未保存: %v", err)
	}
	if string(data) != "fake png data" {
		t.Fatalf("上传内容不一致: %q", data)
	}

	// 同名再传一次应生成不覆盖的新文件
	var body2 bytes.Buffer
	mw2 := multipart.NewWriter(&body2)
	fw2, _ := mw2.CreateFormFile("f", "手机照片.png")
	fw2.Write([]byte("second"))
	mw2.Close()
	if resp, err = http.Post(pt2.url+"/upload", mw2.FormDataContentType(), &body2); err != nil {
		t.Fatalf("二次上传失败: %v", err)
	}
	resp.Body.Close()
	if _, err := os.Stat(filepath.Join(dir, "手机照片(1).png")); err != nil {
		t.Fatalf("同名上传应生成不覆盖的新文件: %v", err)
	}
}
