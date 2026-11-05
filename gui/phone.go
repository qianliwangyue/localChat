// Package gui 手机互传：电脑端起临时 HTTP 服务，手机扫码用浏览器收发文件。
// 复用客户端的接收目录、日志与系统通知；传输本身走标准 HTTP（LocalSend 同思路）。
package gui

import (
	"archive/zip"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"html"
	"io"
	"io/fs"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"image/color"
	"github.com/skip2/go-qrcode"

	"localchat/types"
)

//go:embed app.ico
var phoneFavicon []byte

// pageCSS 手机页面的统一样式：与桌面端一致的品牌靛蓝视觉
const pageCSS = `
:root{--brand:#4c6ef5;--brand-dark:#3b5bdb;--bg:#f5f6fa;--text:#1a1d26;--muted:#98a0b3}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
body{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
background:linear-gradient(180deg,#e8ecfb 0,#f5f6fa 260px);min-height:100vh;color:var(--text)}
.wrap{max-width:520px;margin:0 auto;padding:0 20px 48px}
header{display:flex;align-items:center;gap:12px;padding:26px 0 18px}
.logo{width:48px;height:48px;border-radius:14px;flex:none;background:#fff;
display:flex;align-items:center;justify-content:center;
box-shadow:0 4px 14px rgba(76,110,245,.22)}
.logo svg{width:24px;height:24px;fill:#fff}
h1{font-size:19px;line-height:1.3}
h1 small{display:block;font-size:12px;color:var(--muted);font-weight:400;margin-top:2px}
.tip{font-size:14px;color:#5a6172;margin:2px 2px 16px;line-height:1.6}
.file{display:flex;align-items:center;gap:14px;text-decoration:none;color:var(--text);
background:#fff;border-radius:16px;padding:15px 16px;margin-bottom:12px;
box-shadow:0 2px 10px rgba(26,29,38,.06);transition:transform .12s}
.file:active{transform:scale(.97)}
.fi{width:44px;height:44px;border-radius:12px;background:#eef1fb;flex:none;
display:flex;align-items:center;justify-content:center;font-size:21px}
.fn{font-size:15px;font-weight:600;word-break:break-all;line-height:1.4}
.fs{font-size:12px;color:var(--muted);margin-top:3px}
.dl{margin-left:auto;color:var(--brand);flex:none;display:flex}
.drop{display:block;background:#fff;border:2px dashed #c3cdf5;border-radius:16px;
padding:34px 16px;text-align:center;cursor:pointer;transition:border-color .15s}
.drop:active{border-color:var(--brand)}
.drop .big{font-size:34px}
.drop p{font-size:15px;font-weight:600;margin-top:8px}
.drop span{font-size:12px;color:var(--muted);display:block;margin-top:4px}
.picked{display:inline-block;margin-top:10px;padding:8px 18px;border-radius:999px;
background:linear-gradient(135deg,#5c7cfa,#4c6ef5);color:#fff;font-size:15px;font-weight:600;
box-shadow:0 4px 12px rgba(76,110,245,.3)}
.prog{display:none;margin-top:18px}
.ptrack{height:10px;border-radius:999px;background:#e4e8f7;overflow:hidden}
.pfill{height:100%;width:0;border-radius:999px;
background:linear-gradient(90deg,#5c7cfa,#4c6ef5);transition:width .2s}
.ptext{display:flex;justify-content:space-between;font-size:13px;color:#5a6172;margin-top:8px}
.cancelbtn{margin-top:14px;background:#fff;color:#e8590c;border:2px solid #ffd8a8;
box-shadow:none;font-size:15px;padding:12px}
.btn{width:100%;border:0;border-radius:14px;background:linear-gradient(135deg,#5c7cfa,#4c6ef5);
color:#fff;font-size:16px;font-weight:600;padding:14px;margin-top:16px;cursor:pointer;
box-shadow:0 6px 16px rgba(76,110,245,.3)}
.btn:disabled{opacity:.6}
.foot{text-align:center;color:#b8bfd0;font-size:12px;margin-top:28px;line-height:1.7}
.result{text-align:center;padding:40px 20px 20px}
.result .ok{width:76px;height:76px;margin:0 auto 18px;border-radius:50%;
background:linear-gradient(135deg,#51cf66,#37b24d);display:flex;align-items:center;justify-content:center;
box-shadow:0 8px 20px rgba(55,178,77,.3)}
.result .ok svg{width:38px;height:38px;fill:#fff}
.result h2{font-size:19px}
.result p{color:var(--muted);font-size:14px;margin-top:8px}
`

// phoneTransfer 一次手机互传会话
type phoneTransfer struct {
	ui      *ChatUI
	mode    string   // "send"：电脑->手机；"recv"：手机->电脑
	token   string   // 随机令牌，扫码地址包含它，防止局域网内其他人随意访问
	files   []string // send 模式：要发送给手机的文件路径
	saveDir string   // recv 模式：手机上传文件的保存目录
	url     string   // 二维码内容
	srv     *http.Server
	recvN   int      // recv 模式：已接收文件数
	tempZip []string // 文件夹打包产生的临时 zip，服务停止时删除
}

// randToken 生成 16 位十六进制随机令牌
func randToken() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// startPhoneServer 在随机端口启动临时 HTTP 服务
func (ui *ChatUI) startPhoneServer(mode string, files []string) (*phoneTransfer, error) {
	ln, err := net.Listen("tcp", ":0") // 随机空闲端口
	if err != nil {
		return nil, err
	}
	pt := &phoneTransfer{
		ui:      ui,
		mode:    mode,
		token:   randToken(),
		files:   files,
		saveDir: ui.client.FileDataDir(),
	}
	pt.url = fmt.Sprintf("http://%s:%d/t/%s", ui.client.LocalIP(), ln.Addr().(*net.TCPAddr).Port, pt.token)

	mux := http.NewServeMux()
	// 统一从 /t/ 前缀分发：手机浏览器可能出现末尾斜杠等路径变体，
	// 精确注册会 404，这里自行解析 token 与子路径
	mux.HandleFunc("/t/", pt.dispatch)
	// 浏览器标签页/收藏夹的网站小图标
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/x-icon")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		_, _ = w.Write(phoneFavicon)
	})
	// 裸 IP/错误路径给出友好提示，而不是默认的 404 page not found
	mux.HandleFunc("/", pt.wrongPathHandler)
	pt.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() { _ = pt.srv.Serve(ln) }()
	return pt, nil
}

// Stop 停止 HTTP 服务
func (pt *phoneTransfer) Stop() {
	if pt.srv != nil {
		go func() { _ = pt.srv.Close() }()
	}
	for _, z := range pt.tempZip {
		_ = os.Remove(z)
	}
	pt.tempZip = nil
}

// wellCompressedExts 已压缩格式：打包时直接 Store，避免无谓的 CPU 开销
var wellCompressedExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true,
	".mp4": true, ".mov": true, ".avi": true, ".mkv": true, ".flv": true,
	".mp3": true, ".wav": true, ".flac": true,
	".zip": true, ".rar": true, ".7z": true, ".gz": true, ".bz2": true, ".xz": true,
	".apk": true, ".exe": true, ".msi": true, ".pdf": true,
}

// zipFolder 把文件夹打包为临时 zip（保留目录结构），返回 zip 路径。
// 已压缩格式用 Store 存储，其余 Deflate。
func zipFolder(srcDir string) (string, error) {
	zipPath := filepath.Join(os.TempDir(),
		fmt.Sprintf("localcp_%s_%d.zip", filepath.Base(srcDir), time.Now().UnixNano()))
	zf, err := os.Create(zipPath)
	if err != nil {
		return "", err
	}
	w := zip.NewWriter(zf)
	root := filepath.Dir(srcDir)
	walkErr := filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		hdr := &zip.FileHeader{Name: filepath.ToSlash(rel)}
		if wellCompressedExts[strings.ToLower(filepath.Ext(path))] {
			hdr.Method = zip.Store
		} else {
			hdr.Method = zip.Deflate
		}
		out, err := w.CreateHeader(hdr)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, f)
		f.Close()
		return err
	})
	if cerr := w.Close(); walkErr == nil {
		walkErr = cerr
	}
	if cerr := zf.Close(); walkErr == nil {
		walkErr = cerr
	}
	if walkErr != nil {
		_ = os.Remove(zipPath)
		return "", walkErr
	}
	return zipPath, nil
}

// wrongPathHandler 访问了裸 IP 或错误路径时的友好提示页
func (pt *phoneTransfer) wrongPathHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	var sb strings.Builder
	sb.WriteString(pageHead("LocalCP 手机互传", "无法访问"))
	sb.WriteString(`<div class="result"><div class="ok" style="background:linear-gradient(135deg,#ffa94d,#f76707)">` +
		`<svg viewBox="0 0 24 24"><path d="M11 15h2v2h-2zm0-8h2v6h-2zm1-5C6.5 2 2 6.5 2 12s4.5 10 10 10 10-4.5 10-10S17.5 2 12 2z"/></svg></div>` +
		`<h2>请从电脑端扫码访问</h2>` +
		`<p>网址中包含访问令牌，不能只输入 IP 地址。<br>请用手机扫描电脑端「手机互传」弹窗里的二维码，<br>或完整输入其中显示的网址（含 /t/… 部分）。</p></div>`)
	sb.WriteString(pageFoot())
	_, _ = w.Write([]byte(sb.String()))
}

// dispatch 统一入口：校验 token 并按子路径分发到页面/下载/上传
func (pt *phoneTransfer) dispatch(w http.ResponseWriter, r *http.Request) {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/t/"), "/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] != pt.token {
		// 兼容旧页面里漏掉 token 的相对链接（如 /t/file/0），重定向到正确地址
		if len(parts) > 0 && (parts[0] == "file" || parts[0] == "upload") {
			if r.Method == http.MethodGet {
				http.Redirect(w, r, "/t/"+pt.token+"/"+rest, http.StatusFound)
				return
			}
		}
		pt.wrongPathHandler(w, r)
		return
	}
	sub := parts[1:]
	if len(sub) == 0 { // 打开主页（兼容 /t/<token>/ 形式）
		pt.pageHandler(w, r)
		return
	}
	switch {
	case sub[0] == "file" && len(sub) == 2 && r.Method == http.MethodGet:
		pt.downloadHandler(w, r, sub[1])
	case sub[0] == "upload" && r.Method == http.MethodPost:
		pt.uploadHandler(w, r)
	default:
		http.NotFound(w, r)
	}
}

// logoSVG 页面顶栏的品牌图标：使用应用图标 app.ico
const logoSVG = `<img src="/favicon.ico" alt="LocalCP" style="width:30px;height:30px;display:block">`

// pageHead 生成页面公共头部（doctype、viewport、favicon、样式、顶栏）
func pageHead(title, subtitle string) string {
	return `<!doctype html><html lang="zh"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">` +
		`<title>` + html.EscapeString(title) + `</title>` +
		`<link rel="icon" href="/favicon.ico">` +
		`<style>` + pageCSS + `</style></head><body><div class="wrap">` +
		`<header><div class="logo">` + logoSVG + `</div>` +
		`<h1>` + html.EscapeString(title) + `<small>` + html.EscapeString(subtitle) + `</small></h1></header>`
}

// pageFoot 页面底部
func pageFoot() string {
	return `<div class="foot">LocalCP 局域网传输 · 仅限本次会话访问</div></div></body></html>`
}

// pageHandler 手机打开二维码网址时的页面：发送模式=下载列表，接收模式=上传表单
func (pt *phoneTransfer) pageHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var sb strings.Builder
	if pt.mode == "send" {
		sb.WriteString(pageHead("LocalCP 手机互传", "来自电脑的文件，点击即可下载"))
		for i, f := range pt.files {
			name := filepath.Base(f)
			size := "未知大小"
			if st, err := os.Stat(f); err == nil {
				size = types.FormatFileSize(st.Size())
			}
			sb.WriteString(fmt.Sprintf(
				`<a class="file" href="/t/%s/file/%d"><div class="fi">📄</div>`+
					`<div><div class="fn">%s</div><div class="fs">%s · 点击下载</div></div>`+
					`<div class="dl"><svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor">`+
					`<path d="M12 16 6 10h4V4h4v6h4l-6 6zm-7 4h14v-2H5v2z"/></svg></div></a>`,
				pt.token, i, html.EscapeString(name), size))
		}
	} else {
		sb.WriteString(pageHead("LocalCP 手机互传", "选择文件发送到电脑"))
		sb.WriteString(`<form method="post" action="/t/` + pt.token + `/upload" enctype="multipart/form-data" id="up">` +
			`<label class="drop" for="picker"><div class="big">📤</div>` +
			`<p>点这里选择文件</p><span id="picked">支持多选，手机相册、文档均可</span></label>` +
			`<input type="file" id="picker" name="f" multiple required style="display:none">` +
			`<button class="btn" id="go" type="submit">发送到电脑</button>` +
			// 上传进度：进度条 + 已传/总量 + 速度
			`<div class="prog" id="prog"><div class="ptrack"><div class="pfill" id="pfill"></div></div>` +
			`<div class="ptext"><span id="pbytes"></span><span id="pspeed"></span></div>` +
			`<button class="btn cancelbtn" id="pcancel" type="button">取消发送</button></div></form>` +
			`<script>` +
			`var p=document.getElementById('picker'),s=document.getElementById('picked'),g=document.getElementById('go');` +
			`var prog=document.getElementById('prog'),pf=document.getElementById('pfill'),`+
			`pb=document.getElementById('pbytes'),ps=document.getElementById('pspeed'),pc=document.getElementById('pcancel');` +
			`var curXhr=null;` +
			`function fmt(n){if(n>=1073741824)return (n/1073741824).toFixed(2)+' GB';`+
			`if(n>=1048576)return (n/1048576).toFixed(1)+' MB';`+
			`if(n>=1024)return (n/1024).toFixed(1)+' KB';return n+' B';}` +
			`p.addEventListener('change',function(){var n=p.files.length;` +
			`if(n>0){s.className='picked';s.textContent='✓ 已选择 '+n+' 个文件，点击下方按钮发送';}` +
			`else{s.className='';s.textContent='支持多选，手机相册、文档均可';}` +
			`if(!curXhr)g.textContent='发送到电脑';});` +
			`function resetUI(msg){g.disabled=false;g.textContent=msg;prog.style.display='none';` +
			`pf.style.width='0';curXhr=null;}` +
			`pc.addEventListener('click',function(){if(curXhr){curXhr.abort();}});` +
			`document.getElementById('up').addEventListener('submit',function(ev){` +
			`ev.preventDefault();if(!p.files.length)return;` +
			`g.disabled=true;g.textContent='正在发送…';` +
			`var fd=new FormData();for(var i=0;i<p.files.length;i++)fd.append('f',p.files[i]);` +
			`var xhr=new XMLHttpRequest();curXhr=xhr;var lt=0,ll=0,ls=0;` +
			`xhr.upload.addEventListener('progress',function(e){` +
			`pf.style.width=(e.loaded/e.total*100).toFixed(1)+'%';` +
			`pb.textContent=fmt(e.loaded)+' / '+fmt(e.total);` +
			`var now=Date.now();if(lt){var sp=(e.loaded-ll)/((now-lt)/1000);`+
			`ls=ls?ls*0.7+sp*0.3:sp;ps.textContent=fmt(ls)+'/s';}lt=now;ll=e.loaded;});` +
			`xhr.addEventListener('load',function(){if(xhr.status==200){curXhr=null;` +
			`document.open();document.write(xhr.responseText);document.close();}else{fail();}});` +
			`xhr.addEventListener('error',fail);` +
			`xhr.addEventListener('abort',function(){resetUI('发送已取消');` +
			`s.className='picked';s.textContent='✗ 已取消发送，可重新选择文件';});` +
			`function fail(){resetUI('发送失败，请重试');prog.style.display='block';`+
			`pf.style.width='0';pb.textContent=' ';ps.textContent='网络中断，请重试';}` +
			`prog.style.display='block';xhr.open('POST','/t/` + pt.token + `/upload');xhr.send(fd);});` +
			`</script>`)
	}
	sb.WriteString(pageFoot())
	_, _ = w.Write([]byte(sb.String()))
}

// downloadHandler 发送模式：把选中的文件下载给手机（idx 为路径中的序号文本）
func (pt *phoneTransfer) downloadHandler(w http.ResponseWriter, r *http.Request, idxText string) {
	var idx int
	if _, err := fmt.Sscanf(idxText, "%d", &idx); err != nil || idx < 0 || idx >= len(pt.files) {
		http.NotFound(w, r)
		return
	}
	f := pt.files[idx]
	st, err := os.Stat(f)
	if err != nil || st.IsDir() {
		http.NotFound(w, r)
		return
	}
	pt.ui.uiDo(func() {
		pt.ui.appendLog(fmt.Sprintf("[手机互传] 手机开始下载 %s", filepath.Base(f)))
	})
	w.Header().Set("Content-Disposition", `attachment; filename="`+strings.ReplaceAll(filepath.Base(f), `"`, `_`)+`"`)
	http.ServeFile(w, r, f)
}

// uploadHandler 接收模式：保存手机上传的文件到接收目录
func (pt *phoneTransfer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "上传失败", http.StatusBadRequest)
		return
	}
	saved := 0
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		if part.FileName() == "" { // 普通表单字段，跳过
			_, _ = io.Copy(io.Discard, part)
			continue
		}
		name := sanitizeUploadName(part.FileName())
		dstPath := uniquePath(filepath.Join(pt.saveDir, name))
		n, err := saveMultipartFile(part, dstPath)
		if err != nil {
			pt.ui.uiDo(func() {
				pt.ui.appendLog("[手机互传] 保存上传文件失败: " + err.Error())
			})
			continue
		}
		saved++
		pt.recvN++
		name2, n2 := name, n
		pt.ui.uiDo(func() {
			pt.ui.appendLog(fmt.Sprintf("[手机互传] 已接收手机文件 %s（%s），保存到 %s", name2, types.FormatFileSize(n2), pt.saveDir))
		})
		if isMainWindowMinimized() {
			notifyDesktop("已接收手机文件 " + name2)
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var sb strings.Builder
	if saved > 0 {
		sb.WriteString(pageHead("发送成功", "文件已保存到电脑接收目录"))
		sb.WriteString(`<div class="result"><div class="ok">` +
			`<svg viewBox="0 0 24 24"><path d="M9 16.2 4.8 12l-1.4 1.4L9 19 21 7l-1.4-1.4z"/></svg></div>` +
			`<h2>已发送 ` + fmt.Sprint(saved) + ` 个文件到电脑</h2>` +
			`<p>文件已保存到电脑的接收目录</p></div>`)
	} else {
		sb.WriteString(pageHead("没有收到文件", "请重新选择文件再发送"))
		sb.WriteString(`<div class="result"><div class="ok" style="background:linear-gradient(135deg,#ffa94d,#f76707)">` +
			`<svg viewBox="0 0 24 24"><path d="M11 15h2v2h-2zm0-8h2v6h-2zm1-5C6.5 2 2 6.5 2 12s4.5 10 10 10 10-4.5 10-10S17.5 2 12 2z"/></svg></div>` +
			`<h2>没有收到文件</h2><p>请返回重新选择文件后再发送</p></div>`)
	}
	sb.WriteString(`<form method="get" action="/t/` + pt.token + `"><button class="btn" type="submit">继续发送</button></form>`)
	sb.WriteString(pageFoot())
	_, _ = w.Write([]byte(sb.String()))
}

// saveMultipartFile 流式保存上传内容，避免大文件占用内存。
// 中途失败（如手机取消发送）时删除半截文件：必须先关闭句柄再删，
// Windows 下删除打开中的文件会失败。
func saveMultipartFile(part *multipart.Part, dstPath string) (int64, error) {
	dst, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(dst, part)
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(dstPath)
		return 0, err
	}
	return n, nil
}

// sanitizeUploadName 去掉上传文件名里的路径成分与非法字符，防止路径穿越
func sanitizeUploadName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, `\`, `/`))
	name = strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			return '_'
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		name = "upload.bin"
	}
	return name
}

// uniquePath 目标文件已存在时追加序号，避免覆盖
func uniquePath(p string) string {
	if _, err := os.Stat(p); err != nil {
		return p
	}
	ext := filepath.Ext(p)
	base := strings.TrimSuffix(p, ext)
	for i := 1; ; i++ {
		cand := fmt.Sprintf("%s(%d)%s", base, i, ext)
		if _, err := os.Stat(cand); err != nil {
			return cand
		}
	}
}

// showPhoneTransfer 手机互传入口：两个选择
func (ui *ChatUI) showPhoneTransfer() {
	sendBtn := widget.NewButtonWithIcon("发送文件(夹)到手机", theme.MailSendIcon(), ui.showPhoneDropDialog)
	recvBtn := widget.NewButtonWithIcon("接收手机文件", theme.DownloadIcon(), ui.phoneReceive)

	content := container.NewVBox(sendBtn, recvBtn)
	ui.showPhoneDialog("手机互传", content, nil)
}

// showPhoneDropDialog 显示拖拽发送对话框：把文件/文件夹拖入虚线区域即可发送
func (ui *ChatUI) showPhoneDropDialog() {
	// 拖拽区域：虚线风格用浅靛蓝描边矩形模拟，中央放提示文字
	border := canvas.NewRectangle(color.NRGBA{R: 0xC3, G: 0xCD, B: 0xF5, A: 255})
	border.StrokeWidth = 2
	border.FillColor = color.NRGBA{R: 0xEF, G: 0xF1, B: 0xFB, A: 255}
	hint := widget.NewLabel("拖拽文件(夹)进入此区域内发送\n\n支持多个文件 / 文件夹混合拖入")
	hint.Alignment = fyne.TextAlignCenter
	zone := container.NewStack(container.NewGridWrap(fyne.NewSize(320, 180), border),
		container.NewCenter(hint))
	ui.phoneDropZone = zone

	ui.phoneDropMode = true
	ui.showPhoneDialog("发送文件(夹)到手机", zone, func() {
		ui.phoneDropMode = false
		ui.phoneDropZone = nil
	})
}

// phoneDropInZone 判断拖放松手点是否落在拖拽发送区域内。
// Windows 拖放回调坐标滞后，用系统实时光标位置换算画布坐标再判断。
func (ui *ChatUI) phoneDropInZone(pos fyne.Position) bool {
	if ui.phoneDropZone == nil {
		return false
	}
	if p, ok := cursorCanvasPos(ui.win.Canvas().Size().Width); ok {
		pos = p
	}
	origin := fyne.CurrentApp().Driver().AbsolutePositionForObject(ui.phoneDropZone)
	size := ui.phoneDropZone.Size()
	return pos.X >= origin.X && pos.Y >= origin.Y &&
		pos.X <= origin.X+size.Width && pos.Y <= origin.Y+size.Height
}

// handlePhoneDropped 手机互传的拖拽发送：自动识别文件/文件夹，
// 文件夹先打包 zip（后台进行），完成后统一生成二维码。
func (ui *ChatUI) handlePhoneDropped(pos fyne.Position, uris []fyne.URI) {
	if len(uris) == 0 || !ui.phoneDropInZone(pos) {
		return
	}
	var files, folders []string
	for _, u := range uris {
		if u == nil || u.Scheme() != "file" {
			continue
		}
		path := windowsPath(u.Path())
		info, err := os.Stat(path)
		if err != nil {
			p := path
			ui.uiDo(func() { ui.appendLog("[手机互传] 无法访问拖入的项目: " + p) })
			continue
		}
		if info.IsDir() {
			folders = append(folders, path)
		} else {
			files = append(files, path)
		}
	}
	if len(files) == 0 && len(folders) == 0 {
		ui.uiDo(func() { ui.appendLog("[手机互传] 没有可发送的项目") })
		return
	}

	// 进入传输流程：退出拖放模式并关闭对话框
	ui.phoneDropMode = false
	if ui.phoneDlg != nil {
		ui.phoneDlg.Hide()
		ui.phoneDlg = nil
	}

	go func() {
		var tempZips []string
		for _, dir := range folders {
			base := filepath.Base(dir)
			ui.uiDo(func() { ui.appendLog("[手机互传] 正在打包文件夹 " + base + "…") })
			zipPath, err := zipFolder(dir)
			if err != nil {
				ui.uiDo(func() { ui.appendLog("[手机互传] 打包文件夹失败: " + err.Error()) })
				return
			}
			tempZips = append(tempZips, zipPath)
			files = append(files, zipPath)
		}
		ui.uiDo(func() {
			pt, err := ui.startPhoneServer("send", files)
			if err != nil {
				ui.appendLog("[手机互传] 启动服务失败: " + err.Error())
				return
			}
			pt.tempZip = tempZips
			ui.phone = pt
			ui.appendLog(fmt.Sprintf("[手机互传] 等待手机下载 %d 个项目，地址: %s", len(files), pt.url))
			ui.showPhoneQR(pt)
		})
	}()
}

// phoneReceive 启动接收模式：手机上传的文件保存到接收目录
func (ui *ChatUI) phoneReceive() {
	pt, err := ui.startPhoneServer("recv", nil)
	if err != nil {
		ui.appendLog("[手机互传] 启动服务失败: " + err.Error())
		return
	}
	ui.phone = pt
	ui.appendLog(fmt.Sprintf("[手机互传] 等待手机上传文件，地址: %s", pt.url))
	ui.showPhoneQR(pt)
}

// showPhoneDialog 显示手机互传相关对话框；onClosed 在对话框关闭（含停止）时执行
func (ui *ChatUI) showPhoneDialog(title string, content fyne.CanvasObject, onClosed func()) {
	d := dialog.NewCustom(title, "关闭", content, ui.win)
	if onClosed != nil {
		d.SetOnClosed(onClosed)
	}
	if ui.phoneDlg != nil {
		ui.phoneDlg.Hide()
	}
	ui.phoneDlg = d
	d.Show()
}

// showPhoneQR 显示二维码 + 地址 + 停止按钮
func (ui *ChatUI) showPhoneQR(pt *phoneTransfer) {
	qrImg := canvas.NewImageFromImage(nil)
	if qr, err := qrcode.New(pt.url, qrcode.Medium); err == nil {
		qrImg = canvas.NewImageFromImage(qr.Image(300))
		qrImg.SetMinSize(fyne.NewSize(240, 240))
	}
	urlLabel := widget.NewLabel(pt.url)
	urlLabel.TextStyle = fyne.TextStyle{Monospace: true}
	tip := widget.NewLabel("手机相机/微信扫码，或浏览器输入上方网址\n关闭本窗口即停止服务")
	tip.Wrapping = fyne.TextWrapWord

	// 不设独立「停止」按钮：对话框的「关闭」已通过 SetOnClosed 停止服务
	ui.showPhoneDialog("手机互传", container.NewVBox(tip, container.NewCenter(qrImg), urlLabel), ui.stopPhoneServer)
}

// stopPhoneServer 停止当前手机互传服务
func (ui *ChatUI) stopPhoneServer() {
	if ui.phone != nil {
		ui.phone.Stop()
		ui.phone = nil
		ui.appendLog("[手机互传] 服务已停止")
	}
}
