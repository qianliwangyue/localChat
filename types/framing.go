package types

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// 协议 v2 引入两项能力，均通过 offer/accept 消息里的 Proto 字段逐文件协商：
//  1. 二进制数据块（JSON 头 + 换行 + 原始字节），省掉 base64 的 33% 膨胀；
//  2. 字节级 ACK + 断点续传（.lcpart 临时文件 + 元数据）。
//
// 任一端不支持时自动回落到旧的「base64 + 纯 JSON」通道，老版本对端仍可互传。
const (
	// protoV2 支持二进制分帧与断点续传的协议版本号
	protoV2 = 2

	// ackInterval 接收端每确认这么多字节回一次 ACK。
	// ACK 只用于「确认偏移」的记账与断点信息落盘，不参与逐块流控，
	// 因此可以取得比较稀，避免反向消息抢占带宽。
	ackInterval = 8 << 20

	// chunkFrameSlack 单个数据块帧 JSON 头的最大预留长度
	chunkFrameSlack = 512

	// partSuffix 接收中的临时文件后缀，完成后重命名为正式文件名
	partSuffix = ".lcpart"
	// partMetaSuffix 临时文件的续传元数据后缀
	partMetaSuffix = ".json"

	// resumeMetaMinSize 小于该大小的文件不在开始时写盘续传元数据。
	// 元数据要经历「建文件 + 改名」两次目录操作，实测每个小文件约多 2ms；
	// 成千上万个小文件的文件夹传输会被这个固定开销拖慢数倍，
	// 而小文件重传的代价远低于此。中断时仍会补写元数据，续传能力不受影响。
	resumeMetaMinSize = 4 << 20
)

// stableFileKey 生成跨连接稳定的文件标识，用于判断「这次要发的文件」
// 与上次中断时留下的 .lcpart 是不是同一个文件。
// 文件名、大小、修改时间任一变化都会得到不同的 key，
// 从而在源文件被改动后自动放弃续传，不会拼出错误内容。
func stableFileKey(name string, size int64, mod time.Time) string {
	h := sha1.Sum([]byte(fmt.Sprintf("%s|%d|%d", name, size, mod.UnixNano())))
	return hex.EncodeToString(h[:8])
}

// partMeta 与 .lcpart 并排存放的续传元数据
type partMeta struct {
	Key      string `json:"key"`       // 稳定文件标识
	FileName string `json:"fileName"`  // 原始文件名
	Size     int64  `json:"size"`      // 文件总大小
	Written  int64  `json:"written"`   // 最后一次确认的已接收字节
	Chunk    int    `json:"chunkSize"` // 写入时使用的数据块大小
	Updated  int64  `json:"updated"`   // 更新时间（纳秒）
}

func partPathFor(savePath string) string { return savePath + partSuffix }

func partMetaPathFor(savePath string) string { return savePath + partSuffix + partMetaSuffix }

// readPartMeta 读取续传元数据，文件缺失或损坏时返回 false
func readPartMeta(savePath string) (partMeta, bool) {
	var m partMeta
	data, err := os.ReadFile(partMetaPathFor(savePath))
	if err != nil {
		return m, false
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, false
	}
	return m, true
}

// writePartMeta 原子地更新续传元数据（先写临时文件再改名，避免断电留下半个 JSON）
func writePartMeta(savePath string, m partMeta) error {
	m.Chunk = fileChunkSize
	m.Updated = time.Now().UnixNano()
	data, err := json.Marshal(&m)
	if err != nil {
		return err
	}
	tmp := partMetaPathFor(savePath) + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, partMetaPathFor(savePath))
}

// removePartFiles 删除临时文件与其元数据（传输成功或明确取消时调用）
func removePartFiles(savePath string) {
	os.Remove(partPathFor(savePath))
	os.Remove(partMetaPathFor(savePath))
	os.Remove(partMetaPathFor(savePath) + ".tmp")
}

// alignDownToChunk 把偏移量向下对齐到数据块边界。
// 续传点必须落在块边界上，否则发送端的块序号与接收端的块序号会错开。
func alignDownToChunk(off int64) int64 {
	if off <= 0 {
		return 0
	}
	return off / int64(fileChunkSize) * int64(fileChunkSize)
}

// openPartFile 以续传语义打开 .lcpart：截断到最后确认的偏移并定位到该处
func openPartFile(path string, offset int64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(offset); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// appendChunkFrame 把「JSON 头 + '\n' + 原始字节」拼进同一块缓冲，
// 保证一个数据块只产生一次 Write 系统调用（TCP_NODELAY 下即一个报文段）
func appendChunkFrame(dst []byte, header []byte, payload []byte) []byte {
	dst = append(dst, header...)
	dst = append(dst, '\n')
	dst = append(dst, payload...)
	return dst
}

// readChunkPayload 从连接读取 n 字节原始负载。
// 与 appendChunkFrame 对称，只读取头里声明的长度，不会越界吃掉后续消息。
// buf 为可复用缓冲（容量足够时不再分配），避免高速传输时每块都产生一次大对象。
func readChunkPayload(r *bufio.Reader, n int, buf []byte) ([]byte, error) {
	if n < 0 || n > fileChunkSize {
		return nil, fmt.Errorf("数据块长度非法: %d", n)
	}
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// folderMarkerName 文件夹级续传标记：放在目标文件夹里，记录本次传输的文件夹标识。
// 只有「上次没传完」的文件夹才会留下这个标记，传完即删除，
// 因此用户再次发送同名文件夹时不会被误当成续传目标。
const folderMarkerName = ".localchat-folder"

// stableFolderKey 生成文件夹的稳定标识（名字 + 总大小 + 文件数）
func stableFolderKey(name string, size int64, fileCount int) string {
	h := sha1.Sum([]byte(fmt.Sprintf("%s|%d|%d", name, size, fileCount)))
	return hex.EncodeToString(h[:8])
}

func folderMarkerPath(dir string) string { return filepath.Join(dir, folderMarkerName) }

// readFolderMarker 读取文件夹续传标记
func readFolderMarker(dir string) (string, bool) {
	data, err := os.ReadFile(folderMarkerPath(dir))
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(data)), true
}

// writeFolderMarker 写入文件夹续传标记
func writeFolderMarker(dir, key string) error {
	if key == "" {
		return nil
	}
	return os.WriteFile(folderMarkerPath(dir), []byte(key), 0644)
}

// removeFolderMarker 传输完成或放弃后续传时清除标记
func removeFolderMarker(dir string) {
	os.Remove(folderMarkerPath(dir))
}

// isInternalTransferFile 判断是否为传输过程的中间文件（临时数据/续传元数据/文件夹标记）
func isInternalTransferFile(name string) bool {
	return strings.HasSuffix(name, partSuffix) ||
		strings.HasSuffix(name, partSuffix+partMetaSuffix) ||
		strings.HasSuffix(name, partMetaSuffix+".tmp") ||
		name == folderMarkerName
}
