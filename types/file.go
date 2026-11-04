package types

import (
	"os"
	"time"
)

// 传输中的文件信息
type TransferringFile struct {
	FilePath     string
	FileName     string
	FileSize     int64
	FileID       string
	File         *os.File
	TotalChunks  int
	CurrentChunk int
	Buffer       []byte
	RelativePath string // 相对路径（用于文件夹传输）
	FolderID     string // 所属文件夹ID（如果是文件夹传输中的文件）

	// 接收端统计与进度节流
	Written    int64     // 已写入字节数，用于完成时校验长度
	LastPct    int       // 上次上报的百分比
	LastReport time.Time // 上次上报的时间

	// 协议 v2：二进制分帧 + 断点续传
	Proto2    bool   // 对端支持 proto v2（二进制块 + 续传）
	FileKey   string // 稳定文件标识，续传时用来确认是同一个文件
	PartPath  string // 接收端临时文件路径（xxx.lcpart）；发送端为空
	Resume    int64  // 本次传输的起始偏移（0 表示从头开始）
	Acked     int64  // 发送端：已被对端 ACK 确认的字节数
	LastAckAt int64  // 接收端：上次发送 ACK 时的已写字节数
	Sent      int64  // 发送端：本次已发出的字节数（含续传起点前的部分）

	// 发送端取消标记：写操作必须在 fileMutex 内，读操作用 isSendCanceled()
	// （发送循环与消息处理协程并发访问）
	Canceled bool
}

// 文件夹传输信息
type TransferringFolder struct {
	FolderPath  string
	FolderName  string
	FolderSize  int64
	FileCount   int
	FolderID    string
	CurrentFile int
	TotalFiles  int
	Files       []string // 文件列表
	DoneBytes   int64    // 已完成文件的累计字节数（用于整体进度/速度统计）

	// 发送端取消标记：写操作必须在 fileMutex 内，读操作用 isFolderCanceled()
	Canceled bool
}

// TransferProgress 传输进度信息，回调给 UI 显示大小/速度等明细。
// 单文件传输：Written/Total 为该文件的字节，FileIndex/FileCount 恒为 1/1；
// 文件夹传输：Written/Total 为整个文件夹的累计字节，FileIndex/FileCount 为文件序号。
type TransferProgress struct {
	Sending   bool
	Pct       int    // 百分比 0-100（文件夹为整体进度）
	Label     string // 对话框显示的标题文本
	Written   int64  // 已完成字节数
	Total     int64  // 总字节数
	FileIndex int    // 当前第几个文件（从 1 开始）
	FileCount int    // 文件总数
	FileName  string // 当前文件名
}
