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
}
