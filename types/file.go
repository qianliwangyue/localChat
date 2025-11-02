package types

import "os"

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
