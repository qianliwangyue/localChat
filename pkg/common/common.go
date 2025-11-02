package common

import (
	"encoding/json"
)

// 消息类型常量
const (
	MsgTypeDiscover     = "discover"      // 服务发现
	MsgTypeChat         = "chat"          // 聊天消息
	MsgTypeOnline       = "online"        // 上线通知
	MsgTypeOffline      = "offline"       // 下线通知
	MsgTypeFileOffer    = "file_offer"    // 文件发送请求
	MsgTypeFileAccept   = "file_accept"   // 接受文件
	MsgTypeFileReject   = "file_reject"   // 拒绝文件
	MsgTypeFileData     = "file_data"     // 文件数据
	MsgTypeFileDone     = "file_done"     // 文件传输完成
	MsgTypeFileCancel   = "file_cancel"   // 取消文件传输
	MsgTypeFolderOffer  = "folder_offer"  // 文件夹发送请求
	MsgTypeFolderAccept = "folder_accept" // 接受文件夹
	MsgTypeFolderReject = "folder_reject" // 拒绝文件夹
	MsgTypeFolderDone   = "folder_done"   // 文件夹传输完成
)

// Message 消息结构体
type Message struct {
	Type    string `json:"type"`    // 消息类型
	From    string `json:"from"`    // 发送者代号
	Content string `json:"content"` // 消息内容
	IP      string `json:"ip"`      // IP地址
	Port    int    `json:"port"`    // TCP端口
	// 文件传输相关字段
	FileName    string `json:"fileName,omitempty"`    // 文件名
	FileSize    int64  `json:"fileSize,omitempty"`    // 文件大小
	FileID      string `json:"fileID,omitempty"`      // 文件唯一标识
	FileData    string `json:"fileData,omitempty"`    // 编码后的文件数据
	ChunkIndex  int    `json:"chunkIndex,omitempty"`  // 数据块索引
	TotalChunks int    `json:"totalChunks,omitempty"` // 总块数
	Progress    int    `json:"progress,omitempty"`    // 传输进度百分比
	// 文件夹传输相关字段
	FolderName  string `json:"folderName,omitempty"`  // 文件夹名
	FolderSize  int64  `json:"folderSize,omitempty"`  // 文件夹总大小
	FileCount   int    `json:"fileCount,omitempty"`   // 文件数量
	RelativePath string `json:"relativePath,omitempty"` // 相对路径
	FolderID    string `json:"folderID,omitempty"`    // 文件夹唯一标识
}

// Peer 对等节点信息
type Peer struct {
	Name string `json:"name"` // 用户代号
	IP   string `json:"ip"`   // IP地址
	Port int    `json:"port"` // TCP端口
}

// 序列化消息
func (m *Message) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

// 反序列化消息
func DeserializeMessage(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return &msg, err
}
