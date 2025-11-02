package common

import (
	"encoding/json"
)

// 消息类型常量
const (
	MsgTypeDiscover = "discover" // 服务发现
	MsgTypeChat     = "chat"     // 聊天消息
	MsgTypeOnline   = "online"   // 上线通知
	MsgTypeOffline  = "offline"  // 下线通知
)

// Message 消息结构体
type Message struct {
	Type    string `json:"type"`    // 消息类型
	From    string `json:"from"`    // 发送者代号
	Content string `json:"content"` // 消息内容
	IP      string `json:"ip"`      // IP地址
	Port    int    `json:"port"`    // TCP端口
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
