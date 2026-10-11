package types

import (
	"localchat/pkg/common"
	"testing"
	"time"
)

// TestConnectFailureKeepsPeer 验证TCP连接失败（对方防火墙拦截/端口未监听）时，
// 在线用户不被误删——UDP发现仍证明对方在线，误删会导致对方下次广播时反复"上线了"刷屏。
// 对应场景：对端防火墙拦截入站TCP导致dial超时，但对端UDP广播正常到达。
func TestConnectFailureKeepsPeer(t *testing.T) {
	c := NewChatClient()
	c.SetName("Connector")
	c.SetPortBase(8000)
	c.Start()
	defer c.Stop()

	// 伪造一个"UDP在线但TCP不可达"的用户：127.0.0.1:1 端口1通常无监听，dial立即refused
	key := "127.0.0.1:1"
	c.peersMutex.Lock()
	c.peers[key] = &common.Peer{Name: "Ghost", IP: "127.0.0.1", Port: 1, LastSeen: time.Now()}
	c.peersMutex.Unlock()

	if c.StartChatWith("127.0.0.1", 1) {
		t.Fatal("连接到无监听端口应失败")
	}

	// 连接失败后该用户仍应保留在在线列表中
	c.peersMutex.RLock()
	_, exists := c.peers[key]
	c.peersMutex.RUnlock()
	if !exists {
		t.Fatal("TCP连接失败不应移除UDP仍可发现的在线用户")
	}
}
