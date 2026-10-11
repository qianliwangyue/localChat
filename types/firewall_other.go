//go:build !windows

package types

// ensureFirewallRule 非Windows平台不自动处理防火墙（iptables/ufw等由用户自行管理）
func (c *ChatClient) ensureFirewallRule() {}
