//go:build windows

package types

import (
	"fmt"
	"os/exec"
	"syscall"
)

// firewallRuleName 程序自动维护的防火墙入站放行规则名。
// 使用端口规则而非程序路径规则：go run 每次编译出的临时exe路径不同，
// 按程序路径放行的规则对临时exe不生效，端口规则一劳永逸
const firewallRuleName = "localchat-port"

// ensureFirewallRule 检测并尝试添加Windows防火墙入站放行规则。
// 跨机聊天依赖对端连入本机TCP端口；若入站SYN被防火墙DROP，对端dial表现为i/o timeout。
// 典型坑：Windows首次运行弹出的防火墙提示只在当时勾选的网络配置文件(Public/Private)生效，
// 之后网络切换配置文件（如家用WiFi被识别为专用网络）规则即失效，同机正常但跨机连不上。
func (c *ChatClient) ensureFirewallRule() {
	// 测试场景（非默认端口段）不打扰系统防火墙
	if c.portBase != 0 {
		return
	}

	// 规则已存在则跳过（查询无需管理员权限；规则不存在时netsh退出码为1）
	if _, err := runNetshHide("advfirewall", "firewall", "show", "rule", "name="+firewallRuleName); err == nil {
		return
	}

	tcpStart, udpStart := c.tcpPortStart(), c.udpPortStart()
	_, errTCP := runNetshHide("advfirewall", "firewall", "add", "rule",
		"name="+firewallRuleName, "dir=in", "action=allow", "enable=yes", "profile=any",
		"protocol=TCP", fmt.Sprintf("localport=%d-%d", tcpStart, tcpStart+99))
	_, errUDP := runNetshHide("advfirewall", "firewall", "add", "rule",
		"name="+firewallRuleName, "dir=in", "action=allow", "enable=yes", "profile=any",
		"protocol=UDP", fmt.Sprintf("localport=%d-%d", udpStart, udpStart+99))

	if errTCP != nil || errUDP != nil {
		c.logf("[重要] 未检测到防火墙入站放行规则，且当前无管理员权限无法自动添加。")
		c.logf("其他机器将无法连接本机（表现为连接超时i/o timeout，本机往外连不受影响）。")
		c.logf("修复方法(二选一)：1) 以管理员身份运行一次本程序自动配置；2) 管理员CMD执行：")
		c.logf("netsh advfirewall firewall add rule name=%s dir=in action=allow profile=any protocol=TCP localport=%d-%d",
			firewallRuleName, tcpStart, tcpStart+99)
		return
	}
	c.logf("已自动添加防火墙入站放行规则[%s]：TCP %d-%d / UDP %d-%d（所有网络配置文件）",
		firewallRuleName, tcpStart, tcpStart+99, udpStart, udpStart+99)
}

// runNetshHide 静默执行netsh（隐藏控制台窗口，避免GUI程序弹出黑色控制台）
func runNetshHide(args ...string) ([]byte, error) {
	cmd := exec.Command("netsh", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	return cmd.CombinedOutput()
}
