package conf_test

import (
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/socks"
)

func TestSocksInboundConfig(t *testing.T) {
	creator := func() Buildable {
		return new(SocksServerConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"auth": "password",
				"accounts": [
					{
						"user": "my-username",
						"pass": "my-password"
					}
				],
				"udp": false,
				"ip": "127.0.0.1",
				"userLevel": 1
			}`,
			Parser: loadJSON(creator),
			Output: &socks.ServerConfig{
				AuthType: socks.AuthType_PASSWORD,
				Accounts: map[string]string{
					"my-username": "my-password",
				},
				UdpEnabled: false,
				Address: &net.IPOrDomain{
					Address: &net.IPOrDomain_Ip{
						Ip: []byte{127, 0, 0, 1},
					},
				},
				UserLevel: 1,
			},
		},
		{
			Input: `{
				"auth": "noauth",
				"udp": true,
				"ip": "127.0.0.1",
				"bind": true,
				"bindAddress": "127.0.0.2",
				"bindPort": "10000-11000",
				"bindTimeout": "90s"
			}`,
			Parser: loadJSON(creator),
			Output: &socks.ServerConfig{
				AuthType:   socks.AuthType_NO_AUTH,
				UdpEnabled: true,
				Address: &net.IPOrDomain{
					Address: &net.IPOrDomain_Ip{
						Ip: []byte{127, 0, 0, 1},
					},
				},
				BindEnabled: true,
				BindAddress: &net.IPOrDomain{
					Address: &net.IPOrDomain_Ip{
						Ip: []byte{127, 0, 0, 2},
					},
				},
				BindPortRange: &net.PortRange{From: 10000, To: 11000},
				BindTimeout:   90,
			},
		},
	})
}

func TestSocksOutboundConfig(t *testing.T) {
	creator := func() Buildable {
		return new(SocksClientConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"servers": [{
					"address": "127.0.0.1",
					"port": 1234,
					"users": [
						{"user": "test user", "pass": "test pass", "email": "test@email.com"}
					]
				}]
			}`,
			Parser: loadJSON(creator),
			Output: &socks.ClientConfig{
				Server: &protocol.ServerEndpoint{
					Address: &net.IPOrDomain{
						Address: &net.IPOrDomain_Ip{
							Ip: []byte{127, 0, 0, 1},
						},
					},
					Port: 1234,
					User: &protocol.User{
						Email: "test@email.com",
						Account: serial.ToTypedMessage(&socks.Account{
							Username: "test user",
							Password: "test pass",
						}),
					},
				},
			},
		},
		{
			Input: `{
				"address": "127.0.0.1",
				"port": 1234,
				"user": "test user",
				"pass": "test pass",
				"email": "test@email.com"
			}`,
			Parser: loadJSON(creator),
			Output: &socks.ClientConfig{
				Server: &protocol.ServerEndpoint{
					Address: &net.IPOrDomain{
						Address: &net.IPOrDomain_Ip{
							Ip: []byte{127, 0, 0, 1},
						},
					},
					Port: 1234,
					User: &protocol.User{
						Email: "test@email.com",
						Account: serial.ToTypedMessage(&socks.Account{
							Username: "test user",
							Password: "test pass",
						}),
					},
				},
			},
		},
	})
}
