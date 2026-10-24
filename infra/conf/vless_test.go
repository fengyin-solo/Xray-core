package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/proxy/vless/inbound"
	"github.com/xtls/xray-core/proxy/vless/outbound"
)

func TestVLessOutbound(t *testing.T) {
	creator := func() Buildable {
		return new(VLessOutboundConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"vnext": [{
					"address": "example.com",
					"port": 443,
					"users": [
						{
							"id": "27848739-7e62-4138-9fd3-098a63964b6b",
							"flow": "xtls-rprx-vision-udp443",
							"encryption": "none",
							"level": 0
						}
					]
				}]
			}`,
			Parser: loadJSON(creator),
			Output: &outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: &net.IPOrDomain{
						Address: &net.IPOrDomain_Domain{
							Domain: "example.com",
						},
					},
					Port: 443,
					User: &protocol.User{
						Account: serial.ToTypedMessage(&vless.Account{
							Id:         "27848739-7e62-4138-9fd3-098a63964b6b",
							Flow:       "xtls-rprx-vision-udp443",
							Encryption: "none",
						}),
						Level: 0,
					},
				},
			},
		},
		{
			Input: `{
				"address": "example.com",
				"port": 443,
				"id": "27848739-7e62-4138-9fd3-098a63964b6b",
				"flow": "xtls-rprx-vision-udp443",
				"encryption": "none",
				"level": 0
			}`,
			Parser: loadJSON(creator),
			Output: &outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: &net.IPOrDomain{
						Address: &net.IPOrDomain_Domain{
							Domain: "example.com",
						},
					},
					Port: 443,
					User: &protocol.User{
						Account: serial.ToTypedMessage(&vless.Account{
							Id:         "27848739-7e62-4138-9fd3-098a63964b6b",
							Flow:       "xtls-rprx-vision-udp443",
							Encryption: "none",
						}),
						Level: 0,
					},
				},
			},
		},
	})
}

func buildVLessOutboundAccount(t *testing.T, input string) *vless.Account {
	t.Helper()
	c := new(VLessOutboundConfig)
	if err := json.Unmarshal([]byte(input), c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	msg, err := c.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	user := msg.(*outbound.Config).Vnext.User
	msgAccount, err := user.Account.GetInstance()
	if err != nil {
		t.Fatalf("unmarshal account: %v", err)
	}
	return msgAccount.(*vless.Account)
}

func TestVLessOutboundLegacyNumericTestpre(t *testing.T) {
	account := buildVLessOutboundAccount(t, `{
		"address": "example.com",
		"port": 443,
		"id": "27848739-7e62-4138-9fd3-098a63964b6b",
		"encryption": "none",
		"testpre": 4
	}`)
	if account.Testpre != 4 || account.Pool != nil {
		t.Fatalf("numeric testpre should map to legacy field, got testpre=%d pool=%v", account.Testpre, account.Pool)
	}
}

func TestVLessOutboundZeroTestpreDisabled(t *testing.T) {
	account := buildVLessOutboundAccount(t, `{
		"address": "example.com",
		"port": 443,
		"id": "27848739-7e62-4138-9fd3-098a63964b6b",
		"encryption": "none",
		"testpre": 0
	}`)
	if account.Testpre != 0 || account.Pool != nil {
		t.Fatalf("zero testpre should remain disabled, got testpre=%d pool=%v", account.Testpre, account.Pool)
	}
}

func TestVLessOutboundPoolTestpre(t *testing.T) {
	account := buildVLessOutboundAccount(t, `{
		"address": "example.com",
		"port": 443,
		"id": "27848739-7e62-4138-9fd3-098a63964b6b",
		"encryption": "none",
		"testpre": {
			"minIdle": 2,
			"maxIdle": 5,
			"lifetime": "30s",
			"lifetimeJitter": "5s",
			"dialTimeout": "2s",
			"retryInterval": "100ms",
			"retryMaxInterval": "1s",
			"closeGrace": "3s"
		}
	}`)
	if account.Testpre != 0 {
		t.Fatalf("pool form must not set legacy testpre, got %d", account.Testpre)
	}
	pool := account.Pool
	if pool == nil {
		t.Fatal("pool form should populate account.Pool")
	}
	if pool.MinIdle != 2 || pool.MaxIdle != 5 {
		t.Fatalf("unexpected idle limits: %+v", pool)
	}
	if pool.LifetimeMs != 30000 || pool.LifetimeJitterMs != 5000 || pool.DialTimeoutMs != 2000 ||
		pool.RetryIntervalMs != 100 || pool.RetryMaxIntervalMs != 1000 || pool.CloseGraceMs != 3000 {
		t.Fatalf("pool durations not converted to milliseconds: %+v", pool)
	}
}

func TestVLessOutboundInvalidTestpre(t *testing.T) {
	c := new(VLessOutboundConfig)
	if err := json.Unmarshal([]byte(`{
		"address": "example.com",
		"port": 443,
		"id": "27848739-7e62-4138-9fd3-098a63964b6b",
		"encryption": "none",
		"testpre": "nonsense"
	}`), c); err == nil {
		t.Fatal("invalid testpre value should be rejected")
	}
}

func TestVLessInbound(t *testing.T) {
	creator := func() Buildable {
		return new(VLessInboundConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"clients": [
					{
						"id": "27848739-7e62-4138-9fd3-098a63964b6b",
						"flow": "xtls-rprx-vision",
						"level": 0,
						"email": "love@example.com"
					}
				],
				"decryption": "none",
				"fallbacks": [
					{
						"dest": 80
					},
					{
						"alpn": "h2",
						"dest": "@/dev/shm/domain.socket",
						"xver": 2
					},
					{
						"path": "/innerws",
						"dest": "serve-ws-none"
					}
				]
			}`,
			Parser: loadJSON(creator),
			Output: &inbound.Config{
				Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id:   "27848739-7e62-4138-9fd3-098a63964b6b",
							Flow: "xtls-rprx-vision",
						}),
						Level: 0,
						Email: "love@example.com",
					},
				},
				Decryption: "none",
				Fallbacks: []*inbound.Fallback{
					{
						Alpn: "",
						Path: "",
						Type: "tcp",
						Dest: "localhost:80",
						Xver: 0,
					},
					{
						Alpn: "h2",
						Path: "",
						Type: "unix",
						Dest: "@/dev/shm/domain.socket",
						Xver: 2,
					},
					{
						Alpn: "",
						Path: "/innerws",
						Type: "serve",
						Dest: "serve-ws-none",
						Xver: 0,
					},
				},
			},
		},
	})
}
