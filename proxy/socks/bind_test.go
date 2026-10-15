package socks_test

import (
	"encoding/binary"
	"io"
	gonet "net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/proxy/socks"
	tcptest "github.com/xtls/xray-core/testing/servers/tcp"
)

const bindTestTimeout = 10 * time.Second

func newBindServer(t *testing.T, cfg *socks.ServerConfig) net.Port {
	t.Helper()
	port := tcptest.PickPort()
	coreCfg := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(port)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(cfg),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}
	inst, err := core.New(coreCfg)
	common.Must(err)
	common.Must(inst.Start())
	t.Cleanup(func() { _ = inst.Close() })

	// Wait until the inbound is accepting connections.
	addr := net.TCPDestination(net.LocalHostIP, port).NetAddr()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := gonet.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socks inbound did not become ready: ", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return port
}

func dialInbound(t *testing.T, port net.Port) gonet.Conn {
	t.Helper()
	conn, err := gonet.DialTimeout("tcp", net.TCPDestination(net.LocalHostIP, port).NetAddr(), bindTestTimeout)
	common.Must(err)
	common.Must(conn.SetDeadline(time.Now().Add(bindTestTimeout)))
	return conn
}

// readFull reads exactly len(buf) bytes or fails the test.
func readFull(t *testing.T, r io.Reader, buf []byte, what string) {
	t.Helper()
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal("failed to read ", what, ": ", err)
	}
}

// socks5Bind performs a SOCKS5 greeting and sends a BIND request for the
// given port (0 lets the server allocate one).
func socks5Bind(t *testing.T, conn gonet.Conn, port uint16) {
	t.Helper()
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatal("failed to write method negotiation: ", err)
	}
	resp := make([]byte, 2)
	readFull(t, conn, resp, "method response")
	if resp[0] != 0x05 || resp[1] != 0x00 {
		t.Fatalf("unexpected method response: %v", resp)
	}

	req := []byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, byte(port >> 8), byte(port)}
	if _, err := conn.Write(req); err != nil {
		t.Fatal("failed to write bind request: ", err)
	}
}

// readSocks5Reply reads one SOCKS5 reply (IPv4 address type).
func readSocks5Reply(t *testing.T, conn gonet.Conn, what string) (rep byte, ip net.IP, port int) {
	t.Helper()
	resp := make([]byte, 10)
	readFull(t, conn, resp, what)
	if resp[0] != 0x05 {
		t.Fatalf("%s: unexpected version %d", what, resp[0])
	}
	if resp[3] != 0x01 {
		t.Fatalf("%s: expected IPv4 address type, got %d", what, resp[3])
	}
	return resp[1], gonet.IP(resp[4:8]), int(binary.BigEndian.Uint16(resp[8:10]))
}

// socks4Bind sends a SOCKS4 BIND request for the given port.
func socks4Bind(t *testing.T, conn gonet.Conn, port uint16) {
	t.Helper()
	req := []byte{0x04, 0x02, byte(port >> 8), byte(port), 0, 0, 0, 0, 0x00}
	if _, err := conn.Write(req); err != nil {
		t.Fatal("failed to write socks4 bind request: ", err)
	}
}

func readSocks4Reply(t *testing.T, conn gonet.Conn, what string) (code byte, ip net.IP, port int) {
	t.Helper()
	resp := make([]byte, 8)
	readFull(t, conn, resp, what)
	if resp[0] != 0x00 {
		t.Fatalf("%s: unexpected SOCKS4 version byte %d", what, resp[0])
	}
	return resp[1], gonet.IP(resp[4:8]), int(binary.BigEndian.Uint16(resp[2:4]))
}

func expectString(t *testing.T, conn gonet.Conn, want, what string) {
	t.Helper()
	buf := make([]byte, len(want))
	readFull(t, conn, buf, what)
	if string(buf) != want {
		t.Fatalf("%s: expected %q, got %q", what, want, string(buf))
	}
}

func dialBound(t *testing.T, ip net.IP, port int) *gonet.TCPConn {
	t.Helper()
	c, err := gonet.DialTimeout("tcp4", gonet.JoinHostPort(ip.String(), strconv.Itoa(port)), bindTestTimeout)
	common.Must(err)
	tcpConn := c.(*gonet.TCPConn)
	common.Must(tcpConn.SetDeadline(time.Now().Add(bindTestTimeout)))
	return tcpConn
}

func waitPortReleased(t *testing.T, ip net.IP, port int) {
	t.Helper()
	addr := gonet.JoinHostPort(ip.String(), strconv.Itoa(port))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := gonet.DialTimeout("tcp4", addr, 200*time.Millisecond)
		if err != nil {
			return
		}
		c.Close()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("bound port %d was not released", port)
}

func TestSocks5BindSuccess(t *testing.T) {
	port := newBindServer(t, &socks.ServerConfig{BindEnabled: true, BindTimeout: 30})

	conn := dialInbound(t, port)
	defer conn.Close()
	socks5Bind(t, conn, 0)

	rep, bindIP, bindPort := readSocks5Reply(t, conn, "first reply")
	if rep != 0x00 {
		t.Fatal("first reply rejected, code: ", rep)
	}
	if bindPort == 0 {
		t.Fatal("expected non-zero bound port")
	}
	if bindIP.String() != "127.0.0.1" {
		t.Fatal("expected bound address 127.0.0.1, got ", bindIP)
	}

	// The peer connects back to the bound address. The kernel completes the
	// handshake against the backlog before the server calls Accept.
	peer := dialBound(t, bindIP, bindPort)
	defer peer.Close()

	rep, remoteIP, remotePort := readSocks5Reply(t, conn, "second reply")
	if rep != 0x00 {
		t.Fatal("second reply rejected, code: ", rep)
	}
	if remoteIP.String() != "127.0.0.1" || remotePort != peer.LocalAddr().(*gonet.TCPAddr).Port {
		t.Fatalf("second reply address mismatch: %s:%d vs %s", remoteIP, remotePort, peer.LocalAddr())
	}

	// Bidirectional relay.
	if _, err := peer.Write([]byte("from-peer")); err != nil {
		t.Fatal(err)
	}
	expectString(t, conn, "from-peer", "peer -> client")

	if _, err := conn.Write([]byte("from-client")); err != nil {
		t.Fatal(err)
	}
	expectString(t, peer, "from-client", "client -> peer")

	// Orderly half-close lets the handler finish both relay goroutines.
	common.Must(conn.(*gonet.TCPConn).CloseWrite())
}

func TestSocks4BindSuccess(t *testing.T) {
	port := newBindServer(t, &socks.ServerConfig{BindEnabled: true, BindTimeout: 30})

	conn := dialInbound(t, port)
	defer conn.Close()
	socks4Bind(t, conn, 0)

	code, bindIP, bindPort := readSocks4Reply(t, conn, "first reply")
	if code != 90 {
		t.Fatal("first reply rejected, code: ", code)
	}
	if bindPort == 0 {
		t.Fatal("expected non-zero bound port")
	}
	if bindIP.String() != "127.0.0.1" {
		t.Fatal("expected bound address 127.0.0.1, got ", bindIP)
	}

	peer := dialBound(t, bindIP, bindPort)
	defer peer.Close()

	code, remoteIP, remotePort := readSocks4Reply(t, conn, "second reply")
	if code != 90 {
		t.Fatal("second reply rejected, code: ", code)
	}
	if remoteIP.String() != "127.0.0.1" || remotePort != peer.LocalAddr().(*gonet.TCPAddr).Port {
		t.Fatalf("second reply address mismatch: %s:%d vs %s", remoteIP, remotePort, peer.LocalAddr())
	}

	if _, err := peer.Write([]byte("p4")); err != nil {
		t.Fatal(err)
	}
	expectString(t, conn, "p4", "peer -> client")

	if _, err := conn.Write([]byte("c4")); err != nil {
		t.Fatal(err)
	}
	expectString(t, peer, "c4", "client -> peer")

	common.Must(conn.(*gonet.TCPConn).CloseWrite())
}

func TestSocks5BindRequestedPort(t *testing.T) {
	requested := tcptest.PickPort()
	port := newBindServer(t, &socks.ServerConfig{BindEnabled: true, BindTimeout: 30})

	conn := dialInbound(t, port)
	defer conn.Close()
	socks5Bind(t, conn, requested.Value())

	rep, bindIP, bindPort := readSocks5Reply(t, conn, "first reply")
	if rep != 0x00 {
		t.Fatal("first reply rejected, code: ", rep)
	}
	if bindPort != int(requested.Value()) {
		t.Fatalf("expected bound port %d, got %d", requested, bindPort)
	}

	peer := dialBound(t, bindIP, bindPort)
	defer peer.Close()

	rep, _, _ = readSocks5Reply(t, conn, "second reply")
	if rep != 0x00 {
		t.Fatal("second reply rejected, code: ", rep)
	}
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	expectString(t, peer, "x", "relay")
}

func TestSocks5BindTimeout(t *testing.T) {
	port := newBindServer(t, &socks.ServerConfig{BindEnabled: true, BindTimeout: 1})

	conn := dialInbound(t, port)
	defer conn.Close()
	socks5Bind(t, conn, 0)

	rep, bindIP, bindPort := readSocks5Reply(t, conn, "first reply")
	if rep != 0x00 {
		t.Fatal("first reply rejected, code: ", rep)
	}

	// Nobody connects back; after the timeout the second reply must report
	// a failure and the listening port must be released.
	rep, _, _ = readSocks5Reply(t, conn, "second reply")
	if rep == 0x00 {
		t.Fatal("expected failure status on timeout, got success")
	}
	waitPortReleased(t, bindIP, bindPort)
}

func TestSocks5BindClientCancel(t *testing.T) {
	port := newBindServer(t, &socks.ServerConfig{BindEnabled: true, BindTimeout: 60})

	conn := dialInbound(t, port)
	socks5Bind(t, conn, 0)

	_, bindIP, bindPort := readSocks5Reply(t, conn, "first reply")

	// Client disconnects before any peer connects back.
	common.Must(conn.Close())
	waitPortReleased(t, bindIP, bindPort)
}

func TestSocks5BindPortRange(t *testing.T) {
	occupied := tcptest.PickPort()
	holder, err := gonet.Listen("tcp4", net.TCPDestination(net.LocalHostIP, occupied).NetAddr())
	common.Must(err)
	defer holder.Close()

	outOfRange := tcptest.PickPort()
	port := newBindServer(t, &socks.ServerConfig{
		BindEnabled:   true,
		BindTimeout:   30,
		BindPortRange: &net.PortRange{From: uint32(occupied.Value()), To: uint32(occupied.Value())},
	})

	// Port 0 with a fully occupied range -> general failure.
	conn := dialInbound(t, port)
	defer conn.Close()
	socks5Bind(t, conn, 0)
	if rep, _, _ := readSocks5Reply(t, conn, "exhausted reply"); rep != 0x01 {
		t.Fatal("expected general failure (0x01) for exhausted range, got ", rep)
	}
	conn.Close()

	// Explicit port outside the range -> not allowed by ruleset.
	conn = dialInbound(t, port)
	defer conn.Close()
	socks5Bind(t, conn, outOfRange.Value())
	if rep, _, _ := readSocks5Reply(t, conn, "out-of-range reply"); rep != 0x02 {
		t.Fatal("expected not allowed (0x02) for out-of-range port, got ", rep)
	}
	conn.Close()

	// Explicit port inside the range but already taken -> general failure.
	conn = dialInbound(t, port)
	defer conn.Close()
	socks5Bind(t, conn, occupied.Value())
	if rep, _, _ := readSocks5Reply(t, conn, "occupied reply"); rep != 0x01 {
		t.Fatal("expected general failure (0x01) for occupied port, got ", rep)
	}
}

func TestSocks5BindConcurrent(t *testing.T) {
	base := tcptest.PickPort()
	const sessions = 4
	const span = 20

	port := newBindServer(t, &socks.ServerConfig{
		BindEnabled:   true,
		BindTimeout:   30,
		BindPortRange: &net.PortRange{From: uint32(base.Value()), To: uint32(base.Value()) + span - 1},
	})

	type session struct {
		conn     gonet.Conn
		bindIP   net.IP
		bindPort int
		peer     *gonet.TCPConn
	}
	sessionsCh := make(chan session, sessions)
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := dialInbound(t, port)
			socks5Bind(t, conn, 0)
			rep, bindIP, bindPort := readSocks5Reply(t, conn, "first reply")
			if rep != 0x00 {
				t.Error("first reply rejected, code: ", rep)
				return
			}
			peer := dialBound(t, bindIP, bindPort)
			rep, _, _ = readSocks5Reply(t, conn, "second reply")
			if rep != 0x00 {
				t.Error("second reply rejected, code: ", rep)
				return
			}
			sessionsCh <- session{conn: conn, bindIP: bindIP, bindPort: bindPort, peer: peer}
		}()
	}
	wg.Wait()
	close(sessionsCh)

	seen := make(map[int]bool)
	for s := range sessionsCh {
		if seen[s.bindPort] {
			t.Fatal("two BIND sessions share bound port ", s.bindPort)
		}
		seen[s.bindPort] = true
		if s.bindPort < int(base.Value()) || s.bindPort > int(base.Value())+span-1 {
			t.Fatalf("bound port %d outside configured range", s.bindPort)
		}

		// Relay works for each session.
		if _, err := s.peer.Write([]byte("a")); err != nil {
			t.Fatal(err)
		}
		expectString(t, s.conn, "a", "peer -> client")
		s.peer.Close()
		s.conn.Close()
	}
	if len(seen) != sessions {
		t.Fatalf("expected %d bound sessions, got %d", sessions, len(seen))
	}
}

func TestSocksBindDisabledByDefault(t *testing.T) {
	port := newBindServer(t, &socks.ServerConfig{})

	// SOCKS5 keeps rejecting BIND with command not supported.
	conn := dialInbound(t, port)
	socks5Bind(t, conn, 0)
	if rep, _, _ := readSocks5Reply(t, conn, "socks5 bind reply"); rep != 0x07 {
		t.Fatal("expected command not supported (0x07), got ", rep)
	}
	conn.Close()

	// SOCKS4 keeps rejecting BIND with result code 91.
	conn = dialInbound(t, port)
	socks4Bind(t, conn, 0)
	if code, _, _ := readSocks4Reply(t, conn, "socks4 bind reply"); code != 91 {
		t.Fatal("expected socks4 rejected (91), got ", code)
	}
	conn.Close()
}
