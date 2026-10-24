package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/pipe"
)

// snifferContext builds a context carrying a minimal Xray instance, which
// NewSniffer requires (FakeDNS feature lookup).
func snifferContext(t *testing.T) context.Context {
	t.Helper()
	instance, err := core.New(&core.Config{})
	if err != nil {
		t.Fatalf("failed to create Xray instance: %v", err)
	}
	return context.WithValue(context.Background(), core.XrayKey(1), instance)
}

func newSnifferPipe(t *testing.T) (*cachedReader, *pipe.Writer) {
	t.Helper()
	reader, writer := pipe.New(pipe.WithoutSizeLimit())
	return &cachedReader{reader: reader}, writer
}

func writeToPipe(t *testing.T, w *pipe.Writer, b []byte) {
	t.Helper()
	if err := w.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(b)}); err != nil {
		t.Error(err)
	}
}

// With no inbound override, the built-in defaults (200ms total window, 2
// attempts) must be preserved.
func TestSnifferDefaultTimeout(t *testing.T) {
	cReader, _ := newSnifferPipe(t)
	start := time.Now()
	_, err := sniffer(snifferContext(t), cReader, session.SniffingRequest{}, net.Network_TCP)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected sniffing to fail when client never sends data")
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("default sniffing window shrunk: %v", elapsed)
	}
	if elapsed > 600*time.Millisecond {
		t.Fatalf("default sniffing window stretched: %v", elapsed)
	}
}

// The configured per-inbound window must actually change the wait time, and
// the window covers the whole sniffing session rather than resetting per
// round.
func TestSnifferCustomTimeout(t *testing.T) {
	cReader, _ := newSnifferPipe(t)
	req := session.SniffingRequest{SniffingTimeout: 80 * time.Millisecond}
	start := time.Now()
	_, err := sniffer(snifferContext(t), cReader, req, net.Network_TCP)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected sniffing to fail when client never sends data")
	}
	if elapsed < 50*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("custom 80ms window not honored, elapsed: %v", elapsed)
	}

	// A larger window must wait noticeably longer than the short one.
	cReader2, _ := newSnifferPipe(t)
	req2 := session.SniffingRequest{SniffingTimeout: 260 * time.Millisecond}
	start = time.Now()
	_, _ = sniffer(snifferContext(t), cReader2, req2, net.Network_TCP)
	elapsed2 := time.Since(start)
	if elapsed2-elapsed < 100*time.Millisecond {
		t.Fatalf("longer window did not extend the wait: short=%v long=%v", elapsed, elapsed2)
	}
}

// A protocol first packet arriving inside the configured window must be
// sniffed successfully, even though it comes well after connection start.
func TestSnifferDataWithinWindow(t *testing.T) {
	cReader, writer := newSnifferPipe(t)
	go func() {
		time.Sleep(120 * time.Millisecond)
		writeToPipe(t, writer, []byte("GET / HTTP/1.1\r\nHost: example.com\r\nUser-Agent: xray-test\r\n\r\n"))
	}()

	req := session.SniffingRequest{SniffingTimeout: 2 * time.Second, MaxAttempts: 2}
	start := time.Now()
	result, err := sniffer(snifferContext(t), cReader, req, net.Network_TCP)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected HTTP request to be sniffed: %v", err)
	}
	if result.Domain() != "example.com" {
		t.Fatalf("unexpected sniffed domain: %q", result.Domain())
	}
	if result.Protocol() != "http1" {
		t.Fatalf("unexpected sniffed protocol: %q", result.Protocol())
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("sniffer returned before the packet arrived: %v", elapsed)
	}
}

// Once the configured window is exhausted, the existing timeout branch must
// still be taken. The same applies when the configured attempt count is
// reached before the window ends.
func TestSnifferWindowAndAttemptsExhausted(t *testing.T) {
	// Window exhausted: the reader returns buf.ErrReadTimeout at the deadline.
	cReader, _ := newSnifferPipe(t)
	req := session.SniffingRequest{SniffingTimeout: 60 * time.Millisecond}
	start := time.Now()
	_, err := sniffer(snifferContext(t), cReader, req, net.Network_TCP)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected the timeout branch when no packet arrives")
	}
	if elapsed < 40*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("timeout did not track the 60ms window: %v", elapsed)
	}

	// Attempts exhausted: three inconclusive blobs with maxAttempts=3 end the
	// sniffing long before the generous 2s window, proving attempts (not the
	// window) stopped it.
	cReader2, writer2 := newSnifferPipe(t)
	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(60 * time.Millisecond)
			writeToPipe(t, writer2, []byte{0x00})
		}
	}()
	req2 := session.SniffingRequest{SniffingTimeout: 2 * time.Second, MaxAttempts: 3}
	start = time.Now()
	_, err = sniffer(snifferContext(t), cReader2, req2, net.Network_TCP)
	elapsed = time.Since(start)
	if err != errSniffingTimeout {
		t.Fatalf("expected errSniffingTimeout after 3 attempts, got: %v", err)
	}
	if elapsed < 90*time.Millisecond {
		t.Fatalf("sniffer stopped before consuming 3 attempts: %v", elapsed)
	}
	if elapsed > 900*time.Millisecond {
		t.Fatalf("sniffer ran into the time window instead of stopping at attempt limit: %v", elapsed)
	}

	// maxAttempts=1 ends right after the first inconclusive read.
	cReader3, writer3 := newSnifferPipe(t)
	writeToPipe(t, writer3, []byte{0x00})
	req3 := session.SniffingRequest{SniffingTimeout: 2 * time.Second, MaxAttempts: 1}
	start = time.Now()
	_, err = sniffer(snifferContext(t), cReader3, req3, net.Network_TCP)
	if err != errSniffingTimeout {
		t.Fatalf("expected errSniffingTimeout after 1 attempt, got: %v", err)
	}
	if elapsed = time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("maxAttempts=1 not honored, elapsed: %v", elapsed)
	}
}

// metadataOnly must return without reading (and therefore without waiting
// for) any client input.
func TestSnifferMetadataOnlyZeroWait(t *testing.T) {
	cReader, _ := newSnifferPipe(t) // writer is never fed
	req := session.SniffingRequest{
		MetadataOnly:    true,
		SniffingTimeout: 10 * time.Second,
		MaxAttempts:     99,
	}
	start := time.Now()
	_, err := sniffer(snifferContext(t), cReader, req, net.Network_TCP)
	elapsed := time.Since(start)
	if err != common.ErrNoClue {
		t.Fatalf("metadataOnly should return the metadata sniff result/error, got: %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("metadataOnly waited for client input: %v", elapsed)
	}
}

// Concurrent inbounds with different settings must not affect each other's
// waiting behavior.
func TestSnifferPerInboundIsolation(t *testing.T) {
	const n = 3
	type outcome struct {
		elapsed time.Duration
	}
	shortCh := make(chan outcome, n)
	longCh := make(chan outcome, n)

	run := func(timeout time.Duration, ch chan<- outcome) {
		cReader, _ := newSnifferPipe(t)
		req := session.SniffingRequest{SniffingTimeout: timeout}
		start := time.Now()
		_, _ = sniffer(snifferContext(t), cReader, req, net.Network_TCP)
		ch <- outcome{time.Since(start)}
	}

	for i := 0; i < n; i++ {
		go run(50*time.Millisecond, shortCh)
		go run(300*time.Millisecond, longCh)
	}

	for i := 0; i < n; i++ {
		s := <-shortCh
		if s.elapsed > 200*time.Millisecond {
			t.Fatalf("short-window inbound was delayed by a concurrent inbound: %v", s.elapsed)
		}
		l := <-longCh
		if l.elapsed < 200*time.Millisecond {
			t.Fatalf("long-window inbound was cut short by a concurrent inbound: %v", l.elapsed)
		}
	}
}
