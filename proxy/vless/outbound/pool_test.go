package outbound

import (
	"context"
	stderrors "errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/transport/internet/stat"
)

type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

type fakeDialer struct {
	mu        sync.Mutex
	dialFn    func(ctx context.Context) (stat.Connection, error)
	attempts  []time.Time
	deadlines []time.Duration
	peers     []net.Conn
}

func (d *fakeDialer) Dial(ctx context.Context, _ xnet.Destination) (stat.Connection, error) {
	d.mu.Lock()
	d.attempts = append(d.attempts, time.Now())
	if dl, ok := ctx.Deadline(); ok {
		d.deadlines = append(d.deadlines, time.Until(dl))
	} else {
		d.deadlines = append(d.deadlines, 0)
	}
	fn := d.dialFn
	d.mu.Unlock()
	return fn(ctx)
}

func (d *fakeDialer) DestIpAddress() xnet.IP { return nil }

func (d *fakeDialer) SetOutboundGateway(context.Context, *session.Outbound) {}

func (d *fakeDialer) snapshot() (int, []time.Time, []time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	attempts := make([]time.Time, len(d.attempts))
	copy(attempts, d.attempts)
	deadlines := make([]time.Duration, len(d.deadlines))
	copy(deadlines, d.deadlines)
	return len(attempts), attempts, deadlines
}

func (d *fakeDialer) closePeers() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.peers {
		c.Close()
	}
	d.peers = nil
}

func newTestPool(t *testing.T, cfg poolConfig, fn func(ctx context.Context) (stat.Connection, error)) (*connPool, *fakeDialer) {
	t.Helper()
	d := &fakeDialer{dialFn: fn}
	if fn == nil {
		d.dialFn = func(context.Context) (stat.Connection, error) {
			a, b := net.Pipe()
			d.mu.Lock()
			d.peers = append(d.peers, b)
			d.mu.Unlock()
			return &trackedConn{Conn: a}, nil
		}
	}
	p := newConnPool(cfg)
	if err := p.start(d, xnet.TCPDestination(xnet.LocalHostIP, 443)); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		p.Close()
		d.closePeers()
	})
	return p, d
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func (p *connPool) idleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

func testConfig() poolConfig {
	return poolConfig{
		minIdle:          2,
		maxIdle:          4,
		lifetime:         10 * time.Second,
		lifetimeJitter:   0,
		dialTimeout:      time.Second,
		retryInterval:    5 * time.Millisecond,
		retryMaxInterval: time.Second,
		closeGrace:       time.Second,
	}
}

func TestPoolFillsToMinIdleAndRefills(t *testing.T) {
	cfg := testConfig()
	p, d := newTestPool(t, cfg, nil)

	waitFor(t, time.Second, func() bool { return p.idleCount() == 2 })
	n, _, _ := d.snapshot()
	if n != 2 {
		t.Fatalf("expected 2 initial dials, got %d", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range 2 {
		c, err := p.Get(ctx)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		c.Close()
	}

	waitFor(t, time.Second, func() bool {
		n, _, _ := d.snapshot()
		return p.idleCount() == 2 && n == 4
	})

	// The producer must stop once idle reaches the lower bound: no extra dials.
	time.Sleep(50 * time.Millisecond)
	n, _, _ = d.snapshot()
	if n != 4 {
		t.Fatalf("expected no dials beyond min idle, got %d", n)
	}
}

func TestPoolExpiredEntryIsClosedAndRefilled(t *testing.T) {
	cfg := testConfig()
	cfg.minIdle = 1
	cfg.maxIdle = 1
	cfg.lifetime = 40 * time.Millisecond
	p, _ := newTestPool(t, cfg, nil)

	waitFor(t, time.Second, func() bool { return p.idleCount() == 1 })
	p.mu.Lock()
	original := p.idle[0].conn.(*trackedConn)
	p.mu.Unlock()

	time.Sleep(60 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer c.Close()
	if c == stat.Connection(original) {
		t.Fatal("expected an expired connection to be replaced")
	}
	if !original.closed.Load() {
		t.Fatal("expected expired connection to be closed immediately")
	}
}

func TestPoolFailureBackoff(t *testing.T) {
	cfg := testConfig()
	cfg.minIdle = 1
	cfg.maxIdle = 1
	cfg.retryInterval = 20 * time.Millisecond
	cfg.retryMaxInterval = 80 * time.Millisecond

	d := &fakeDialer{dialFn: func(context.Context) (stat.Connection, error) {
		return nil, stderrors.New("dial failed")
	}}
	p := newConnPool(cfg)
	if err := p.start(d, xnet.TCPDestination(xnet.LocalHostIP, 443)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer p.Close()

	waitFor(t, time.Second, func() bool {
		n, _, _ := d.snapshot()
		return n >= 6
	})
	_, attempts, _ := d.snapshot()

	expected := []time.Duration{20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond, 80 * time.Millisecond}
	for i, want := range expected {
		gap := attempts[i+1].Sub(attempts[i])
		if gap < want-15*time.Millisecond {
			t.Fatalf("attempt gap %d = %v, want >= %v (exponential backoff)", i, gap, want)
		}
		if gap > want+80*time.Millisecond {
			t.Fatalf("attempt gap %d = %v, want around %v", i, gap, want)
		}
	}
}

func TestPoolBackoffRecoversAfterSuccess(t *testing.T) {
	cfg := testConfig()
	cfg.minIdle = 1
	cfg.maxIdle = 1
	cfg.retryInterval = 5 * time.Millisecond
	cfg.retryMaxInterval = 100 * time.Millisecond

	var attempts atomic.Int32
	var d *fakeDialer
	d = &fakeDialer{dialFn: func(context.Context) (stat.Connection, error) {
		if attempts.Add(1) <= 3 {
			return nil, stderrors.New("dial failed")
		}
		a, b := net.Pipe()
		d.mu.Lock()
		d.peers = append(d.peers, b)
		d.mu.Unlock()
		return &trackedConn{Conn: a}, nil
	}}
	p := newConnPool(cfg)
	if err := p.start(d, xnet.TCPDestination(xnet.LocalHostIP, 443)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer p.Close()
	defer d.closePeers()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("expected Get to recover after transient dial failures, got %v", err)
	}
	c.Close()
}

func TestPoolDialTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.minIdle = 1
	cfg.maxIdle = 1
	cfg.dialTimeout = 100 * time.Millisecond
	cfg.retryInterval = 5 * time.Millisecond
	cfg.retryMaxInterval = 500 * time.Millisecond
	cfg.closeGrace = 20 * time.Millisecond

	d := &fakeDialer{dialFn: func(ctx context.Context) (stat.Connection, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	p := newConnPool(cfg)
	if err := p.start(d, xnet.TCPDestination(xnet.LocalHostIP, 443)); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		n, _, _ := d.snapshot()
		return n >= 2
	})
	p.Close()

	_, _, deadlines := d.snapshot()
	for _, dl := range deadlines {
		if dl < 70*time.Millisecond || dl > 130*time.Millisecond {
			t.Fatalf("dial deadline %v, want around 100ms", dl)
		}
	}
}

func TestPoolLifetimeJitterBounds(t *testing.T) {
	cfg := testConfig()
	cfg.lifetime = time.Second
	cfg.lifetimeJitter = 100 * time.Millisecond
	p := newConnPool(cfg)
	now := time.Now()

	delays := make(map[time.Duration]struct{})
	for range 500 {
		ttl := p.newExpire(now).Sub(now)
		if ttl < time.Second || ttl >= 1100*time.Millisecond {
			t.Fatalf("jittered TTL %v outside [1s, 1.1s)", ttl)
		}
		delays[ttl] = struct{}{}
	}
	if len(delays) < 5 {
		t.Fatalf("expected jittered TTLs to spread, got %d distinct values", len(delays))
	}

	cfg.lifetimeJitter = 0
	p2 := newConnPool(cfg)
	if ttl := p2.newExpire(now).Sub(now); ttl != time.Second {
		t.Fatalf("TTL with zero jitter = %v, want exactly 1s", ttl)
	}
}

func TestPoolConcurrentClose(t *testing.T) {
	cfg := testConfig()
	cfg.minIdle = 3
	cfg.maxIdle = 8
	p, _ := newTestPool(t, cfg, nil)

	waitFor(t, time.Second, func() bool { return p.idleCount() == 3 })

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				c, err := p.Get(ctx)
				cancel()
				if err == nil {
					c.Close()
				}
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Close() // must be idempotent and never panic
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx); err != errPoolClosed {
		t.Fatalf("Get after close = %v, want errPoolClosed", err)
	}
}

func TestPoolCloseDrainsCachedConnections(t *testing.T) {
	cfg := testConfig()
	cfg.minIdle = 2
	cfg.maxIdle = 2
	cfg.lifetime = time.Minute
	p, _ := newTestPool(t, cfg, nil)

	waitFor(t, time.Second, func() bool { return p.idleCount() == 2 })
	p.mu.Lock()
	cached := make([]*trackedConn, len(p.idle))
	for i, e := range p.idle {
		cached[i] = e.conn.(*trackedConn)
	}
	p.mu.Unlock()

	p.Close()

	for _, c := range cached {
		if !c.closed.Load() {
			t.Fatal("Close must close cached idle connections")
		}
	}
	if n := p.idleCount(); n != 0 {
		t.Fatalf("idle after close = %d, want 0", n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx); err != errPoolClosed {
		t.Fatalf("Get after close = %v, want errPoolClosed", err)
	}
}

func TestPoolCloseBeforeStart(t *testing.T) {
	cfg := testConfig()
	p := newConnPool(cfg)
	p.Close()

	d := &fakeDialer{dialFn: func(context.Context) (stat.Connection, error) {
		t.Fatal("dialer must never run when pool was closed first")
		return nil, nil
	}}
	if err := p.start(d, xnet.TCPDestination(xnet.LocalHostIP, 443)); err != errPoolClosed {
		t.Fatalf("start after close = %v, want errPoolClosed", err)
	}
}

func TestPoolConcurrentStartAndClose(t *testing.T) {
	for range 20 {
		cfg := testConfig()
		cfg.minIdle = 1
		cfg.maxIdle = 1
		cfg.closeGrace = 50 * time.Millisecond
		p, d := newTestPool(t, cfg, nil)

		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = p.start(d, xnet.TCPDestination(xnet.LocalHostIP, 443))
			}()
		}
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.Close()
			}()
		}
		wg.Wait()
	}
}

func TestPoolCloseRespectsGraceDuringBlockingDial(t *testing.T) {
	cfg := testConfig()
	cfg.minIdle = 1
	cfg.maxIdle = 1
	cfg.dialTimeout = 2 * time.Second
	cfg.closeGrace = 50 * time.Millisecond

	d := &fakeDialer{dialFn: func(ctx context.Context) (stat.Connection, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	p := newConnPool(cfg)
	if err := p.start(d, xnet.TCPDestination(xnet.LocalHostIP, 443)); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		n, _, _ := d.snapshot()
		return n >= 1
	})

	start := time.Now()
	p.Close()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Close blocked for %v on an in-flight dial, grace is 50ms", elapsed)
	}

	// Producer must not retry after shutdown once the in-flight dial returns.
	time.Sleep(2100 * time.Millisecond)
	n, _, _ := d.snapshot()
	if n != 1 {
		t.Fatalf("expected producer to stop after close, saw %d dial attempts", n)
	}
}

func TestPoolConfigFromAccountLegacyTestpre(t *testing.T) {
	cfg, ok := poolConfigFromMemoryAccount(&vless.MemoryAccount{Testpre: 3})
	if !ok {
		t.Fatal("legacy numeric testpre must enable the pool")
	}
	if cfg.minIdle != 3 || cfg.maxIdle != 3 {
		t.Fatalf("legacy testpre must map to min=max=3, got min=%d max=%d", cfg.minIdle, cfg.maxIdle)
	}
	if cfg.lifetime != 2*time.Minute || cfg.lifetimeJitter != 0 {
		t.Fatalf("legacy mapping must keep the fixed 2 minute expiry, got %v (+%v)", cfg.lifetime, cfg.lifetimeJitter)
	}
	if cfg.retryInterval != 200*time.Millisecond || cfg.retryMaxInterval != 200*time.Millisecond {
		t.Fatalf("legacy mapping must use the 200ms pacing, got %v/%v", cfg.retryInterval, cfg.retryMaxInterval)
	}
	if cfg.dialTimeout != 10*time.Second || cfg.closeGrace != 5*time.Second {
		t.Fatalf("legacy mapping must fill dial timeout and close grace defaults, got %v/%v", cfg.dialTimeout, cfg.closeGrace)
	}
}

func TestPoolConfigDisabledWhenZeroOrUnset(t *testing.T) {
	if _, ok := poolConfigFromMemoryAccount(&vless.MemoryAccount{}); ok {
		t.Fatal("unset testpre/pool must disable pre-connect")
	}
	if _, ok := poolConfigFromMemoryAccount(&vless.MemoryAccount{Pool: &vless.PoolConfig{}}); ok {
		t.Fatal("pool config without idle limits must disable pre-connect")
	}
}

func TestPoolConfigFromExplicitPool(t *testing.T) {
	account := &vless.MemoryAccount{
		Testpre: 9, // explicit pool must take precedence
		Pool: &vless.PoolConfig{
			MinIdle:            2,
			MaxIdle:            5,
			LifetimeMs:         30000,
			LifetimeJitterMs:   5000,
			DialTimeoutMs:      2000,
			RetryIntervalMs:    100,
			RetryMaxIntervalMs: 1000,
			CloseGraceMs:       3000,
		},
	}
	cfg, ok := poolConfigFromMemoryAccount(account)
	if !ok {
		t.Fatal("explicit pool config must enable the pool")
	}
	if cfg.minIdle != 2 || cfg.maxIdle != 5 {
		t.Fatalf("unexpected idle limits %d/%d", cfg.minIdle, cfg.maxIdle)
	}
	if cfg.lifetime != 30*time.Second || cfg.lifetimeJitter != 5*time.Second ||
		cfg.dialTimeout != 2*time.Second || cfg.retryInterval != 100*time.Millisecond ||
		cfg.retryMaxInterval != time.Second || cfg.closeGrace != 3*time.Second {
		t.Fatalf("pool parameters not mapped correctly: %+v", cfg)
	}
}

func TestPoolConfigNormalization(t *testing.T) {
	cfg, ok := poolConfigFromMemoryAccount(&vless.MemoryAccount{Pool: &vless.PoolConfig{MinIdle: 5, MaxIdle: 2}})
	if !ok || cfg.minIdle != 2 || cfg.maxIdle != 2 {
		t.Fatalf("min idle above max idle should be clamped, got %+v ok=%v", cfg, ok)
	}

	cfg, ok = poolConfigFromMemoryAccount(&vless.MemoryAccount{Pool: &vless.PoolConfig{MaxIdle: 3}})
	if !ok || cfg.minIdle != 1 || cfg.maxIdle != 3 {
		t.Fatalf("missing min idle should default to 1, got %+v", cfg)
	}

	cfg, _ = poolConfigFromMemoryAccount(&vless.MemoryAccount{Pool: &vless.PoolConfig{MaxIdle: 2}})
	if cfg.lifetime != 2*time.Minute || cfg.lifetimeJitter != 30*time.Second ||
		cfg.dialTimeout != 10*time.Second || cfg.retryInterval != 200*time.Millisecond ||
		cfg.retryMaxInterval != 30*time.Second || cfg.closeGrace != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}
