package outbound

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	xctx "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

const (
	defaultPoolLifetime        = 2 * time.Minute
	defaultPoolLifetimeJitter  = 30 * time.Second
	defaultPoolDialTimeout     = 10 * time.Second
	defaultPoolRetryInterval   = 200 * time.Millisecond
	defaultPoolRetryMaxBackoff = 30 * time.Second
	defaultPoolCloseGrace      = 5 * time.Second

	// Legacy numeric testpre used a fixed 2 minute expiry and paced (re)connects by 200ms.
	legacyPoolLifetime      = 2 * time.Minute
	legacyPoolRetryInterval = 200 * time.Millisecond
)

// poolConfig is the normalized pre-connection pool configuration.
type poolConfig struct {
	minIdle          int
	maxIdle          int
	lifetime         time.Duration
	lifetimeJitter   time.Duration
	dialTimeout      time.Duration
	retryInterval    time.Duration
	retryMaxInterval time.Duration
	closeGrace       time.Duration
}

// poolConfigFromMemoryAccount converts the account level configuration into a
// normalized pool configuration. The explicit Pool config takes precedence over
// the legacy numeric Testpre field. A zero/unset value disables the pool.
func poolConfigFromMemoryAccount(a *vless.MemoryAccount) (poolConfig, bool) {
	if a.Pool != nil && (a.Pool.MaxIdle > 0 || a.Pool.MinIdle > 0) {
		cfg := poolConfig{
			minIdle:          int(a.Pool.MinIdle),
			maxIdle:          int(a.Pool.MaxIdle),
			lifetime:         time.Duration(a.Pool.LifetimeMs) * time.Millisecond,
			lifetimeJitter:   time.Duration(a.Pool.LifetimeJitterMs) * time.Millisecond,
			dialTimeout:      time.Duration(a.Pool.DialTimeoutMs) * time.Millisecond,
			retryInterval:    time.Duration(a.Pool.RetryIntervalMs) * time.Millisecond,
			retryMaxInterval: time.Duration(a.Pool.RetryMaxIntervalMs) * time.Millisecond,
			closeGrace:       time.Duration(a.Pool.CloseGraceMs) * time.Millisecond,
		}
		cfg = normalizePoolConfig(cfg)
		// Unset jitter gets a default spread so that pooled conns do not expire together.
		if cfg.lifetimeJitter == 0 {
			cfg.lifetimeJitter = defaultPoolLifetimeJitter
		}
		return cfg, true
	}
	if a.Testpre > 0 {
		n := int(a.Testpre)
		cfg := poolConfig{
			minIdle:          n,
			maxIdle:          n,
			lifetime:         legacyPoolLifetime,
			lifetimeJitter:   0,
			dialTimeout:      defaultPoolDialTimeout,
			retryInterval:    legacyPoolRetryInterval,
			retryMaxInterval: legacyPoolRetryInterval,
			closeGrace:       defaultPoolCloseGrace,
		}
		return cfg, true
	}
	return poolConfig{}, false
}

func normalizePoolConfig(cfg poolConfig) poolConfig {
	if cfg.minIdle <= 0 {
		cfg.minIdle = 1
	}
	if cfg.maxIdle <= 0 {
		cfg.maxIdle = cfg.minIdle
	}
	if cfg.minIdle > cfg.maxIdle {
		cfg.minIdle = cfg.maxIdle
	}
	if cfg.lifetime <= 0 {
		cfg.lifetime = defaultPoolLifetime
	}
	if cfg.lifetimeJitter < 0 {
		cfg.lifetimeJitter = 0
	}
	if cfg.dialTimeout <= 0 {
		cfg.dialTimeout = defaultPoolDialTimeout
	}
	if cfg.retryInterval <= 0 {
		cfg.retryInterval = defaultPoolRetryInterval
	}
	if cfg.retryMaxInterval < cfg.retryInterval {
		cfg.retryMaxInterval = defaultPoolRetryMaxBackoff
		if cfg.retryMaxInterval < cfg.retryInterval {
			cfg.retryMaxInterval = cfg.retryInterval
		}
	}
	if cfg.closeGrace <= 0 {
		cfg.closeGrace = defaultPoolCloseGrace
	}
	return cfg
}

type pooledConn struct {
	conn   stat.Connection
	expire time.Time
}

// connPool keeps a configurable number of warm, unexpired pre-connections.
//
// A single producer tops the idle slice up to minIdle and stops once the count
// is sufficient or the pool is closed. Consumers only take unexpired entries;
// expired entries are closed immediately and trigger a refill. Close stops the
// producer before closing cached connections, and entries are handed over under
// a mutex, so a producer can never deliver a connection after shutdown.
type connPool struct {
	cfg    poolConfig
	dialer internet.Dialer
	dest   net.Destination

	mu      sync.Mutex
	idle    []*pooledConn
	cond    *sync.Cond
	started bool
	closed  bool

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	now func() time.Time
}

func newConnPool(cfg poolConfig) *connPool {
	p := &connPool{
		cfg:  cfg,
		done: make(chan struct{}),
		now:  time.Now,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

var errPoolClosed = errors.New("preconnect pool closed")

// start binds the pool to the dialer of the first Process call and launches the
// producer. It is safe to call concurrently; it fails once the pool is closed.
func (p *connPool) start(dialer internet.Dialer, dest net.Destination) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errPoolClosed
	}
	if p.started {
		return nil
	}
	p.dialer = dialer
	p.dest = dest
	p.wg.Add(1)
	p.started = true
	go p.produce()
	return nil
}

// Get returns a fresh, unexpired connection, waiting until one is available,
// ctx is canceled or the pool is closed.
func (p *connPool) Get(ctx context.Context) (stat.Connection, error) {
	wakeDone := make(chan struct{})
	var wakeOnce sync.Once
	stopWake := func() { wakeOnce.Do(func() { close(wakeDone) }) }
	go func() {
		select {
		case <-ctx.Done():
			p.cond.Broadcast()
		case <-wakeDone:
		}
	}()
	defer stopWake()

	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		for len(p.idle) > 0 {
			entry := p.idle[0]
			p.idle[0] = nil
			p.idle = p.idle[1:]
			if p.now().Before(entry.expire) {
				if len(p.idle) < p.cfg.minIdle {
					p.cond.Broadcast()
				}
				return entry.conn, nil
			}
			needFill := len(p.idle) < p.cfg.minIdle
			p.mu.Unlock()
			entry.conn.Close()
			if needFill {
				p.cond.Broadcast()
			}
			p.mu.Lock()
		}
		if p.closed {
			return nil, errPoolClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.cond.Wait()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// Close stops the producer, waits up to closeGrace for it, and closes all cached
// connections. It is safe to call multiple times and before start.
func (p *connPool) Close() error {
	p.closeOnce.Do(func() { close(p.done) })

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	started := p.started
	idle := p.idle
	p.idle = nil
	p.cond.Broadcast()
	p.mu.Unlock()

	for _, entry := range idle {
		entry.conn.Close()
	}

	if started {
		exited := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(exited)
		}()
		select {
		case <-exited:
		case <-time.After(p.cfg.closeGrace):
		}
	}
	return nil
}

func (p *connPool) produce() {
	defer p.wg.Done()
	backoff := p.cfg.retryInterval
	for {
		p.mu.Lock()
		for len(p.idle) >= p.cfg.minIdle && !p.closed {
			p.cond.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		conn, err := p.dialOnce()
		if err != nil {
			errors.LogWarningInner(context.Background(), err, "pre-connect failed")
			if !p.waitBackoff(backoff) {
				return
			}
			backoff *= 2
			if backoff > p.cfg.retryMaxInterval || backoff <= 0 {
				backoff = p.cfg.retryMaxInterval
			}
			continue
		}
		backoff = p.cfg.retryInterval

		entry := &pooledConn{conn: conn, expire: p.newExpire(p.now())}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			conn.Close()
			return
		}
		if len(p.idle) >= p.cfg.maxIdle {
			p.mu.Unlock()
			conn.Close()
			continue
		}
		p.idle = append(p.idle, entry)
		p.cond.Broadcast()
		p.mu.Unlock()
	}
}

func (p *connPool) dialOnce() (stat.Connection, error) {
	ctx, cancel := context.WithTimeout(xctx.ContextWithID(context.Background(), session.NewID()), p.cfg.dialTimeout)
	defer cancel()
	return p.dialer.Dial(ctx, p.dest)
}

// waitBackoff sleeps for d, returning false when the pool is closed meanwhile.
func (p *connPool) waitBackoff(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-p.done:
		return false
	case <-t.C:
		return true
	}
}

// newExpire returns now+lifetime with a uniformly sampled jitter in
// [0, lifetimeJitter), i.e. the TTL stays within [lifetime, lifetime+jitter).
func (p *connPool) newExpire(now time.Time) time.Time {
	ttl := p.cfg.lifetime
	if p.cfg.lifetimeJitter > 0 {
		ttl += time.Duration(rand.Int64N(int64(p.cfg.lifetimeJitter)))
	}
	return now.Add(ttl)
}
