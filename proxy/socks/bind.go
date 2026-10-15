package socks

import (
	"context"
	"io"
	gonet "net"
	"strconv"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport/internet/stat"
	"math/rand/v2"
)

const defaultBindTimeout = 120 * time.Second

var errBindPortRangeExhausted = errors.New("bind port range exhausted")

// handleTCPBind implements the SOCKS BIND command (RFC 1928 for SOCKS5,
// SOCKS4/4a for SOCKS4). It opens a local listener, reports the real bound
// address in the first protocol reply, waits for the first inbound
// connection, reports its address in the second reply and then relays
// traffic bidirectionally between that connection and the original control
// connection of the client.
func (s *Server) handleTCPBind(ctx context.Context, conn stat.Connection, reader *buf.BufferedReader, svrSession *ServerSession, request *protocol.RequestHeader) error {
	inbound := session.InboundFromContext(ctx)

	listenIP, err := s.bindListenIP(svrSession)
	if err != nil {
		s.writeBindReply(conn, request, statusGeneralFailure, net.AnyIP, 0)
		recordBindRejected(inbound, request, err)
		return err
	}

	var allowedRange net.MemoryPortRange
	hasRange := false
	if r := s.config.BindPortRange; r != nil {
		if r.From == 0 || r.From > r.To || r.To > 65535 {
			err := errors.New("invalid BIND port range: ", r.From, "-", r.To)
			s.writeBindReply(conn, request, statusGeneralFailure, net.AnyIP, 0)
			recordBindRejected(inbound, request, err)
			return err
		}
		allowedRange = net.MemoryPortRange{From: net.Port(r.From), To: net.Port(r.To)}
		hasRange = true
	}

	// A specifically requested port must lie within the configured range.
	if request.Port != 0 && hasRange && !allowedRange.Contains(request.Port) {
		err := errors.New("requested BIND port ", request.Port, " is outside of the allowed port range ", allowedRange.From, "-", allowedRange.To)
		s.writeBindReply(conn, request, statusNotAllowed, net.AnyIP, 0)
		recordBindRejected(inbound, request, err)
		return err
	}

	listener, boundPort, err := s.openBindListener(listenIP, request.Port, hasRange, allowedRange)
	if err != nil {
		s.writeBindReply(conn, request, statusGeneralFailure, net.AnyIP, 0)
		recordBindRejected(inbound, request, err)
		return err
	}

	replyAddress := net.IPAddress(listenIP)
	if ta, ok := listener.Addr().(*gonet.TCPAddr); ok && ta.IP != nil {
		if ta.IP.IsUnspecified() {
			// The bound address is not connectable; report the address the
			// client itself reached the inbound on.
			replyAddress = svrSession.localAddress
		} else {
			replyAddress = net.IPAddress(ta.IP)
		}
	}

	if err := s.writeBindReply(conn, request, statusSuccess, replyAddress, boundPort); err != nil {
		_ = listener.Close()
		return errors.New("failed to send first BIND reply").Base(err)
	}

	errors.LogInfo(ctx, "TCP Bind listening on ", replyAddress, ":", boundPort, " waiting for inbound connection")

	waitTimeout := defaultBindTimeout
	if s.config.BindTimeout > 0 {
		waitTimeout = time.Duration(s.config.BindTimeout) * time.Second
	}

	type acceptResult struct {
		conn gonet.Conn
		err  error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		c, err := listener.Accept()
		acceptCh <- acceptResult{conn: c, err: err}
	}()

	// Watch the control connection while waiting. A BIND client must not
	// send data before the second reply; early bytes are discarded, while a
	// closed connection aborts the wait and releases the listener.
	stopWatch := make(chan struct{})
	var watchDone sync.WaitGroup
	watchDone.Add(1)
	go func() {
		defer watchDone.Done()
		for {
			mb, rerr := reader.ReadMultiBuffer()
			select {
			case <-stopWatch:
				return
			default:
			}
			if rerr != nil {
				_ = listener.Close()
				return
			}
			buf.ReleaseMulti(mb)
		}
	}()
	stopWatcher := func() {
		close(stopWatch)
		// Unblock a pending read on the control connection.
		_ = conn.SetReadDeadline(time.Now())
		watchDone.Wait()
		_ = conn.SetReadDeadline(time.Time{})
	}

	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	select {
	case result := <-acceptCh:
		if result.err != nil {
			_ = listener.Close()
			stopWatcher()
			// The client is likely already gone, so the second reply is best
			// effort and its failure must not mask the real error.
			_ = s.writeBindReply(conn, request, statusGeneralFailure, net.AnyIP, 0)
			err := errors.New("failed to accept BIND inbound connection").Base(result.err)
			recordBindRejected(inbound, request, err)
			return err
		}
		inboundConn := result.conn
		// The listener has served its purpose: only the first inbound
		// connection belongs to this session, release the port immediately.
		_ = listener.Close()
		stopWatcher()

		remote := inboundConn.RemoteAddr().(*gonet.TCPAddr)
		if err := s.writeBindReply(conn, request, statusSuccess, net.IPAddress(remote.IP), net.Port(remote.Port)); err != nil {
			_ = inboundConn.Close()
			return errors.New("failed to send second BIND reply").Base(err)
		}

		if inbound != nil && inbound.Source.IsValid() {
			ctx = log.ContextWithAccessMessage(ctx, &log.AccessMessage{
				From:   inbound.Source,
				To:     net.TCPDestination(net.IPAddress(remote.IP), net.Port(remote.Port)),
				Status: log.AccessAccepted,
			})
		}
		errors.LogInfo(ctx, "TCP Bind inbound connection from ", remote.IP, ":", remote.Port)

		return relayBindConnections(reader, conn, inboundConn)
	case <-timer.C:
		_ = listener.Close()
		result := <-acceptCh
		if result.conn != nil {
			_ = result.conn.Close()
		}
		stopWatcher()
		_ = s.writeBindReply(conn, request, statusGeneralFailure, net.AnyIP, 0)
		err := errors.New("timed out after ", waitTimeout, " waiting for BIND inbound connection")
		recordBindRejected(inbound, request, err)
		return err
	case <-ctx.Done():
		_ = listener.Close()
		result := <-acceptCh
		if result.conn != nil {
			_ = result.conn.Close()
		}
		stopWatcher()
		return errors.New("BIND session canceled").Base(ctx.Err())
	}
}

// openBindListener creates the TCP listener for a BIND session. A non-zero
// requested port is used as-is. With port 0 and a configured range, free
// ports within the range are tried (starting at a random offset) until one
// is available; an OS-assigned ephemeral port is used without a range.
//
// A plain net.Listen is used instead of internet.ListenSystem on purpose:
// the latter enables SO_REUSEPORT, which would let concurrent BIND sessions
// share a port and let the kernel deliver a peer connection to the wrong
// session. Standard listen calls fail with EADDRINUSE on an occupied port,
// providing the required mutual exclusion.
func (s *Server) openBindListener(listenIP gonet.IP, requested net.Port, hasRange bool, allowedRange net.MemoryPortRange) (gonet.Listener, net.Port, error) {
	listen := func(port int) (gonet.Listener, error) {
		return gonet.Listen("tcp", gonet.JoinHostPort(listenIP.String(), strconv.Itoa(port)))
	}

	if requested != 0 {
		l, err := listen(int(requested.Value()))
		if err != nil {
			return nil, 0, errors.New("failed to listen on requested BIND port ", requested).Base(err)
		}
		return l, portFromListener(l), nil
	}

	if hasRange {
		from := int(allowedRange.From.Value())
		to := int(allowedRange.To.Value())
		span := to - from + 1
		offset := 0
		if span > 1 {
			offset = rand.IntN(span)
		}
		for i := 0; i < span; i++ {
			port := from + (offset+i)%span
			l, err := listen(port)
			if err == nil {
				return l, portFromListener(l), nil
			}
		}
		return nil, 0, errBindPortRangeExhausted
	}

	l, err := listen(0)
	if err != nil {
		return nil, 0, errors.New("failed to create BIND listener").Base(err)
	}
	return l, portFromListener(l), nil
}

func portFromListener(listener gonet.Listener) net.Port {
	ta, ok := listener.Addr().(*gonet.TCPAddr)
	if !ok {
		return 0
	}
	return net.Port(ta.Port)
}

func (s *Server) bindListenIP(svrSession *ServerSession) (gonet.IP, error) {
	if s.config.BindAddress == nil {
		// Follow the inbound listen address by default.
		return svrSession.address.IP(), nil
	}
	addr := s.config.BindAddress.AsAddress()
	if addr == nil || addr.Family().IsDomain() {
		return nil, errors.New("BIND listen address must be an IP address")
	}
	return addr.IP(), nil
}

// writeBindReply sends one of the two BIND replies, mapping SOCKS5 reply
// codes to SOCKS4 result codes and coercing addresses to the 4-byte form
// required by SOCKS4 replies.
func (s *Server) writeBindReply(writer io.Writer, request *protocol.RequestHeader, code byte, address net.Address, port net.Port) error {
	if request.Version == socks4Version {
		result := byte(socks4RequestGranted)
		if code != statusSuccess {
			result = socks4RequestRejected
		}
		if ip := address.IP(); ip != nil && ip.To4() != nil {
			address = net.IPAddress(ip.To4())
		} else {
			address = net.AnyIP
		}
		return writeSocks4Response(writer, result, address, port)
	}
	return writeSocks5Response(writer, code, address, port)
}

func recordBindRejected(inbound *session.Inbound, request *protocol.RequestHeader, err error) {
	if inbound == nil || !inbound.Source.IsValid() {
		return
	}
	to := ""
	if request != nil && request.Port != 0 {
		to = net.TCPDestination(request.Address, request.Port).String()
	}
	log.Record(&log.AccessMessage{
		From:   inbound.Source,
		To:     to,
		Status: log.AccessRejected,
		Reason: err,
	})
}

// relayBindConnections copies traffic in both directions between the
// client control connection and the inbound connection. It returns once
// either direction terminates and the other is unblocked by closing both.
func relayBindConnections(clientReader *buf.BufferedReader, clientConn stat.Connection, inboundConn gonet.Conn) error {
	errCh := make(chan error, 2)
	var closeOnce sync.Once
	closeBoth := func() {
		_ = clientConn.Close()
		_ = inboundConn.Close()
	}

	go func() {
		err := buf.Copy(clientReader, buf.NewWriter(inboundConn))
		closeOnce.Do(closeBoth)
		errCh <- err
	}()
	go func() {
		err := buf.Copy(buf.NewReader(inboundConn), buf.NewWriter(clientConn))
		closeOnce.Do(closeBoth)
		errCh <- err
	}()

	<-errCh
	closeOnce.Do(closeBoth)
	return <-errCh
}
