//go:build unix

package rpcnet

import (
	"errors"
	"net"
	"syscall"
	"time"

	fomoxa "github.com/fomoxa/go"
)

type SocketTransport struct {
	conn    *net.TCPConn
	raw     syscall.RawConn
	backlog []byte
	refused bool
	closed  bool
	err     error
	remote  net.Addr
}

func NewSocketTransport(conn *net.TCPConn) (*SocketTransport, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	_ = conn.SetNoDelay(true)
	return &SocketTransport{conn: conn, raw: raw, remote: conn.RemoteAddr()}, nil
}

func Dial(address string, timeout time.Duration) (*SocketTransport, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("rpcnet: the dialled connection is not TCP")
	}
	transport, err := NewSocketTransport(tcp)
	if err != nil {
		_ = tcp.Close()
		return nil, err
	}
	return transport, nil
}

func (t *SocketTransport) Kind() fomoxa.Kind { return fomoxa.KindStream }

func (t *SocketTransport) LastError() error { return t.err }

func (t *SocketTransport) RemoteAddr() net.Addr { return t.remote }

func (t *SocketTransport) Backlogged() bool { return len(t.backlog) > 0 || t.refused }

func (t *SocketTransport) Send(frame []byte) fomoxa.Status {
	if t.closed {
		return fomoxa.StatusClosed
	}
	if len(t.backlog) > 0 {
		if status := t.pushBacklog(); status != fomoxa.StatusOK {
			return status
		}
		if len(t.backlog) > 0 {
			t.refused = true
			return fomoxa.StatusPending
		}
	}
	written, err := t.write(frame)
	if written == len(frame) {
		t.refused = false
		return fomoxa.StatusOK
	}
	if err != nil && !wouldBlock(err) {
		return t.fail(err)
	}
	if written <= 0 {
		t.refused = true
		return fomoxa.StatusPending
	}
	t.refused = false
	t.backlog = append(t.backlog, frame[written:]...)
	return fomoxa.StatusOK
}

func (t *SocketTransport) Receive(buf []byte) (int, int, fomoxa.Status) {
	if t.closed {
		return 0, 0, fomoxa.StatusClosed
	}
	if len(t.backlog) > 0 {
		if status := t.pushBacklog(); status != fomoxa.StatusOK {
			return 0, 0, status
		}
	}
	read, err := t.read(buf)
	if err != nil {
		if wouldBlock(err) {
			return 0, 0, fomoxa.StatusPending
		}
		return 0, 0, t.fail(err)
	}
	if read == 0 {
		t.closed = true
		return 0, 0, fomoxa.StatusClosed
	}
	return read, 0, fomoxa.StatusOK
}

func (t *SocketTransport) CloseSend() {
	if t.closed {
		return
	}
	t.pushBacklog()
	_ = t.conn.CloseWrite()
}

func (t *SocketTransport) Close() {
	t.closed = true
	_ = t.conn.Close()
}

func (t *SocketTransport) WaitReady(writable bool, timeout time.Duration) (bool, error) {
	if t.closed {
		return true, nil
	}
	return waitReady(t.raw, writable, timeout)
}

func (t *SocketTransport) pushBacklog() fomoxa.Status {
	for len(t.backlog) > 0 {
		written, err := t.write(t.backlog)
		if written > 0 {
			t.backlog = append(t.backlog[:0], t.backlog[written:]...)
		}
		if err != nil {
			if wouldBlock(err) {
				return fomoxa.StatusOK
			}
			return t.fail(err)
		}
		if written == 0 {
			return fomoxa.StatusOK
		}
	}
	return fomoxa.StatusOK
}

func (t *SocketTransport) fail(err error) fomoxa.Status {
	t.closed = true
	t.err = err
	return fomoxa.StatusError
}

func (t *SocketTransport) read(buf []byte) (int, error) {
	var read int
	var err error
	if control := t.raw.Read(func(fd uintptr) bool {
		read, err = syscall.Read(int(fd), buf)
		return true
	}); control != nil {
		return 0, control
	}
	return max(read, 0), err
}

func (t *SocketTransport) write(frame []byte) (int, error) {
	var written int
	var err error
	if control := t.raw.Write(func(fd uintptr) bool {
		written, err = syscall.Write(int(fd), frame)
		return true
	}); control != nil {
		return 0, control
	}
	return max(written, 0), err
}

func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EINTR)
}
