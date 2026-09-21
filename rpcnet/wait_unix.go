//go:build unix

package rpcnet

import (
	"errors"
	"math"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func waitReady(raw syscall.RawConn, writable bool, timeout time.Duration) (bool, error) {
	events := int16(unix.POLLIN)
	if writable {
		events |= unix.POLLOUT
	}
	millis := timeout.Milliseconds()
	if millis == 0 && timeout > 0 {
		millis = 1
	}
	millis = min(max(millis, 0), math.MaxInt32)

	var ready int
	var err error
	if control := raw.Control(func(fd uintptr) {
		ready, err = unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: events}}, int(millis))
	}); control != nil {
		return false, control
	}
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return false, nil
		}
		return false, err
	}
	return ready > 0, nil
}
