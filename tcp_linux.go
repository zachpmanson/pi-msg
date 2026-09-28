//go:build linux

package main

import (
	"syscall"
	"time"
)

// tcpUserTimeout bounds how long the kernel keeps retransmitting
// unacknowledged data before failing a write with ETIMEDOUT. Linux otherwise
// waits out tcp_retries2 (default ~15 minutes), which is far too long for a
// bridge that is supposed to reconnect: while a peer stops acknowledging our
// data but keeps sending us stanzas, every write hangs for minutes and the
// bridge's single recovery attempt looks like it did nothing (issue #90).
const tcpUserTimeout = 45 * time.Second

// tcpUserTimeoutOpt is TCP_USER_TIMEOUT (linux/include/uapi/linux/tcp.h). The
// syscall package does not export it; golang.org/x/sys/unix does, but taking
// that as a direct dependency would churn the module's vendorHash for the sake
// of one setsockopt.
const tcpUserTimeoutOpt = 18

// hardenTCP applies the Linux-only socket options that bound a wedged
// connection's failure time.
func hardenTCP(conn syscall.RawConn) error {
	var sockErr error
	if err := conn.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(
			int(fd), syscall.IPPROTO_TCP, tcpUserTimeoutOpt,
			int(tcpUserTimeout/time.Millisecond),
		)
	}); err != nil {
		return err
	}
	return sockErr
}
