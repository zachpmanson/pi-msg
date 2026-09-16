//go:build !linux

package main

import (
	"syscall"
	"time"
)

// tcpUserTimeout exists on every platform so the dialer configuration and the
// logs read the same everywhere; only the setsockopt that enforces it is
// Linux-specific.
const tcpUserTimeout = 45 * time.Second

// hardenTCP is a no-op off Linux: TCP_USER_TIMEOUT has no portable stdlib
// equivalent, and the bridge's own keepalive still bounds detection.
func hardenTCP(syscall.RawConn) error { return nil }
