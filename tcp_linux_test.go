//go:build linux

package main

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// TestConnectSocketHardening dials a real socket through the same dialer
// configuration connect() uses and reads the options back, so a wrong option
// number or unit fails loudly instead of silently leaving the bridge
// unhardened.
func TestConnectSocketHardening(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			time.Sleep(time.Second)
		}
	}()

	var d net.Dialer
	d.Control = func(network, address string, c syscall.RawConn) error {
		return hardenTCP(c)
	}
	d.KeepAliveConfig = net.KeepAliveConfig{
		Enable:   true,
		Idle:     tcpKeepAliveIdle,
		Interval: tcpKeepAliveInterval,
		Count:    tcpKeepAliveCount,
	}
	conn, err := d.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}

	var optErr error
	opt := func(level, name int) int {
		var v int
		if err := raw.Control(func(fd uintptr) {
			v, optErr = syscall.GetsockoptInt(int(fd), level, name)
		}); err != nil {
			optErr = err
		}
		return v
	}

	if got, want := opt(syscall.IPPROTO_TCP, tcpUserTimeoutOpt), int(tcpUserTimeout/time.Millisecond); got != want {
		t.Errorf("TCP_USER_TIMEOUT = %d ms, want %d ms (err %v)", got, want, optErr)
	}
	if got := opt(syscall.SOL_SOCKET, syscall.SO_KEEPALIVE); got != 1 {
		t.Errorf("SO_KEEPALIVE = %d, want 1 (err %v)", got, optErr)
	}
	if got, want := opt(syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE), int(tcpKeepAliveIdle/time.Second); got != want {
		t.Errorf("TCP_KEEPIDLE = %d s, want %d s (err %v)", got, want, optErr)
	}
	if got, want := opt(syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT), tcpKeepAliveCount; got != want {
		t.Errorf("TCP_KEEPCNT = %d, want %d (err %v)", got, want, optErr)
	}
}
