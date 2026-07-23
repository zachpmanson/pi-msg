package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakePi writes a shell script that mimics `pi --mode rpc`: it emits an
// agent_start event, then for every stdin command carrying an "id" it replies
// with a matching success response.
func fakePi(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "fakepi.sh")
	script := `#!/bin/sh
printf '{"type":"agent_start"}\n'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  if [ -n "$id" ]; then
    printf '{"type":"response","id":"%s","success":true,"data":{"ok":1}}\n' "$id"
  fi
done
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fakepi: %v", err)
	}
	return path
}

func TestRPCEventsAndRequest(t *testing.T) {
	c := NewRPCClient(fakePi(t), "", "", nil)
	if err := c.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Stop()

	// First event should be agent_start.
	select {
	case ev := <-c.Events():
		if ev.Type() != "agent_start" {
			t.Fatalf("first event type = %q, want agent_start", ev.Type())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for agent_start event")
	}

	// A request should get a correlated success response (not leak into Events).
	ctx := context.Background()
	resp, err := c.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if !resp.success() {
		t.Errorf("response success = false, want true")
	}
	if resp.Str("id") == "" {
		t.Errorf("response missing id")
	}
	if data := resp.Obj("data"); data == nil {
		t.Errorf("response missing data object")
	}
}

func TestRPCStopClosesDone(t *testing.T) {
	c := NewRPCClient(fakePi(t), "", "", nil)
	if err := c.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	c.Stop()
	select {
	case <-c.Done():
		if !c.StoppedIntentionally() {
			t.Error("StoppedIntentionally() = false after Stop()")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Done() not closed after Stop()")
	}
}

func TestRequestAfterExitFails(t *testing.T) {
	c := NewRPCClient(fakePi(t), "", "", nil)
	if err := c.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	c.Stop()
	<-c.Done()
	if _, err := c.NewSession(context.Background()); err == nil {
		t.Error("expected error requesting after exit, got nil")
	}
}
