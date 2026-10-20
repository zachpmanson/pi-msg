package pimsg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Capture asynchronous read_messages responses without a server or a shared
// bytes.Buffer (the MAM fetch runs in a goroutine).
type mucRelayResponse chan string

func (r mucRelayResponse) Write(p []byte) (int, error) {
	r <- string(p)
	return len(p), nil
}

func (r mucRelayResponse) Close() error { return nil }

// Exercise the actual JSON -> resolved account -> bridge allowlists, rather
// than constructing Rooms directly. Single-room configurations must continue
// to work, and adding a second normal room must authorize it on every path
// without opting into arbitrary peer JIDs or exposing the error room.
func TestConfiguredMUCAllowlist(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rooms string
		want  []string
	}{
		{"single", `[{"jid":"team@muc.x"}]`, []string{"team@muc.x"}},
		{"multiple", `[{"jid":"team@muc.x"},{"jid":"testing@muc.x","trigger":"tester"},{"jid":"errors@muc.x","role":"error"}]`, []string{"team@muc.x", "testing@muc.x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			body := fmt.Sprintf(`{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"zach@x","rooms":%s}}}`, tc.rooms)
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := loadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			acct, err := resolveAccount(cfg, "")
			if err != nil {
				t.Fatal(err)
			}
			if !acct.RoomMode() || len(acct.Rooms) != len(tc.want) || acct.AllowArbitraryJid {
				t.Fatalf("unexpected resolved account: rooms=%v roomMode=%v arbitrary=%v", acct.Rooms, acct.RoomMode(), acct.AllowArbitraryJid)
			}
			var received []InboundMessage
			x := NewXMPPBridge(acct, func(m InboundMessage) { received = append(received, m) }, func(string, string) {})
			for _, room := range tc.want {
				if got := x.classifyMessageDest(room, false); got != destRoom {
					t.Errorf("send destination %s = %v, want groupchat", room, got)
				}
				x.dispatch(incomingMsg{typ: "groupchat", from: room + "/alice", body: "pi tester: hello", id: room})
			}
			for _, room := range []string{"unlisted@muc.x", "errors@muc.x"} {
				if got := x.classifyMessageDest(room, false); got != destBlocked {
					t.Errorf("send destination %s = %v, want blocked", room, got)
				}
				x.dispatch(incomingMsg{typ: "groupchat", from: room + "/alice", body: "pi: hello", id: room})
			}
			if len(received) != len(tc.want) {
				t.Fatalf("received %d messages, want only %d configured rooms", len(received), len(tc.want))
			}
			for i, room := range tc.want {
				if received[i].Room != room || received[i].From != room+"/alice" {
					t.Errorf("inbound message %d lost its source room: %+v", i, received[i])
				}
			}

			// Allowed tool calls reach the offline transport, not an authorization
			// error. Read calls really enter the asynchronous MAM fetch path.
			checks := []struct{ target, send, read string }{
				{"zach@x", "failed: no stanza was sent", "failed: not online"},
				{"unlisted@muc.x", "not an allowed destination", "not an allowed target"},
				{"errors@muc.x", "not an allowed destination", "not an allowed target"},
			}
			if acct.ErrorRoom != "" {
				checks[2].read = "not a readable conversation"
			}
			for _, room := range tc.want {
				checks = append(checks, struct{ target, send, read string }{room, "failed: no stanza was sent", "failed: not online"})
			}
			for _, check := range checks {
				for _, action := range []string{"send_message", "read_messages"} {
					t.Run(action+"/"+check.target, func(t *testing.T) {
						b := newTestBridge(acct)
						responses := make(mucRelayResponse, 1)
						b.rpc = &RPCClient{stdin: responses}
						payload, err := json.Marshal(map[string]string{
							"action": action, "to": check.target, "target": check.target, "text": "hello",
						})
						if err != nil {
							t.Fatal(err)
						}
						b.handleToolRelay("r1", string(payload))
						want := check.send
						if action == "read_messages" {
							want = check.read
						}
						select {
						case out := <-responses:
							if !strings.Contains(out, want) {
								t.Fatalf("response = %s, want %q", out, want)
							}
						case <-time.After(5 * time.Second):
							t.Fatal("tool relay did not respond")
						}
					})
				}
			}
		})
	}
}
