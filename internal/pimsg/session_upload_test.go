package pimsg

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
)

type sessionUploadMessage struct {
	To   string `xml:"to,attr"`
	Body string `xml:"body"`
}

// Capture real outgoing XML without a server. Bound the reader so restoring
// the original oversized-inline fallback fails rather than allocating a giant
// stanza in the test harness.
func sessionUploadBridge(t *testing.T) (*Bridge, <-chan sessionUploadMessage) {
	t.Helper()
	b := roomBridge()
	b.xmpp = NewXMPPBridge(b.acct, func(InboundMessage) {}, b.log)
	b.setTurnDest("team@muc.x.com", false)
	r, w := io.Pipe()
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	rw := struct {
		io.Reader
		io.Writer
	}{strings.NewReader(""), w}
	session, err := xmpp.NewSession(context.Background(), jid.JID{}, jid.JID{}, rw, xmpp.Ready,
		xmpp.NewNegotiator(func(*xmpp.Session, *xmpp.StreamConfig) xmpp.StreamConfig {
			return xmpp.StreamConfig{}
		}))
	if err != nil {
		t.Fatal(err)
	}
	b.xmpp.session = session
	b.xmpp.online = true
	messages := make(chan sessionUploadMessage, 4)
	go func() {
		defer close(messages)
		defer r.Close()
		decoder := xml.NewDecoder(io.LimitReader(r, 2*maxBody))
		for {
			var msg sessionUploadMessage
			if err := decoder.Decode(&msg); err != nil {
				return
			}
			messages <- msg
		}
	}()
	return b, messages
}

func nextSessionUploadMessage(t *testing.T, messages <-chan sessionUploadMessage) sessionUploadMessage {
	t.Helper()
	select {
	case msg, ok := <-messages:
		if !ok {
			t.Fatal("outgoing XML ended or exceeded the safe stanza budget")
		}
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("no outgoing XMPP message")
		return sessionUploadMessage{}
	}
}

func TestSessionUploadFailureNeverSendsInline(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"session.html", 128},
		// Exact size of Falco's export that disconnected the account. The
		// single long line mirrors HTML with an embedded session payload.
		{"falco-session.html", 113610537},
		{"session-raw.jsonl", maxBody + 1},
		{"session-table.tsv", maxBody + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, messages := sessionUploadBridge(t)
			orig := uploadDumpFile
			uploadDumpFile = func(_ *XMPPBridge, to, path string) (string, error) {
				info, err := os.Stat(path)
				if err != nil || info.Size() != int64(tc.size) || to != b.acct.Owner {
					return "", fmt.Errorf("incorrect upload: owner=%q stat=%v", to, err)
				}
				return "", fmt.Errorf("requesting upload slot: file too large")
			}
			t.Cleanup(func() { uploadDumpFile = orig })
			content := bytes.Repeat([]byte("x"), tc.size)
			if strings.HasSuffix(tc.name, ".html") {
				path := filepath.Join(t.TempDir(), tc.name)
				if err := os.WriteFile(path, content, 0o600); err != nil {
					t.Fatal(err)
				}
				b.sendRenderedFile(tc.name, path)
			} else {
				b.sendDumpFile(tc.name, content)
			}
			msg := nextSessionUploadMessage(t, messages)
			for _, want := range []string{"file upload failed", fmt.Sprintf("%d bytes", tc.size), "requesting upload slot: file too large", "Not sending inline", "web link"} {
				if !strings.Contains(msg.Body, want) {
					t.Errorf("diagnostic %q missing %q", msg.Body, want)
				}
			}
			if msg.To != b.acct.Owner || len(msg.Body) > 1024 || strings.Contains(msg.Body, "xxxxxxxx") {
				t.Fatalf("unsafe diagnostic: to=%q bytes=%d", msg.To, len(msg.Body))
			}
			// The next stanza must be ordinary chat, not any inline file
			// content, and the same XMPP session must remain usable.
			b.reply("still connected")
			if next := nextSessionUploadMessage(t, messages); next.Body != "still connected" || next.To != b.acct.Owner {
				t.Fatalf("unexpected follow-up: to=%q body bytes=%d", next.To, len(next.Body))
			}
		})
	}
}

func TestSessionUploadSuccessPreservesFile(t *testing.T) {
	b, messages := sessionUploadBridge(t)
	orig := uploadDumpFile
	uploadDumpFile = func(_ *XMPPBridge, to, path string) (string, error) {
		content, err := os.ReadFile(path)
		if err != nil || string(content) != "<html>session</html>" || to != b.acct.Owner || !strings.HasSuffix(path, ".html") {
			return "", fmt.Errorf("incorrect successful upload: to=%q read=%v", to, err)
		}
		// Stand in for SendFile's normal XEP-0066 URL delivery.
		b.xmpp.SendChatReply(to, "https://upload.example/session.html", nil)
		return "https://upload.example/session.html", nil
	}
	t.Cleanup(func() { uploadDumpFile = orig })
	path := filepath.Join(t.TempDir(), "session.html")
	if err := os.WriteFile(path, []byte("<html>session</html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	b.sendRenderedFile("session.html", path)
	msg := nextSessionUploadMessage(t, messages)
	if msg.To != b.acct.Owner || msg.Body != "https://upload.example/session.html" {
		t.Fatalf("normal export delivery changed: %+v", msg)
	}
}
