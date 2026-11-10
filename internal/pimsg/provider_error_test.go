package pimsg

import (
	"encoding/xml"
	"io"
	"strings"
	"testing"
)

func assistantEnd(stop, errMsg string) Event {
	return Event{
		"type": "message_end",
		"message": map[string]any{
			"role": "assistant", "content": []any{},
			"stopReason": stop, "errorMessage": errMsg,
		},
	}
}

// Check the actual owner-DM stanza and RPC output, not just repliedThisRun.
func TestProviderFailureReportedAtSettle(t *testing.T) {
	for _, tt := range []struct {
		name, errMsg string
	}{
		{"ChatGPT usage limit", "You have hit your ChatGPT usage limit (prolite plan). Try again in ~1650 min."},
		{"OpenRouter credits", `402: {"message":"This request requires more credits, or fewer max_tokens."}`},
		{"quota", "429 RESOURCE_EXHAUSTED: quota exceeded"},
		{"authentication", "401: invalid API key"},
		{"timeout", "Request timed out."},
		{"overloaded", "529 overloaded_error: Overloaded"},
		{"unrelated insufficient", "403: insufficient permissions"},
		{"unknown", ""},
		{"whitespace", " \n\t"},
		{"multiline", "Provider error:\n\nquota exceeded"},
		{"long Unicode", strings.Repeat("限", 200)},
	} {
		for _, partialReply := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{false: "/no reply", true: "/partial reply"}[partialReply], func(t *testing.T) {
				b, wire, rpc := messageTransportBridge(t, false)
				b.handleRPCEvent(Event{"type": "agent_start"})
				b.setTurnDest("team@muc.x", false)
				b.countInbound("zach", "one", "first request")
				b.countInbound("zach", "two", "second request")
				if partialReply {
					b.recordDelivery("answer", "working on it")
					b.setReplied(true)
				}
				wire.Reset()
				b.handleRPCEvent(assistantEnd("error", tt.errMsg))
				if wire.Len() != 0 {
					t.Fatalf("failure reported before retries settled: %s", wire.String())
				}
				b.handleRPCEvent(Event{"type": "agent_settled"})

				decoder := xml.NewDecoder(strings.NewReader(wire.String()))
				messages := 0
				for {
					tok, err := decoder.Token()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					start, ok := tok.(xml.StartElement)
					if !ok || start.Name.Local != "message" {
						continue
					}
					var msg struct {
						To   string `xml:"to,attr"`
						Type string `xml:"type,attr"`
						Body string `xml:"body"`
					}
					if err := decoder.DecodeElement(&msg, &start); err != nil {
						t.Fatal(err)
					}
					messages++
					wantErr := strings.TrimSpace(tt.errMsg)
					if wantErr == "" {
						wantErr = "provider returned no error details"
					}
					if msg.To != b.acct.Owner || msg.Type != "chat" || msg.Body != "⚠️ Model run failed: "+shortError(wantErr) {
						t.Fatalf("incorrect failure alert: %+v", msg)
					}
				}
				if messages != 1 || !b.replied() || rpc.Len() != 0 {
					t.Fatalf("want one alert, replied, no model recovery; messages=%d replied=%v rpc=%s", messages, b.replied(), rpc.String())
				}
				wire.Reset()
				b.handleRPCEvent(Event{"type": "agent_settled"})
				if strings.Contains(wire.String(), "Model run failed") {
					t.Fatalf("duplicate failure alert: %s", wire.String())
				}
			})
		}
	}
}

func TestProviderFailureRetryRecovery(t *testing.T) {
	for _, stop := range []string{"stop", "toolUse", "aborted"} {
		t.Run(stop, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, false)
			b.handleRPCEvent(Event{"type": "agent_start"})
			b.handleRPCEvent(assistantEnd("error", "429: rate limited"))
			b.handleRPCEvent(Event{"type": "auto_retry_start"})
			// Pi starts another low-level agent run for the retry.
			b.handleRPCEvent(Event{"type": "agent_start"})
			b.handleRPCEvent(assistantEnd(stop, ""))
			b.handleRPCEvent(Event{"type": "auto_retry_end", "success": true})
			wire.Reset()
			b.handleRPCEvent(Event{"type": "agent_settled"})
			if strings.Contains(wire.String(), "Model run failed") || b.replied() || rpc.Len() != 0 {
				t.Fatalf("recovered/aborted run reported as failed: wire=%s rpc=%s", wire.String(), rpc.String())
			}
		})
	}
}

func TestProviderFailureRetryExhausted(t *testing.T) {
	b, wire, rpc := messageTransportBridge(t, false)
	for i := 0; i < 3; i++ {
		b.handleRPCEvent(Event{"type": "agent_start"})
		b.handleRPCEvent(assistantEnd("error", "529: overloaded"))
		if i < 2 {
			b.handleRPCEvent(Event{"type": "auto_retry_start"})
		}
	}
	b.handleRPCEvent(Event{"type": "auto_retry_end", "success": false, "finalError": "529: overloaded"})
	if strings.Contains(wire.String(), "Model run failed") {
		t.Fatal("transient errors alerted before retry exhaustion settled")
	}
	wire.Reset()
	b.handleRPCEvent(Event{"type": "agent_settled"})
	if strings.Count(wire.String(), "Model run failed: 529: overloaded") != 1 || rpc.Len() != 0 {
		t.Fatalf("want one final alert and no model recovery: wire=%s rpc=%s", wire.String(), rpc.String())
	}
}

// Unlike an aborted assistant turn, cancelling backoff has no subsequent
// agent_start or assistant message: only auto_retry_end then agent_settled.
func TestProviderFailureRetryCancelled(t *testing.T) {
	b, wire, _ := messageTransportBridge(t, false)
	b.handleRPCEvent(Event{"type": "agent_start"})
	b.handleRPCEvent(assistantEnd("error", "429: rate limited"))
	b.handleRPCEvent(Event{"type": "auto_retry_start"})
	b.handleRPCEvent(Event{"type": "auto_retry_end", "success": false, "finalError": "Retry cancelled"})
	wire.Reset()
	b.handleRPCEvent(Event{"type": "agent_settled"})
	if strings.Contains(wire.String(), "Model run failed") || b.replied() {
		t.Fatalf("cancelled retry reported as failed: %s", wire.String())
	}
}

func TestProviderFailureLocalAbort(t *testing.T) {
	b, wire, rpc := messageTransportBridge(t, false)
	b.handleRPCEvent(Event{"type": "agent_start"})
	b.countInbound("zach", "one", "request")
	b.handleRPCEvent(assistantEnd("error", "429: rate limited"))
	b.handleRPCEvent(Event{"type": "auto_retry_start"})
	b.settleLocally() // /abort, /interrupt and session replacement cleanup
	wire.Reset()
	b.handleRPCEvent(Event{"type": "agent_settled"}) // defensive late settle
	if strings.Contains(wire.String(), "Model run failed") || rpc.Len() != 0 {
		t.Fatalf("aborted run produced failure/recovery: wire=%s rpc=%s", wire.String(), rpc.String())
	}
}

func TestProviderFailureClearedByLaterAssistantMessage(t *testing.T) {
	b, wire, _ := messageTransportBridge(t, false)
	b.handleRPCEvent(assistantEnd("error", "context overflow"))
	// Compaction/queued continuation can recover without a new agent_start.
	b.handleRPCEvent(assistantEnd("stop", ""))
	b.handleRPCEvent(Event{"type": "agent_settled"})
	if strings.Contains(wire.String(), "Model run failed") {
		t.Fatalf("stale error after successful continuation: %s", wire.String())
	}
}
