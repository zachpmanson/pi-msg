package pimsg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Full Go lifecycle -> real Pi extension/native switch -> durable pointer/XA.
// The optional worker is a local deterministic fake; no provider or XMPP server.
func TestAutoreconcileNativeBridgeLifecycle(t *testing.T) {
	pkg := os.Getenv("PI_MSG_PI_PACKAGE")
	if pkg == "" {
		t.Skip("set PI_MSG_PI_PACKAGE for isolated native bridge lifecycle")
	}
	for _, tc := range []struct{ skill, stale bool }{{false, false}, {true, false}, {false, true}} {
		t.Run(fmt.Sprintf("skill=%v/stale=%v", tc.skill, tc.stale), func(t *testing.T) {
			skill := tc.skill
			root := t.TempDir()
			t.Setenv("HOME", root)
			t.Setenv("PI_MSG_CONFIG", filepath.Join(root, "config.json"))
			t.Setenv("PI_CODING_AGENT_DIR", filepath.Join(root, "agent"))
			t.Setenv("PI_OFFLINE", "1")
			agentDir := filepath.Join(root, "agent")
			if err := os.MkdirAll(agentDir, 0700); err != nil {
				t.Fatal(err)
			}
			models := `{"providers":{"probe":{"baseUrl":"http://127.0.0.1:1","apiKey":"offline-test-not-a-credential","api":"openai-completions","models":[{"id":"probe","name":"Probe","reasoning":true,"input":["text"],"contextWindow":32000,"maxTokens":1024}]}}}`
			if err := os.WriteFile(filepath.Join(agentDir, "models.json"), []byte(models), 0600); err != nil {
				t.Fatal(err)
			}
			setup := fmt.Sprintf(`import {SessionManager} from %q;import {openSync,closeSync} from 'node:fs';const sm=SessionManager.create(%q,%q);const file=sm.getSessionFile();closeSync(openSync(file,'wx',0o600));sm.setSessionFile(file);sm.appendModelChange('probe','probe');sm.appendThinkingLevelChange('high');sm.appendCustomMessageEntry('fixture-source','old context',false);console.log(JSON.stringify({file,id:sm.getSessionId()}));`, "file://"+filepath.Join(pkg, "dist/core/session-manager.js"), root, filepath.Join(root, "sessions"))
			raw, err := exec.Command("node", "--input-type=module", "-e", setup).CombinedOutput()
			if err != nil {
				t.Fatalf("native source: %v %s", err, raw)
			}
			var source struct{ File, ID string }
			if err = json.Unmarshal(raw, &source); err != nil {
				t.Fatal(err)
			}
			b, wire, _ := messageTransportBridge(t, false)
			b.acct.Name = "native"
			b.acct.AutoreconcileTimeout = time.Millisecond
			b.acct.AutoreconcileHistoryMessages = 10
			b.acct.AutoreconcileOwnerHistoryMessages = 10
			b.ctx = context.Background()
			b.inbox = newInbox(inboxPath(b.acct.Name), b.log)
			b.xmpp.onAcceptedSent = b.captureAutoSent
			cwd, _ := os.Getwd()
			ext := filepath.Join(cwd, "piext/xmpp-tools.ts")
			wrapper := filepath.Join(root, "pi")
			// Native primary, but the restricted worker's JSON print mode is wholly
			// deterministic and cannot reach gh/wiki/providers or any live account.
			script := fmt.Sprintf("#!/bin/sh\nif [ \"$2\" = json ]; then printf '%%s\\n' '{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"stop\",\"content\":[{\"type\":\"text\",\"text\":\"issues unchanged; wiki skipped: unavailable\"}]}}'; exit 0; fi\nexec node %q --no-extensions --no-context-files --no-skills --no-prompt-templates --no-themes \"$@\"\n", filepath.Join(pkg, "dist/cli.js"))
			if tc.stale {
				hook := filepath.Join(root, "stale.ts")
				signal := filepath.Join(root, "switch-started")
				content := fmt.Sprintf("import {writeFileSync} from 'node:fs';export default pi=>{pi.on('session_before_switch',async()=>{writeFileSync(%q,'switching');await new Promise(r=>setTimeout(r,250));});};", signal)
				if err = os.WriteFile(hook, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				script = strings.Replace(script, `"$@"`, fmt.Sprintf("-e %q \"$@\"", hook), 1)
			}
			if err = os.WriteFile(wrapper, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			if skill {
				b.acct.ReconcileSkill = filepath.Join(root, "reconcile", "SKILL.md")
				if err = os.MkdirAll(filepath.Dir(b.acct.ReconcileSkill), 0700); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(b.acct.ReconcileSkill, []byte("---\nname: reconcile\ndescription: local fixture\n---\nReturn local fixture result."), 0600); err != nil {
					t.Fatal(err)
				}
			}
			b.rpc = NewRPCClient(wrapper, "", root, ext, nil)
			b.rpc.sessionPath = source.File
			b.rpc.env = rpcEnv(b.acct)
			if err = b.rpc.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { b.rpc.Stop(); <-b.rpc.Done() }()
			b.idleSince = time.Now().Add(-time.Hour)
			invalidated := false
			arrivalDone := make(chan struct{})
			autoIteration(t, b, func() {
				if tc.stale && !invalidated {
					if _, e := os.Stat(filepath.Join(root, "switch-started")); e == nil {
						invalidated = true
						// Actual concurrent arrival revokes native authority before
						// waiting for the short transition lock, then is durably held.
						go func() {
							b.onInbound(InboundMessage{Direct: true, From: b.acct.Owner + "/phone", FromOwner: true, Body: "new protected input", ID: "native-arrival"})
							close(arrivalDone)
						}()
					}
				}
			})
			a := b.auto.state.Attempt
			if tc.stale {
				if invalidated {
					select {
					case <-arrivalDone:
					case <-time.After(5 * time.Second):
						t.Fatal("concurrent arrival did not finish admission")
					}
				}
				if held := b.inbox.protectedPending(); len(held) != 1 || held[0].ID != "native-arrival" {
					t.Fatal("native concurrent arrival not protected", held)
				}
				if !invalidated || a == nil || a.Phase != "blocked" || a.TargetFile == "" || loadSessionState(b.acct.Name) != a.TargetFile || b.presenceShow() == "xa" {
					t.Fatalf("stale completed switch lost exact target or published XA: %+v", a)
				}
				if b.next.record == nil || b.next.record.FreshSessionID != a.TargetID {
					t.Fatal("stale target not recoverable through exact protected release")
				}
				restarted := NewBridge(b.acct, false)
				if err := restarted.loadAuto(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if a == nil || a.Phase != "done" {
				t.Fatalf("native cleanup not complete: %+v", a)
			}
			if b.presenceShow() != "xa" || a.TargetID == "id:"+source.ID || loadSessionState(b.acct.Name) != a.TargetFile {
				t.Fatalf("XA/exact target pointer not proven: %+v", a)
			}
			if _, err = b.readAutoReceipt(a); err != nil {
				t.Fatal(err)
			}
			entries := strings.Split(strings.TrimSpace(fileText(a.TargetFile)), "\n")
			if len(entries) != 4 {
				t.Fatalf("unexpected model/thinking/seed entries: %d", len(entries))
			}
			if skill {
				if !strings.Contains(a.Seed, "wiki skipped: unavailable") || !strings.Contains(wire.String(), `to="pi@x"`) {
					t.Fatal("self report not before recap")
				}
			} else if strings.Contains(wire.String(), "autoreconcile") {
				t.Fatal("optional worker unexpectedly ran")
			}
			b.onInbound(InboundMessage{Direct: true, From: "pi@x/echo", Body: "/new", ID: "self-copy"})
			if b.presenceShow() != "xa" {
				t.Fatal("self echo woke XA")
			}
			// A real owner control wakes without asking any model. It must retain the
			// exact fresh session and cannot re-arm another cleanup of this empty seed.
			b.onInbound(InboundMessage{Direct: true, From: b.acct.Owner + "/phone", FromOwner: true, RealJID: b.acct.Owner, Body: "/session", ID: "wake"})
			if b.presenceShow() == "xa" {
				t.Fatal("owner DM could not wake XA")
			}
			b.mu.Lock()
			b.idleSince = time.Now().Add(-time.Hour)
			b.mu.Unlock()
			before := a.Token
			autoIteration(t, b, nil)
			if b.auto.state.Attempt.Token != before {
				t.Fatal("empty replacement session reconciled repeatedly")
			}
		})
	}
}
