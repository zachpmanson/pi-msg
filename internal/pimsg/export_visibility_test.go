package pimsg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func exportFixture(payload string) []byte {
	return []byte("<!doctype html>\n" + sessionDataTag + base64.StdEncoding.EncodeToString([]byte(payload)) + "</script>\n<script>/* native renderer unchanged */</script>")
}

func exportPayload(t *testing.T, html []byte) map[string]any {
	t.Helper()
	start := bytes.Index(html, []byte(sessionDataTag)) + len(sessionDataTag)
	end := start + bytes.Index(html[start:], []byte("</script>"))
	raw, err := base64.StdEncoding.DecodeString(string(html[start:end]))
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&data); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestExportContextVisibility(t *testing.T) {
	original := exportFixture(`{
 "header":{"id":"fixture-uuid","unknown":9007199254740993},"leafId":"seed",
 "systemPrompt":"fixture system","tools":[{"name":"fixture","parameters":{"type":"object"}}],
 "renderedTools":{"call":{"callHtml":"<b>fixture</b>"}},"future":{"nested":[true,null,1e99]},
 "entries":[
 {"type":"custom_message","id":"seed","customType":"pi-msg-idle-seed","display":false,"content":"Recap 雪 🦊 </script><script>alert('fixture')</script>","details":{"token":"fixture-token","sourceID":"fixture-source"},"unknown":[1,2]},
 {"type":"custom_message","customType":"visible","display":true,"content":"already visible"},
 {"type":"custom_message","customType":"unrelated-context","display":false,"content":[{"type":"text","text":"block context"},{"type":"image","mimeType":"image/png","data":"fixture"}]},
 {"type":"custom_message","customType":"legacy-no-display","content":"also context"},
 {"type":"custom_message","display":false,"content":"untyped extension context"},
 {"type":"custom","customType":"opaque-state","display":false,"data":{"display":false,"content":"not model context"}},
 {"type":"message","message":{"role":"user","content":"fixture"},"display":false}
 ]}`)
	snapshot := bytes.Clone(original)
	got, err := visibleExportContext(original)
	if err != nil {
		t.Fatal(err)
	}
	want := exportPayload(t, original)
	for _, e := range want["entries"].([]any) {
		entry := e.(map[string]any)
		if entry["type"] == "custom_message" {
			entry["display"] = true
		}
	}
	if actual := exportPayload(t, got); !reflect.DeepEqual(actual, want) {
		t.Fatalf("fields/content changed beyond display: got %#v want %#v", actual, want)
	}
	if !bytes.Equal(original, snapshot) || bytes.Count(got, []byte("</script>")) != 2 {
		t.Fatal("source mutated or script-like content escaped its base64 payload")
	}
	if !bytes.HasPrefix(got, []byte("<!doctype html>\n"+sessionDataTag)) || !bytes.HasSuffix(got, []byte("</script>\n<script>/* native renderer unchanged */</script>")) {
		t.Fatal("native HTML outside payload changed")
	}
}

func TestExportContextNoopExactBytes(t *testing.T) {
	for _, payload := range []string{`{ "entries" : [] }`, `{"entries":[{"type":"custom_message","customType":"visible","content":"fixture","display":true},{"type":"custom","display":false}]}`, `{"entries":[{"type":"custom_message","content":"untyped command output","display":true}]}`} {
		original := exportFixture(payload)
		got, err := visibleExportContext(original)
		if err != nil || !bytes.Equal(got, original) {
			t.Fatalf("no-op changed bytes: %v", err)
		}
	}
}

func TestExportContextRejectsUnsupportedPayload(t *testing.T) {
	cases := map[string][]byte{
		"missing":       []byte("<html>fixture</html>"),
		"multiple":      append(exportFixture(`{"entries":[]}`), exportFixture(`{"entries":[]}`)...),
		"alternate tag": []byte(`<script type="application/json" id='session-data'>e30=</script>`),
		"missing close": []byte(sessionDataTag + "e30="),
		"bad base64":    []byte(sessionDataTag + "not base64!</script>"),
		"invalid UTF8":  exportFixture(string([]byte{'"', 0xff, '"'})),
	}
	for _, payload := range []string{`{`, `null`, `[]`, `{}`, `{"entries":null}`, `{"entries":{}}`, `{"entries":[null]}`, `{"entries":[1]}`, `{"entries":[{}]}`, `{"entries":[],"entries":[]}`, `{"entries":[],"nested":{"x":1,"x":2}}`, `{"entries":[]} {}`, `{"entries":[{"type":"custom_message","display":false}]}`, `{"entries":[{"type":"custom_message","customType":"fixture","content":42}]}`, `{"entries":[{"type":"custom_message","customType":"fixture","content":"fixture","display":"false"}]}`, `{"entries":[{"type":"custom_message","customType":"fixture","content":"fixture","display":null}]}`} {
		cases[payload] = exportFixture(payload)
	}
	for name, html := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := visibleExportContext(html)
			if err == nil || !strings.Contains(err.Error(), "session-data export") || got != nil {
				t.Fatalf("expected strict diagnostic, got %q %v", got, err)
			}
		})
	}
}

func TestExportContextRejectsInvalidCustomType(t *testing.T) {
	for _, typ := range []string{`null`, `42`, `{}`} {
		html := exportFixture(`{"entries":[{"type":"custom_message","customType":` + typ + `,"content":"fixture","display":false}]}`)
		if got, err := visibleExportContext(html); err == nil || got != nil {
			t.Fatal("invalid custom type accepted", string(got), err)
		}
	}
}

func TestExportContextRejectsInvalidBlocks(t *testing.T) {
	for _, content := range []string{`[null]`, `["text"]`, `[1]`, `[[]]`, `[{}]`, `[{"type":"text"}]`, `[{"type":"text","text":null}]`, `[{"type":"text","text":42}]`, `[{"type":"image","data":"fixture"}]`, `[{"type":"image","data":null,"mimeType":"image/png"}]`, `[{"type":"thinking","thinking":"fixture"}]`} {
		t.Run(content, func(t *testing.T) {
			for _, display := range []string{"true", "false"} {
				html := exportFixture(`{"entries":[{"type":"custom_message","customType":"fixture","display":` + display + `,"content":` + content + `}]}`)
				if got, err := visibleExportContext(html); err == nil || got != nil {
					t.Fatal("invalid blocks accepted", string(got), err)
				}
			}
		})
	}
}

func TestExportContextUnicodeEscapes(t *testing.T) {
	for _, escaped := range []string{`\ud800`, `\udfff`, `\ud800\u0061`, `\ud800\ud800`, `\ud800x`} {
		for _, payload := range []string{
			`{"entries":[],"` + escaped + `":"fixture"}`,
			`{"entries":[],"unknown":"` + escaped + `"}`,
		} {
			if got, err := visibleExportContext(exportFixture(payload)); err == nil || got != nil {
				t.Fatalf("unpaired surrogate accepted: %s %v", payload, err)
			}
		}
	}
	original := exportFixture(`{"entries":[{"type":"custom_message","customType":"fixture","display":false,"content":[{"type":"text","text":"paired \ud83e\udd8a; literal \\ud800"}]}],"\ud83e\udd8a":{"\\ud800":"literal \\udfff","paired":"\ud83e\udd8a","ordinary":"\u0061"}}`)
	got, err := visibleExportContext(original)
	if err != nil {
		t.Fatal("valid paired/escaped-backslash Unicode rejected", err)
	}
	want := exportPayload(t, original)
	want["entries"].([]any)[0].(map[string]any)["display"] = true
	if !reflect.DeepEqual(exportPayload(t, got), want) {
		t.Fatal("paired Unicode or escaped-backslash semantics changed")
	}
}

type exportFixtureWriter struct{ write func([]byte) (int, error) }

func (w exportFixtureWriter) Write(p []byte) (int, error) { return w.write(p) }
func (w exportFixtureWriter) Close() error                { return nil }

func TestSessionExportVisibilityUpload(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(fmt.Sprint(valid), func(t *testing.T) {
			b, messages := sessionUploadBridge(t)
			orig := uploadDumpFile
			t.Cleanup(func() { uploadDumpFile = orig })
			uploaded := make(chan []byte, 1)
			uploadDumpFile = func(_ *XMPPBridge, to, path string) (string, error) {
				if to != b.acct.Owner {
					return "", fmt.Errorf("not owner")
				}
				raw, err := os.ReadFile(path)
				uploaded <- raw
				return "fixture-url", err
			}
			raw := exportFixture(`{"entries":[{"type":"custom_message","customType":"legacy","content":"fixture","display":false}]}`)
			if !valid {
				raw = []byte("unsupported fixture HTML")
			}
			var path string
			b.ctx = context.Background()
			b.rpc = NewRPCClient("", "", "", "", nil)
			b.rpc.stdin = exportFixtureWriter{write: func(p []byte) (int, error) {
				var command Event
				if err := json.Unmarshal(p, &command); err != nil {
					return 0, err
				}
				if command.Type() != "export_html" {
					t.Fatal("unexpected model/startup command", command)
				}
				path = command.Str("outputPath")
				info, err := os.Stat(filepath.Dir(path))
				if err != nil || info.Mode().Perm() != 0700 {
					t.Fatal("export directory not private", err)
				}
				if err := os.WriteFile(path, raw, 0600); err != nil {
					return 0, err
				}
				b.rpc.pending[command.Str("id")] <- Event{"success": true}
				return len(p), nil
			}}
			b.handleExport("")
			if msg := nextSessionUploadMessage(t, messages); !strings.Contains(msg.Body, "exporting session") {
				t.Fatal("missing progress", msg)
			}
			if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
				t.Fatal("native private artifact not cleaned up", err)
			}
			if valid {
				var got []byte
				select {
				case got = <-uploaded:
				case <-time.After(5 * time.Second):
					t.Fatal("no upload")
				}
				if exportPayload(t, got)["entries"].([]any)[0].(map[string]any)["display"] != true {
					t.Fatal("hidden upload")
				}
			} else {
				msg := nextSessionUploadMessage(t, messages)
				if !strings.Contains(msg.Body, "/export failed:") || strings.Contains(msg.Body, "unsupported fixture HTML") {
					t.Fatal("missing diagnostic or inline fallback", msg)
				}
				select {
				case <-uploaded:
					t.Fatal("malformed export uploaded")
				default:
				}
			}
		})
	}
}

// Installed-Pi regression on an isolated read-only legacy fixture. No live
// sessions, accounts, provider turns, credentials, or chat content are used.
func TestExportContextNativeLegacyReadOnly(t *testing.T) {
	pkg := os.Getenv("PI_MSG_PI_PACKAGE")
	if pkg == "" {
		t.Skip("set PI_MSG_PI_PACKAGE for native HTML regression")
	}
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("PI_CODING_AGENT_DIR", filepath.Join(root, "agent"))
	t.Setenv("PI_OFFLINE", "1")
	source, htmlPath := filepath.Join(root, "fixture.jsonl"), filepath.Join(root, "fixture.html")
	setup := fmt.Sprintf(`import {SessionManager} from %q;import {openSync,closeSync,chmodSync} from 'node:fs';const sm=SessionManager.create(%q);closeSync(openSync(%q,'wx',0o600));sm.setSessionFile(%q);sm.appendCustomMessageEntry('pi-msg-idle-seed','Historical fixture 雪 </script>',false,{token:'fixture-token',sourceID:'fixture-source'});sm.appendCustomMessageEntry(undefined,'Untyped command fixture',true);sm.appendCustomEntry('fixture-state',{display:false});chmodSync(%q,0o400);`, "file://"+filepath.Join(pkg, "dist/core/session-manager.js"), root, source, source, source)
	if raw, err := exec.Command("node", "--input-type=module", "-e", setup).CombinedOutput(); err != nil {
		t.Fatalf("setup: %v %s", err, raw)
	}
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	export := fmt.Sprintf(`import {exportSessionToHtml} from %q;import {SessionManager} from %q;await exportSessionToHtml(SessionManager.open(%q),{systemPrompt:'fixture system',tools:[{name:'fixture',description:'fixture tool',parameters:{type:'object'}}]},{outputPath:%q});`, "file://"+filepath.Join(pkg, "dist/core/export-html/index.js"), "file://"+filepath.Join(pkg, "dist/core/session-manager.js"), source, htmlPath)
	if raw, err := exec.Command("node", "--input-type=module", "-e", export).CombinedOutput(); err != nil {
		t.Fatalf("native export: %v %s", err, raw)
	}
	original, err := os.ReadFile(htmlPath)
	if err != nil {
		t.Fatal(err)
	}
	if exportPayload(t, original)["entries"].([]any)[0].(map[string]any)["display"] != false {
		t.Fatal("fixture not hidden")
	}
	got, err := visibleExportContext(original)
	if err != nil {
		t.Fatal(err)
	}
	data := exportPayload(t, got)
	if data["entries"].([]any)[0].(map[string]any)["display"] != true || data["systemPrompt"] != "fixture system" {
		t.Fatal("native context not visible or state lost")
	}
	untyped := data["entries"].([]any)[1].(map[string]any)
	if _, exists := untyped["customType"]; exists || untyped["content"] != "Untyped command fixture" || untyped["display"] != true {
		t.Fatal("native untyped extension output changed")
	}
	after, err := os.ReadFile(source)
	if err != nil || sha256.Sum256(before) != sha256.Sum256(after) || !bytes.Contains(after, []byte(`"display":false`)) {
		t.Fatal("read-only source hash/display changed", err)
	}
	t.Logf("native HTML transformed; read-only source SHA256 unchanged: %x", sha256.Sum256(before))
}
