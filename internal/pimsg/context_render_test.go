package pimsg

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Opt-in installed-runtime probe: the actual bridge environment feeds the real
// extension loader, prompt builder and TypeBox schemas, not a registration mock.
func TestRenderedContext(t *testing.T) {
	t.Setenv("PI_MSG_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	pkg := os.Getenv("PI_MSG_PI_PACKAGE")
	if pkg == "" {
		t.Skip("set PI_MSG_PI_PACKAGE to installed pi-coding-agent package for schema render probe")
	}
	for _, tc := range []struct {
		name      string
		rooms     []string
		arbitrary bool
	}{
		{"restricted-dm", nil, false},
		{"restricted-rooms", []string{"testing@muc.example.com", "agents@muc.example.com"}, false},
		{"unrestricted-rooms", []string{"testing@muc.example.com", "agents@muc.example.com"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acct := ResolvedAccount{Name: "sample", Owner: "owner@example.com", Rooms: tc.rooms, AllowArbitraryJid: tc.arbitrary}
			b := NewBridge(acct, false)
			prompt := b.composePrompt("hello", acct.Owner, "", "", "", "", nil)
			cmd := exec.Command("node", "piext/render-context.mjs", pkg)
			cmd.Env = append(os.Environ(), rpcEnv(acct)...)
			cmd.Env = append(cmd.Env, "PI_MSG_ACCOUNT=sample", "PI_MSG_PROCESS_EXTENSION=", "PI_MSG_BEFORE_AGENT_START_TEXT=")
			cmd.Stdin = strings.NewReader(prompt)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("render: %v: %s", err, out)
			}
			var rendered struct {
				Version          string   `json:"version"`
				Loader           string   `json:"loader"`
				Builder          string   `json:"builder"`
				BaseSystem       string   `json:"baseSystem"`
				System           string   `json:"system"`
				Prompt           string   `json:"prompt"`
				PromptGuidelines []string `json:"promptGuidelines"`
				Tools            []struct {
					Name       string `json:"name"`
					Parameters struct {
						Properties map[string]struct {
							Description string `json:"description"`
						} `json:"properties"`
					} `json:"parameters"`
				} `json:"tools"`
			}
			if err := json.Unmarshal(out, &rendered); err != nil {
				t.Fatal(err)
			}
			if rendered.Version != "0.84.4" || rendered.Loader != "loadExtensions" || rendered.Builder != "buildSystemPrompt" {
				t.Fatalf("unexpected installed runtime: %s", out)
			}
			if rendered.System != "You are sample.\n\n"+rendered.BaseSystem || !strings.Contains(rendered.BaseSystem, "You are an expert coding assistant operating inside pi") {
				t.Fatal(rendered.System)
			}
			if len(rendered.PromptGuidelines) != 0 {
				t.Fatalf("companion must not inject prompt guidelines: %v", rendered.PromptGuidelines)
			}
			for _, shared := range []string{"Render fixture shared agent instructions.", "Render fixture shared personality instructions."} {
				if strings.Count(rendered.System, shared) != 1 {
					t.Fatalf("shared context not preserved exactly once: %s", rendered.System)
				}
			}
			if rendered.Prompt != prompt || strings.Count(rendered.Prompt, "[pi-msg: messaging:") != 1 || !strings.Contains(rendered.Prompt, "Owner: "+acct.Owner) || !strings.HasSuffix(rendered.Prompt, "hello") {
				t.Fatalf("conversation seed missing or altered: %s", rendered.Prompt)
			}
			if strings.Contains(rendered.Prompt, "[pi-msg: rooms:") != (len(tc.rooms) > 0) {
				t.Fatalf("room/nonroom seed boundary: %s", rendered.Prompt)
			}
			for _, room := range tc.rooms {
				if !strings.Contains(rendered.Prompt, room) {
					t.Fatalf("configured room missing from seed: %s", rendered.Prompt)
				}
			}
			if next := b.composePrompt("second", acct.Owner, "", "", "", "", nil); strings.Contains(next, "[pi-msg:") {
				t.Fatalf("session seed repeated: %s", next)
			}
			if len(rendered.Tools) != 4 {
				t.Fatalf("tools: %s", out)
			}
			for _, tool := range rendered.Tools {
				field := ""
				if tool.Name == "send_message" {
					field = "to"
				}
				if tool.Name == "read_messages" {
					field = "target"
				}
				if field != "" {
					description := tool.Parameters.Properties[field].Description
					if !strings.Contains(description, acct.Owner) || strings.Contains(description, "also other valid peer JIDs") != tc.arbitrary {
						t.Fatal(description)
					}
					if len(tc.rooms) == 0 && strings.Contains(description, "configured rooms") {
						t.Fatalf("roomless capability must not advertise rooms: %s", description)
					}
				}
			}
			t.Log(string(out))
		})
	}
}

func TestCapabilityChangeRefreshesContract(t *testing.T) {
	t.Setenv("PI_MSG_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	b := NewBridge(ResolvedAccount{Owner: "owner@x"}, false)
	old := b.contractHash()
	b.acct.AllowArbitraryJid = true
	if old == b.contractHash() {
		t.Fatal("permission change must refresh resumed contract")
	}
	b.messagingSeeded = false // same result as startup detecting an old hash
	if got := b.composePrompt("resume", "owner@x", "", "", "", "", nil); !strings.Contains(got, "[pi-msg: messaging:") {
		t.Fatal(got)
	}
}
