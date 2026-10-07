package pimsg

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, cfg Config) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestResolveAccountResetSessionOnAway(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com"},
	}}
	got, err := resolveAccount(cfg, "")
	if err != nil {
		t.Fatalf("resolveAccount default: %v", err)
	}
	if got.ResetSessionOnAway {
		t.Fatal("ResetSessionOnAway = true by default, want false")
	}

	cfg.Accounts["default"] = Account{
		JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com",
		ResetSessionOnAway: true,
	}
	path := writeConfig(t, *cfg)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	got, err = resolveAccount(loaded, "")
	if err != nil {
		t.Fatalf("resolveAccount enabled: %v", err)
	}
	if !got.ResetSessionOnAway {
		t.Fatal("ResetSessionOnAway = false, want true from config")
	}
}

func TestResolveAccountDefaults(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com"},
	}}
	got, err := resolveAccount(cfg, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.Service != "chat.example.com:5222" {
		t.Errorf("Service = %q, want chat.example.com:5222", got.Service)
	}
	if got.Resource != "pi-msg" {
		t.Errorf("Resource = %q, want pi-msg", got.Resource)
	}
	if got.Nick != "pi" {
		t.Errorf("Nick = %q, want pi", got.Nick)
	}
	if got.RoomTrigger != "pi" {
		t.Errorf("RoomTrigger = %q, want pi", got.RoomTrigger)
	}
	if got.RoomMode() {
		t.Error("RoomMode() = true, want false (no room set)")
	}
	if got.BeforeAgentStartText != "" {
		t.Errorf("BeforeAgentStartText = %q, want empty by default", got.BeforeAgentStartText)
	}
	if got.AllowArbitraryJid {
		t.Error("AllowArbitraryJid = true by default, want false")
	}
}

func TestResolveAccountBeforeAgentStartText(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com",
			BeforeAgentStartText: "  terse mode: be brief  "},
	}}
	got, err := resolveAccount(cfg, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	// Trimmed, so a whitespace-only value counts as unset rather than injecting
	// a blank paragraph every turn.
	if got.BeforeAgentStartText != "terse mode: be brief" {
		t.Errorf("BeforeAgentStartText = %q, want trimmed text", got.BeforeAgentStartText)
	}

	// The value must survive the config round trip under its JSON name.
	path := writeConfig(t, *cfg)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if loaded.Accounts["default"].BeforeAgentStartText != "  terse mode: be brief  " {
		t.Error("beforeAgentStartText not parsed from JSON")
	}

	blank := &Config{Accounts: map[string]Account{
		"default": {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com", BeforeAgentStartText: "   "},
	}}
	got, err = resolveAccount(blank, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.BeforeAgentStartText != "" {
		t.Errorf("whitespace-only value resolved to %q, want empty", got.BeforeAgentStartText)
	}
}

func TestResolveAccountAllowArbitraryJid(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com", AllowArbitraryJid: true},
	}}
	path := writeConfig(t, *cfg)
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	got, err := resolveAccount(loaded, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if !got.AllowArbitraryJid {
		t.Error("AllowArbitraryJid did not survive config round trip")
	}
}

func TestResolveAccountRoomMode(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {
			JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com",
			Rooms: roomList{{JID: "team@muc.chat.example.com"}}, Nick: "botpi",
		},
	}}
	got, err := resolveAccount(cfg, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if !got.RoomMode() {
		t.Error("RoomMode() = false, want true")
	}
	if len(got.Rooms) != 1 || got.Rooms[0] != "team@muc.chat.example.com" {
		t.Errorf("Rooms = %v, want [team@muc.chat.example.com]", got.Rooms)
	}
	if got.Nick != "botpi" {
		t.Errorf("Nick = %q, want botpi", got.Nick)
	}
	if got.RoomTrigger != "botpi" {
		t.Errorf("RoomTrigger defaults to Nick: got %q, want botpi", got.RoomTrigger)
	}
	// The room's resolved trigger inherits that default (#106).
	if trig := got.TriggerFor("team@muc.chat.example.com"); trig != "botpi" {
		t.Errorf("TriggerFor(room) = %q, want botpi", trig)
	}
}

func TestResolveAccountPingInterval(t *testing.T) {
	base := func(pi string) *Config {
		return &Config{Accounts: map[string]Account{
			"default": {JID: "a@x.com", Password: "p", Owner: "o@x.com", PingInterval: pi},
		}}
	}
	// Unset → default cadence.
	got, err := resolveAccount(base(""), "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.PingInterval != defaultPingInterval {
		t.Errorf("default PingInterval = %s, want %s", got.PingInterval, defaultPingInterval)
	}
	// Explicit duration string is parsed.
	got, err = resolveAccount(base("2m"), "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.PingInterval != 2*time.Minute {
		t.Errorf("PingInterval = %s, want 2m", got.PingInterval)
	}
	// "0" disables keepalive.
	got, err = resolveAccount(base("0"), "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.PingInterval != 0 {
		t.Errorf("PingInterval = %s, want 0", got.PingInterval)
	}
	// Garbage is a config error.
	if _, err := resolveAccount(base("soon"), ""); err == nil {
		t.Error("expected error for invalid pingInterval, got nil")
	}
}

func TestResolveAccountSelection(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "a@x.com", Password: "p", Owner: "o@x.com"},
		"work":    {JID: "b@x.com", Password: "p", Owner: "o@x.com"},
	}}
	got, err := resolveAccount(cfg, "work")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.Name != "work" || got.JID != "b@x.com" {
		t.Errorf("selected %q/%q, want work/b@x.com", got.Name, got.JID)
	}
	// Unknown requested falls back to default.
	got, err = resolveAccount(cfg, "nope")
	if err != nil {
		t.Fatalf("resolveAccount fallback: %v", err)
	}
	if got.Name != "default" {
		t.Errorf("fallback selected %q, want default", got.Name)
	}
}

func TestResolveAccountMissingFields(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "a@x.com"},
	}}
	if _, err := resolveAccount(cfg, ""); err == nil {
		t.Fatal("expected error for missing password/owner, got nil")
	}
}

func TestLoadConfigMissing(t *testing.T) {
	_, err := loadConfig(filepath.Join(t.TempDir(), "nope.json"))
	if err == nil {
		t.Fatal("expected errNoConfig, got nil")
	}
}

func TestLoadConfigRoundTrip(t *testing.T) {
	path := writeConfig(t, Config{Accounts: map[string]Account{
		"default": {JID: "a@x.com", Password: "p", Owner: "o@x.com"},
	}})
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if _, ok := cfg.Accounts["default"]; !ok {
		t.Error("default account not loaded")
	}
}

// TestRoomConfigParsing covers the #106 format: an array of objects, strictly
// validated, with the retired spellings rejected rather than ignored.
func TestRoomConfigParsing(t *testing.T) {
	// Per-room trigger and reactions overrides, plus the error room folded in as
	// a typed entry.
	var cfg Config
	body := `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x",
		"rooms":[
		  {"jid":"a@muc.x"},
		  {"jid":"b@muc.x","trigger":"bob","reactions":true},
		  {"jid":"errors@muc.x","role":"error"}
		]}}}`
	if err := json.Unmarshal([]byte(body), &cfg); err != nil {
		t.Fatalf("object form: %v", err)
	}
	got, err := resolveAccount(&cfg, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if len(got.Rooms) != 2 || got.Rooms[0] != "a@muc.x" || got.Rooms[1] != "b@muc.x" {
		t.Errorf("Rooms = %v, want [a@muc.x b@muc.x]", got.Rooms)
	}
	if got.ErrorRoom != "errors@muc.x" {
		t.Errorf("ErrorRoom = %q, want errors@muc.x", got.ErrorRoom)
	}
	// The default trigger (the nick, here the JID localpart) applies only to the
	// room that did not override it.
	if trig := got.TriggerFor("a@muc.x"); trig != "pi" {
		t.Errorf("TriggerFor(a@muc.x) = %q, want pi (account default)", trig)
	}
	if trig := got.TriggerFor("b@muc.x"); trig != "bob" {
		t.Errorf("TriggerFor(b@muc.x) = %q, want bob (room override)", trig)
	}
	if !got.ReactionsFor("b@muc.x") {
		t.Error("ReactionsFor(b@muc.x) = false, want the room override true")
	}
	if got.ReactionsFor("a@muc.x") {
		t.Error("ReactionsFor(a@muc.x) = true, want the account default false")
	}
	// An explicit false is distinguishable from unset.
	cfg2 := Config{Accounts: map[string]Account{"default": {JID: "pi@x", Password: "p", Owner: "o@x",
		RoomReactions: true, Rooms: roomList{{JID: "a@muc.x", Reactions: boolPtr(false)}}}}}
	got2, err := resolveAccount(&cfg2, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got2.ReactionsFor("a@muc.x") {
		t.Error("explicit reactions:false did not override the account default of true")
	}
}

func boolPtr(v bool) *bool { return &v }

// TestRoomConfigRejectsRetiredFormats pins the loud-failure contract: silently
// resolving these to no rooms would leave a MUC account outside its room.
func TestRoomConfigRejectsRetiredFormats(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"singular string", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","room":"a@muc.x"}}}`},
		{"singular errorRoom", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","errorRoom":"errors@muc.x"}}}`},
		{"array of strings", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","rooms":["a@muc.x"]}}}`},
		{"object instead of array", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","rooms":{"jid":"a@muc.x"}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			err := json.Unmarshal([]byte(tc.body), &cfg)
			if err == nil {
				_, err = resolveAccount(&cfg, "")
			}
			if err == nil {
				t.Fatalf("%s resolved without error", tc.name)
			}
		})
	}
}

// TestRoomConfigValidation covers the per-entry rules that each take an account
// out of a room it believes it is in.
func TestRoomConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing jid", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","rooms":[{},{"jid":"a@muc.x"}]}}}`},
		{"unknown key", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","rooms":[{"jid":"a@muc.x","ambient":"none"}]}}}`},
		{"unknown role", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","rooms":[{"jid":"a@muc.x","role":"readonly"}]}}}`},
		{"two error rooms", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","rooms":[{"jid":"a@muc.x","role":"error"},{"jid":"b@muc.x","role":"error"}]}}}`},
		{"duplicate jid", `{"accounts":{"default":{"jid":"pi@x","password":"p","owner":"o@x","rooms":[{"jid":"a@muc.x"},{"jid":"a@muc.x"}]}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			err := json.Unmarshal([]byte(tc.body), &cfg)
			if err == nil {
				_, err = resolveAccount(&cfg, "")
			}
			if err == nil {
				t.Fatalf("%s resolved without error", tc.name)
			}
		})
	}
}

func TestResolveAccountErrorRoom(t *testing.T) {
	got, err := resolveAccount(&Config{Accounts: map[string]Account{
		"default": {JID: "pi@x", Password: "p", Owner: "o@x",
			Rooms: roomList{{JID: " errors@muc.x ", Role: "error"}}},
	}}, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.ErrorRoom != "errors@muc.x" {
		t.Errorf("ErrorRoom = %q, want %q", got.ErrorRoom, "errors@muc.x")
	}
	// The error room is write-only: it must not appear in the joined/readable set.
	if got.RoomMode() {
		t.Error("an error room must not put the account in room mode")
	}
	for _, r := range got.Rooms {
		if r == got.ErrorRoom {
			t.Fatal("error room leaked into Rooms")
		}
	}
}

func TestSessionStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.json")
	t.Setenv("PI_MSG_CONFIG", cfg)

	if got := loadSessionState("slippy"); got != "" {
		t.Fatalf("no state saved, got %q", got)
	}
	var logged []string
	logf := func(level, msg string) { logged = append(logged, level+": "+msg) }

	saveSessionState(logf, "slippy", "/some/path/session.jsonl")
	if got := loadSessionState("slippy"); got != "/some/path/session.jsonl" {
		t.Errorf("after save, load = %q", got)
	}
	if len(logged) != 0 {
		t.Errorf("unexpected warnings: %v", logged)
	}
	// Per-account isolation.
	if got := loadSessionState("beltino"); got != "" {
		t.Errorf("different account read %q, want empty", got)
	}
}

func TestReplayWindowMarkers(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.json")
	t.Setenv("PI_MSG_CONFIG", cfg)
	ts := time.Date(2026, 8, 11, 12, 34, 56, 0, time.UTC)
	var logged []string
	logf := func(level, msg string) { logged = append(logged, level+": "+msg) }

	// Nothing written → no window, and reads are empty.
	if start, ok := replayWindowStart("slippy"); ok || !start.IsZero() {
		t.Fatalf("no markers, got (%v,%v)", start, ok)
	}
	if got := readSwapStart("slippy"); got != "" {
		t.Fatalf("no swapstart, got %q", got)
	}

	// swapstart is one-shot: read once, consumed.
	markSwapStart(logf, "slippy", ts)
	if got := readSwapStart("slippy"); got != ts.UTC().Format(time.RFC3339) {
		t.Errorf("swapstart read = %q", got)
	}
	if got := readSwapStart("slippy"); got != "" {
		t.Errorf("swapstart should be consumed on first read, got %q", got)
	}

	// lastout is persistent: read does not consume it.
	markLastOut(logf, "slippy", ts)
	if got := readLastOut("slippy"); got != ts.UTC().Format(time.RFC3339) {
		t.Errorf("lastout read = %q", got)
	}
	if got := readLastOut("slippy"); got != ts.UTC().Format(time.RFC3339) {
		t.Errorf("lastout should persist across reads, got %q", got)
	}

	// lastin is the inbound counterpart (#94): persistent, and absent before the
	// first message is handled.
	if _, ok := readLastIn("slippy"); ok {
		t.Error("lastin should be absent before any inbound message")
	}
	markLastIn(logf, "slippy", ts)
	if got, ok := readLastIn("slippy"); !ok || !got.UTC().Equal(ts) {
		t.Errorf("lastin read = (%v,%v), want %v", got, ok, ts)
	}

	// replayWindowStart prefers swapstart over lastout.
	later := ts.Add(5 * time.Minute)
	markSwapStart(logf, "slippy", later)
	markLastOut(logf, "slippy", ts)
	start, ok := replayWindowStart("slippy")
	if !ok || !start.UTC().Equal(later) {
		t.Errorf("replayWindowStart = (%v,%v), want swapstart %v", start, ok, later)
	}

	// Once the swapstart is consumed, lastout is the fallback.
	markLastOut(logf, "slippy", later)
	start, ok = replayWindowStart("slippy")
	if !ok || !start.UTC().Equal(later) {
		t.Errorf("replayWindowStart fallback = (%v,%v), want lastout %v", start, ok, later)
	}

	// Invalid swapstart is treated as absent and consumed.
	if err := os.WriteFile(windowMarkerPath("slippy", "swapstart"), []byte("bogus\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := readSwapStart("slippy"); got != "" {
		t.Errorf("invalid swapstart should be ignored+consumed, got %q", got)
	}
	if len(logged) != 0 {
		t.Errorf("unexpected warnings: %v", logged)
	}
}

// TestBusyMarkerTracksWorkInFlight covers the per-account busy marker that the
// fleet deploy reads: present while a run streams or a background process
// runs, absent once the agent is idle, and cleared at startup after a previous
// process died mid-run.
func TestBusyMarkerTracksWorkInFlight(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PI_MSG_CONFIG", filepath.Join(dir, "config.json"))
	acct := ResolvedAccount{Name: "slippy", Owner: "zach@x"}
	marker := busyMarkerPath(acct.Name)

	clearBusyMarker(acct.Name)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("marker should start absent, stat err = %v", err)
	}

	b := NewBridge(acct, false)
	b.setStreaming(true)
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("busy marker missing while a run streams: %v", err)
	}
	b.setStreaming(false)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("busy marker should be gone once idle")
	}

	// A background process alone counts as busy. b.xmpp is nil here and the
	// marker must not depend on presence.
	b.setBgProcesses(1)
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("busy marker missing with a background process running: %v", err)
	}
	b.setStreaming(true) // a run starts while the process still runs
	b.setStreaming(false)
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("marker dropped while a background process still runs: %v", err)
	}
	b.setBgProcesses(0)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("busy marker should be gone with no work in flight")
	}

	// A stale marker from a process killed mid-run is removed at startup.
	markBusy(nil, acct.Name, true)
	clearBusyMarker(acct.Name)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("clearBusyMarker should remove the stale marker")
	}
}

func TestStartDirectiveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.json")
	t.Setenv("PI_MSG_CONFIG", cfg)

	// Nothing written → no directive, and the call is a no-op.
	if kind, payload := loadStartDirective("slippy"); kind != "" || payload != "" {
		t.Fatalf("no directive written, got kind=%q payload=%q", kind, payload)
	}
	var logged []string
	logf := func(level, msg string) { logged = append(logged, level+": "+msg) }

	writeStartDirective(logf, "slippy", StartProactive)
	if kind, payload := loadStartDirective("slippy"); kind != StartProactive || payload != "" {
		t.Errorf("after write, load = (%q,%q), want (%q,\"\")", kind, payload, StartProactive)
	}
	// The directive is one-shot: consumed when read.
	if kind, _ := loadStartDirective("slippy"); kind != "" {
		t.Errorf("directive should be consumed on first read, got %q", kind)
	}

	// Idle round-trips too.
	writeStartDirective(logf, "slippy", StartIdle)
	if kind, _ := loadStartDirective("slippy"); kind != StartIdle {
		t.Errorf("idle directive, load = %q", kind)
	}

	// Invalid contents are treated as absent and consumed (no error).
	if err := os.WriteFile(startDirectivePath("slippy"), []byte("bogus\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if kind, _ := loadStartDirective("slippy"); kind != "" {
		t.Errorf("invalid directive should be ignored, got %q", kind)
	}
	if len(logged) != 0 {
		t.Errorf("unexpected warnings: %v", logged)
	}
}

// TestPromptDirective covers the invocation-time initial prompt payload shape
// (pi-msg#35): writePromptDirective → loadStartDirective must yield the prompt
// kind with the exact task text, survive a multi-line body, be one-shot, and
// treat blank payloads as absent (no directive, no crash).
func TestPromptDirective(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.json")
	t.Setenv("PI_MSG_CONFIG", cfg)

	var logged []string
	logf := func(level, msg string) { logged = append(logged, level+": "+msg) }

	// Round-trip a single-line task.
	task := "resolve zachpmanson/pi-msg#35 and open a PR"
	writePromptDirective(logf, "slippy", task)
	kind, payload := loadStartDirective("slippy")
	if kind != StartPrompt {
		t.Fatalf("kind = %q, want %q", kind, StartPrompt)
	}
	if payload != task {
		t.Errorf("payload = %q, want %q", payload, task)
	}

	// One-shot: consumed on first read, like the enum directives.
	if kind, _ := loadStartDirective("slippy"); kind != "" {
		t.Errorf("prompt directive should be consumed on first read, got %q", kind)
	}

	// A multi-line task body survives intact.
	multi := "resolve issue #1:\n  - run the tests\n  - push the branch"
	writePromptDirective(logf, "slippy", multi)
	kind, payload = loadStartDirective("slippy")
	if kind != StartPrompt || payload != multi {
		t.Errorf("multi-line round-trip = (%q,%q), want (%q,%q)", kind, payload, StartPrompt, multi)
	}

	// A file whose payload is all whitespace is treated as absent (consumed).
	if err := os.WriteFile(startDirectivePath("slippy"), []byte(StartPrompt+"\n   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if kind, payload := loadStartDirective("slippy"); kind != "" || payload != "" {
		t.Errorf("blank prompt payload should be absent, got (%q,%q)", kind, payload)
	}

	// writePromptDirective refuses blank bodies with a warning, no file.
	writePromptDirective(logf, "slippy", "   ")
	if kind, _ := loadStartDirective("slippy"); kind != "" {
		t.Errorf("blank writePromptDirective should not write a directive, got %q", kind)
	}
	if len(logged) != 1 || !strings.Contains(logged[0], "empty prompt payload") {
		t.Errorf("expected one empty-payload warning, got %v", logged)
	}
}

func TestResolveAccountCreditWatch(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com",
			CreditWatch: &CreditWatch{MinBelowUsd: 2}},
	}}
	got, err := resolveAccount(cfg, "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.MinCreditUsd != 2 {
		t.Fatalf("MinCreditUsd = %v, want 2", got.MinCreditUsd)
	}
}

func TestResolveAccountCreditWatchDisabled(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {JID: "j@chat.example.com", Password: "pw", Owner: "zach@chat.example.com"},
	}}
	got, err := resolveAccount(cfg, "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.MinCreditUsd != 0 {
		t.Fatalf("MinCreditUsd = %v, want 0", got.MinCreditUsd)
	}
}

func TestMAMMarkers(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PI_MSG_CONFIG", filepath.Join(dir, "config.json"))
	ts := time.Date(2026, 9, 15, 3, 4, 5, 0, time.UTC)

	if _, ok := readMAMSeen("slippy"); ok {
		t.Fatal("no marker written, but readMAMSeen reported one")
	}

	markMAMSeen(nil, "slippy", ts)
	got, ok := readMAMSeen("slippy")
	if !ok || !got.Equal(ts) {
		t.Fatalf("readMAMSeen = (%v,%v), want %v", got, ok, ts)
	}
	// Persistent: a read must not consume the marker.
	if again, ok := readMAMSeen("slippy"); !ok || !again.Equal(ts) {
		t.Fatalf("marker not persistent: (%v,%v)", again, ok)
	}
	// Accounts are namespaced.
	if _, ok := readMAMSeen("peppy"); ok {
		t.Error("marker leaked across accounts")
	}
}

func TestResolveAccountMAM(t *testing.T) {
	on := &Config{Accounts: map[string]Account{
		"default":     {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com"},
		"explicitOn":  {JID: "pi1@chat.example.com", Password: "pw", Owner: "zach@chat.example.com", MAM: boolPtr(true)},
		"explicitOff": {JID: "pi2@chat.example.com", Password: "pw", Owner: "zach@chat.example.com", MAM: boolPtr(false)},
	}}
	tests := []struct {
		name string
		want bool
	}{
		{"default", true}, // absent means enabled
		{"explicitOn", true},
		{"explicitOff", false},
	}
	for _, tc := range tests {
		got, err := resolveAccount(on, tc.name)
		if err != nil {
			t.Fatalf("resolve %s: %v", tc.name, err)
		}
		if got.MAM != tc.want {
			t.Errorf("account %s: MAM = %v, want %v", tc.name, got.MAM, tc.want)
		}
	}
}

// An explicit false must survive through JSON too (the config is the only place
// the opt-out is expressed).
func TestMAMConfigRoundTrip(t *testing.T) {
	path := writeConfig(t, Config{Accounts: map[string]Account{
		"default": {JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com", MAM: boolPtr(false)},
	}})
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got, err := resolveAccount(cfg, "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.MAM {
		t.Error("explicit mam:false did not survive load+resolve")
	}
}
