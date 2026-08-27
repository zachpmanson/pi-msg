package main

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
}

func TestResolveAccountRoomMode(t *testing.T) {
	cfg := &Config{Accounts: map[string]Account{
		"default": {
			JID: "pi@chat.example.com", Password: "pw", Owner: "zach@chat.example.com",
			Room: roomList{"team@muc.chat.example.com"}, Nick: "botpi",
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

func TestRoomConfigParsing(t *testing.T) {
	// "room" accepts a single string...
	var single Config
	if err := json.Unmarshal([]byte(`{"accounts":{"default":{"room":"a@muc.x"}}}`), &single); err != nil {
		t.Fatalf("string form: %v", err)
	}
	if got := []string(single.Accounts["default"].Room); len(got) != 1 || got[0] != "a@muc.x" {
		t.Errorf("string form Room = %v, want [a@muc.x]", got)
	}
	// ...or an array of strings.
	var multi Config
	if err := json.Unmarshal([]byte(`{"accounts":{"default":{"room":["a@muc.x","b@muc.x"]}}}`), &multi); err != nil {
		t.Fatalf("array form: %v", err)
	}
	if got := []string(multi.Accounts["default"].Room); len(got) != 2 || got[1] != "b@muc.x" {
		t.Errorf("array form Room = %v, want [a@muc.x b@muc.x]", got)
	}
	// resolveAccount dedupes/cleans and drives RoomMode + multiple Rooms.
	got, err := resolveAccount(&Config{Accounts: map[string]Account{
		"default": {JID: "pi@x", Password: "p", Owner: "o@x",
			Room: roomList{"a@muc.x", " a@muc.x ", "b@muc.x", ""}},
	}}, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if len(got.Rooms) != 2 || got.Rooms[0] != "a@muc.x" || got.Rooms[1] != "b@muc.x" {
		t.Errorf("resolved Rooms = %v, want [a@muc.x b@muc.x]", got.Rooms)
	}
}

func TestResolveAccountErrorRoom(t *testing.T) {
	got, err := resolveAccount(&Config{Accounts: map[string]Account{
		"default": {JID: "pi@x", Password: "p", Owner: "o@x",
			ErrorRoom: " errors@muc.x "},
	}}, "")
	if err != nil {
		t.Fatalf("resolveAccount: %v", err)
	}
	if got.ErrorRoom != "errors@muc.x" {
		t.Errorf("ErrorRoom = %q, want %q", got.ErrorRoom, "errors@muc.x")
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
