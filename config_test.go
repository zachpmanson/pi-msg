package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
