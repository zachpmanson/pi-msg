package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// roomList is the "rooms" config field: the MUC rooms to join, each with its
// own addressing rules (issue #106). The format is an array of objects:
//
//	"rooms": [
//	  {"jid": "team@muc.example.com"},
//	  {"jid": "chatter@muc.example.com", "trigger": "pi"},
//	  {"jid": "errors@muc.example.com", "role": "error"}
//	]
//
// Only this form is accepted — the older single-JID string and array-of-strings
// spellings are rejected at load time with the replacement spelling in the
// message, because silently resolving them to no rooms would take a MUC account
// out of its room without saying so.
//
// There is deliberately no buffering policy here. A room message either
// addresses this agent (a turn) or it is not part of its world at all: the
// ambient buffer was removed in #106, so there is nothing per-room left to
// configure about it.
type roomList []roomSpec

// roomSpec is one entry of the "rooms" array, as written in the config file.
// Keys are validated strictly: an unknown key is a load error rather than being
// ignored, so a typo cannot silently disable a room's rules.
type roomSpec struct {
	// JID is the bare MUC JID to join.
	JID string `json:"jid"`
	// Role is "error" for the write-only error room, or empty for a normal room.
	// At most one entry may be the error room (see ErrorRoom).
	Role string `json:"role,omitempty"`
	// Trigger overrides the account-level roomTrigger for this room. A pointer so
	// that an explicit empty string is distinguishable from an absent key (and
	// rejected: see resolveRooms).
	Trigger *string `json:"trigger,omitempty"`
	// Reactions overrides the account-level roomReactions for this room. Only
	// present when set, so an explicit false is distinguishable from unset.
	Reactions *bool `json:"reactions,omitempty"`
}

// roomRoles are the accepted values of roomSpec.Role.
const (
	roomRoleNormal = ""
	roomRoleError  = "error"
)

func (r *roomList) UnmarshalJSON(b []byte) error {
	// An explicit null must not resolve to "no rooms": that is exactly the silent
	// failure this strict parser exists to prevent — an MUC account sitting
	// outside its room, with the config looking deliberately empty (#106 review).
	if string(bytes.TrimSpace(b)) == "null" {
		return errors.New("\"rooms\" must be an array of objects, not null — remove the key entirely for a 1:1 account, or list the rooms to join")
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("\"rooms\" must be an array of objects, e.g. [{\"jid\": \"team@muc.example.com\"}]")
	}
	out := make(roomList, 0, len(raw))
	for i, entry := range raw {
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(entry, &keys); err != nil {
			return fmt.Errorf("rooms[%d] must be an object, e.g. {\"jid\": \"team@muc.example.com\"}", i)
		}
		for k := range keys {
			switch k {
			case "jid", "role", "trigger", "reactions":
			default:
				return fmt.Errorf("rooms[%d]: unknown key %q (allowed: jid, role, trigger, reactions)", i, k)
			}
		}
		var spec roomSpec
		if err := json.Unmarshal(entry, &spec); err != nil {
			return fmt.Errorf("rooms[%d]: %w", i, err)
		}
		out = append(out, spec)
	}
	*r = out
	return nil
}

// RoomSpec is one resolved room: the JID plus the addressing rules actually in
// force for it (account defaults already applied).
type RoomSpec struct {
	JID     string
	Trigger string
	// Reactions is the effective XEP-0444 setting for this room.
	Reactions bool
	// reactionsSet records that the room carried an explicit value, so the
	// account default is applied only when it did not.
	reactionsSet bool
}

// Account is one XMPP account the bridge can connect as, as stored in the
// config file. Only jid/password/owner are required; the rest have defaults.
type Account struct {
	// JID is the bare JID of the bot account, e.g. "pi@chat.example.com".
	JID string `json:"jid"`
	// Password for the bot account.
	Password string `json:"password"`
	// Owner is the JID of the human this account relays to. In 1:1 mode it is
	// also the only JID whose messages drive the agent. In room mode it is the
	// canonical (trusted) participant.
	Owner string `json:"owner"`
	// Service is the connection endpoint. Defaults to "<jid-domain>:5222". A
	// leading "xmpp://" is tolerated and stripped; a "wss://…" value connects
	// via XMPP-over-WebSocket.
	Service string `json:"service,omitempty"`
	// Resource is the XMPP client-session label. Defaults to "pi-msg".
	Resource string `json:"resource,omitempty"`
	// ToolActivity mirrors a one-line notice each time a tool starts.
	ToolActivity bool `json:"toolActivity,omitempty"`
	// Reactions, when true, enables XEP-0444 emoji reactions on 1:1 owner
	// messages: the run lifecycle maps to 👀 (picked up) / ✅ (done) / ⛔
	// (aborted), and the agent may react deliberately via a "react: <emoji>"
	// line. Off by default so it doesn't double up with read receipts + presence.
	Reactions bool `json:"reactions,omitempty"`
	// RoomReactions, when true, enables XEP-0444 emoji reactions on room
	// messages (both owner and addressed non-owner commentary). Independent of
	// the 1:1 reactions flag — you can opt into one, both, or neither.
	RoomReactions bool `json:"roomReactions,omitempty"`
	// BeforeAgentStartText, when set, is injected into the agent's system prompt
	// at the start of every turn (via the companion extension's
	// before_agent_start hook). Re-applying it each turn is the point: a steer
	// survives long sessions where a one-off instruction fades, the same property
	// the equivalent Claude Code UserPromptSubmit hook relies on. Empty means no
	// injection. A shell-command variant (beforeAgentStartHook) is not built yet.
	BeforeAgentStartText string `json:"beforeAgentStartText,omitempty"`
	// Model is the model pattern to launch pi with (e.g.
	// "anthropic/claude-sonnet-latest"). Optional.
	Model string `json:"model,omitempty"`
	// Workdir is the working directory for the pi agent. Defaults to the
	// process cwd.
	Workdir string `json:"workdir,omitempty"`

	// Rooms, when set, additionally joins these bare MUC JIDs (e.g.
	// "team@muc.chat.example.com") and relays group chat. An array of objects,
	// each {"jid": …, "trigger": …, "reactions": …, "role": "error"}. The owner
	// can still DM the bot 1:1 in either mode; each reply goes back to whichever
	// channel the message arrived on.
	Rooms roomList `json:"rooms,omitempty"`
	// Room is the retired singular "room" field, retained only so a config still
	// using it fails with the replacement spelling instead of silently resolving
	// to no rooms.
	Room json.RawMessage `json:"room,omitempty"`
	// Nick is the occupant nickname used in the rooms. Defaults to the JID
	// localpart.
	Nick string `json:"nick,omitempty"`
	// RoomTrigger is the case-insensitive address prefix that makes a room
	// message a prompt for the agent (e.g. "pi" matches "pi: …" / "pi, …").
	// Defaults to Nick.
	RoomTrigger string `json:"roomTrigger,omitempty"`
	// UploadService is the XEP-0363 HTTP-upload component JID used for file
	// transfer. Optional; if unset the bridge probes "upload.<domain>" and
	// "httpupload.<domain>".
	UploadService string `json:"uploadService,omitempty"`
	// PingInterval is how often to send an XEP-0199 keepalive ping to the
	// server (and, in room mode, an XEP-0410 self-ping to each joined room) to
	// detect silent disconnects. A Go duration string ("60s", "2m"). Defaults
	// to "60s"; "0" disables keepalive.
	PingInterval string `json:"pingInterval,omitempty"`
	// ErrorRoom is the RETIRED singular spelling. It is parsed only so that
	// resolveAccount can reject it by name: the error room is now a "rooms" entry
	// with "role": "error", and quietly ignoring the old key would leave dropped
	// agent replies with nowhere to go. A non-empty value here is a load error.
	ErrorRoom string `json:"errorRoom,omitempty"`
	// Avatar is a path to a local image (PNG/JPEG/GIF) published as the bot's
	// XEP-0153 vCard avatar on connect. Optional; a missing/invalid file is a
	// logged warning, not fatal.
	Avatar string `json:"avatar,omitempty"`
	// CreditWatch, when set, reports the remaining OpenRouter credit whenever
	// the agent runs /new (a fresh session). Only active when the configured
	// pi provider is OpenRouter (i.e. an openrouter api key is found in pi's
	// auth file).
	CreditWatch *CreditWatch `json:"creditWatch,omitempty"`
	// MAM controls XEP-0313 archive backfill on startup (pi-msg issue #84):
	// anything missed while the bridge was offline is fetched from the server's
	// archive and handed to the resumed session alongside the restart-swap
	// replay. **On by default** — a nil value means enabled; set `"mam": false`
	// to opt out. Requires the server to have an archive (ejabberd mod_mam);
	// without one the bridge logs a warning and falls back to the existing
	// delay-stanza replay, so enabling it can't break a server that lacks MAM.
	MAM *bool `json:"mam,omitempty"`
}

// CreditWatch configures the on-\/new OpenRouter credit report.
type CreditWatch struct {
	// MinBelowUsd is the remaining-credit floor in USD. If remaining credit
	// (total_credits - total_usage) is below this, the /new notice highlights
	// it as low. The report always shows the current remaining credit; this
	// only controls the alert emphasis.
	MinBelowUsd float64 `json:"minBelowUsd,omitempty"`
}

// Config is the on-disk config: an arbitrary number of named accounts.
// "default" is used when no account is selected.
type Config struct {
	Accounts map[string]Account `json:"accounts"`
}

// ResolvedAccount is a fully-resolved account ready to connect with, defaults
// applied. RoomMode reports whether any room was set.
type ResolvedAccount struct {
	Name          string
	JID           string
	Password      string
	Owner         string
	Service       string
	Resource      string
	ToolActivity  bool
	Reactions     bool
	RoomReactions bool
	Model         string
	Workdir       string
	// Rooms is the joined room JIDs (normal rooms only — the error room is not
	// in here, and must never be). It is derived from RoomSpecs at resolve time.
	Rooms []string
	// RoomSpecs is the per-room addressing rules, in config order. A room absent
	// from here falls back to the account-level values below.
	RoomSpecs     []RoomSpec
	Nick          string
	RoomTrigger   string
	UploadService string
	PingInterval  time.Duration
	// BeforeAgentStartText is the literal text injected (verbatim, after the
	// identity line) into the system prompt at the start of every turn; empty
	// means no injection.
	BeforeAgentStartText string
	Avatar               string
	ErrorRoom            string
	MinCreditUsd         float64
	MAM                  bool
}

// RoomMode reports whether this account operates in MUC (group-chat) mode.
func (a ResolvedAccount) RoomMode() bool { return len(a.Rooms) > 0 }

// TriggerFor returns the address prefix in force in room, falling back to the
// account-level trigger when the room carries no override (which includes the
// error room and any account built directly in tests).
func (a ResolvedAccount) TriggerFor(room string) string {
	bare := bareJid(room)
	for _, s := range a.RoomSpecs {
		if bareJid(s.JID) == bare && s.Trigger != "" {
			return s.Trigger
		}
	}
	return a.RoomTrigger
}

// ReactionsFor returns whether XEP-0444 reactions are enabled in room, falling
// back to the account-level setting.
func (a ResolvedAccount) ReactionsFor(room string) bool {
	bare := bareJid(room)
	for _, s := range a.RoomSpecs {
		if bareJid(s.JID) == bare {
			return s.Reactions
		}
	}
	return a.RoomReactions
}

const (
	defaultAccount  = "default"
	defaultResource = "pi-msg"
	// defaultPingInterval is the keepalive cadence when pingInterval is unset.
	defaultPingInterval = 60 * time.Second
)

// configPath returns the config file path: $PI_MSG_CONFIG or
// ~/.config/pi-msg/config.json.
func configPath() string {
	if p := os.Getenv("PI_MSG_CONFIG"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".config", "pi-msg", "config.json")
	}
	return filepath.Join(home, ".config", "pi-msg", "config.json")
}

// sessionStatePath returns the per-account session state file (the absolute
// path of the pi session to resume on the next launch), stored alongside the
// loadSeededContract reads the hash of the routing contract text that was last
// injected into this account's session, returning "" when none is recorded.
func loadSeededContract(acct string) string {
	raw, err := os.ReadFile(seededContractPath(acct))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// saveSeededContract records the contract hash so a later restart can tell
// whether the session's context still describes the rules the bridge applies.
// Errors are logged, not fatal.
func saveSeededContract(log func(level, msg string), acct, hash string) {
	p := seededContractPath(acct)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		if log != nil {
			log("warning", "contract seed state: mkdir: "+err.Error())
		}
		return
	}
	if err := os.WriteFile(p, []byte(strings.TrimSpace(hash)+"\n"), 0o600); err != nil {
		if log != nil {
			log("warning", "contract seed state: write: "+err.Error())
		}
	}
}

// seededContractPath is the file recording which contract text a session was
// seeded with: <config-dir>/<account>.contract.
func seededContractPath(acct string) string {
	return filepath.Join(filepath.Dir(configPath()), acct+".contract")
}

// config file as <config-dir>/<account>.session.
func sessionStatePath(acct string) string {
	return filepath.Join(filepath.Dir(configPath()), acct+".session")
}

// loadSessionState reads the persisted pi session file path for an account,
// returning "" when none is saved.
func loadSessionState(acct string) string {
	raw, err := os.ReadFile(sessionStatePath(acct))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// saveSessionState writes the account's pi session file path so a restart can
// resume it, or removes the state file when path is empty (defensive; the
// bridge never explicitly clears it). Errors are logged, not fatal.
func saveSessionState(log func(level, msg string), acct, path string) {
	p := sessionStatePath(acct)
	if path == "" {
		_ = os.Remove(p)
		return
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		if log != nil {
			log("warning", "session persistence: mkdir: "+err.Error())
		}
		return
	}
	if err := os.WriteFile(p, []byte(strings.TrimSpace(path)+"\n"), 0o600); err != nil {
		if log != nil {
			log("warning", "session persistence: write: "+err.Error())
		}
	}
}

// StartDirective is the per-restart choice of whether the resumed agent should
// proactively trigger a reply ("proactive") or stay silent ("idle"). The
// operator CLIs (deploy-service, persona-ctl) write it to a directive file
// before restarting; the bridge reads and consumes it once at startup.
//
// A third kind, StartPrompt, extends the same file: the directive carries an
// invocation-time initial prompt — the task an on-demand persona is spawned
// with — parsed by loadStartDirective and fired by Bridge.fireInitialPrompt.
const (
	StartProactive = "proactive" // resume + fire a volunteer turn (agent offers a line)
	StartIdle      = "idle"      // resume + stay silent (just the resumed presence)
	StartPrompt    = "prompt"    // fresh on-demand spawn + fire an initial task prompt (payload follows on later lines)
)

// startDirectivePath returns the per-account restart-directive file, stored
// alongside the session state as <config-dir>/<account>.start.
func startDirectivePath(acct string) string {
	return filepath.Join(filepath.Dir(configPath()), acct+".start")
}

// busyMarkerPath returns the per-account busy marker, stored alongside the
// session state as <config-dir>/<account>.busy. The file exists only while the
// account has work in flight (a run streaming, or a background process
// running). It is how `deploy-service pi-msg --proactive` tells which agents
// were busy at deploy time: the operator CLI reprompts those and leaves the
// idle rest silent.
func busyMarkerPath(acct string) string {
	return filepath.Join(filepath.Dir(configPath()), acct+".busy")
}

// markBusy creates or removes the per-account busy marker. Best-effort like the
// other state writers: errors are logged, never fatal.
func markBusy(log func(level, msg string), acct string, busy bool) {
	p := busyMarkerPath(acct)
	if !busy {
		_ = os.Remove(p)
		return
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		if log != nil {
			log("warning", "busy marker: mkdir: "+err.Error())
		}
		return
	}
	if err := os.WriteFile(p, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o600); err != nil {
		if log != nil {
			log("warning", "busy marker: write: "+err.Error())
		}
	}
}

// clearBusyMarker removes a stale busy marker at startup. A bridge killed
// mid-run leaves one behind, and the account is idle until its next run starts,
// so an uncleared marker would claim it was busy to a later deploy.
func clearBusyMarker(acct string) {
	_ = os.Remove(busyMarkerPath(acct))
}

// loadStartDirective reads and consumes the per-account restart directive,
// returning the directive kind ("", "proactive", "idle", "prompt") and, for
// the prompt kind, its payload (the invocation-time initial prompt). The
// directive is a one-shot handoff from the operator CLI: it is removed once
// read so it never leaks into a later, unrelated restart. Invalid contents are
// treated as absent and the file is removed.
//
// File format — first line is the kind, a prompt payload follows on the
// remaining lines (so a multi-line task body survives one write):
//
//	proactive\n
//	idle\n
//	prompt\n
//	<any task text, possibly spanning lines>\n
func loadStartDirective(acct string) (kind, payload string) {
	p := startDirectivePath(acct)
	raw, err := os.ReadFile(p)
	if err != nil {
		return "", ""
	}
	_ = os.Remove(p)
	raw = []byte(strings.ReplaceAll(string(raw), "\r\n", "\n"))
	head, body, _ := strings.Cut(strings.TrimRight(string(raw), "\n"), "\n")
	kind = strings.TrimSpace(head)
	switch kind {
	case StartProactive:
		return StartProactive, ""
	case StartIdle:
		return StartIdle, ""
	case StartPrompt:
		payload = strings.TrimSpace(body)
		if payload == "" {
			return "", "" // empty prompt payload = no directive
		}
		return StartPrompt, payload
	}
	return "", ""
}

// writeStartDirective records a restart directive so the next launch behaves
// accordingly. Errors are logged, not fatal.
func writeStartDirective(log func(level, msg string), acct string, v string) {
	p := startDirectivePath(acct)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		if log != nil {
			log("warning", "start directive: mkdir: "+err.Error())
		}
		return
	}
	if err := os.WriteFile(p, []byte(v+"\n"), 0o600); err != nil {
		if log != nil {
			log("warning", "start directive: write: "+err.Error())
		}
	}
}

// writePromptDirective records an invocation-time initial prompt so the next
// launch spawns a fresh on-demand persona with the task as its very first
// prompt (see loadStartDirective). Empty payloads are ignored. Errors are
// logged, not fatal.
func writePromptDirective(log func(level, msg string), acct, prompt string) {
	body := strings.TrimSpace(prompt)
	if body == "" {
		if log != nil {
			log("warning", "start directive: empty prompt payload ignored")
		}
		return
	}
	p := startDirectivePath(acct)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		if log != nil {
			log("warning", "start directive: mkdir: "+err.Error())
		}
		return
	}
	if err := os.WriteFile(p, []byte(StartPrompt+"\n"+body+"\n"), 0o600); err != nil {
		if log != nil {
			log("warning", "start directive: write: "+err.Error())
		}
	}
}

// windowMarkerPath returns the per-account replay-window marker file, stored
// alongside the config as <config-dir>/<account>.<kind>. Four kinds exist:
// "swapstart" (one-shot, written on graceful shutdown), "lastout" (persistent
// floor, updated on every outbound message), "mamseen" (the timestamp of the
// last completed MAM backfill) and "lastin" (persistent cursor, updated on
// every inbound message handed to the agent; issue #94).
func windowMarkerPath(acct, kind string) string {
	return filepath.Join(filepath.Dir(configPath()), acct+"."+kind)
}

// inboxPath returns the per-account durable inbound inbox
// (<config-dir>/<account>.inbox.jsonl), holding the inbound messages handed to
// pi that no settled run has acknowledged yet (issue #96).
func inboxPath(acct string) string {
	return filepath.Join(filepath.Dir(configPath()), acct+".inbox.jsonl")
}

// writeWindowMarker writes an RFC3339 window marker for an account. Best-effort
// like the session/start directive writers: errors are logged, never fatal.
func writeWindowMarker(log func(level, msg string), acct, kind string, t time.Time) {
	p := windowMarkerPath(acct, kind)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		if log != nil {
			log("warning", "replay window: mkdir: "+err.Error())
		}
		return
	}
	if err := os.WriteFile(p, []byte(t.UTC().Format(time.RFC3339)+"\n"), 0o600); err != nil {
		if log != nil {
			log("warning", "replay window: write: "+err.Error())
		}
	}
}

// markSwapStart records the instant the account went offline for a graceful
// restart. One-shot: the next launch reads and consumes it to open its replay
// window, so it never leaks into a later, unrelated restart.
func markSwapStart(log func(level, msg string), acct string, t time.Time) {
	writeWindowMarker(log, acct, "swapstart", t)
}

// markLastOut updates the persistent last-outbound floor — the time the bridge
// last emitted a chat message. Kept (never consumed) so an ungraceful crash can
// still bound its replay window: any inbound stamped after the last outbound
// cannot have been answered, hence was never processed.
func markLastOut(log func(level, msg string), acct string, t time.Time) {
	writeWindowMarker(log, acct, "lastout", t)
}

// readSwapStart reads and consumes the graceful-swap marker, returning its
// RFC3339 timestamp or "" when absent. Invalid contents are treated as absent
// and the file is removed.
func readSwapStart(acct string) string {
	p := windowMarkerPath(acct, "swapstart")
	raw, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	_ = os.Remove(p)
	v := strings.TrimSpace(string(raw))
	if _, err := time.Parse(time.RFC3339, v); err != nil {
		return ""
	}
	return v
}

// mamMarkerPath returns the per-account XEP-0313 marker: the archive timestamp
// of the most recent successful backfill. Stored alongside the config as
// <config-dir>/<account>.mamseen.
func mamMarkerPath(acct string) string {
	return windowMarkerPath(acct, "mamseen")
}

// readMAMSeen reads the persistent MAM backfill marker without consuming it.
func readMAMSeen(acct string) (time.Time, bool) {
	raw, err := os.ReadFile(mamMarkerPath(acct))
	if err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(string(raw)))
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// markMAMSeen records the archive timestamp of the most recent successful MAM
// backfill so the next launch queries only from there.
func markMAMSeen(log func(level, msg string), acct string, t time.Time) {
	writeWindowMarker(log, acct, "mamseen", t)
}

// markLastIn updates the persistent last-inbound cursor: the instant the bridge
// last handed an inbound message to the agent. It is the lower bound for a
// mid-session reconnect backfill (#94) — anything the running bridge handled
// live is at or before this instant, so it is never fetched twice.
func markLastIn(log func(level, msg string), acct string, t time.Time) {
	writeWindowMarker(log, acct, "lastin", t)
}

// readLastIn reads the persistent last-inbound cursor without consuming it.
func readLastIn(acct string) (time.Time, bool) {
	raw, err := os.ReadFile(windowMarkerPath(acct, "lastin"))
	if err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(string(raw)))
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// readLastOut reads the persistent last-outbound floor without consuming it.
func readLastOut(acct string) string {
	raw, err := os.ReadFile(windowMarkerPath(acct, "lastout"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// replayWindowStart resolves the inbound-replay window start at startup: the
// graceful-swap marker when present (consumed), else the last-outbound fallback
// (kept). Returns the resolved start time and whether a window is active.
func replayWindowStart(acct string) (time.Time, bool) {
	if s := readSwapStart(acct); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t, true
		}
	}
	if s := readLastOut(acct); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// errNoConfig is returned by loadConfig when the config file does not exist,
// so main can distinguish "not set up" from a real read/parse error.
var errNoConfig = errors.New("pi-msg: no config file")

// loadConfig reads and parses the config file. It returns errNoConfig
// (wrapped) if the file does not exist.
func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w at %s", errNoConfig, path)
		}
		return nil, fmt.Errorf("pi-msg: cannot read config at %s: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("pi-msg: config at %s is invalid: %w", path, err)
	}
	if cfg.Accounts == nil {
		return nil, fmt.Errorf("pi-msg: config at %s must have an \"accounts\" object", path)
	}
	return &cfg, nil
}

// defaultServiceFor derives the default XMPP service endpoint (host:port) from
// a bare JID's domain.
func defaultServiceFor(jid string) string {
	domain := jid
	if at := strings.IndexByte(jid, '@'); at >= 0 {
		domain = jid[at+1:]
	}
	return domain + ":5222"
}

// localpart returns the part of a bare JID before '@', or the whole string if
// there is no '@'.
func localpart(jid string) string {
	if at := strings.IndexByte(jid, '@'); at >= 0 {
		return jid[:at]
	}
	return jid
}

// resolveAccount selects and validates an account. Selection order:
// requested (if present in the file) -> "default". It returns a
// human-readable error on any misconfiguration.
func resolveAccount(cfg *Config, requested string) (ResolvedAccount, error) {
	if len(cfg.Accounts) == 0 {
		return ResolvedAccount{}, errors.New("pi-msg: config has no accounts")
	}

	name := defaultAccount
	if _, ok := cfg.Accounts[requested]; requested != "" && ok {
		name = requested
	}
	acct, ok := cfg.Accounts[name]
	if !ok {
		names := accountNames(cfg)
		if requested != "" {
			return ResolvedAccount{}, fmt.Errorf("pi-msg: account %q not found and no %q account defined", requested, defaultAccount)
		}
		return ResolvedAccount{}, fmt.Errorf("pi-msg: no %q account defined (set PI_MSG_ACCOUNT to one of: %s)", defaultAccount, strings.Join(names, ", "))
	}

	var missing []string
	if acct.JID == "" {
		missing = append(missing, "jid")
	}
	if acct.Password == "" {
		missing = append(missing, "password")
	}
	if acct.Owner == "" {
		missing = append(missing, "owner")
	}
	if len(missing) > 0 {
		return ResolvedAccount{}, fmt.Errorf("pi-msg: account %q is missing required field(s): %s", name, strings.Join(missing, ", "))
	}

	// The retired singular spellings are rejected explicitly rather than being
	// ignored: with "rooms" absent, a config still using "room" would resolve to
	// no rooms at all — a MUC account silently sitting outside its room.
	if len(acct.Room) > 0 {
		return ResolvedAccount{}, fmt.Errorf("pi-msg: account %q: \"room\" was replaced by \"rooms\", an array of objects — use \"rooms\": [{\"jid\": %s}]", name, string(acct.Room))
	}
	if len(acct.ErrorRoom) > 0 {
		return ResolvedAccount{}, fmt.Errorf("pi-msg: account %q: \"errorRoom\" (%q) was replaced by a \"rooms\" entry with \"role\": \"error\" — use \"rooms\": [{\"jid\": %q, \"role\": \"error\"}]", name, acct.ErrorRoom, acct.ErrorRoom)
	}

	rooms, specs, errorRoom, err := resolveRooms(acct)
	if err != nil {
		return ResolvedAccount{}, fmt.Errorf("pi-msg: account %q: %w", name, err)
	}

	nick := acct.Nick
	if nick == "" {
		nick = localpart(acct.JID)
	}
	trigger := acct.RoomTrigger
	if trigger == "" {
		trigger = nick
	}
	// Apply the account defaults to each room's resolved rules, so callers only
	// ever consult RoomSpecs. A room with no explicit "reactions" inherits the
	// account-level flag; an explicit false stays false.
	for i := range specs {
		if specs[i].Trigger == "" {
			specs[i].Trigger = trigger
		}
		if !specs[i].reactionsSet {
			specs[i].Reactions = acct.RoomReactions
		}
	}
	service := acct.Service
	if service == "" {
		service = defaultServiceFor(acct.JID)
	}
	resource := acct.Resource
	if resource == "" {
		resource = defaultResource
	}
	pingInterval := defaultPingInterval
	if s := strings.TrimSpace(acct.PingInterval); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return ResolvedAccount{}, fmt.Errorf("pi-msg: account %q has invalid pingInterval %q: %w", name, s, err)
		}
		pingInterval = d
	}

	return ResolvedAccount{
		Name:          name,
		JID:           acct.JID,
		Password:      acct.Password,
		Owner:         acct.Owner,
		Service:       service,
		Resource:      resource,
		ToolActivity:  acct.ToolActivity,
		Reactions:     acct.Reactions,
		RoomReactions: acct.RoomReactions,
		Model:         acct.Model,
		Workdir:       acct.Workdir,
		Rooms:         rooms,
		RoomSpecs:     specs,
		Nick:          nick,
		RoomTrigger:   trigger,
		UploadService: strings.TrimSpace(acct.UploadService),
		// Trimmed so a whitespace-only value counts as "not set" rather than
		// injecting a blank paragraph every turn.
		BeforeAgentStartText: strings.TrimSpace(acct.BeforeAgentStartText),
		PingInterval:         pingInterval,
		Avatar:               strings.TrimSpace(acct.Avatar),
		ErrorRoom:            errorRoom,
		MinCreditUsd:         maxCreditUsd(acct.CreditWatch),
		MAM:                  acct.MAM == nil || *acct.MAM,
	}, nil
}

// resolveRooms turns the configured "rooms" entries into the joined room list
// (normal rooms only), their resolved per-room rules, and the error room JID.
// Validation is strict because each of these failures takes an account out of a
// room it believes it is in: a missing jid, an unknown role, two error rooms, or
// the same JID listed twice.
func resolveRooms(acct Account) (rooms []string, specs []RoomSpec, errorRoom string, err error) {
	seen := make(map[string]bool, len(acct.Rooms))
	for i, spec := range acct.Rooms {
		jid := strings.TrimSpace(spec.JID)
		if jid == "" {
			return nil, nil, "", fmt.Errorf("rooms[%d] has no \"jid\"", i)
		}
		if seen[jid] {
			return nil, nil, "", fmt.Errorf("rooms[%d]: %q is listed twice", i, jid)
		}
		seen[jid] = true
		switch spec.Role {
		case roomRoleNormal, "normal":
			if spec.Trigger != nil && strings.TrimSpace(*spec.Trigger) == "" {
				// An empty trigger would leave the room with no way to address the
				// agent but the owner and broadcasts — almost certainly a mistake, and
				// silent if inherited, so say so. Omit the key to inherit the account
				// trigger instead.
				return nil, nil, "", fmt.Errorf("rooms[%d]: \"trigger\" is empty; omit it to inherit the account trigger, or give the word that addresses this agent in %q", i, jid)
			}
			rooms = append(rooms, jid)
			rs := RoomSpec{JID: jid}
			if spec.Trigger != nil {
				rs.Trigger = strings.TrimSpace(*spec.Trigger)
			}
			if spec.Reactions != nil {
				rs.Reactions, rs.reactionsSet = *spec.Reactions, true
			}
			specs = append(specs, rs)
		case roomRoleError:
			if errorRoom != "" {
				return nil, nil, "", fmt.Errorf("rooms[%d]: %q is a second error room; at most one entry may have \"role\": \"error\"", i, jid)
			}
			errorRoom = jid
		default:
			return nil, nil, "", fmt.Errorf("rooms[%d]: unknown role %q (allowed: \"normal\", \"error\")", i, spec.Role)
		}
	}
	return rooms, specs, errorRoom, nil
}

// maxCreditUsd extracts the remaining-credit floor from a CreditWatch config
// (0 / empty when the watch is disabled).
func maxCreditUsd(cw *CreditWatch) float64 {
	if cw == nil {
		return 0
	}
	return cw.MinBelowUsd
}

// accountNames returns the configured account names (unsorted).
func accountNames(cfg *Config) []string {
	names := make([]string, 0, len(cfg.Accounts))
	for n := range cfg.Accounts {
		names = append(names, n)
	}
	return names
}
