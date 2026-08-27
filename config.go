package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// roomList is the set of MUC JIDs from the "room" config field. It accepts
// either a single JID string ("room": "a@muc…") or an array of JID strings
// ("room": ["a@muc…", "b@muc…"]) so older single-room configs keep working.
type roomList []string

func (r *roomList) UnmarshalJSON(b []byte) error {
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		*r = roomList{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return fmt.Errorf("\"room\" must be a JID string or an array of JID strings")
	}
	*r = many
	return nil
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
	// Model is the model pattern to launch pi with (e.g.
	// "anthropic/claude-sonnet-latest"). Optional.
	Model string `json:"model,omitempty"`
	// Workdir is the working directory for the pi agent. Defaults to the
	// process cwd.
	Workdir string `json:"workdir,omitempty"`

	// Room, when set, additionally joins these bare MUC JIDs (e.g.
	// "team@muc.chat.example.com") and relays group chat. Accepts a single JID
	// string or an array of JID strings. The owner can still DM the bot 1:1 in
	// either mode; each reply goes back to whichever channel the message arrived
	// on.
	Room roomList `json:"room,omitempty"`
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
	// ErrorRoom, when set, is a bare MUC JID (e.g.
	// "errors@muc.chat.example.com") used as a write-only dumping ground for
	// dropped/unrouteable agent replies. The bridge joins it at the XMPP layer
	// so it can send groupchat there, but it is deliberately NOT exposed to the
	// agent (not a readable room, not in the reply/send allowlist), so the
	// agents can't read each other's rejected output or act on it. If unset,
	// unrouteable replies fall back to the owner's 1:1.
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
	Rooms         []string
	Nick          string
	RoomTrigger   string
	UploadService string
	PingInterval  time.Duration
	Avatar        string
	ErrorRoom     string
	MinCreditUsd  float64
}

// RoomMode reports whether this account operates in MUC (group-chat) mode.
func (a ResolvedAccount) RoomMode() bool { return len(a.Rooms) > 0 }

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
// alongside the config as <config-dir>/<account>.<kind>. Two kinds exist:
// "swapstart" (one-shot, written on graceful shutdown) and "lastout"
// (persistent floor, updated on every outbound message).
func windowMarkerPath(acct, kind string) string {
	return filepath.Join(filepath.Dir(configPath()), acct+"."+kind)
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
		return nil, fmt.Errorf("pi-msg: config at %s is not valid JSON: %w", path, err)
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

	var rooms []string
	seen := make(map[string]bool)
	for _, rm := range acct.Room {
		rm = strings.TrimSpace(rm)
		if rm == "" || seen[rm] {
			continue
		}
		seen[rm] = true
		rooms = append(rooms, rm)
	}

	nick := acct.Nick
	if nick == "" {
		nick = localpart(acct.JID)
	}
	trigger := acct.RoomTrigger
	if trigger == "" {
		trigger = nick
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
		Nick:          nick,
		RoomTrigger:   trigger,
		UploadService: strings.TrimSpace(acct.UploadService),
		PingInterval:  pingInterval,
		Avatar:        strings.TrimSpace(acct.Avatar),
		ErrorRoom:     strings.TrimSpace(acct.ErrorRoom),
		MinCreditUsd:  maxCreditUsd(acct.CreditWatch),
	}, nil
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
