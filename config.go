package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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
	Model         string
	Workdir       string
	Rooms         []string
	Nick          string
	RoomTrigger   string
	UploadService string
}

// RoomMode reports whether this account operates in MUC (group-chat) mode.
func (a ResolvedAccount) RoomMode() bool { return len(a.Rooms) > 0 }

const (
	defaultAccount  = "default"
	defaultResource = "pi-msg"
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

	return ResolvedAccount{
		Name:          name,
		JID:           acct.JID,
		Password:      acct.Password,
		Owner:         acct.Owner,
		Service:       service,
		Resource:      resource,
		ToolActivity:  acct.ToolActivity,
		Model:         acct.Model,
		Workdir:       acct.Workdir,
		Rooms:         rooms,
		Nick:          nick,
		RoomTrigger:   trigger,
		UploadService: strings.TrimSpace(acct.UploadService),
	}, nil
}

// accountNames returns the configured account names (unsorted).
func accountNames(cfg *Config) []string {
	names := make([]string, 0, len(cfg.Accounts))
	for n := range cfg.Accounts {
		names = append(names, n)
	}
	return names
}
