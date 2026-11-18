package pimsg

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUntaggedOwnerToFreeConfig(t *testing.T) {
	for _, setting := range []string{"", `,"untaggedOwnerToFree":false`, `,"untaggedOwnerToFree":true`} {
		t.Run(setting, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			raw := fmt.Sprintf(`{"accounts":{"default":{"jid":"pi@x.com","password":"pw","owner":"zach@x.com"%s},"other":{"jid":"other@x.com","password":"pw","owner":"zach@x.com"}}}`, setting)
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := loadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := resolveAccount(cfg, "default")
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Contains(setting, "true")
			if got.UntaggedOwnerToFree != want {
				t.Errorf("UntaggedOwnerToFree = %v, want %v", got.UntaggedOwnerToFree, want)
			}
			other, err := resolveAccount(cfg, "other")
			if err != nil {
				t.Fatal(err)
			}
			if other.UntaggedOwnerToFree {
				t.Error("fan-out option leaked to another account")
			}
		})
	}
}

func TestUntaggedOwnerToFreeRouting(t *testing.T) {
	cases := []struct {
		name                   string
		body                   string
		show                   string
		ran                    bool
		spoke                  time.Duration
		peer                   bool
		admitted               bool
		defaultWant, optInWant roomAction
	}{
		{"fresh", "status please", "", false, 0, false, false, actionNotOurs, actionCanonical},
		{"away", "status please", "away", true, 0, false, false, actionNotOurs, actionCanonical},
		{"extended away", "status please", "xa", true, 0, false, false, actionNotOurs, actionCanonical},
		{"settled", "status please", "", true, 0, false, false, actionNotOurs, actionNotOurs},
		{"working", "status please", "dnd", true, 0, false, false, actionNotOurs, actionNotOurs},
		{"fresh but working", "status please", "dnd", false, 0, false, false, actionNotOurs, actionNotOurs},
		{"recent participant", "status please", "", true, time.Minute, false, false, actionCanonical, actionCanonical},
		{"working participant", "status please", "dnd", true, time.Minute, false, false, actionCanonical, actionCanonical},
		{"away participant", "status please", "away", true, time.Minute, false, false, actionCanonical, actionCanonical},
		{"expired participant", "status please", "away", true, ParticipationHorizon + time.Minute, false, false, actionNotOurs, actionCanonical},
		{"durable replay", "status please", "", true, 0, false, true, actionCanonical, actionCanonical},
		{"explicit free", "@free report in", "away", true, 0, false, false, actionCanonical, actionCanonical},
		{"explicit free fresh", "@free report in", "", false, 0, false, false, actionCanonical, actionCanonical},
		{"explicit free working participant", "@free report in", "dnd", true, time.Minute, false, false, actionNotOurs, actionNotOurs},
		{"explicit everyone", "@everyone report in", "dnd", true, 0, false, false, actionCanonical, actionCanonical},
		{"explicit self", "pi: take this", "dnd", true, 0, false, false, actionCanonical, actionCanonical},
		{"explicit peer", "@peppy take this", "away", true, 0, false, false, actionNotOurs, actionNotOurs},
		{"untagged peer", "status please", "away", true, time.Minute, true, false, actionNotOurs, actionNotOurs},
		{"peer explicit free", "@free report in", "away", true, 0, true, false, actionCommentary, actionCommentary},
	}
	for _, c := range cases {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%v", c.name, enabled), func(t *testing.T) {
				b, room := roomWith("peppy")
				b.acct.UntaggedOwnerToFree = enabled
				setShow(b, c.show, c.ran)
				if c.spoke > 0 {
					spokeAgo(b, room, c.spoke)
				}
				got, _, _ := b.classify(InboundMessage{
					Room: room, Nick: "peppy", Body: c.body,
					FromOwner: !c.peer, Addressed: c.admitted,
				})
				want := c.defaultWant
				if enabled {
					want = c.optInWant
				}
				if got != want {
					t.Errorf("classify = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestUntaggedOwnerToFreeContract(t *testing.T) {
	b := roomBridge()
	if got := b.roomsContract(); !strings.Contains(got, "not other idle agents") {
		t.Errorf("default contract does not describe participant-only routing: %s", got)
	}
	before := b.contractHash()
	b.acct.UntaggedOwnerToFree = true
	if got := b.roomsContract(); !strings.Contains(got, "may reach idle agents or recent room participants") {
		t.Errorf("opt-in contract does not describe idle fan-out: %s", got)
	}
	if before == b.contractHash() {
		t.Error("routing change must update the seeded contract hash")
	}
}
