package pimsg

import (
	"strings"
	"testing"
	"time"
)

func TestAwayTimeoutConfig(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        time.Duration
		bad         bool
	}{
		{name: "default", want: 20 * time.Minute},
		{name: "five minutes", value: "5m", want: 5 * time.Minute},
		{name: "duration", value: "1h30m", want: 90 * time.Minute},
		{name: "zero", value: "0", bad: true},
		{name: "zero unit", value: "0m", bad: true},
		{name: "negative", value: "-5m", bad: true},
		{name: "malformed", value: "five minutes", bad: true},
		{name: "no unit", value: "5", bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Accounts: map[string]Account{"default": {JID: "fixture@example.com", Owner: "owner@example.com", Password: "fixture-only", AwayTimeout: tc.value}}}
			loaded, err := loadConfig(writeConfig(t, cfg))
			if err != nil {
				t.Fatal(err)
			}
			got, err := resolveAccount(loaded, "")
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "awayTimeout") {
					t.Fatal("expected awayTimeout diagnostic", err)
				}
				return
			}
			if err != nil || got.AwayTimeout != tc.want {
				t.Fatalf("awayTimeout=%v want %v err=%v", got.AwayTimeout, tc.want, err)
			}
			if got.AutoreconcileTimeout != 0 || got.ResetSessionOnAway {
				t.Fatal("away timeout enabled automatic reset/cleanup")
			}
		})
	}
	cfg := &Config{Accounts: map[string]Account{"fixture": {JID: "fixture@example.com", Owner: "owner@example.com", Password: "fixture-only", AwayTimeout: "5m", AutoreconcileTimeout: "10m"}}}
	got, err := resolveAccount(cfg, "fixture")
	if err != nil || got.AwayTimeout != 5*time.Minute || got.AutoreconcileTimeout != 10*time.Minute {
		t.Fatal("independent away/cleanup thresholds not retained", got, err)
	}
}

func TestIdleTickAwayTimeout(t *testing.T) {
	for _, tc := range []struct {
		name               string
		threshold, elapsed time.Duration
		away               bool
	}{
		{"five minutes before", 5 * time.Minute, 4 * time.Minute, false},
		{"five minutes after", 5 * time.Minute, 6 * time.Minute, true},
		{"default before", 0, 6 * time.Minute, false},
		{"default just before", 0, 19 * time.Minute, false},
		{"default after", 0, 21 * time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := bgBridge()
			b.acct.AwayTimeout = tc.threshold
			b.idleSince = time.Now().Add(-tc.elapsed)
			b.idleTick()
			if b.awayAnnounced != tc.away || (b.presenceShow() == "away") != tc.away {
				t.Fatalf("away=%v show=%q want %v", b.awayAnnounced, b.presenceShow(), tc.away)
			}
			if b.acct.AutoreconcileTimeout != 0 || b.auto.running {
				t.Fatal("away threshold enabled automatic cleanup")
			}
			if ParticipationHorizon != 20*time.Minute {
				t.Fatal("room participation horizon changed")
			}
		})
	}
}
