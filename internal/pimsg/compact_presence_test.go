package pimsg

import "testing"

func TestCompactBackgroundPresence(t *testing.T) {
	for _, tt := range []struct {
		subagents int
		shells    int
		want      string
	}{
		{0, 0, "listening"},
		{1, 0, "1 subagent"},
		{2, 0, "2 subagents"},
		{0, 1, "1 shell"},
		{0, 2, "2 shells"},
		{1, 1, "1 subagent; 1 shell"},
		{1, 2, "1 subagent; 2 shells"},
		{2, 1, "2 subagents; 1 shell"},
		{2, 2, "2 subagents; 2 shells"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			b := bgBridge()
			b.bgSubagents, b.bgProcesses = tt.subagents, tt.shells
			b.announceSettledPresence()
			show := "dnd"
			if tt.subagents == 0 && tt.shells == 0 {
				show = ""
			}
			if b.xmpp.show != show || b.xmpp.presence != tt.want {
				t.Fatalf("presence = %q/%q, want %q/%q", b.xmpp.show, b.xmpp.presence, show, tt.want)
			}
		})
	}
}
