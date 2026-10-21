package pimsg

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

// Watches are process-local, survive reconnects, and are discarded on shutdown.
// replyWatchMu serializes successful-send registration with inbound observation,
// expiry, replacement, and queued delivery. Never acquire it while holding mu.
type replyWatch struct {
	conversation, stanzaID, text string
	room                         bool
	seconds                      float64
	started                      time.Time
	timer                        *time.Timer
	expired                      bool
}

func parseReplyTimeout(raw json.RawMessage) (time.Duration, float64, error) {
	if len(raw) == 0 {
		return 0, 0, nil
	}
	var seconds float64
	if err := json.Unmarshal(raw, &seconds); err != nil || string(raw) == "null" || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 || seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return 0, 0, fmt.Errorf("reply_timeout_seconds must be a finite positive number of seconds within the supported timer range")
	}
	d := time.Duration(seconds * float64(time.Second))
	if d <= 0 {
		return 0, 0, fmt.Errorf("reply_timeout_seconds is below timer precision")
	}
	return d, seconds, nil
}

// Called with replyWatchMu held, immediately after a successful send.
func (b *Bridge) armReplyWatch(to, stanzaID, text string, room bool, started time.Time, timeout time.Duration, seconds float64) {
	if b.replyWatches == nil {
		b.replyWatches = make(map[string]*replyWatch)
	}
	to = bareJid(to)
	if old := b.replyWatches[to]; old != nil {
		old.timer.Stop()
	}
	excerpt := []rune(strings.Join(strings.Fields(text), " "))
	if len(excerpt) > 300 {
		excerpt = append(excerpt[:300], '…')
	}
	w := &replyWatch{conversation: to, stanzaID: stanzaID, text: string(excerpt), room: room, seconds: seconds, started: started}
	b.replyWatches[to] = w
	w.timer = time.AfterFunc(timeout, func() { b.expireReplyWatch(w) })
}

func (b *Bridge) expireReplyWatch(w *replyWatch) {
	b.replyWatchMu.Lock()
	defer b.replyWatchMu.Unlock()
	if b.replyWatches[w.conversation] != w {
		return
	}
	w.expired = true
	// RPC prompts and synthetic-run flags belong to the bridge event loop, not
	// this timer goroutine. A coalesced wake scans all expired watches.
	b.wakeReplyWatches()
}

func (b *Bridge) wakeReplyWatches() {
	select {
	case b.replyWatchWake <- struct{}{}:
	default:
	}
}

// Called under replyWatchMu, including at settle. Keeping expired watches in the
// map lets a fresh reply or replacement invalidate a busy-agent queued nudge.
func (b *Bridge) deliverReplyWatch(w *replyWatch) {
	b.mu.Lock()
	blocked := b.streamingRun || b.shuttingDown || b.heartbeatRun || b.volunteered
	b.mu.Unlock()
	if blocked || !w.expired || b.replyWatches[w.conversation] != w {
		return
	}
	delete(b.replyWatches, w.conversation)
	text := fmt.Sprintf("[pi-msg: reply-timeout: No qualifying reply received from %s within %g seconds of stanza %s. Sent text: %q. Check conversation history to see whether unaddressed chatter answered it, then decide whether any follow-up is needed. Remaining silent is valid; this is not an instruction to send a message.]", w.conversation, w.seconds, w.stanzaID, w.text)
	// Reuse the heartbeat synthetic-run exemption: silence must not cause send
	// recovery, unanswered-message hints, or lifecycle reactions to an old sender.
	b.mu.Lock()
	b.heartbeatRun = true
	b.mu.Unlock()
	// Reserve the run before RPC's asynchronous agent_start acknowledgement so
	// simultaneous expiries cannot submit competing prompts or change routing.
	b.setStreaming(true)
	b.setLifecycleReactTarget("", "")
	b.setTurnDest(w.conversation, w.room)
	var prompt strings.Builder
	b.seedContracts(&prompt)
	prompt.WriteString(text)
	b.rpc.Prompt(prompt.String(), "")
	b.xmpp.SetPresence("dnd", "thinking…")
}

func (b *Bridge) flushReplyWatches() {
	b.sessionTransitionMu.Lock()
	defer b.sessionTransitionMu.Unlock()
	b.replyWatchMu.Lock()
	defer b.replyWatchMu.Unlock()
	for _, w := range b.replyWatches {
		b.deliverReplyWatch(w)
	}
}

// The transport calls this only for deduplicated fresh body-bearing messages,
// before owner-only DM dispatch and before room routing/classification.
func (b *Bridge) observeReplyMessage(m InboundMessage) {
	observed := time.Now()
	b.replyWatchMu.Lock()
	defer b.replyWatchMu.Unlock()
	conversation := bareJid(m.From)
	if m.Room != "" {
		conversation = bareJid(m.Room)
	}
	w := b.replyWatches[conversation]
	if w == nil || observed.Before(w.started) || m.Own || !m.Stamp.IsZero() || strings.TrimSpace(m.Body) == "" {
		return
	}
	if w.room && m.ReplyToID != w.stanzaID && !b.directReplyPing(m.Room, m.Body) {
		return
	}
	w.timer.Stop()
	delete(b.replyWatches, conversation)
}

// Aimed addressing only, not the bare-name mention or broadcast routing rules.
func (b *Bridge) directReplyPing(room, body string) bool {
	scan := stripUnquoted(strings.TrimSpace(body))
	for _, name := range []string{b.acct.TriggerFor(room), b.xmpp.ownNick(room)} {
		if name == "" {
			continue
		}
		for _, handle := range mentionsIn(scan) {
			if strings.EqualFold(handle, name) {
				return true
			}
		}
		if regexp.MustCompile(`(?i)(^|[^[:alnum:]_@-])` + regexp.QuoteMeta(name) + `[:,]`).MatchString(scan) {
			return true
		}
	}
	return false
}

func (b *Bridge) stopReplyWatches() {
	b.replyWatchMu.Lock()
	defer b.replyWatchMu.Unlock()
	for _, w := range b.replyWatches {
		w.timer.Stop()
	}
	b.replyWatches = nil
}
