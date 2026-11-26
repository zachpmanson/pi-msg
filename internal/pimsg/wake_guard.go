package pimsg

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"slices"
	"sort"
	"strings"

	"mellium.im/xmpp/jid"
)

// peerPresence is live notification evidence, not an authenticated identity or
// resource lock. Keys are canonical presence senders (room/nick or a direct JID).
// An advertised pi-msg caps node identifies peers without treating humans as bots.
type peerPresence struct {
	jid   string // real bare JID, if disclosed by the room
	show  string
	agent bool
}

func parseAllowWake(raw json.RawMessage) (bool, error) {
	if len(raw) == 0 {
		return false, nil
	}
	// json.Unmarshal into bool accepts null; an explicit override must really
	// be a boolean, including when a caller bypasses the companion schema.
	switch strings.TrimSpace(string(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("allow_wake must be a boolean")
	}
}

// canonicalPeerJID uses the same normalization as stanza delivery. In
// particular the bare JID is case-mapped but resource case remains significant.
func canonicalPeerJID(raw string) string {
	parsed, err := jid.Parse(raw)
	if err != nil {
		return ""
	}
	return parsed.String()
}

// recordPeerPresenceLocked records only available presence. A missing caps
// element may retain identity on the same live endpoint; an explicit different
// client or a changed real JID replaces it. Unavailable/error clears it instead.
func (b *XMPPBridge) recordPeerPresenceLocked(from, real string, toks []xml.Token) {
	from = canonicalPeerJID(from)
	if from == "" {
		return
	}
	real = bareJid(canonicalPeerJID(real))
	old := b.peerPresence[from]
	node, hasCaps := peerCapsNode(toks)
	agent := old.agent && old.jid == real
	if hasCaps {
		agent = node == capsNode
	}
	if b.peerPresence == nil {
		b.peerPresence = make(map[string]peerPresence)
	}
	b.peerPresence[from] = peerPresence{jid: real, show: childText(toks, "show"), agent: agent}
}

// Only a direct caps child describes the sending client, not a nested payload.
func peerCapsNode(toks []xml.Token) (string, bool) {
	depth := 0
	for _, tok := range toks {
		switch t := tok.(type) {
		case xml.StartElement:
			if depth == 0 && t.Name.Space == capsNS && t.Name.Local == "c" {
				return attr(t.Attr, "node"), true
			}
			depth++
		case xml.EndElement:
			depth--
		}
	}
	return "", false
}

func (b *XMPPBridge) forgetPeerPresenceLocked(from string) {
	from = canonicalPeerJID(from)
	if from == "" {
		return
	}
	delete(b.peerPresence, from)
	// A bare unavailable/error invalidates every resource of that direct JID.
	if resourcepart(from) == "" {
		for endpoint := range b.peerPresence {
			if bareJid(endpoint) == from {
				delete(b.peerPresence, endpoint)
			}
		}
	}
}

func sleepingShow(show string) bool { return show == "away" || show == "xa" }

func (b *XMPPBridge) wakeExemptJID(to string) bool {
	bare := bareJid(canonicalPeerJID(to))
	return bare != "" && (bare == bareJid(canonicalPeerJID(b.ownerBare)) || bare == bareJid(canonicalPeerJID(b.acct.JID)))
}

// sleepingMessageTargets snapshots all relevant live evidence under one lock.
// Room notifications use the exact room/nick; direct bare JIDs aggregate all
// observed resources/room witnesses, allowing any known nonsleeping witness.
// An unknown full resource never inherits a sibling resource's sleeping state.
func (b *XMPPBridge) sleepingMessageTargets(to, text string, reply *replyTarget, room bool) []string {
	to = canonicalPeerJID(to)
	if to == "" {
		return nil
	}
	// Preflight exactly what SendRoomReply will deliver, since splitting a
	// quoted line can expose a mention in a later, now-unquoted stanza.
	var parts []string
	if room {
		parts = chunk(text, maxBody)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !room {
		if b.wakeExemptJID(to) {
			return nil
		}
		if resourcepart(to) != "" {
			p, ok := b.peerPresence[to]
			if ok && p.agent && sleepingShow(p.show) {
				return []string{to + " (" + p.show + ")"}
			}
			return nil
		}
		var shows []string
		knownAgent := false
		for _, p := range b.peerPresence {
			if p.jid != bareJid(to) {
				continue
			}
			if !sleepingShow(p.show) {
				return nil
			}
			knownAgent = knownAgent || p.agent
			shows = append(shows, p.show)
		}
		if knownAgent && len(shows) > 0 {
			sort.Strings(shows)
			return []string{to + " (" + strings.Join(slices.Compact(shows), "/") + ")"}
		}
		return nil
	}

	roomJID := bareJid(to)
	ownNick := b.selfNick[roomJID]
	if ownNick == "" {
		ownNick = b.acct.Nick
	}
	targetSet := make(map[string]bool)
	for i, part := range parts {
		addr := parseRoomAddressing(part)
		broadcast := addr.hasAny(broadcastHandles) || addr.hasAny(freeHandles)
		partReply := replyForChunk(i, reply)
		for endpoint, p := range b.peerPresence {
			if bareJid(endpoint) != roomJID || !p.agent || !sleepingShow(p.show) || b.wakeExemptJID(p.jid) {
				continue
			}
			nick := resourcepart(endpoint)
			if nick == "" || nick == ownNick {
				continue
			}
			addressed, _ := addr.matchName(nick)
			if broadcast || addressed || (partReply != nil && canonicalPeerJID(partReply.author) == endpoint) {
				targetSet[nick+" ("+p.show+")"] = true
			}
		}
	}
	targets := make([]string, 0, len(targetSet))
	for target := range targetSet {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}
