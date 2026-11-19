package pimsg

import (
	"context"
	"encoding/xml"
	"sync"
	"time"
)

const (
	archiveReplyTimeout  = 3 * time.Second
	archiveReplyPageSize = 100
	archiveReplyPages    = 5
	archiveIngressLimit  = 64
)

// dispatchReplyAware leaves the normal delivery path synchronous. A cache miss
// hands off to a worker before any admission decision: waiting for MAM on the
// read loop would block the IQ response itself. Later input stays in wire order.
func (b *XMPPBridge) dispatchReplyAware(m InboundMessage) {
	b.ingressMu.Lock()
	for b.ingressCancel != nil && len(b.ingressQueue) >= archiveIngressLimit {
		// Prioritize live input over archive work during a burst. Cancel the
		// IQ wait before applying backpressure so the reader does not have
		// to receive a response to make space.
		b.ingressCancel()
		b.ingressSpace.Wait()
	}
	if b.ingressCancel != nil {
		b.ingressQueue = append(b.ingressQueue, m)
		b.ingressMu.Unlock()
		return
	}
	_, known := b.lookupRoomReply(m.Room, m.ReplyToID)
	if !b.acct.MAM || known || m.Room == "" || m.ReplyToID == "" || len(m.Reactions) != 0 || b.currentSession() == nil {
		b.ingressMu.Unlock()
		b.onMsg(m)
		return
	}
	// One time budget for the whole batch keeps a burst of misses from
	// blocking admission indefinitely. Cache hits work after expiry.
	ctx, cancel := context.WithTimeout(context.Background(), archiveReplyTimeout)
	b.ingressCancel = cancel
	if b.ingressSpace == nil {
		b.ingressSpace = sync.NewCond(&b.ingressMu)
	}
	b.ingressMu.Unlock()
	go b.drainReplyIngress(ctx, cancel, m)
}

func (b *XMPPBridge) drainReplyIngress(ctx context.Context, cancel context.CancelFunc, m InboundMessage) {
	defer cancel()
	for {
		if m.Room != "" && m.ReplyToID != "" && len(m.Reactions) == 0 {
			b.resolveArchivedRoomReply(ctx, m.Room, m.ReplyToID, m.ReplyToJID)
		}
		b.onMsg(m)
		b.ingressMu.Lock()
		if len(b.ingressQueue) == 0 {
			b.ingressCancel = nil
			b.ingressQueue = nil
			b.ingressSpace.Broadcast()
			b.ingressMu.Unlock()
			return
		}
		m = b.ingressQueue[0]
		b.ingressQueue[0] = InboundMessage{}
		b.ingressQueue = b.ingressQueue[1:]
		b.ingressSpace.Signal()
		b.ingressMu.Unlock()
	}
}

// resolveArchivedRoomReply fetches only the parent and indexes its IDs, not the
// scanned page. No archived body enters the inbound delivery or watch paths.
func (b *XMPPBridge) resolveArchivedRoomReply(ctx context.Context, room, id, to string) {
	if _, known := b.lookupRoomReply(room, id); known || ctx.Err() != nil || !b.isRoomJID(room) {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, archiveReplyTimeout)
	defer cancel()
	fetch := func(before string) ([]InboundMessage, bool, error) {
		return b.fetchMAM(ctx, room, "", time.Time{}, archiveReplyPageSize, before, before == "", false)
	}
	parent, err := findRoomReplyParent(room, id, to, fetch)
	if err != nil {
		b.log("info", "room reply archive lookup unavailable: "+err.Error())
	} else if parent.From != "" {
		b.rememberArchivedReply(room, id, parent)
	}
}

func findRoomReplyParent(room, id, to string, fetch func(string) ([]InboundMessage, bool, error)) (InboundMessage, error) {
	if to != "" && bareJid(to) != room {
		return InboundMessage{}, nil
	}
	before := ""
	var senderMatch InboundMessage
	for range archiveReplyPages {
		msgs, complete, err := fetch(before)
		if err != nil {
			return InboundMessage{}, err
		}
		for _, parent := range msgs {
			if parent.Room != room || bareJid(parent.From) != room || resourcepart(parent.From) == "" ||
				(resourcepart(to) != "" && parent.From != to) {
				continue
			}
			if id == parent.RoomStanzaID || id == parent.ArchiveID {
				return parent, nil
			}
			if id == parent.ID {
				if senderMatch.From != "" && senderMatch.ArchiveID != parent.ArchiveID {
					return InboundMessage{}, nil // ambiguous legacy ID
				}
				senderMatch = parent
			}
		}
		if complete || len(msgs) == 0 || msgs[0].ArchiveID == "" || msgs[0].ArchiveID == before {
			break
		}
		before = msgs[0].ArchiveID
	}
	return senderMatch, nil
}

func (b *XMPPBridge) rememberArchivedReply(room, id string, parent InboundMessage) {
	self := b.archivedParentIsSelf(room, parent)
	b.recordRoomReply(room, parent.ID, id, parent.From, parent.Body, self, false)
}

// The nickname may have changed hands. Use verified historical provenance or
// the room's XEP-0421 ID from our own server-confirmed presence, never the nick.
func (b *XMPPBridge) archivedParentIsSelf(room string, parent InboundMessage) bool {
	if parent.OriginJID != "" {
		return parent.OriginJID == bareJid(b.acct.JID)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return parent.OccupantID != "" && parent.OccupantID == b.selfOccupant[room]
}

func roomOccupantID(toks []xml.Token) string {
	depth := 0
	for _, tok := range toks {
		switch t := tok.(type) {
		case xml.StartElement:
			if depth == 0 && t.Name == (xml.Name{Space: "urn:xmpp:occupant-id:0", Local: "occupant-id"}) {
				return attr(t.Attr, "id")
			}
			depth++
		case xml.EndElement:
			if depth == 0 {
				return ""
			}
			depth--
		}
	}
	return ""
}
