package main

import (
	"context"
	"os"
	"testing"
	"time"

	"mellium.im/xmpp"
)

// Live XEP-0313 smoke test against the real server. Skipped unless
// PI_MSG_LIVE_MAM=1 and credentials are supplied:
//
//	PI_MSG_LIVE_MAM=1 PI_MSG_LIVE_JID=b2-1@chat.zachmanson.com \
//	  PI_MSG_LIVE_PW=… go test -run TestLiveMAMQuery -v
//
// It validates the query shape (data form, RSM, addressing, <fin>) that unit
// tests can only assert structurally, and that archived results reach
// collectMAMResult in the read loop.
func TestLiveMAMQuery(t *testing.T) {
	if os.Getenv("PI_MSG_LIVE_MAM") == "" {
		t.Skip("set PI_MSG_LIVE_MAM=1 with PI_MSG_LIVE_JID/PI_MSG_LIVE_PW to run")
	}
	acct := ResolvedAccount{
		Name:        "live-mam",
		JID:         os.Getenv("PI_MSG_LIVE_JID"),
		Password:    os.Getenv("PI_MSG_LIVE_PW"),
		Owner:       os.Getenv("PI_MSG_LIVE_OWNER"),
		Resource:    "pi-msg-mamtest",
		Nick:        "b2-1",
		Rooms:       []string{"testing-2@muc.chat.zachmanson.com"},
		RoomTrigger: "b2-1",
	}
	if acct.JID == "" || acct.Password == "" {
		t.Fatal("PI_MSG_LIVE_JID / PI_MSG_LIVE_PW required")
	}
	b := NewXMPPBridge(acct, nil, func(level, msg string) { t.Logf("[%s] %s", level, msg) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	session, err := b.connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()
	b.mu.Lock()
	b.session = session
	b.online = true
	b.mu.Unlock()
	go func() { _ = session.Serve(xmpp.HandlerFunc(b.handle)) }()

	since := time.Now().Add(-7 * 24 * time.Hour)
	// Personal archive: no `with` filter, so every message b2-1 has seen comes back.
	msgs, complete, err := b.FetchMAM(ctx, "", "", since, 20)
	if err != nil {
		t.Fatalf("FetchMAM: %v", err)
	}
	t.Logf("fetched %d message(s), complete=%v", len(msgs), complete)
	for i, m := range msgs {
		t.Logf("  [%d] id=%s from=%s direct=%v room=%q stamp=%s body=%.60q",
			i, m.ID, m.From, m.Direct, m.Room, m.Stamp.Format(time.RFC3339), m.Body)
	}
	if len(msgs) == 0 {
		t.Log("archive returned nothing in the last 7 days — query shape may still be wrong (check stderr for an IQ error)")
	}

	// MUC archive: same query addressed at the room, results carry room/nick.
	roomMsgs, roomComplete, err := b.FetchMAM(ctx, "testing-2@muc.chat.zachmanson.com", "", since, 20)
	if err != nil {
		t.Fatalf("FetchMAM(room): %v", err)
	}
	t.Logf("room fetched %d message(s), complete=%v", len(roomMsgs), roomComplete)
	for i, m := range roomMsgs {
		t.Logf("  room[%d] id=%s nick=%s room=%q stamp=%s body=%.60q",
			i, m.ID, m.Nick, m.Room, m.Stamp.Format(time.RFC3339), m.Body)
	}
}
