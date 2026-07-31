//go:build integration

// Live OMEMO integration test against a real XMPP server. It is excluded from
// normal `go test` by the integration build tag; run it explicitly with real
// credentials for two accounts on the same server:
//
//	PIMSG_A_JID=bot@chat.example.com   PIMSG_A_PW=...   \
//	PIMSG_B_JID=peer@chat.example.com  PIMSG_B_PW=...   \
//	PIMSG_SERVICE=chat.example.com:5222                 \
//	  go test -tags integration -run TestLiveOMEMO -v .
//
// It exercises the real path: account A publishes its bundle + devicelist to
// PEP, account B fetches them, establishes a session, encrypts a message, and
// sends the <encrypted> stanza; A receives and decrypts it. Then the reverse
// direction (an established-session whisper message) is checked.
package main

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/zachpmanson/pi-msg/internal/omemo"
)

func liveDial(t *testing.T, jidStr, pw, service string) *xmpp.Session {
	t.Helper()
	j, err := jid.Parse(jidStr)
	if err != nil {
		t.Fatalf("parse jid %q: %v", jidStr, err)
	}
	if service == "" {
		service = j.Domain().String() + ":5222"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(service, "xmpp://"))
	if err != nil {
		t.Fatalf("dial %s: %v", service, err)
	}
	tlsCfg := &tls.Config{ServerName: j.Domain().String()}
	if os.Getenv("PIMSG_INSECURE_TLS") == "1" {
		tlsCfg.InsecureSkipVerify = true // local test server with a self-signed cert
	}
	s, err := xmpp.NewClientSession(ctx, j, conn,
		xmpp.StartTLS(tlsCfg),
		xmpp.SASL("", pw, sasl.ScramSha256, sasl.Plain),
		xmpp.BindResource(),
	)
	if err != nil {
		conn.Close()
		t.Fatalf("negotiate %s: %v", jidStr, err)
	}
	return s
}

// receiver runs a session's read loop and surfaces decrypted OMEMO bodies from
// `fromBare` on a channel.
func receiver(ctx context.Context, s *xmpp.Session, acct *omemo.Account, fromBare string, out chan<- string) {
	go s.Serve(xmpp.HandlerFunc(func(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
		if start.Name.Local != "message" {
			_, err := xmlstream.Copy(xmlstream.Discard(), t)
			return err
		}
		toks, err := xmlstream.ReadAll(t)
		if err != nil {
			return err
		}
		enc, ok := parseEncrypted(toks)
		if !ok {
			return nil
		}
		plain, err := acct.Decrypt(fromBare, enc)
		if err != nil {
			// Skip stanzas we can't decrypt (e.g. a stale offline message from a
			// previous run, encrypted to a now-defunct device id) so reruns stay
			// reliable; the crypto itself is covered by the loopback unit tests.
			return nil
		}
		out <- string(plain)
		return nil
	}))
	go func() {
		<-ctx.Done()
		s.Close()
	}()
}

func sendEncryptedRaw(t *testing.T, s *xmpp.Session, toBare string, m *omemo.Message) {
	t.Helper()
	toJID, _ := jid.Parse(toBare)
	msg := buildEncryptedMessage(toJID, stanza.ChatMessage, m)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := s.Encode(ctx, msg); err != nil {
		t.Fatalf("send encrypted: %v", err)
	}
}

func TestLiveOMEMO(t *testing.T) {
	aJID := os.Getenv("PIMSG_A_JID")
	bJID := os.Getenv("PIMSG_B_JID")
	if aJID == "" || bJID == "" {
		t.Skip("set PIMSG_A_JID/PIMSG_A_PW/PIMSG_B_JID/PIMSG_B_PW to run the live test")
	}
	service := os.Getenv("PIMSG_SERVICE")
	aBare := bareJid(aJID)
	bBare := bareJid(bJID)

	aAcct, err := omemo.NewAccount(t.TempDir(), aBare)
	if err != nil {
		t.Fatalf("A account: %v", err)
	}
	bAcct, err := omemo.NewAccount(t.TempDir(), bBare)
	if err != nil {
		t.Fatalf("B account: %v", err)
	}

	aSess := liveDial(t, aJID, os.Getenv("PIMSG_A_PW"), service)
	bSess := liveDial(t, bJID, os.Getenv("PIMSG_B_PW"), service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	aMsgs := make(chan string, 4)
	bMsgs := make(chan string, 4)
	receiver(ctx, aSess, aAcct, bBare, aMsgs)
	receiver(ctx, bSess, bAcct, aBare, bMsgs)

	// Announce presence so the server routes bare-JID messages to these
	// resources (an unavailable resource isn't a delivery target).
	for _, s := range []*xmpp.Session{aSess, bSess} {
		pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
		if err := s.Send(pctx, stanza.Presence{}.Wrap(nil)); err != nil {
			pcancel()
			t.Fatalf("presence: %v", err)
		}
		pcancel()
	}
	time.Sleep(500 * time.Millisecond) // let presence settle before routing

	pubCtx, pubCancel := context.WithTimeout(ctx, 20*time.Second)
	defer pubCancel()

	// A and B both publish bundle + devicelist.
	for _, e := range []struct {
		s *xmpp.Session
		a *omemo.Account
	}{{aSess, aAcct}, {bSess, bAcct}} {
		b, err := e.a.Bundle()
		if err != nil {
			t.Fatalf("bundle: %v", err)
		}
		if err := publishBundle(pubCtx, e.s, e.a.DeviceID(), b); err != nil {
			t.Fatalf("publish bundle: %v", err)
		}
		if err := publishDeviceList(pubCtx, e.s, []uint32{e.a.DeviceID()}); err != nil {
			t.Fatalf("publish devicelist: %v", err)
		}
	}
	t.Logf("A device %d, B device %d", aAcct.DeviceID(), bAcct.DeviceID())

	// B fetches A's list + bundle, establishes a session, encrypts to A.
	aJIDParsed, _ := jid.Parse(aBare)
	devices, err := fetchDeviceList(pubCtx, bSess, aJIDParsed)
	if err != nil {
		t.Fatalf("B fetch A devicelist: %v", err)
	}
	if len(devices) == 0 || devices[0] != aAcct.DeviceID() {
		t.Fatalf("A devicelist = %v, want [%d]", devices, aAcct.DeviceID())
	}
	bundle, err := fetchBundle(pubCtx, bSess, aJIDParsed, aAcct.DeviceID())
	if err != nil {
		t.Fatalf("B fetch A bundle: %v", err)
	}
	if err := bAcct.ProcessBundle(aBare, aAcct.DeviceID(), bundle); err != nil {
		t.Fatalf("B process A bundle: %v", err)
	}

	want := "live prekey message over the wire 🔐"
	m, err := bAcct.Encrypt([]omemo.Recipient{{JID: aBare, DeviceID: aAcct.DeviceID()}}, []byte(want))
	if err != nil {
		t.Fatalf("B encrypt: %v", err)
	}
	if !m.Keys[0].PreKey {
		t.Fatalf("first message should be a prekey message")
	}
	sendEncryptedRaw(t, bSess, aBare, m)

	select {
	case got := <-aMsgs:
		if got != want {
			t.Fatalf("A received %q, want %q", got, want)
		}
		t.Logf("A decrypted prekey message ✓")
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for A to receive the prekey message")
	}

	// Reverse: A now has a session with B (from the prekey message); A replies
	// with a whisper message.
	reply := "live whisper reply 👍"
	rm, err := aAcct.Encrypt([]omemo.Recipient{{JID: bBare, DeviceID: bAcct.DeviceID()}}, []byte(reply))
	if err != nil {
		t.Fatalf("A encrypt reply: %v", err)
	}
	if rm.Keys[0].PreKey {
		t.Fatalf("A's reply should be a whisper message on the established session")
	}
	sendEncryptedRaw(t, aSess, bBare, rm)

	select {
	case got := <-bMsgs:
		if got != reply {
			t.Fatalf("B received %q, want %q", got, reply)
		}
		t.Logf("B decrypted whisper reply ✓")
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for B to receive the whisper reply")
	}
}
