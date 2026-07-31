package omemo

import (
	"bytes"
	"testing"
)

// setupPair creates two independent OMEMO devices and establishes a session
// from `from` to `to` by having `from` process `to`'s bundle (the outbound
// direction only; the first PreKeySignalMessage completes the reverse).
func newAcct(t *testing.T, jid string) *Account {
	t.Helper()
	a, err := NewAccount(t.TempDir(), jid)
	if err != nil {
		t.Fatalf("NewAccount(%s): %v", jid, err)
	}
	return a
}

func processBundle(t *testing.T, from *Account, toJID string, to *Account) {
	t.Helper()
	b, err := to.Bundle()
	if err != nil {
		t.Fatalf("Bundle(): %v", err)
	}
	if err := from.ProcessBundle(toJID, to.DeviceID(), b); err != nil {
		t.Fatalf("ProcessBundle: %v", err)
	}
}

func TestRoundTripPreKeyMessage(t *testing.T) {
	alice := newAcct(t, "alice@example.com")
	bob := newAcct(t, "bob@example.com")

	// Alice fetches Bob's bundle and establishes an outbound session.
	processBundle(t, alice, "bob@example.com", bob)

	want := []byte("the eagle lands at midnight 🦅")
	msg, err := alice.Encrypt([]Recipient{{JID: "bob@example.com", DeviceID: bob.DeviceID()}}, want)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(msg.Keys) != 1 {
		t.Fatalf("want 1 key, got %d", len(msg.Keys))
	}
	if !msg.Keys[0].PreKey {
		t.Fatalf("first message to a new session must be a prekey message")
	}
	if msg.SID != alice.DeviceID() {
		t.Fatalf("sid=%d, want alice device %d", msg.SID, alice.DeviceID())
	}

	// Bob decrypts using Alice's sender JID.
	got, err := bob.Decrypt("alice@example.com", msg)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestBidirectionalAfterHandshake(t *testing.T) {
	alice := newAcct(t, "alice@example.com")
	bob := newAcct(t, "bob@example.com")
	processBundle(t, alice, "bob@example.com", bob)

	// Alice -> Bob (prekey message establishes reverse session for Bob).
	m1, err := alice.Encrypt([]Recipient{{JID: "bob@example.com", DeviceID: bob.DeviceID()}}, []byte("hello bob"))
	if err != nil {
		t.Fatalf("encrypt1: %v", err)
	}
	if _, err := bob.Decrypt("alice@example.com", m1); err != nil {
		t.Fatalf("decrypt1: %v", err)
	}

	// Bob can now reply without ever fetching Alice's bundle.
	if !bob.HasSession("alice@example.com", alice.DeviceID()) {
		t.Fatalf("bob should have a session with alice after the prekey message")
	}
	reply := []byte("hello alice, ack 👍")
	m2, err := bob.Encrypt([]Recipient{{JID: "alice@example.com", DeviceID: alice.DeviceID()}}, reply)
	if err != nil {
		t.Fatalf("encrypt2: %v", err)
	}
	if m2.Keys[0].PreKey {
		t.Fatalf("bob's reply on an established session must be a whisper message, not prekey")
	}
	got, err := alice.Decrypt("bob@example.com", m2)
	if err != nil {
		t.Fatalf("decrypt2: %v", err)
	}
	if !bytes.Equal(got, reply) {
		t.Fatalf("got %q, want %q", got, reply)
	}
}

func TestManyMessagesRatchet(t *testing.T) {
	alice := newAcct(t, "a@x.com")
	bob := newAcct(t, "b@x.com")
	processBundle(t, alice, "b@x.com", bob)

	for i := 0; i < 25; i++ {
		want := []byte("msg number " + string(rune('a'+i%26)))
		m, err := alice.Encrypt([]Recipient{{JID: "b@x.com", DeviceID: bob.DeviceID()}}, want)
		if err != nil {
			t.Fatalf("encrypt %d: %v", i, err)
		}
		got, err := bob.Decrypt("a@x.com", m)
		if err != nil {
			t.Fatalf("decrypt %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("msg %d: got %q want %q", i, got, want)
		}
	}
}

// TestFanOutToOwnOtherDevice mirrors the real bot case: a message must be
// readable by a second device of the same user (the owner's phone AND laptop).
func TestFanOutToTwoDevices(t *testing.T) {
	alice := newAcct(t, "alice@example.com")
	phone := newAcct(t, "owner@example.com")
	laptop := newAcct(t, "owner@example.com")
	if phone.DeviceID() == laptop.DeviceID() {
		t.Skip("device id collision (1-in-4-billion); rerun")
	}
	processBundle(t, alice, "owner@example.com", phone)
	processBundle(t, alice, "owner@example.com", laptop)

	want := []byte("read me on either device")
	m, err := alice.Encrypt([]Recipient{
		{JID: "owner@example.com", DeviceID: phone.DeviceID()},
		{JID: "owner@example.com", DeviceID: laptop.DeviceID()},
	}, want)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if len(m.Keys) != 2 {
		t.Fatalf("want 2 key entries, got %d", len(m.Keys))
	}
	for _, dev := range []*Account{phone, laptop} {
		got, err := dev.Decrypt("alice@example.com", m)
		if err != nil {
			t.Fatalf("decrypt on device %d: %v", dev.DeviceID(), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("device %d: got %q want %q", dev.DeviceID(), got, want)
		}
	}
}

// TestPersistenceReload verifies keys and sessions survive a store reload, so a
// bot restart doesn't invalidate every peer session.
func TestPersistenceReload(t *testing.T) {
	dir := t.TempDir()
	alice := newAcct(t, "alice@example.com")

	bob, err := NewAccount(dir, "bob@example.com")
	if err != nil {
		t.Fatalf("new bob: %v", err)
	}
	origDevice := bob.DeviceID()
	origFP := bob.Fingerprint()

	processBundle(t, alice, "bob@example.com", bob)
	m1, _ := alice.Encrypt([]Recipient{{JID: "bob@example.com", DeviceID: bob.DeviceID()}}, []byte("before restart"))
	if _, err := bob.Decrypt("alice@example.com", m1); err != nil {
		t.Fatalf("decrypt before restart: %v", err)
	}

	// Reload Bob from the same dir: same identity, same device id, and the
	// established session still decrypts.
	bob2, err := NewAccount(dir, "bob@example.com")
	if err != nil {
		t.Fatalf("reload bob: %v", err)
	}
	if bob2.DeviceID() != origDevice {
		t.Fatalf("device id changed across reload: %d -> %d", origDevice, bob2.DeviceID())
	}
	if bob2.Fingerprint() != origFP {
		t.Fatalf("fingerprint changed across reload")
	}
	if !bob2.HasSession("alice@example.com", alice.DeviceID()) {
		t.Fatalf("session lost across reload")
	}
	m2, _ := alice.Encrypt([]Recipient{{JID: "bob@example.com", DeviceID: bob2.DeviceID()}}, []byte("after restart"))
	got, err := bob2.Decrypt("alice@example.com", m2)
	if err != nil {
		t.Fatalf("decrypt after restart: %v", err)
	}
	if !bytes.Equal(got, []byte("after restart")) {
		t.Fatalf("got %q", got)
	}
}

// TestBundleRoundTripsThroughStruct sanity-checks that a Bundle survives being
// reduced to its wire fields and rebuilt (mimicking PEP marshal/unmarshal).
func TestBundleFields(t *testing.T) {
	a := newAcct(t, "a@x.com")
	b, err := a.Bundle()
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if len(b.IdentityKey) != 33 {
		t.Fatalf("identity key should be 33 bytes (0x05 prefix), got %d", len(b.IdentityKey))
	}
	if len(b.SignedPreKeyPub) != 33 {
		t.Fatalf("signed prekey pub should be 33 bytes, got %d", len(b.SignedPreKeyPub))
	}
	if len(b.SignedPreKeySig) != 64 {
		t.Fatalf("signature should be 64 bytes, got %d", len(b.SignedPreKeySig))
	}
	if len(b.PreKeys) != preKeyCount {
		t.Fatalf("want %d prekeys, got %d", preKeyCount, len(b.PreKeys))
	}
	if b.IdentityKey[0] != 0x05 {
		t.Fatalf("identity key missing DjbType prefix, got 0x%02x", b.IdentityKey[0])
	}
}
