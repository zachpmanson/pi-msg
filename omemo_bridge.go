package main

import (
	"context"
	"fmt"
	"time"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/zachpmanson/pi-msg/internal/omemo"
)

// omemoManager wires the transport-agnostic omemo.Account to this bridge's XMPP
// session and PEP helpers. v1 scope: 1:1 encryption with the owner only.
type omemoManager struct {
	acct   *omemo.Account
	bridge *XMPPBridge
}

// newOMEMOManager loads (or initialises) the account's OMEMO device from its
// state dir. A load failure is returned so the caller can log it and run
// unencrypted rather than crash.
func newOMEMOManager(b *XMPPBridge) (*omemoManager, error) {
	dir := omemoStateDir(b.acct.Name)
	acct, err := omemo.NewAccount(dir, b.ownerBare)
	if err != nil {
		return nil, err
	}
	return &omemoManager{acct: acct, bridge: b}, nil
}

// bootstrap publishes our bundle and ensures our device id is in our own
// devicelist node, so the owner's clients discover us. Best-effort: a failure
// is logged, and encryption still works opportunistically once PEP recovers.
func (m *omemoManager) bootstrap(ctx context.Context, s *xmpp.Session) {
	m.bridge.log("info", fmt.Sprintf("omemo: device %d, fingerprint %s", m.acct.DeviceID(), m.acct.Fingerprint()))

	bundle, err := m.acct.Bundle()
	if err != nil {
		m.bridge.log("warning", "omemo: build bundle: "+err.Error())
		return
	}
	if err := publishBundle(ctx, s, m.acct.DeviceID(), bundle); err != nil {
		m.bridge.log("warning", "omemo: publish bundle: "+err.Error())
		return
	}
	if err := m.ensureInDeviceList(ctx, s); err != nil {
		m.bridge.log("warning", "omemo: publish devicelist: "+err.Error())
		return
	}
	m.bridge.log("info", "omemo: bundle + devicelist published")
}

// ensureInDeviceList fetches our own devicelist, adds our device id if missing,
// and republishes. Merging (rather than overwriting) preserves any other
// devices the same bare JID runs.
func (m *omemoManager) ensureInDeviceList(ctx context.Context, s *xmpp.Session) error {
	self, err := jid.Parse(m.bridge.acct.JID)
	if err != nil {
		return err
	}
	existing, err := fetchDeviceList(ctx, s, self.Bare())
	if err != nil {
		// A missing node (first run) reads as an error; start from just us.
		existing = nil
	}
	have := false
	for _, d := range existing {
		if d == m.acct.DeviceID() {
			have = true
			break
		}
	}
	if have && len(existing) > 0 {
		return nil
	}
	return publishDeviceList(ctx, s, append(existing, m.acct.DeviceID()))
}

// encrypt builds an OMEMO <encrypted> message body for the owner: it refreshes
// the owner's device list, establishes any missing sessions (fetching bundles),
// and fans the ciphertext out to every reachable owner device. It returns an
// error if no owner device could be reached, so the caller can decide whether
// to fall back to plaintext.
func (m *omemoManager) encrypt(ctx context.Context, plaintext string) (*omemo.Message, error) {
	s := m.bridge.currentSession()
	if s == nil {
		return nil, fmt.Errorf("omemo: not online")
	}
	owner, err := jid.Parse(m.bridge.ownerBare)
	if err != nil {
		return nil, err
	}
	devices, err := fetchDeviceList(ctx, s, owner)
	if err != nil {
		return nil, fmt.Errorf("omemo: fetch owner devicelist: %w", err)
	}
	if len(devices) == 0 {
		return nil, fmt.Errorf("omemo: owner %s has no OMEMO devices", m.bridge.ownerBare)
	}

	var recipients []omemo.Recipient
	for _, dev := range devices {
		if !m.acct.HasSession(m.bridge.ownerBare, dev) {
			bundle, err := fetchBundle(ctx, s, owner, dev)
			if err != nil {
				m.bridge.log("warning", fmt.Sprintf("omemo: fetch bundle %s/%d: %v", m.bridge.ownerBare, dev, err))
				continue
			}
			if err := m.acct.ProcessBundle(m.bridge.ownerBare, dev, bundle); err != nil {
				m.bridge.log("warning", fmt.Sprintf("omemo: session with %s/%d: %v", m.bridge.ownerBare, dev, err))
				continue
			}
		}
		recipients = append(recipients, omemo.Recipient{JID: m.bridge.ownerBare, DeviceID: dev})
	}
	if len(recipients) == 0 {
		return nil, fmt.Errorf("omemo: no reachable owner devices (of %d)", len(devices))
	}
	return m.acct.Encrypt(recipients, []byte(plaintext))
}

// sendEncrypted encrypts text for the owner and sends it as an OMEMO
// <encrypted> chat message to `to`. Returns an error (so the caller can fall
// back to plaintext) if encryption or send fails.
func (b *XMPPBridge) sendEncrypted(to, text string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return fmt.Errorf("invalid recipient %q: %w", to, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msg, err := b.omemo.encrypt(ctx, text)
	if err != nil {
		return err
	}
	stanzaMsg := buildEncryptedMessage(toJID, stanza.ChatMessage, msg)
	encCtx, encCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer encCancel()
	return session.Encode(encCtx, stanzaMsg)
}

// decryptInbound decrypts an <encrypted> message from fromFull and returns the
// plaintext. After a prekey message (which consumes one of our one-time
// prekeys) it replenishes the pool and republishes the bundle.
func (m *omemoManager) decryptInbound(fromBare string, msg *omemo.Message) (string, error) {
	wasPreKey := m.acct.WasPreKeyFor(msg)
	plain, err := m.acct.Decrypt(fromBare, msg)
	if err != nil {
		return "", err
	}
	if wasPreKey {
		go m.replenishAndRepublish()
	}
	return string(plain), nil
}

// replenishAndRepublish tops up one-time prekeys and, if any were added,
// republishes the bundle so PEP reflects the fresh keys. Runs off the read loop.
func (m *omemoManager) replenishAndRepublish() {
	added, err := m.acct.ReplenishPreKeys()
	if err != nil {
		m.bridge.log("warning", "omemo: replenish prekeys: "+err.Error())
		return
	}
	if !added {
		return
	}
	s := m.bridge.currentSession()
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bundle, err := m.acct.Bundle()
	if err != nil {
		return
	}
	if err := publishBundle(ctx, s, m.acct.DeviceID(), bundle); err != nil {
		m.bridge.log("warning", "omemo: republish bundle after replenish: "+err.Error())
		return
	}
	m.bridge.log("info", "omemo: replenished one-time prekeys and republished bundle")
}
