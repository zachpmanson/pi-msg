package omemo

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/serialize"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"
)

// aesKeyLen is 16: OMEMO 0.3 mandates AES-128-GCM. ivLen is the 12-byte GCM
// nonce we emit; on decrypt we honour whatever length the sender used.
const (
	aesKeyLen = 16
	gcmTagLen = 16
	ivLen     = 12
)

// PreKey is one published one-time prekey (33-byte serialized public key).
type PreKey struct {
	ID  uint32
	Pub []byte
}

// Bundle is an OMEMO device bundle as carried in the PEP
// eu.siacs.conversations.axolotl.bundles:<id> node. All key fields are the
// libsignal-serialized (0x05-prefixed, 33-byte) public keys, except
// SignedPreKeySig which is the raw 64-byte signature.
type Bundle struct {
	SignedPreKeyID  uint32
	SignedPreKeyPub []byte
	SignedPreKeySig []byte
	IdentityKey     []byte
	PreKeys         []PreKey
}

// KeyEntry is one <key rid=…> in an <encrypted> header: the per-device Signal
// ciphertext of the shared 32-byte key||tag blob. PreKey mirrors the
// prekey="true" attribute (a PreKeySignalMessage that also establishes a
// session).
type KeyEntry struct {
	RID    uint32
	PreKey bool
	Data   []byte
}

// Message is a decoded <encrypted> element. Payload is nil for a bare
// KeyTransportElement (used to set up a session with no body).
type Message struct {
	SID     uint32
	Keys    []KeyEntry
	IV      []byte
	Payload []byte
}

// Recipient is one target device to encrypt to (a bare JID + device id). A
// session with it must already exist (via ProcessBundle).
type Recipient struct {
	JID      string
	DeviceID uint32
}

// Account is a single OMEMO device: its persistent key store plus the crypto
// operations layered on go.mau.fi/libsignal. The device id is the libsignal
// registration id, which is also the sid/rid used on the wire and the PEP
// bundle-node suffix.
type Account struct {
	jid      string
	store    *Store
	ser      *serialize.Serializer
	deviceID uint32
}

// NewAccount loads (or initialises) the OMEMO device for jid, persisting its
// keys under dir. The first call for a jid generates a fresh identity; later
// calls reuse it so the device id stays stable across restarts.
func NewAccount(dir, jid string) (*Account, error) {
	ser := serialize.NewProtoBufSerializer()
	st, err := loadStore(dir, ser)
	if err != nil {
		return nil, err
	}
	return &Account{jid: jid, store: st, ser: ser, deviceID: st.GetLocalRegistrationID()}, nil
}

// DeviceID returns this device's OMEMO id (sid).
func (a *Account) DeviceID() uint32 { return a.deviceID }

// Fingerprint is the hex of this device's identity public key, for display /
// out-of-band verification.
func (a *Account) Fingerprint() string {
	return hex.EncodeToString(a.store.GetIdentityKeyPair().PublicKey().Serialize())
}

// Bundle assembles this device's public bundle for PEP publication.
func (a *Account) Bundle() (Bundle, error) {
	signed, err := a.store.LoadSignedPreKey(context.Background(), a.store.data.SignedPreKeyID)
	if err != nil {
		return Bundle{}, err
	}
	sig := signed.Signature()
	b := Bundle{
		SignedPreKeyID:  signed.ID(),
		SignedPreKeyPub: signed.KeyPair().PublicKey().Serialize(),
		SignedPreKeySig: sig[:],
		IdentityKey:     a.store.GetIdentityKeyPair().PublicKey().Serialize(),
	}
	for _, id := range a.store.unusedPreKeyIDs() {
		pk, err := a.store.LoadPreKey(context.Background(), id)
		if err != nil {
			continue
		}
		b.PreKeys = append(b.PreKeys, PreKey{ID: id, Pub: pk.KeyPair().PublicKey().Serialize()})
	}
	return b, nil
}

// ReplenishPreKeys tops the one-time prekey pool back up if it has run low
// (each new inbound session consumes one). It returns true if keys were added,
// signalling the caller to republish its bundle to PEP.
func (a *Account) ReplenishPreKeys() (bool, error) {
	return a.store.replenishPreKeys()
}

// WasPreKeyFor reports whether msg's key entry addressed to this device is a
// prekey message — i.e. decrypting it consumed one of our one-time prekeys.
func (a *Account) WasPreKeyFor(msg *Message) bool {
	for _, k := range msg.Keys {
		if k.RID == a.deviceID {
			return k.PreKey
		}
	}
	return false
}

// HasSession reports whether a ratchet session already exists for a device, so
// the caller can avoid a needless bundle fetch.
func (a *Account) HasSession(jid string, dev uint32) bool {
	ok, _ := a.store.ContainsSession(context.Background(), protocol.NewSignalAddress(jid, dev))
	return ok
}

// ProcessBundle runs X3DH against a fetched peer bundle, establishing an
// outbound session for (jid, dev). One of the peer's one-time prekeys is chosen
// at random.
func (a *Account) ProcessBundle(jid string, dev uint32, b Bundle) error {
	if len(b.PreKeys) == 0 {
		return fmt.Errorf("omemo: bundle for %s/%d has no prekeys", jid, dev)
	}
	chosen := b.PreKeys[randIndex(len(b.PreKeys))]
	preKeyPub, err := ecc.DecodePoint(chosen.Pub, 0)
	if err != nil {
		return fmt.Errorf("omemo: bad prekey in bundle: %w", err)
	}
	signedPub, err := ecc.DecodePoint(b.SignedPreKeyPub, 0)
	if err != nil {
		return fmt.Errorf("omemo: bad signed prekey in bundle: %w", err)
	}
	idPub, err := ecc.DecodePoint(b.IdentityKey, 0)
	if err != nil {
		return fmt.Errorf("omemo: bad identity key in bundle: %w", err)
	}
	if len(b.SignedPreKeySig) != 64 {
		return fmt.Errorf("omemo: signed prekey signature must be 64 bytes, got %d", len(b.SignedPreKeySig))
	}
	var sig [64]byte
	copy(sig[:], b.SignedPreKeySig)

	addr := protocol.NewSignalAddress(jid, dev)
	bundle := prekey.NewBundle(dev, dev, optional.NewOptionalUint32(chosen.ID), b.SignedPreKeyID,
		preKeyPub, signedPub, sig, identity.NewKey(idPub))

	builder := session.NewBuilderFromSignal(a.store, addr, a.ser)
	if err := builder.ProcessBundle(context.Background(), bundle); err != nil {
		return fmt.Errorf("omemo: process bundle for %s/%d: %w", jid, dev, err)
	}
	return nil
}

// Encrypt encrypts plaintext once under a fresh AES-128-GCM key and fans the
// key||tag blob out to every recipient device (which must already have a
// session). The returned Message carries one KeyEntry per recipient plus the
// shared IV and ciphertext payload.
func (a *Account) Encrypt(recipients []Recipient, plaintext []byte) (*Message, error) {
	if len(recipients) == 0 {
		return nil, fmt.Errorf("omemo: no recipient devices")
	}
	key := make([]byte, aesKeyLen)
	iv := make([]byte, ivLen)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, len(iv))
	if err != nil {
		return nil, err
	}
	sealed := gcm.Seal(nil, iv, plaintext, nil) // ciphertext || tag
	payload := sealed[:len(sealed)-gcmTagLen]
	tag := sealed[len(sealed)-gcmTagLen:]
	keyBlob := append(append([]byte{}, key...), tag...) // 16-byte key || 16-byte tag

	msg := &Message{SID: a.deviceID, IV: iv, Payload: payload}
	for _, r := range recipients {
		addr := protocol.NewSignalAddress(r.JID, r.DeviceID)
		builder := session.NewBuilderFromSignal(a.store, addr, a.ser)
		c := session.NewCipher(builder, addr)
		ct, err := c.Encrypt(context.Background(), keyBlob)
		if err != nil {
			return nil, fmt.Errorf("omemo: encrypt to %s/%d: %w", r.JID, r.DeviceID, err)
		}
		msg.Keys = append(msg.Keys, KeyEntry{
			RID:    r.DeviceID,
			PreKey: ct.Type() == protocol.PREKEY_TYPE,
			Data:   ct.Serialize(),
		})
	}
	return msg, nil
}

// Decrypt recovers the plaintext of an <encrypted> message addressed to this
// device. It finds the KeyEntry with rid == our device id, unwraps the shared
// key||tag via the Signal session (establishing one if this is a prekey
// message), then AES-128-GCM-decrypts the payload. A message with no payload
// (KeyTransportElement) returns nil, nil after processing the session.
func (a *Account) Decrypt(senderJID string, msg *Message) ([]byte, error) {
	var entry *KeyEntry
	for i := range msg.Keys {
		if msg.Keys[i].RID == a.deviceID {
			entry = &msg.Keys[i]
			break
		}
	}
	if entry == nil {
		return nil, fmt.Errorf("omemo: message not encrypted to this device (%d)", a.deviceID)
	}

	addr := protocol.NewSignalAddress(senderJID, msg.SID)
	builder := session.NewBuilderFromSignal(a.store, addr, a.ser)
	c := session.NewCipher(builder, addr)

	var keyBlob []byte
	var err error
	if entry.PreKey {
		pkMsg, e := protocol.NewPreKeySignalMessageFromBytes(entry.Data, a.ser.PreKeySignalMessage, a.ser.SignalMessage)
		if e != nil {
			return nil, fmt.Errorf("omemo: parse prekey message: %w", e)
		}
		keyBlob, err = c.DecryptMessage(context.Background(), pkMsg)
	} else {
		sMsg, e := protocol.NewSignalMessageFromBytes(entry.Data, a.ser.SignalMessage)
		if e != nil {
			return nil, fmt.Errorf("omemo: parse signal message: %w", e)
		}
		keyBlob, err = c.Decrypt(context.Background(), sMsg)
	}
	if err != nil {
		return nil, fmt.Errorf("omemo: session decrypt: %w", err)
	}
	if len(keyBlob) != aesKeyLen+gcmTagLen {
		return nil, fmt.Errorf("omemo: unexpected key blob length %d", len(keyBlob))
	}
	if msg.Payload == nil {
		return nil, nil // KeyTransportElement: session now established, no body
	}
	key := keyBlob[:aesKeyLen]
	tag := keyBlob[aesKeyLen:]

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, len(msg.IV))
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, msg.IV, append(append([]byte{}, msg.Payload...), tag...), nil)
	if err != nil {
		return nil, fmt.Errorf("omemo: gcm open: %w", err)
	}
	return plaintext, nil
}

// randIndex returns a crypto-random index in [0,n).
func randIndex(n int) int {
	if n <= 1 {
		return 0
	}
	b := make([]byte, 2)
	_, _ = io.ReadFull(rand.Reader, b)
	return int(uint16(b[0])<<8|uint16(b[1])) % n
}
