// Package omemo implements legacy OMEMO (XEP-0384 v0.3, namespace
// eu.siacs.conversations.axolotl) end-to-end encryption on top of
// go.mau.fi/libsignal. It is deliberately transport-agnostic: this package owns
// the Signal double-ratchet sessions, the persistent key material, and the
// AES-128-GCM payload envelope, but knows nothing about XMPP. The caller
// (package main) marshals the <encrypted> stanza and drives PEP.
package omemo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/protocol"
	groupRecord "go.mau.fi/libsignal/groups/state/record"
	"go.mau.fi/libsignal/serialize"
	"go.mau.fi/libsignal/state/record"
	"go.mau.fi/libsignal/util/keyhelper"
)

// persisted is the on-disk shape of a store. All byte slices are base64. It
// holds this device's long-term identity plus every ratchet session and unused
// one-time prekey, so sessions survive restarts (a fresh identity would make
// every peer see an untrusted new device and silently drop our messages).
//
// Prekeys and the signed prekey are stored as their raw 32-byte private keys
// (not as libsignal record.Serialize() bytes) and the public key is recomputed
// on load via ecc.CreateKeyPair. This deliberately sidesteps a truncation bug
// in libsignal's JSON record serializer, whose deserializer copies only the
// first 32 bytes of the 33-byte (0x05-prefixed) serialized public key and thus
// reconstructs a wrong key — which would make signed-prekey signatures fail to
// verify. Recomputing the public key from the private key is exact.
type persisted struct {
	RegistrationID   uint32            `json:"registration_id"`
	IdentityPriv     string            `json:"identity_priv"`      // 32-byte curve25519 seed
	SignedPreKeyID   uint32            `json:"signed_prekey_id"`
	SignedPreKeyPriv string            `json:"signed_prekey_priv"` // 32-byte private key
	SignedPreKeySig  string            `json:"signed_prekey_sig"`  // 64-byte signature
	SignedPreKeyTS   int64             `json:"signed_prekey_ts"`
	PreKeys          map[string]string `json:"prekeys"`    // id -> 32-byte private key
	Sessions         map[string]string `json:"sessions"`   // "name:deviceid" -> record.Session.Serialize()
	Identities       map[string]string `json:"identities"` // "name:deviceid" -> identity.Key.Serialize() (33 bytes)
}

// Store is a file-backed implementation of libsignal's composite
// store.SignalProtocol interface. It is safe for concurrent use; every mutating
// method flushes the whole state to disk atomically. The SenderKey methods are
// stubs: OMEMO 1:1 fan-out never uses group (sender-key) sessions, but the
// composite interface requires them to exist.
type Store struct {
	mu   sync.Mutex
	path string
	ser  *serialize.Serializer

	regID    uint32
	identity *identity.KeyPair
	data     persisted
}

func addrKey(a *protocol.SignalAddress) string {
	return fmt.Sprintf("%s:%d", a.Name(), a.DeviceID())
}

// loadStore reads (or, if absent, initialises) the store at dir/state.json,
// generating a fresh identity + signed prekey + `preKeyCount` one-time prekeys
// on first use. serializer must be the protobuf serializer so wire messages
// stay interoperable with Conversations/converse.js.
func loadStore(dir string, ser *serialize.Serializer) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("omemo: create state dir: %w", err)
	}
	s := &Store{
		path: filepath.Join(dir, "state.json"),
		ser:  ser,
		data: persisted{
			PreKeys:    map[string]string{},
			Sessions:   map[string]string{},
			Identities: map[string]string{},
		},
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := s.initIdentity(); err != nil {
				return nil, err
			}
			return s, s.flushLocked()
		}
		return nil, fmt.Errorf("omemo: read state: %w", err)
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return nil, fmt.Errorf("omemo: parse state: %w", err)
	}
	if s.data.PreKeys == nil {
		s.data.PreKeys = map[string]string{}
	}
	if s.data.Sessions == nil {
		s.data.Sessions = map[string]string{}
	}
	if s.data.Identities == nil {
		s.data.Identities = map[string]string{}
	}
	return s, s.rehydrate()
}

// rehydrate rebuilds the in-memory identity keypair from the persisted seed.
func (s *Store) rehydrate() error {
	seed, err := base64.StdEncoding.DecodeString(s.data.IdentityPriv)
	if err != nil || len(seed) != 32 {
		return fmt.Errorf("omemo: corrupt identity key in state")
	}
	kp := ecc.CreateKeyPair(seed)
	s.identity = identity.NewKeyPair(identity.NewKey(kp.PublicKey()), kp.PrivateKey())
	s.regID = s.data.RegistrationID
	return nil
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// keyPairFromB64 rebuilds an EC key pair from a base64 32-byte private key,
// recomputing the public key deterministically. Used for prekeys/signed
// prekeys to avoid libsignal's buggy record public-key round-trip.
func keyPairFromB64(enc string) (*ecc.ECKeyPair, error) {
	priv, err := base64.StdEncoding.DecodeString(enc)
	if err != nil || len(priv) != 32 {
		return nil, fmt.Errorf("omemo: corrupt private key in state")
	}
	return ecc.CreateKeyPair(priv), nil
}

// preKeyCount is how many one-time prekeys to generate on init and to keep
// topped up. The XEP recommends ~100; 20 is the practical minimum.
const preKeyCount = 100

// initIdentity generates a fresh device identity: a registration/device id, an
// identity keypair, one signed prekey, and preKeyCount one-time prekeys. Called
// once, on first use, before anything is persisted.
func (s *Store) initIdentity() error {
	kp, err := keyhelper.GenerateIdentityKeyPair()
	if err != nil {
		return fmt.Errorf("omemo: generate identity: %w", err)
	}
	s.identity = kp
	s.regID = keyhelper.GenerateRegistrationID()
	s.data.RegistrationID = s.regID
	priv := kp.PrivateKey().Serialize()
	s.data.IdentityPriv = b64(priv[:])

	signed, err := keyhelper.GenerateSignedPreKey(kp, 1, s.ser.SignedPreKeyRecord)
	if err != nil {
		return fmt.Errorf("omemo: generate signed prekey: %w", err)
	}
	spkPriv := signed.KeyPair().PrivateKey().Serialize()
	spkSig := signed.Signature()
	s.data.SignedPreKeyID = signed.ID()
	s.data.SignedPreKeyPriv = b64(spkPriv[:])
	s.data.SignedPreKeySig = b64(spkSig[:])
	s.data.SignedPreKeyTS = signed.Timestamp()

	preKeys, err := keyhelper.GeneratePreKeys(1, preKeyCount, s.ser.PreKeyRecord)
	if err != nil {
		return fmt.Errorf("omemo: generate prekeys: %w", err)
	}
	for _, pk := range preKeys {
		pkPriv := pk.KeyPair().PrivateKey().Serialize()
		s.data.PreKeys[fmt.Sprint(pk.ID().Value)] = b64(pkPriv[:])
	}
	return nil
}

// flushLocked writes the whole state to disk atomically. Caller holds s.mu.
func (s *Store) flushLocked() error {
	s.data.RegistrationID = s.regID
	raw, err := json.MarshalIndent(&s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("omemo: marshal state: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("omemo: write state: %w", err)
	}
	return os.Rename(tmp, s.path)
}

// ---- libsignal store.IdentityKey ----

func (s *Store) GetIdentityKeyPair() *identity.KeyPair { return s.identity }
func (s *Store) GetLocalRegistrationID() uint32        { return s.regID }

// SaveIdentity records a peer device's identity key (trust-on-first-use). We
// never reject on change here; trust policy lives in IsTrustedIdentity.
func (s *Store) SaveIdentity(_ context.Context, address *protocol.SignalAddress, key *identity.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Identities[addrKey(address)] = b64(key.Serialize())
	return s.flushLocked()
}

// IsTrustedIdentity implements blind-trust-before-verification: a device we have
// never seen is trusted; a known device is trusted only if its key is unchanged
// (a changed key on a known device is a red flag and is rejected).
func (s *Store) IsTrustedIdentity(_ context.Context, address *protocol.SignalAddress, key *identity.Key) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.data.Identities[addrKey(address)]
	if !ok {
		return true, nil
	}
	return stored == b64(key.Serialize()), nil
}

// ---- libsignal store.PreKey ----

func (s *Store) LoadPreKey(_ context.Context, id uint32) (*record.PreKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	enc, ok := s.data.PreKeys[fmt.Sprint(id)]
	if !ok {
		return nil, fmt.Errorf("omemo: prekey %d not found", id)
	}
	kp, err := keyPairFromB64(enc)
	if err != nil {
		return nil, err
	}
	return record.NewPreKey(id, kp, s.ser.PreKeyRecord), nil
}

func (s *Store) StorePreKey(_ context.Context, id uint32, rec *record.PreKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	priv := rec.KeyPair().PrivateKey().Serialize()
	s.data.PreKeys[fmt.Sprint(id)] = b64(priv[:])
	return s.flushLocked()
}

func (s *Store) ContainsPreKey(_ context.Context, id uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data.PreKeys[fmt.Sprint(id)]
	return ok, nil
}

func (s *Store) RemovePreKey(_ context.Context, id uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.PreKeys, fmt.Sprint(id))
	return s.flushLocked()
}

// ---- libsignal store.SignedPreKey ----

func (s *Store) LoadSignedPreKey(_ context.Context, id uint32) (*record.SignedPreKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != s.data.SignedPreKeyID {
		return nil, fmt.Errorf("omemo: signed prekey %d not found", id)
	}
	kp, err := keyPairFromB64(s.data.SignedPreKeyPriv)
	if err != nil {
		return nil, err
	}
	sigBytes, err := base64.StdEncoding.DecodeString(s.data.SignedPreKeySig)
	if err != nil || len(sigBytes) != 64 {
		return nil, fmt.Errorf("omemo: corrupt signed prekey signature")
	}
	var sig [64]byte
	copy(sig[:], sigBytes)
	return record.NewSignedPreKey(id, s.data.SignedPreKeyTS, kp, sig, s.ser.SignedPreKeyRecord), nil
}

func (s *Store) LoadSignedPreKeys(_ context.Context) ([]*record.SignedPreKey, error) {
	rec, err := s.LoadSignedPreKey(context.Background(), s.data.SignedPreKeyID)
	if err != nil {
		return nil, err
	}
	return []*record.SignedPreKey{rec}, nil
}

func (s *Store) StoreSignedPreKey(_ context.Context, id uint32, rec *record.SignedPreKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	priv := rec.KeyPair().PrivateKey().Serialize()
	sig := rec.Signature()
	s.data.SignedPreKeyID = id
	s.data.SignedPreKeyPriv = b64(priv[:])
	s.data.SignedPreKeySig = b64(sig[:])
	s.data.SignedPreKeyTS = rec.Timestamp()
	return s.flushLocked()
}

func (s *Store) ContainsSignedPreKey(_ context.Context, id uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return id == s.data.SignedPreKeyID && s.data.SignedPreKeyPriv != "", nil
}

func (s *Store) RemoveSignedPreKey(_ context.Context, id uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == s.data.SignedPreKeyID {
		s.data.SignedPreKeyPriv = ""
	}
	return s.flushLocked()
}

// ---- libsignal store.Session ----

func (s *Store) LoadSession(_ context.Context, address *protocol.SignalAddress) (*record.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	enc, ok := s.data.Sessions[addrKey(address)]
	if !ok {
		return record.NewSession(s.ser.Session, s.ser.State), nil
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, err
	}
	return record.NewSessionFromBytes(raw, s.ser.Session, s.ser.State)
}

func (s *Store) GetSubDeviceSessions(_ context.Context, name string) ([]uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []uint32
	prefix := name + ":"
	for k := range s.data.Sessions {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			var dev uint32
			if _, err := fmt.Sscanf(k[len(prefix):], "%d", &dev); err == nil {
				out = append(out, dev)
			}
		}
	}
	return out, nil
}

func (s *Store) StoreSession(_ context.Context, address *protocol.SignalAddress, rec *record.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Sessions[addrKey(address)] = b64(rec.Serialize())
	return s.flushLocked()
}

func (s *Store) ContainsSession(_ context.Context, address *protocol.SignalAddress) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data.Sessions[addrKey(address)]
	return ok, nil
}

func (s *Store) DeleteSession(_ context.Context, address *protocol.SignalAddress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Sessions, addrKey(address))
	return s.flushLocked()
}

func (s *Store) DeleteAllSessions(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Sessions = map[string]string{}
	return s.flushLocked()
}

// ---- libsignal groups store.SenderKey (unused by 1:1 OMEMO) ----

func (s *Store) StoreSenderKey(context.Context, *protocol.SenderKeyName, *groupRecord.SenderKey) error {
	return nil
}

func (s *Store) LoadSenderKey(context.Context, *protocol.SenderKeyName) (*groupRecord.SenderKey, error) {
	return groupRecord.NewSenderKey(s.ser.SenderKeyRecord, s.ser.SenderKeyState), nil
}

// unusedPreKeyIDs returns the ids of one-time prekeys still available, so the
// caller can decide whether to replenish and what to advertise in its bundle.
func (s *Store) unusedPreKeyIDs() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint32, 0, len(s.data.PreKeys))
	for k := range s.data.PreKeys {
		var id uint32
		if _, err := fmt.Sscanf(k, "%d", &id); err == nil {
			out = append(out, id)
		}
	}
	return out
}
