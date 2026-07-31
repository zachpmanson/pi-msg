# OMEMO end-to-end encryption

pi-msg can end-to-end encrypt the 1:1 conversation with the owner using **legacy
OMEMO** — XEP-0384 v0.3, the `eu.siacs.conversations.axolotl` namespace that
Conversations (Android) and Converse (web) actually interoperate on. OMEMO:2
(`urn:xmpp:omemo:2`) is deliberately **not** targeted: neither of those clients
ships it (Conversations issue #55 is still open), so legacy 0.3 is the only
format that reaches a real client.

## Design

The crypto is split from the transport so the hard part is testable in
isolation:

| Layer | File | Responsibility |
|-------|------|----------------|
| Signal core | `internal/omemo/` | double-ratchet sessions, persistent key store, AES-128-GCM payload + per-device fan-out. Transport-agnostic; no XMPP. Built on `go.mau.fi/libsignal`. |
| Wire | `omemo_wire.go` | marshal/parse the `<encrypted>` stanza (header/sid, `<key rid= prekey=>`, `<iv>`, `<payload>`). |
| PEP | `omemo_pep.go` | publish/fetch the devicelist and per-device bundle nodes (hand-rolled pubsub IQs with `publish-options access_model=open`). |
| Glue | `omemo_bridge.go` + hooks in `xmpp.go` | bootstrap on connect, encrypt owner-bound sends, decrypt owner receives. |

### How a message flows

- **Send (reply to owner):** refresh the owner's devicelist → for each device
  without a session, fetch its bundle and run X3DH → encrypt the body once under
  a fresh AES-128-GCM key, concatenate `key‖tag` (16+16 bytes) and Signal-encrypt
  that blob to every owner device → emit `<encrypted>`.
- **Receive:** find the `<key rid=…>` addressed to our device → Signal-decrypt to
  recover `key‖tag` → AES-128-GCM-open the payload. A prekey message also
  establishes the reverse session and consumes one of our one-time prekeys, which
  we then replenish and republish.

### Key decisions

- **AES-128-GCM, `key‖tag` in the `<key>`.** OMEMO 0.3 encrypts the body with a
  128-bit key and puts the 16-byte GCM tag *with the key* inside each per-device
  Signal ciphertext; `<payload>` holds ciphertext only. (OMEMO:2 uses AES-256 and
  a different envelope — not us.)
- **ProtoBuf message serializer.** `serialize.NewProtoBufSerializer()` — the JSON
  serializers are for local state only; using them on the wire would make
  Conversations/Converse reject the `<key>` bytes.
- **33-byte (0x05-prefixed) public keys** everywhere on the wire, matching
  libsignal's `Serialize()`/`DecodePoint`.
- **Raw private-key persistence.** The store persists 32-byte private keys and
  recomputes public keys via `ecc.CreateKeyPair`, sidestepping a truncation bug
  in libsignal's JSON *record* serializer (its deserializer copies only 32 of the
  33 serialized public-key bytes, which would break signed-prekey verification).
- **Blind trust (BTBV).** New owner device → trusted; changed key on a known
  device → rejected. Fingerprint logged on startup.
- **Opportunistic.** No owner OMEMO device yet → warn loudly and send plaintext,
  so the bot stays usable during setup.

## Testing

### Unit tests (`go test ./...`)

- `internal/omemo/omemo_test.go` — crypto loopback: prekey handshake,
  bidirectional ratchet, 25-message ratchet advance, fan-out to two devices,
  key/session persistence across a store reload.
- `omemo_wire_test.go` — `<encrypted>` marshal (golden bytes; asserts children
  inherit the axolotl namespace, never `xmlns=""`) and parse round-trip.
- `omemo_pep_test.go` — bundle + devicelist marshal/round-trip through the
  publish and fetch-response structs.

### Live integration test (real server, both directions)

`omemo_live_test.go` (build tag `integration`) stands up two accounts on a real
XMPP server and runs the full path: publish bundle+devicelist → fetch → X3DH →
encrypt → send `<encrypted>` → receive → decrypt, then a reverse whisper reply.
It uses the exact wire/PEP/crypto functions the bridge uses.

Run it against a throwaway local Prosody (no real credentials needed):

```sh
# from the repo root; config lives at testdata/omemo/prosody.cfg.lua
cd testdata/omemo
mkdir -p certs && openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj /CN=localhost -keyout certs/localhost.key -out certs/localhost.crt
chmod 644 certs/localhost.*

docker run -d --name pi-omemo-test -p 15222:5222 \
  -v "$PWD/prosody.cfg.lua:/etc/prosody/prosody.cfg.lua:ro" \
  -v "$PWD/certs:/etc/prosody/certs:ro" prosody/prosody:latest
docker exec pi-omemo-test prosodyctl register bot  localhost botpass
docker exec pi-omemo-test prosodyctl register peer localhost peerpass

# 2. run the round-trip
PIMSG_INSECURE_TLS=1 PIMSG_SERVICE=localhost:15222 \
PIMSG_A_JID=bot@localhost  PIMSG_A_PW=botpass \
PIMSG_B_JID=peer@localhost PIMSG_B_PW=peerpass \
  go test -tags integration -run TestLiveOMEMO -v .
```

(`PIMSG_INSECURE_TLS=1` accepts the self-signed cert; drop it against a real
server. The same command works against any server by pointing the env vars at
two real accounts.)

### Manual interop checklist (before relying on it in production)

Automated tests prove the protocol against Prosody and against our own second
endpoint; the last mile is a **real client**, which must be checked by hand:

- [ ] Enable `"omemo": true`, start the bot, confirm the startup log shows the
      device id + fingerprint and "bundle + devicelist published".
- [ ] From **Conversations**: open the chat with the bot, verify it shows the
      lock/encrypted state, send a message, confirm the agent replies and the
      reply shows as encrypted (not the `[OMEMO-encrypted…]` fallback).
- [ ] From **Converse**: same, to confirm the second client interoperates.
- [ ] Verify multi-device: with both clients online, one reply should be
      readable on **both** (fan-out).
- [ ] Restart the bot; confirm the device id is unchanged and the existing
      session still decrypts (no "new device" prompt in the client).
