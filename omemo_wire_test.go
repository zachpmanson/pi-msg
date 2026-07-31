package main

import (
	"bytes"
	"encoding/xml"
	"testing"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/zachpmanson/pi-msg/internal/omemo"
)

func tokenize(t *testing.T, v any) []xml.Token {
	t.Helper()
	data, err := xml.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	dec := xml.NewDecoder(bytes.NewReader(data))
	var toks []xml.Token
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		toks = append(toks, xml.CopyToken(tok))
	}
	return toks
}

func sampleMessage() *omemo.Message {
	return &omemo.Message{
		SID: 27183,
		Keys: []omemo.KeyEntry{
			{RID: 31415, PreKey: false, Data: []byte("whisper-bytes")},
			{RID: 12321, PreKey: true, Data: []byte("prekey-bytes")},
		},
		IV:      []byte("twelve-byte!"),
		Payload: []byte("ciphertext-here"),
	}
}

// TestEncryptedMarshalNamespaces is the interop-critical check: the header/key/
// iv/payload children must sit in the axolotl namespace (inherited or explicit),
// never xmlns="" (no namespace), or Conversations/converse.js reject the stanza.
func TestEncryptedMarshalNamespaces(t *testing.T) {
	to, _ := jid.Parse("juliet@capulet.lit")
	msg := buildEncryptedMessage(to, stanza.ChatMessage, sampleMessage())
	data, err := xml.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := string(data)
	t.Logf("wire: %s", out)

	if bytes.Contains(data, []byte(`xmlns=""`)) {
		t.Fatalf("child element escaped to no-namespace (xmlns=\"\"); breaks interop:\n%s", out)
	}
	for _, want := range []string{
		`<encrypted xmlns="eu.siacs.conversations.axolotl">`,
		`<header sid="27183">`,
		`rid="31415"`,
		`rid="12321"`,
		`prekey="true"`,
		`<iv>`,
		`<payload>`,
	} {
		if !bytes.Contains(data, []byte(want)) {
			t.Errorf("marshalled stanza missing %q\nfull: %s", want, out)
		}
	}
	// The non-prekey key must NOT carry a prekey attribute.
	if bytes.Contains(data, []byte(`rid="31415" prekey`)) {
		t.Errorf("non-prekey key should omit prekey attr:\n%s", out)
	}
}

func TestEncryptedRoundTrip(t *testing.T) {
	orig := sampleMessage()
	to, _ := jid.Parse("juliet@capulet.lit")
	msg := buildEncryptedMessage(to, stanza.ChatMessage, orig)

	toks := tokenize(t, msg.Encrypted)
	got, ok := parseEncrypted(toks)
	if !ok {
		t.Fatalf("parseEncrypted returned false on an encrypted element")
	}
	if got.SID != orig.SID {
		t.Errorf("sid: got %d want %d", got.SID, orig.SID)
	}
	if len(got.Keys) != len(orig.Keys) {
		t.Fatalf("keys: got %d want %d", len(got.Keys), len(orig.Keys))
	}
	for i, k := range orig.Keys {
		if got.Keys[i].RID != k.RID || got.Keys[i].PreKey != k.PreKey || !bytes.Equal(got.Keys[i].Data, k.Data) {
			t.Errorf("key %d: got %+v want %+v", i, got.Keys[i], k)
		}
	}
	if !bytes.Equal(got.IV, orig.IV) {
		t.Errorf("iv: got %q want %q", got.IV, orig.IV)
	}
	if !bytes.Equal(got.Payload, orig.Payload) {
		t.Errorf("payload: got %q want %q", got.Payload, orig.Payload)
	}
}

// TestParseNonOMEMO confirms a plain chat message is not mistaken for OMEMO.
func TestParseNonOMEMO(t *testing.T) {
	toks := []xml.Token{
		xml.StartElement{Name: xml.Name{Local: "body"}},
		xml.CharData("hello world"),
		xml.EndElement{Name: xml.Name{Local: "body"}},
	}
	if _, ok := parseEncrypted(toks); ok {
		t.Fatalf("plain message wrongly detected as OMEMO")
	}
}

// TestKeyTransportNoPayload verifies a payload-less message (session setup)
// round-trips with a nil payload.
func TestKeyTransportNoPayload(t *testing.T) {
	orig := &omemo.Message{SID: 5, Keys: []omemo.KeyEntry{{RID: 9, PreKey: true, Data: []byte("k")}}, IV: []byte("iv")}
	to, _ := jid.Parse("a@b")
	msg := buildEncryptedMessage(to, stanza.ChatMessage, orig)
	if msg.Encrypted.Payload != "" {
		t.Fatalf("expected empty payload element for keytransport")
	}
	got, ok := parseEncrypted(tokenize(t, msg.Encrypted))
	if !ok {
		t.Fatal("not parsed")
	}
	if got.Payload != nil {
		t.Fatalf("expected nil payload, got %q", got.Payload)
	}
}
