package main

import (
	"encoding/base64"
	"encoding/xml"
	"strconv"
	"strings"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/zachpmanson/pi-msg/internal/omemo"
)

// OMEMO namespaces (legacy XEP-0384 v0.3 / Conversations axolotl).
const (
	axolotlNS         = "eu.siacs.conversations.axolotl"
	deviceListNode    = "eu.siacs.conversations.axolotl.devicelist"
	bundlesNodePrefix = "eu.siacs.conversations.axolotl.bundles:"
	hintsNS           = "urn:xmpp:hints"
)

// ---- outgoing <encrypted> stanza ----
//
// The header/key/iv/payload children carry no explicit namespace: they inherit
// the axolotl default namespace declared on <encrypted>, which is exactly what
// Conversations/converse.js emit and expect. omemo_wire_test.go pins the
// marshalled bytes so this stays true.

type encKeyElem struct {
	XMLName xml.Name `xml:"key"`
	RID     uint32   `xml:"rid,attr"`
	PreKey  bool     `xml:"prekey,attr,omitempty"`
	Data    string   `xml:",chardata"`
}

type encHeaderElem struct {
	XMLName xml.Name     `xml:"header"`
	SID     uint32       `xml:"sid,attr"`
	Keys    []encKeyElem `xml:"key"`
	IV      string       `xml:"iv"`
}

type encryptedElem struct {
	XMLName xml.Name      `xml:"eu.siacs.conversations.axolotl encrypted"`
	Header  encHeaderElem `xml:"header"`
	Payload string        `xml:"payload,omitempty"`
}

// encryptedMessage is a full <message> carrying an OMEMO <encrypted> element. A
// <store/> hint (XEP-0334) asks the server to archive it despite having no
// <body>, and an empty <body> fallback nudges non-OMEMO clients to show
// something rather than silently dropping it.
type encryptedMessage struct {
	stanza.Message
	Encrypted encryptedElem `xml:"eu.siacs.conversations.axolotl encrypted"`
	Store     *struct{}     `xml:"urn:xmpp:hints store"`
	Body      string        `xml:"body,omitempty"`
}

// omemoFallbackBody is shown by clients that can't decrypt OMEMO. Conversations
// and converse.js both replace it with the decrypted text, so it's only ever
// seen by a non-OMEMO client.
const omemoFallbackBody = "[This message is OMEMO-encrypted; your client does not support it.]"

// buildEncryptedMessage marshals an omemo.Message into a sendable stanza.
func buildEncryptedMessage(to jid.JID, typ stanza.MessageType, m *omemo.Message) encryptedMessage {
	hdr := encHeaderElem{SID: m.SID, IV: base64.StdEncoding.EncodeToString(m.IV)}
	for _, k := range m.Keys {
		hdr.Keys = append(hdr.Keys, encKeyElem{
			RID:    k.RID,
			PreKey: k.PreKey,
			Data:   base64.StdEncoding.EncodeToString(k.Data),
		})
	}
	msg := encryptedMessage{
		Message:   stanza.Message{ID: newStanzaID(), To: to, Type: typ},
		Encrypted: encryptedElem{Header: hdr},
		Store:     &struct{}{},
		Body:      omemoFallbackBody,
	}
	if m.Payload != nil {
		msg.Encrypted.Payload = base64.StdEncoding.EncodeToString(m.Payload)
	}
	return msg
}

// parseEncrypted extracts an omemo.Message from the flat token slice of a
// received <message>. It returns false if the message carries no axolotl
// <encrypted> element. The <key>/<iv>/<payload> local names appear only inside
// <encrypted>, so a flat scan is unambiguous.
func parseEncrypted(toks []xml.Token) (*omemo.Message, bool) {
	if _, ok := element(toks, axolotlNS, "encrypted"); !ok {
		return nil, false
	}
	m := &omemo.Message{}
	for i, tok := range toks {
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch se.Name.Local {
		case "header":
			m.SID = parseUint32(attr(se.Attr, "sid"))
		case "key":
			ke := omemo.KeyEntry{RID: parseUint32(attr(se.Attr, "rid"))}
			if pk := attr(se.Attr, "prekey"); pk == "true" || pk == "1" {
				ke.PreKey = true
			}
			if raw, err := b64decode(nextText(toks, i)); err == nil {
				ke.Data = raw
			}
			m.Keys = append(m.Keys, ke)
		case "iv":
			if raw, err := b64decode(nextText(toks, i)); err == nil {
				m.IV = raw
			}
		case "payload":
			if raw, err := b64decode(nextText(toks, i)); err == nil {
				m.Payload = raw
			}
		}
	}
	return m, true
}

// nextText concatenates the CharData tokens immediately following index i (up to
// the next element boundary) and trims surrounding whitespace — base64 bodies
// may arrive split across tokens or padded with newlines.
func nextText(toks []xml.Token, i int) string {
	var sb strings.Builder
	for j := i + 1; j < len(toks); j++ {
		cd, ok := toks[j].(xml.CharData)
		if !ok {
			break
		}
		sb.Write(cd)
	}
	return strings.TrimSpace(sb.String())
}

func b64decode(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimSpace(s))
}

func parseUint32(s string) uint32 {
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

func bundleNode(deviceID uint32) string {
	return bundlesNodePrefix + strconv.FormatUint(uint64(deviceID), 10)
}
