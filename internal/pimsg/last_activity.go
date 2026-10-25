package pimsg

import (
	"context"
	"encoding/xml"
	"fmt"
	"time"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const lastActivityNS = "jabber:iq:last"

type lastActivityQuery struct {
	XMLName xml.Name `xml:"jabber:iq:last query"`
	Seconds *uint64  `xml:"seconds,attr,omitempty"`
}

// Only a direct IQ child is a request, not a query nested inside another payload.
func hasLastActivityQuery(toks []xml.Token) bool {
	depth := 0
	for _, tok := range toks {
		switch t := tok.(type) {
		case xml.StartElement:
			if depth == 0 && t.Name.Space == lastActivityNS && t.Name.Local == "query" {
				return true
			}
			depth++
		case xml.EndElement:
			depth--
		}
	}
	return false
}

// encodeLastActivity implements XEP-0012's online-resource query (§4), not
// server uptime or offline last-seen. The server MUST enforce presence
// subscriptions, privacy lists and blocking before forwarding these queries
// (§§4,8); messaging permissions and MUC membership are not subscriptions.
func (b *XMPPBridge) encodeLastActivity(to, id, typ string) error {
	// Never answer results/errors (including unsolicited ones), avoiding loops.
	if typ != "get" && typ != "set" {
		return nil
	}
	if to == "" {
		return nil // no requester to reply to; never send idle data to the server
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return fmt.Errorf("invalid last activity sender %q: %w", to, err)
	}
	resp := struct {
		stanza.IQ
		Query lastActivityQuery
		Error *stanza.Error `xml:",omitempty"`
	}{IQ: stanza.IQ{ID: id, To: toJID, Type: stanza.ResultIQ}}
	if !b.acct.LastActivity {
		resp.Type = stanza.ErrorIQ
		resp.Error = &stanza.Error{Type: stanza.Cancel, Condition: stanza.ServiceUnavailable}
	} else if typ != "get" {
		resp.Type = stanza.ErrorIQ
		resp.Error = &stanza.Error{Type: stanza.Modify, Condition: stanza.BadRequest}
	} else {
		b.mu.Lock()
		since := b.idleSince
		b.mu.Unlock()
		var seconds uint64
		if !since.IsZero() && since.Before(time.Now()) {
			seconds = uint64(time.Since(since) / time.Second)
		}
		resp.Query.Seconds = &seconds
	}
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	return b.encode(ctx, session, resp)
}
