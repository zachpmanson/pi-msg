package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"sort"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/zachpmanson/pi-msg/internal/omemo"
)

const (
	pubsubNS     = "http://jabber.org/protocol/pubsub"
	dataFormsNS  = "jabber:x:data"
	itemIDCurr   = "current"
	pubOptionsFT = "http://jabber.org/protocol/pubsub#publish-options"
)

// ---- XEP-0004 data form for publish-options (open access model) ----

type dataField struct {
	Var    string   `xml:"var,attr"`
	Type   string   `xml:"type,attr,omitempty"`
	Values []string `xml:"value"`
}

type dataForm struct {
	XMLName xml.Name    `xml:"jabber:x:data x"`
	Type    string      `xml:"type,attr"`
	Fields  []dataField `xml:"field"`
}

type publishOptions struct {
	XMLName xml.Name `xml:"publish-options"`
	Form    dataForm `xml:"jabber:x:data x"`
}

// openAccessOptions requests pubsub#access_model=open so contacts (and our own
// other devices) can fetch our bundles/devicelist without a subscription — the
// pragmatic default for OMEMO 0.3, which predates mandatory publish-options.
func openAccessOptions() publishOptions {
	return publishOptions{Form: dataForm{
		Type: "submit",
		Fields: []dataField{
			{Var: "FORM_TYPE", Type: "hidden", Values: []string{pubOptionsFT}},
			{Var: "pubsub#access_model", Values: []string{"open"}},
		},
	}}
}

// ---- devicelist node payload ----

type deviceItem struct {
	ID uint32 `xml:"id,attr"`
}

type deviceListXML struct {
	XMLName xml.Name     `xml:"eu.siacs.conversations.axolotl list"`
	Devices []deviceItem `xml:"device"`
}

// ---- bundle node payload ----

type spkPublicXML struct {
	ID   uint32 `xml:"signedPreKeyId,attr"`
	Data string `xml:",chardata"`
}

type preKeyPublicXML struct {
	ID   uint32 `xml:"preKeyId,attr"`
	Data string `xml:",chardata"`
}

type bundleXML struct {
	XMLName               xml.Name     `xml:"eu.siacs.conversations.axolotl bundle"`
	SignedPreKeyPublic    spkPublicXML `xml:"signedPreKeyPublic"`
	SignedPreKeySignature string       `xml:"signedPreKeySignature"`
	IdentityKey           string       `xml:"identityKey"`
	PreKeys               struct {
		Keys []preKeyPublicXML `xml:"preKeyPublic"`
	} `xml:"prekeys"`
}

func bundleToXML(b omemo.Bundle) bundleXML {
	var x bundleXML
	x.SignedPreKeyPublic = spkPublicXML{ID: b.SignedPreKeyID, Data: base64.StdEncoding.EncodeToString(b.SignedPreKeyPub)}
	x.SignedPreKeySignature = base64.StdEncoding.EncodeToString(b.SignedPreKeySig)
	x.IdentityKey = base64.StdEncoding.EncodeToString(b.IdentityKey)
	for _, pk := range b.PreKeys {
		x.PreKeys.Keys = append(x.PreKeys.Keys, preKeyPublicXML{ID: pk.ID, Data: base64.StdEncoding.EncodeToString(pk.Pub)})
	}
	return x
}

func bundleFromXML(x bundleXML) (omemo.Bundle, error) {
	spk, err := b64decode(x.SignedPreKeyPublic.Data)
	if err != nil {
		return omemo.Bundle{}, fmt.Errorf("bad signedPreKeyPublic: %w", err)
	}
	sig, err := b64decode(x.SignedPreKeySignature)
	if err != nil {
		return omemo.Bundle{}, fmt.Errorf("bad signedPreKeySignature: %w", err)
	}
	ik, err := b64decode(x.IdentityKey)
	if err != nil {
		return omemo.Bundle{}, fmt.Errorf("bad identityKey: %w", err)
	}
	b := omemo.Bundle{
		SignedPreKeyID:  x.SignedPreKeyPublic.ID,
		SignedPreKeyPub: spk,
		SignedPreKeySig: sig,
		IdentityKey:     ik,
	}
	for _, pk := range x.PreKeys.Keys {
		raw, err := b64decode(pk.Data)
		if err != nil {
			continue // skip a single malformed prekey rather than fail the whole bundle
		}
		b.PreKeys = append(b.PreKeys, omemo.PreKey{ID: pk.ID, Pub: raw})
	}
	if len(b.PreKeys) == 0 {
		return omemo.Bundle{}, fmt.Errorf("bundle has no usable prekeys")
	}
	return b, nil
}

// ---- publish (IQ set) ----

type pubsubPublishList struct {
	XMLName xml.Name `xml:"http://jabber.org/protocol/pubsub pubsub"`
	Publish struct {
		Node string `xml:"node,attr"`
		Item struct {
			ID   string        `xml:"id,attr"`
			List deviceListXML `xml:"eu.siacs.conversations.axolotl list"`
		} `xml:"item"`
	} `xml:"publish"`
	PublishOptions publishOptions `xml:"publish-options"`
}

type pubsubPublishBundle struct {
	XMLName xml.Name `xml:"http://jabber.org/protocol/pubsub pubsub"`
	Publish struct {
		Node string `xml:"node,attr"`
		Item struct {
			ID     string    `xml:"id,attr"`
			Bundle bundleXML `xml:"eu.siacs.conversations.axolotl bundle"`
		} `xml:"item"`
	} `xml:"publish"`
	PublishOptions publishOptions `xml:"publish-options"`
}

// setIQ marshals payload, sends it as an IQ set to `to` (empty = own PEP), and
// returns any error reply. A nil result means we don't decode the response body.
func setIQ(ctx context.Context, s *xmpp.Session, to jid.JID, payload any) error {
	r, err := marshalReader(payload)
	if err != nil {
		return err
	}
	return s.UnmarshalIQElement(ctx, r, stanza.IQ{Type: stanza.SetIQ, To: to}, nil)
}

// getIQ marshals a query payload, sends it as an IQ get to `to`, and decodes the
// response payload into out.
func getIQ(ctx context.Context, s *xmpp.Session, to jid.JID, payload, out any) error {
	r, err := marshalReader(payload)
	if err != nil {
		return err
	}
	return s.UnmarshalIQElement(ctx, r, stanza.IQ{Type: stanza.GetIQ, To: to}, out)
}

func marshalReader(v any) (*xml.Decoder, error) {
	data, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return xml.NewDecoder(bytes.NewReader(data)), nil
}

// publishDeviceList publishes our device id set to the devicelist PEP node.
func publishDeviceList(ctx context.Context, s *xmpp.Session, devices []uint32) error {
	sort.Slice(devices, func(i, j int) bool { return devices[i] < devices[j] })
	var p pubsubPublishList
	p.Publish.Node = deviceListNode
	p.Publish.Item.ID = itemIDCurr
	for _, d := range devices {
		p.Publish.Item.List.Devices = append(p.Publish.Item.List.Devices, deviceItem{ID: d})
	}
	p.PublishOptions = openAccessOptions()
	return setIQ(ctx, s, jid.JID{}, p)
}

// publishBundle publishes our bundle to bundles:<deviceID>.
func publishBundle(ctx context.Context, s *xmpp.Session, deviceID uint32, b omemo.Bundle) error {
	var p pubsubPublishBundle
	p.Publish.Node = bundleNode(deviceID)
	p.Publish.Item.ID = itemIDCurr
	p.Publish.Item.Bundle = bundleToXML(b)
	p.PublishOptions = openAccessOptions()
	return setIQ(ctx, s, jid.JID{}, p)
}

// ---- fetch (IQ get) ----

type pubsubItemsQuery struct {
	XMLName xml.Name `xml:"http://jabber.org/protocol/pubsub pubsub"`
	Items   struct {
		Node     string `xml:"node,attr"`
		MaxItems uint64 `xml:"max_items,attr,omitempty"`
	} `xml:"items"`
}

type pubsubListResp struct {
	XMLName xml.Name `xml:"http://jabber.org/protocol/pubsub pubsub"`
	Items   struct {
		Item []struct {
			List deviceListXML `xml:"list"`
		} `xml:"item"`
	} `xml:"items"`
}

type pubsubBundleResp struct {
	XMLName xml.Name `xml:"http://jabber.org/protocol/pubsub pubsub"`
	Items   struct {
		Item []struct {
			Bundle bundleXML `xml:"bundle"`
		} `xml:"item"`
	} `xml:"items"`
}

// fetchDeviceList retrieves a peer's (or our own) published device ids.
func fetchDeviceList(ctx context.Context, s *xmpp.Session, target jid.JID) ([]uint32, error) {
	var q pubsubItemsQuery
	q.Items.Node = deviceListNode
	q.Items.MaxItems = 1
	var resp pubsubListResp
	if err := getIQ(ctx, s, target, q, &resp); err != nil {
		return nil, err
	}
	var out []uint32
	for _, it := range resp.Items.Item {
		for _, d := range it.List.Devices {
			out = append(out, d.ID)
		}
	}
	return out, nil
}

// fetchBundle retrieves a specific device's bundle.
func fetchBundle(ctx context.Context, s *xmpp.Session, target jid.JID, deviceID uint32) (omemo.Bundle, error) {
	var q pubsubItemsQuery
	q.Items.Node = bundleNode(deviceID)
	q.Items.MaxItems = 1
	var resp pubsubBundleResp
	if err := getIQ(ctx, s, target, q, &resp); err != nil {
		return omemo.Bundle{}, err
	}
	if len(resp.Items.Item) == 0 {
		return omemo.Bundle{}, fmt.Errorf("no bundle published for %s/%d", target, deviceID)
	}
	return bundleFromXML(resp.Items.Item[0].Bundle)
}
