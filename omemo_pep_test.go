package main

import (
	"bytes"
	"encoding/xml"
	"testing"

	"github.com/zachpmanson/pi-msg/internal/omemo"
)

func sampleBundle() omemo.Bundle {
	return omemo.Bundle{
		SignedPreKeyID:  7,
		SignedPreKeyPub: bytes.Repeat([]byte{0x05, 0xAB}, 17)[:33],
		SignedPreKeySig: bytes.Repeat([]byte{0x11}, 64),
		IdentityKey:     bytes.Repeat([]byte{0x05, 0xCD}, 17)[:33],
		PreKeys: []omemo.PreKey{
			{ID: 1, Pub: bytes.Repeat([]byte{0x05, 0x01}, 17)[:33]},
			{ID: 2, Pub: bytes.Repeat([]byte{0x05, 0x02}, 17)[:33]},
		},
	}
}

func TestBundleXMLRoundTrip(t *testing.T) {
	orig := sampleBundle()
	x := bundleToXML(orig)
	got, err := bundleFromXML(x)
	if err != nil {
		t.Fatalf("bundleFromXML: %v", err)
	}
	if got.SignedPreKeyID != orig.SignedPreKeyID {
		t.Errorf("spk id: %d != %d", got.SignedPreKeyID, orig.SignedPreKeyID)
	}
	if !bytes.Equal(got.SignedPreKeyPub, orig.SignedPreKeyPub) {
		t.Error("signed prekey pub mismatch")
	}
	if !bytes.Equal(got.SignedPreKeySig, orig.SignedPreKeySig) {
		t.Error("signature mismatch")
	}
	if !bytes.Equal(got.IdentityKey, orig.IdentityKey) {
		t.Error("identity key mismatch")
	}
	if len(got.PreKeys) != 2 {
		t.Fatalf("prekeys: got %d want 2", len(got.PreKeys))
	}
	for i := range orig.PreKeys {
		if got.PreKeys[i].ID != orig.PreKeys[i].ID || !bytes.Equal(got.PreKeys[i].Pub, orig.PreKeys[i].Pub) {
			t.Errorf("prekey %d mismatch", i)
		}
	}
}

// TestBundleThroughWireXML marshals the bundle inside a publish, then unmarshals
// it through the fetch-response struct — the real path a bundle takes from us to
// the server and back to a peer.
func TestBundleThroughWireXML(t *testing.T) {
	var p pubsubPublishBundle
	p.Publish.Node = bundleNode(42)
	p.Publish.Item.ID = itemIDCurr
	p.Publish.Item.Bundle = bundleToXML(sampleBundle())
	p.PublishOptions = openAccessOptions()

	data, err := xml.Marshal(p)
	if err != nil {
		t.Fatalf("marshal publish: %v", err)
	}
	out := string(data)
	t.Logf("wire: %s", out)
	if bytes.Contains(data, []byte(`xmlns=""`)) {
		t.Fatalf("escaped namespace in publish:\n%s", out)
	}
	for _, want := range []string{
		`<pubsub xmlns="http://jabber.org/protocol/pubsub">`,
		`node="eu.siacs.conversations.axolotl.bundles:42"`,
		`<bundle xmlns="eu.siacs.conversations.axolotl">`,
		`<signedPreKeyPublic signedPreKeyId="7">`,
		`<preKeyPublic preKeyId="1">`,
		`<publish-options>`,
		`<value>open</value>`,
	} {
		if !bytes.Contains(data, []byte(want)) {
			t.Errorf("publish XML missing %q\nfull: %s", want, out)
		}
	}

	// Simulate the server echoing the item back inside an <items> result.
	respXML := `<pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="x">` +
		`<item id="current">` + string(mustMarshal(t, p.Publish.Item.Bundle)) + `</item></items></pubsub>`
	var resp pubsubBundleResp
	if err := xml.Unmarshal([]byte(respXML), &resp); err != nil {
		t.Fatalf("unmarshal resp: %v", err)
	}
	if len(resp.Items.Item) != 1 {
		t.Fatalf("want 1 item, got %d", len(resp.Items.Item))
	}
	b, err := bundleFromXML(resp.Items.Item[0].Bundle)
	if err != nil {
		t.Fatalf("bundleFromXML from resp: %v", err)
	}
	if b.SignedPreKeyID != 7 || len(b.PreKeys) != 2 {
		t.Errorf("bundle from resp wrong: %+v", b)
	}
}

func TestDeviceListXML(t *testing.T) {
	var p pubsubPublishList
	p.Publish.Node = deviceListNode
	p.Publish.Item.ID = itemIDCurr
	p.Publish.Item.List.Devices = []deviceItem{{ID: 111}, {ID: 222}}
	p.PublishOptions = openAccessOptions()
	data, err := xml.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := string(data)
	if bytes.Contains(data, []byte(`xmlns=""`)) {
		t.Fatalf("escaped namespace:\n%s", out)
	}
	for _, want := range []string{
		`<list xmlns="eu.siacs.conversations.axolotl">`,
		`<device id="111">`,
		`<device id="222">`,
	} {
		if !bytes.Contains(data, []byte(want)) {
			t.Errorf("devicelist XML missing %q\nfull: %s", want, out)
		}
	}

	// Round-trip through the fetch-response struct.
	respXML := `<pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="x"><item>` +
		string(mustMarshal(t, p.Publish.Item.List)) + `</item></items></pubsub>`
	var resp pubsubListResp
	if err := xml.Unmarshal([]byte(respXML), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var ids []uint32
	for _, it := range resp.Items.Item {
		for _, d := range it.List.Devices {
			ids = append(ids, d.ID)
		}
	}
	if len(ids) != 2 || ids[0] != 111 || ids[1] != 222 {
		t.Errorf("devicelist ids wrong: %v", ids)
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	data, err := xml.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
