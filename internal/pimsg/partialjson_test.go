package pimsg

import "testing"

func TestCompleteJSONStringField(t *testing.T) {
	tests := []struct {
		name string
		json string
		want string
		ok   bool
	}{
		{name: "complete first field", json: `{"to":"person@example.org","text":`, want: "person@example.org", ok: true},
		{name: "complete after nested field", json: `{"meta":{"to":"wrong","items":[1,{"x":"}"}]},"to":"right@example.org",`, want: "right@example.org", ok: true},
		{name: "unescapes value", json: `{"to":"person\\u0040example.org",`, want: `person\u0040example.org`, ok: true},
		{name: "incomplete string", json: `{"to":"person@example.org`, ok: false},
		{name: "closed string without member boundary", json: `{"to":"person@example.org"`, ok: false},
		{name: "nested key only", json: `{"meta":{"to":"person@example.org"},"text":`, ok: false},
		{name: "non-string value", json: `{"to":42,`, ok: false},
		{name: "target not yet arrived", json: `{"text":"hi",`, ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := completeJSONStringField(tt.json, "to")
			if got != tt.want || ok != tt.ok {
				t.Fatalf("completeJSONStringField() = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}
