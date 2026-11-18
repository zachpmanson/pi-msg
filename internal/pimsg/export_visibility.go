package pimsg

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"unicode/utf8"
)

const sessionDataTag = `<script id="session-data" type="application/json">`

var sessionDataScripts = regexp.MustCompile(`(?i)<script\b[^>]*\bid\s*=\s*["']session-data["'][^>]*>`)

// visibleExportContext changes only the native HTML artifact, never SessionManager
// or its JSONL. Pi 0.84.4 renders custom_message only when display is truthy;
// custom entries are opaque extension state, not injected model context.
func visibleExportContext(html []byte) ([]byte, error) {
	fail := func(reason string) ([]byte, error) {
		return nil, fmt.Errorf("unsupported or malformed Pi session-data export: %s", reason)
	}
	matches := sessionDataScripts.FindAllIndex(html, -1)
	if len(matches) != 1 {
		return fail("expected exactly one session-data script")
	}
	m := matches[0]
	if string(html[m[0]:m[1]]) != sessionDataTag {
		return fail("unsupported session-data script tag")
	}
	end := bytes.Index(html[m[1]:], []byte("</script>"))
	if end < 0 {
		return fail("missing session-data closing tag")
	}
	end += m[1]
	payload, err := base64.StdEncoding.Strict().DecodeString(string(html[m[1]:end]))
	if err != nil || !utf8.Valid(payload) {
		return fail("invalid base64 or UTF-8 payload")
	}
	// Reject duplicate keys rather than guessing which value Pi/JSON.parse uses.
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := uniqueJSONValue(dec); err != nil {
		return fail("invalid or ambiguous JSON payload")
	}
	if _, err := dec.Token(); err != io.EOF {
		return fail("trailing JSON payload")
	}
	if !pairedUnicodeEscapes(payload) {
		return fail("unpaired Unicode surrogate escape")
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(payload, &data) != nil || data == nil {
		return fail("expected session data object")
	}
	var entries []map[string]json.RawMessage
	if raw := data["entries"]; len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &entries) != nil {
		return fail("expected entries array of objects")
	}
	changed := false
	for _, entry := range entries {
		var typ string
		if entry == nil || json.Unmarshal(entry["type"], &typ) != nil || typ == "" {
			return fail("entry missing type")
		}
		if typ != "custom_message" {
			continue
		}
		// Untyped extensions can omit customType (e.g. installed subagent
		// workflow-list output); native Pi still renders that context normally.
		if raw, ok := entry["customType"]; ok {
			var customType string
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &customType) != nil {
				return fail("custom_message customType must be a string when present")
			}
		}
		content := bytes.TrimSpace(entry["content"])
		if len(content) == 0 || (content[0] != '"' && content[0] != '[') {
			return fail("custom_message requires string or block content")
		}
		if content[0] == '[' {
			var blocks []map[string]json.RawMessage
			if json.Unmarshal(content, &blocks) != nil {
				return fail("custom_message content blocks must be objects")
			}
			for _, block := range blocks {
				var kind string
				if block == nil || json.Unmarshal(block["type"], &kind) != nil {
					return fail("custom_message content block missing type")
				}
				fields := []string{"text"}
				if kind == "image" {
					fields = []string{"data", "mimeType"}
				} else if kind != "text" {
					return fail("unsupported custom_message content block type")
				}
				for _, field := range fields {
					var value string
					if raw := bytes.TrimSpace(block[field]); len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil {
						return fail("custom_message content block requires string " + field)
					}
				}
			}
		}
		var display bool
		if raw, ok := entry["display"]; ok {
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &display) != nil {
				return fail("custom_message display must be boolean")
			}
		}
		if !display {
			entry["display"] = json.RawMessage("true")
			changed = true
		}
	}
	if !changed {
		return html, nil // Preserve exact original bytes for a no-op.
	}
	// RawMessage preserves unknown fields and numbers without float conversion.
	data["entries"], err = json.Marshal(entries)
	if err != nil {
		return fail("cannot encode entries")
	}
	payload, err = json.Marshal(data)
	if err != nil {
		return fail("cannot encode session data")
	}
	out := make([]byte, 0, len(html)+len(payload))
	out = append(out, html[:m[1]]...)
	out = base64.StdEncoding.AppendEncode(out, payload)
	out = append(out, html[end:]...)
	return out, nil
}

// pairedUnicodeEscapes runs after JSON syntax validation. Go replaces lone
// surrogates with U+FFFD, which would corrupt unknown object keys on re-encoding.
func pairedUnicodeEscapes(payload []byte) bool {
	for i := 0; i < len(payload); i++ {
		if payload[i] != '\\' {
			continue
		}
		i++ // Skip escaped backslashes/quotes without interpreting their text.
		if payload[i] != 'u' {
			continue
		}
		code, _ := strconv.ParseUint(string(payload[i+1:i+5]), 16, 16)
		i += 4
		if code >= 0xdc00 && code <= 0xdfff {
			return false
		}
		if code >= 0xd800 && code <= 0xdbff {
			if i+7 > len(payload) || payload[i+1] != '\\' || payload[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(payload[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

// uniqueJSONValue validates one JSON value, including nested duplicate keys.
func uniqueJSONValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	var close json.Delim
	switch delim {
	case '{':
		close = '}'
		seen := make(map[string]bool)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid object key")
			}
			seen[name] = true
			if err := uniqueJSONValue(dec); err != nil {
				return err
			}
		}
	case '[':
		close = ']'
		for dec.More() {
			if err := uniqueJSONValue(dec); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected delimiter")
	}
	end, err := dec.Token()
	if err != nil || end != close {
		return fmt.Errorf("invalid closing delimiter")
	}
	return nil
}
