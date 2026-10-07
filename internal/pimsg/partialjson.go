package pimsg

import (
	"encoding/json"
	"strings"
)

// completeJSONStringField returns a complete top-level string field from a
// JSON object prefix. Unlike a permissive partial-JSON parser, it waits for the
// value's closing quote and member delimiter so a still-growing JID is never
// used as a destination.
func completeJSONStringField(src, field string) (string, bool) {
	i := skipJSONSpace(src, 0)
	if i >= len(src) || src[i] != '{' {
		return "", false
	}
	i++
	for {
		i = skipJSONSpace(src, i)
		if i >= len(src) || src[i] == '}' {
			return "", false
		}
		key, next, ok := scanJSONString(src, i)
		if !ok {
			return "", false
		}
		i = skipJSONSpace(src, next)
		if i >= len(src) || src[i] != ':' {
			return "", false
		}
		i = skipJSONSpace(src, i+1)
		if key == field {
			value, next, ok := scanJSONString(src, i)
			if !ok {
				return "", false
			}
			next = skipJSONSpace(src, next)
			if next >= len(src) || (src[next] != ',' && src[next] != '}') {
				return "", false
			}
			return value, true
		}
		i, ok = skipJSONValue(src, i)
		if !ok {
			return "", false
		}
		i = skipJSONSpace(src, i)
		if i >= len(src) || src[i] != ',' {
			return "", false
		}
		i++
	}
}

func scanJSONString(src string, start int) (string, int, bool) {
	if start >= len(src) || src[start] != '"' {
		return "", start, false
	}
	for i := start + 1; i < len(src); i++ {
		switch src[i] {
		case '\\':
			i++ // The escaped byte cannot terminate the string.
		case '"':
			var value string
			if err := json.Unmarshal([]byte(src[start:i+1]), &value); err != nil {
				return "", start, false
			}
			return value, i + 1, true
		}
	}
	return "", start, false
}

// skipJSONValue skips one complete JSON value, including nested arrays and
// objects, while respecting quoted strings. It is used only to walk fields
// before the target field in an incomplete argument object.
func skipJSONValue(src string, start int) (int, bool) {
	if start >= len(src) {
		return start, false
	}
	if src[start] == '"' {
		_, next, ok := scanJSONString(src, start)
		return next, ok
	}
	if src[start] == '{' || src[start] == '[' {
		stack := []byte{src[start]}
		for i := start + 1; i < len(src); i++ {
			if src[i] == '"' {
				_, next, ok := scanJSONString(src, i)
				if !ok {
					return start, false
				}
				i = next - 1
				continue
			}
			switch src[i] {
			case '{', '[':
				stack = append(stack, src[i])
			case '}', ']':
				if len(stack) == 0 || (src[i] == '}' && stack[len(stack)-1] != '{') || (src[i] == ']' && stack[len(stack)-1] != '[') {
					return start, false
				}
				stack = stack[:len(stack)-1]
				if len(stack) == 0 {
					return i + 1, true
				}
			}
		}
		return start, false
	}
	for i := start; i < len(src); i++ {
		if src[i] == ',' || src[i] == '}' {
			return i, i > start
		}
		if strings.ContainsRune(" \t\r\n", rune(src[i])) {
			return i, i > start
		}
	}
	return len(src), len(src) > start
}

func skipJSONSpace(src string, i int) int {
	for i < len(src) && strings.ContainsRune(" \t\r\n", rune(src[i])) {
		i++
	}
	return i
}
