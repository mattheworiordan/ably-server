package protocol

import "unicode/utf8"

// DecodeSimpleMessageJSON is a fast path for the most common REST publish
// body (DESIGN.md §2.2): a single JSON object whose members are all
// plain strings drawn from name, data, id, clientId, encoding and
// connectionKey, written without escape sequences. For such a body it
// returns the Message exactly as json.Unmarshal into a Message would
// (including the payload normalisation of Message.UnmarshalJSON, such as
// decoding base64 data) and ok=true, without reflection or interface{}
// decoding.
//
// Any other input — an array, another member, a non-string value, null,
// a key in another case, an escape sequence, invalid UTF-8, trailing
// content — returns ok=false, and the caller decodes with encoding/json
// as before. A malformed body is therefore reported by encoding/json,
// never here. err is non-nil only when the body matched the fast shape
// but its payload failed normalisation (for example bad base64), exactly
// where Message.UnmarshalJSON would fail.
func DecodeSimpleMessageJSON(b []byte) (m *Message, ok bool, err error) {
	i := skipJSONSpace(b, 0)
	if i >= len(b) || b[i] != '{' {
		return nil, false, nil
	}
	i = skipJSONSpace(b, i+1)
	var msg Message
	var data string
	var haveData bool
	if i < len(b) && b[i] == '}' {
		i++
	} else {
		for {
			var key, val string
			var good bool
			if key, i, good = scanPlainJSONString(b, i); !good {
				return nil, false, nil
			}
			i = skipJSONSpace(b, i)
			if i >= len(b) || b[i] != ':' {
				return nil, false, nil
			}
			i = skipJSONSpace(b, i+1)
			if val, i, good = scanPlainJSONString(b, i); !good {
				return nil, false, nil
			}
			switch key {
			case "name":
				msg.Name = val
			case "data":
				data, haveData = val, true
			case "id":
				msg.ID = val
			case "clientId":
				msg.ClientID = val
			case "encoding":
				msg.Encoding = val
			case "connectionKey":
				msg.ConnectionKey = val
			default:
				return nil, false, nil
			}
			i = skipJSONSpace(b, i)
			if i >= len(b) {
				return nil, false, nil
			}
			if b[i] == '}' {
				i++
				break
			}
			if b[i] != ',' {
				return nil, false, nil
			}
			i = skipJSONSpace(b, i+1)
		}
	}
	if skipJSONSpace(b, i) != len(b) {
		return nil, false, nil
	}
	if haveData {
		d, enc, nerr := normaliseInboundData(data, msg.Encoding, false)
		if nerr != nil {
			return nil, true, nerr
		}
		msg.Data = d
		msg.Encoding = enc
	}
	return &msg, true, nil
}

func skipJSONSpace(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// scanPlainJSONString reads a JSON string starting at b[i] that contains
// no escape sequences and no control characters and is valid UTF-8,
// returning its contents and the index after the closing quote.
func scanPlainJSONString(b []byte, i int) (s string, next int, ok bool) {
	if i >= len(b) || b[i] != '"' {
		return "", i, false
	}
	start := i + 1
	ascii := true
	for j := start; j < len(b); j++ {
		c := b[j]
		switch {
		case c == '"':
			raw := b[start:j]
			if !ascii && !utf8.Valid(raw) {
				return "", i, false
			}
			return string(raw), j + 1, true
		case c == '\\' || c < 0x20:
			return "", i, false
		case c >= utf8.RuneSelf:
			ascii = false
		}
	}
	return "", i, false
}
