package qx

import (
	"bytes"
	"fmt"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// jsonValueDecoder parses and validates the fixed QX wire shape without
// recursively re-entering encoding/json for every Expr node. It uses strict
// JSON rules for object names and strings so direct UnmarshalJSON calls behave
// the same as calls routed through encoding/json.
type jsonValueDecoder struct {
	data  []byte
	pos   int
	depth int
}

const maxJSONNestingDepth = 10_000

type jsonObjectKey struct {
	raw     []byte
	decoded string
}

// jsonNameSet avoids allocating a map for the common case of an object with
// at most one name that cannot be represented by a fixed-field bitset.
type jsonNameSet struct {
	first string
	more  map[string]struct{}
	set   bool
}

func (names *jsonNameSet) add(name string) bool {
	if !names.set {
		names.first = name
		names.set = true
		return true
	}
	if names.more == nil {
		if names.first == name {
			return false
		}
		names.more = map[string]struct{}{names.first: {}, name: {}}
		return true
	}
	if _, exists := names.more[name]; exists {
		return false
	}
	names.more[name] = struct{}{}
	return true
}

func decodeExprJSON(data []byte, dst *Expr) error {
	decoder := jsonValueDecoder{data: data}
	var expr Expr
	if err := decoder.parseExpr(&expr); err != nil {
		return err
	}
	if err := decoder.finish(); err != nil {
		return err
	}
	*dst = expr
	return nil
}

func decodeQXJSON(data []byte, dst *QX) error {
	decoder := jsonValueDecoder{data: data}
	var qx QX
	if err := decoder.parseQX(&qx); err != nil {
		return err
	}
	if err := decoder.finish(); err != nil {
		return err
	}
	*dst = qx
	return nil
}

func (decoder *jsonValueDecoder) finish() error {
	decoder.skipSpace()
	if decoder.pos != len(decoder.data) {
		return decoder.errorf("unexpected trailing data")
	}
	return nil
}

func (decoder *jsonValueDecoder) parseQX(dst *QX) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = QX{}
		}
		return err
	}
	if err := decoder.consume('{'); err != nil {
		return err
	}

	decoder.skipSpace()
	if decoder.consumeIf('}') {
		return nil
	}

	const (
		fieldFilter uint64 = 1 << iota
		fieldReduction
		fieldOrder
		fieldWindow
		fieldProjection
		fieldMeta
	)
	var seen uint64
	var unknown jsonNameSet
	for {
		key, err := decoder.parseObjectKey()
		if err != nil {
			return err
		}
		if err = decoder.consume(':'); err != nil {
			return err
		}

		switch {

		case key.equal("filter"):
			if err = decoder.rejectDuplicateField(&seen, fieldFilter, key); err != nil {
				return err
			}
			err = decoder.parseExpr(&dst.Filter)

		case key.equal("reduction"):
			if err = decoder.rejectDuplicateField(&seen, fieldReduction, key); err != nil {
				return err
			}
			dst.Reduction, err = decoder.parseReduction()

		case key.equal("order"):
			if err = decoder.rejectDuplicateField(&seen, fieldOrder, key); err != nil {
				return err
			}
			dst.Order, err = decoder.parseOrderArray()

		case key.equal("window"):
			if err = decoder.rejectDuplicateField(&seen, fieldWindow, key); err != nil {
				return err
			}
			err = decoder.parseWindow(&dst.Window)

		case key.equal("projection"):
			if err = decoder.rejectDuplicateField(&seen, fieldProjection, key); err != nil {
				return err
			}
			dst.Projection, err = decoder.parseExprArray()

		case key.equal("meta"):
			if err = decoder.rejectDuplicateField(&seen, fieldMeta, key); err != nil {
				return err
			}
			dst.Metadata, err = decoder.parseMetaArray()

		default:
			if err = decoder.rejectDuplicateName(&unknown, key); err != nil {
				return err
			}
			err = decoder.skipValue()
		}
		if err != nil {
			return err
		}

		decoder.skipSpace()
		if decoder.consumeIf('}') {
			return nil
		}
		if err = decoder.consume(','); err != nil {
			return err
		}
	}
}

func (decoder *jsonValueDecoder) parseExpr(dst *Expr) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = Expr{}
		}
		return err
	}
	if err := decoder.consume('{'); err != nil {
		return err
	}

	var expr Expr
	decoder.skipSpace()
	if decoder.consumeIf('}') {
		*dst = expr
		return nil
	}

	const (
		fieldKind uint64 = 1 << iota
		fieldName
		fieldAlias
		fieldValue
		fieldArgs
	)
	var seen uint64
	var unknown jsonNameSet
	for {
		key, err := decoder.parseObjectKey()
		if err != nil {
			return err
		}
		if err = decoder.consume(':'); err != nil {
			return err
		}

		switch {

		case key.equal("kind"):
			if err = decoder.rejectDuplicateField(&seen, fieldKind, key); err != nil {
				return err
			}
			err = decoder.parseStringField(&expr.Kind)

		case key.equal("name"):
			if err = decoder.rejectDuplicateField(&seen, fieldName, key); err != nil {
				return err
			}
			err = decoder.parseStringField(&expr.Name)

		case key.equal("alias"):
			if err = decoder.rejectDuplicateField(&seen, fieldAlias, key); err != nil {
				return err
			}
			err = decoder.parseStringField(&expr.Alias)

		case key.equal("value"):
			if err = decoder.rejectDuplicateField(&seen, fieldValue, key); err != nil {
				return err
			}
			expr.Value, err = decoder.parseValue()

		case key.equal("args"):
			if err = decoder.rejectDuplicateField(&seen, fieldArgs, key); err != nil {
				return err
			}
			expr.Args, err = decoder.parseExprArray()

		default:
			if err = decoder.rejectDuplicateName(&unknown, key); err != nil {
				return err
			}
			err = decoder.skipValue()
		}
		if err != nil {
			return err
		}

		decoder.skipSpace()
		if decoder.consumeIf('}') {
			*dst = expr
			return nil
		}
		if err = decoder.consume(','); err != nil {
			return err
		}
	}
}

func (decoder *jsonValueDecoder) parseExprArray() ([]Expr, error) {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		return nil, err
	}
	if err := decoder.consume('['); err != nil {
		return nil, err
	}

	decoder.skipSpace()
	if decoder.consumeIf(']') {
		return []Expr{}, nil
	}

	values := make([]Expr, 0, 4)
	for {
		var value Expr
		if err := decoder.parseExpr(&value); err != nil {
			return nil, err
		}
		values = append(values, value)

		decoder.skipSpace()
		if decoder.consumeIf(']') {
			return values, nil
		}
		if err := decoder.consume(','); err != nil {
			return nil, err
		}
	}
}

func (decoder *jsonValueDecoder) parseReduction() (*Reduction, error) {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		return nil, err
	}
	if err := decoder.consume('{'); err != nil {
		return nil, err
	}

	reduction := new(Reduction)
	decoder.skipSpace()
	if decoder.consumeIf('}') {
		return nil, nil
	}

	const (
		fieldGroup uint64 = 1 << iota
		fieldMetrics
		fieldHaving
	)
	var seen uint64
	var unknown jsonNameSet
	for {
		key, err := decoder.parseObjectKey()
		if err != nil {
			return nil, err
		}
		if err = decoder.consume(':'); err != nil {
			return nil, err
		}

		switch {

		case key.equal("group"):
			if err = decoder.rejectDuplicateField(&seen, fieldGroup, key); err != nil {
				return nil, err
			}
			reduction.Group, err = decoder.parseExprArray()

		case key.equal("metrics"):
			if err = decoder.rejectDuplicateField(&seen, fieldMetrics, key); err != nil {
				return nil, err
			}
			reduction.Metrics, err = decoder.parseExprArray()

		case key.equal("having"):
			if err = decoder.rejectDuplicateField(&seen, fieldHaving, key); err != nil {
				return nil, err
			}
			err = decoder.parseExpr(&reduction.Having)

		default:
			if err = decoder.rejectDuplicateName(&unknown, key); err != nil {
				return nil, err
			}
			err = decoder.skipValue()
		}
		if err != nil {
			return nil, err
		}

		decoder.skipSpace()
		if decoder.consumeIf('}') {
			if len(reduction.Group) == 0 && len(reduction.Metrics) == 0 && isZeroExpr(&reduction.Having) {
				return nil, nil
			}
			return reduction, nil
		}
		if err = decoder.consume(','); err != nil {
			return nil, err
		}
	}
}

func (decoder *jsonValueDecoder) parseOrderArray() ([]Order, error) {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		return nil, err
	}
	if err := decoder.consume('['); err != nil {
		return nil, err
	}

	decoder.skipSpace()
	if decoder.consumeIf(']') {
		return []Order{}, nil
	}

	values := make([]Order, 0, 4)
	for {
		var value Order
		if err := decoder.parseOrder(&value); err != nil {
			return nil, err
		}
		values = append(values, value)

		decoder.skipSpace()
		if decoder.consumeIf(']') {
			return values, nil
		}
		if err := decoder.consume(','); err != nil {
			return nil, err
		}
	}
}

func (decoder *jsonValueDecoder) parseOrder(dst *Order) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = Order{}
		}
		return err
	}
	if err := decoder.consume('{'); err != nil {
		return err
	}

	var order Order
	decoder.skipSpace()
	if decoder.consumeIf('}') {
		*dst = order
		return nil
	}

	const (
		fieldBy uint64 = 1 << iota
		fieldDesc
	)
	var seen uint64
	var unknown jsonNameSet
	for {
		key, err := decoder.parseObjectKey()
		if err != nil {
			return err
		}
		if err = decoder.consume(':'); err != nil {
			return err
		}

		switch {

		case key.equal("by"):
			if err = decoder.rejectDuplicateField(&seen, fieldBy, key); err != nil {
				return err
			}
			err = decoder.parseExpr(&order.By)

		case key.equal("desc"):
			if err = decoder.rejectDuplicateField(&seen, fieldDesc, key); err != nil {
				return err
			}
			err = decoder.parseBoolField(&order.Desc)

		default:
			if err = decoder.rejectDuplicateName(&unknown, key); err != nil {
				return err
			}
			err = decoder.skipValue()
		}
		if err != nil {
			return err
		}

		decoder.skipSpace()
		if decoder.consumeIf('}') {
			*dst = order
			return nil
		}
		if err = decoder.consume(','); err != nil {
			return err
		}
	}
}

func (decoder *jsonValueDecoder) parseWindow(dst *Window) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = Window{}
		}
		return err
	}
	if err := decoder.consume('{'); err != nil {
		return err
	}

	var window Window
	decoder.skipSpace()
	if decoder.consumeIf('}') {
		*dst = window
		return nil
	}

	const (
		fieldOffset uint64 = 1 << iota
		fieldLimit
	)
	var seen uint64
	var unknown jsonNameSet
	for {
		key, err := decoder.parseObjectKey()
		if err != nil {
			return err
		}
		if err = decoder.consume(':'); err != nil {
			return err
		}

		switch {

		case key.equal("offset"):
			if err = decoder.rejectDuplicateField(&seen, fieldOffset, key); err != nil {
				return err
			}
			err = decoder.parseUint64Field(&window.Offset)

		case key.equal("limit"):
			if err = decoder.rejectDuplicateField(&seen, fieldLimit, key); err != nil {
				return err
			}
			err = decoder.parseUint64Field(&window.Limit)

		default:
			if err = decoder.rejectDuplicateName(&unknown, key); err != nil {
				return err
			}
			err = decoder.skipValue()
		}
		if err != nil {
			return err
		}

		decoder.skipSpace()
		if decoder.consumeIf('}') {
			*dst = window
			return nil
		}
		if err = decoder.consume(','); err != nil {
			return err
		}
	}
}

func (decoder *jsonValueDecoder) parseMetaArray() ([]MetaEntry, error) {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		return nil, err
	}
	if err := decoder.consume('['); err != nil {
		return nil, err
	}

	decoder.skipSpace()
	if decoder.consumeIf(']') {
		return []MetaEntry{}, nil
	}

	values := make([]MetaEntry, 0, 4)
	for {
		var value MetaEntry
		if err := decoder.parseMetaEntry(&value); err != nil {
			return nil, err
		}
		values = append(values, value)

		decoder.skipSpace()
		if decoder.consumeIf(']') {
			return values, nil
		}
		if err := decoder.consume(','); err != nil {
			return nil, err
		}
	}
}

func (decoder *jsonValueDecoder) parseMetaEntry(dst *MetaEntry) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = MetaEntry{}
		}
		return err
	}
	if err := decoder.consume('{'); err != nil {
		return err
	}

	var entry MetaEntry
	decoder.skipSpace()
	if decoder.consumeIf('}') {
		*dst = entry
		return nil
	}

	const (
		fieldKey uint64 = 1 << iota
		fieldValue
	)
	var seen uint64
	var unknown jsonNameSet
	for {
		key, err := decoder.parseObjectKey()
		if err != nil {
			return err
		}
		if err = decoder.consume(':'); err != nil {
			return err
		}

		switch {

		case key.equal("key"):
			if err = decoder.rejectDuplicateField(&seen, fieldKey, key); err != nil {
				return err
			}
			err = decoder.parseStringField(&entry.Key)

		case key.equal("value"):
			if err = decoder.rejectDuplicateField(&seen, fieldValue, key); err != nil {
				return err
			}
			entry.Value, err = decoder.parseValue()

		default:
			if err = decoder.rejectDuplicateName(&unknown, key); err != nil {
				return err
			}
			err = decoder.skipValue()
		}
		if err != nil {
			return err
		}

		decoder.skipSpace()
		if decoder.consumeIf('}') {
			*dst = entry
			return nil
		}
		if err = decoder.consume(','); err != nil {
			return err
		}
	}
}

func (decoder *jsonValueDecoder) parseValue() (any, error) {
	decoder.skipSpace()
	if decoder.pos >= len(decoder.data) {
		return nil, decoder.errorf("expected value")
	}

	switch decoder.data[decoder.pos] {

	case 'n':
		ok, err := decoder.consumeNull()
		if err != nil {
			return nil, err
		}
		if ok {
			return nil, nil
		}

	case 't':
		if err := decoder.consumeLiteral("true"); err != nil {
			return nil, err
		}
		return true, nil

	case 'f':
		if err := decoder.consumeLiteral("false"); err != nil {
			return nil, err
		}
		return false, nil

	case '"':
		return decoder.parseString()

	case '[':
		return decoder.parseAnyArray()

	case '{':
		return decoder.parseAnyMap()

	default:
		if isJSONNumberLead(decoder.data[decoder.pos]) {
			data, fractional, err := decoder.parseNumberBytes()
			if err != nil {
				return nil, err
			}
			return parseJSONNumber(data, fractional)
		}
	}

	return nil, decoder.errorf("expected value")
}

func (decoder *jsonValueDecoder) parseAnyArray() (any, error) {
	if err := decoder.consume('['); err != nil {
		return nil, err
	}
	decoder.skipSpace()
	if decoder.consumeIf(']') {
		return []any{}, nil
	}

	values := make([]any, 0, 16)
	for {
		value, err := decoder.parseValue()
		if err != nil {
			return nil, err
		}
		values = append(values, value)

		decoder.skipSpace()
		if decoder.consumeIf(']') {
			return values, nil
		}
		if err := decoder.consume(','); err != nil {
			return nil, err
		}
	}
}

func (decoder *jsonValueDecoder) parseAnyMap() (any, error) {
	if err := decoder.consume('{'); err != nil {
		return nil, err
	}
	decoder.skipSpace()
	if decoder.consumeIf('}') {
		return map[string]any{}, nil
	}

	values := make(map[string]any)
	for {
		key, err := decoder.parseObjectKey()
		if err != nil {
			return nil, err
		}
		if err = decoder.consume(':'); err != nil {
			return nil, err
		}
		keyText := key.text()
		if _, exists := values[keyText]; exists {
			return nil, decoder.duplicateNameError(key)
		}
		value, err := decoder.parseValue()
		if err != nil {
			return nil, err
		}
		values[keyText] = value

		decoder.skipSpace()
		if decoder.consumeIf('}') {
			return values, nil
		}
		if err = decoder.consume(','); err != nil {
			return nil, err
		}
	}
}

func (decoder *jsonValueDecoder) skipValue() error {
	decoder.skipSpace()
	if decoder.pos >= len(decoder.data) {
		return decoder.errorf("expected value")
	}

	switch decoder.data[decoder.pos] {

	case 'n':
		return decoder.consumeLiteral("null")

	case 't':
		return decoder.consumeLiteral("true")

	case 'f':
		return decoder.consumeLiteral("false")

	case '"':
		_, _, _, err := decoder.scanString()
		return err

	case '[':
		if err := decoder.consume('['); err != nil {
			return err
		}
		decoder.skipSpace()
		if decoder.consumeIf(']') {
			return nil
		}
		for {
			if err := decoder.skipValue(); err != nil {
				return err
			}
			decoder.skipSpace()
			if decoder.consumeIf(']') {
				return nil
			}
			if err := decoder.consume(','); err != nil {
				return err
			}
		}

	case '{':
		if err := decoder.consume('{'); err != nil {
			return err
		}
		decoder.skipSpace()
		if decoder.consumeIf('}') {
			return nil
		}
		var names jsonNameSet
		for {
			key, err := decoder.parseObjectKey()
			if err != nil {
				return err
			}
			if err = decoder.rejectDuplicateName(&names, key); err != nil {
				return err
			}
			if err = decoder.consume(':'); err != nil {
				return err
			}
			if err = decoder.skipValue(); err != nil {
				return err
			}
			decoder.skipSpace()
			if decoder.consumeIf('}') {
				return nil
			}
			if err = decoder.consume(','); err != nil {
				return err
			}
		}

	default:
		if isJSONNumberLead(decoder.data[decoder.pos]) {
			_, _, err := decoder.parseNumberBytes()
			return err
		}
		return decoder.errorf("expected value")
	}
}

func (decoder *jsonValueDecoder) parseStringField(dst *string) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = ""
		}
		return err
	}
	value, err := decoder.parseString()
	if err != nil {
		return err
	}
	*dst = value
	return nil
}

func (decoder *jsonValueDecoder) parseBoolField(dst *bool) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = false
		}
		return err
	}
	if decoder.pos < len(decoder.data) && decoder.data[decoder.pos] == 't' {
		if err := decoder.consumeLiteral("true"); err != nil {
			return err
		}
		*dst = true
		return nil
	}
	if err := decoder.consumeLiteral("false"); err != nil {
		return err
	}
	*dst = false
	return nil
}

func (decoder *jsonValueDecoder) parseUint64Field(dst *uint64) error {
	decoder.skipSpace()
	if ok, err := decoder.consumeNull(); ok || err != nil {
		if ok {
			*dst = 0
		}
		return err
	}
	data, _, err := decoder.parseNumberBytes()
	if err != nil {
		return err
	}
	value, err := strconv.ParseUint(transientBytesToString(data), 10, 64)
	if err != nil {
		return decoder.errorf("expected unsigned integer")
	}
	*dst = value
	return nil
}

func (decoder *jsonValueDecoder) parseObjectKey() (jsonObjectKey, error) {
	decoder.skipSpace()
	start, end, escaped, err := decoder.scanString()
	if err != nil {
		return jsonObjectKey{}, err
	}
	raw := decoder.data[start:end]
	if !escaped {
		return jsonObjectKey{raw: raw}, nil
	}
	return jsonObjectKey{decoded: unescapeJSONString(raw)}, nil
}

func (decoder *jsonValueDecoder) parseString() (string, error) {
	decoder.skipSpace()
	start, end, escaped, err := decoder.scanString()
	if err != nil {
		return "", err
	}
	raw := decoder.data[start:end]
	if !escaped {
		return string(raw), nil
	}
	return unescapeJSONString(raw), nil
}

func (decoder *jsonValueDecoder) scanString() (start, end int, escaped bool, err error) {
	if err = decoder.consume('"'); err != nil {
		return 0, 0, false, err
	}

	start = decoder.pos

	for decoder.pos < len(decoder.data) {
		ch := decoder.data[decoder.pos]
		switch {

		case ch == '"':
			end = decoder.pos
			decoder.pos++
			return start, end, escaped, nil

		case ch == '\\':
			escaped = true
			escapeStart := decoder.pos
			if decoder.pos+2 > len(decoder.data) {
				return 0, 0, false, decoder.errorf("unterminated escape sequence")
			}
			switch decoder.data[decoder.pos+1] {

			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				decoder.pos += 2

			case 'u':
				if decoder.pos+6 > len(decoder.data) {
					return 0, 0, false, decoder.errorf("invalid Unicode escape")
				}
				first, ok := parseJSONHexUint16(decoder.data[decoder.pos+2 : decoder.pos+6])
				if !ok {
					return 0, 0, false, decoder.errorf("invalid Unicode escape")
				}
				decoder.pos += 6
				r := rune(first)
				switch {

				case 0xd800 <= r && r <= 0xdbff:
					if decoder.pos+6 > len(decoder.data) ||
						decoder.data[decoder.pos] != '\\' || decoder.data[decoder.pos+1] != 'u' {
						decoder.pos = escapeStart
						return 0, 0, false, decoder.errorf("invalid Unicode surrogate pair")
					}
					second, ok := parseJSONHexUint16(decoder.data[decoder.pos+2 : decoder.pos+6])
					if !ok || second < 0xdc00 || second > 0xdfff {
						decoder.pos = escapeStart
						return 0, 0, false, decoder.errorf("invalid Unicode surrogate pair")
					}
					decoder.pos += 6

				case 0xdc00 <= r && r <= 0xdfff:
					decoder.pos = escapeStart
					return 0, 0, false, decoder.errorf("invalid Unicode surrogate pair")
				}

			default:
				return 0, 0, false, decoder.errorf("invalid escape sequence")
			}

		case ch < 0x20:
			return 0, 0, false, decoder.errorf("invalid control character in string")

		case ch < utf8.RuneSelf:
			decoder.pos++

		default:
			_, size := utf8.DecodeRune(decoder.data[decoder.pos:])
			if size == 1 {
				return 0, 0, false, decoder.errorf("invalid UTF-8 in string")
			}
			decoder.pos += size
		}
	}
	return 0, 0, false, decoder.errorf("unterminated string")
}

func unescapeJSONString(raw []byte) string {
	decoded := make([]byte, 0, len(raw))
	start := 0
	for i := 0; i < len(raw); {
		if raw[i] != '\\' {
			i++
			continue
		}

		decoded = append(decoded, raw[start:i]...)
		switch raw[i+1] {

		case '"', '\\', '/':
			decoded = append(decoded, raw[i+1])
			i += 2

		case 'b':
			decoded = append(decoded, '\b')
			i += 2

		case 'f':
			decoded = append(decoded, '\f')
			i += 2

		case 'n':
			decoded = append(decoded, '\n')
			i += 2

		case 'r':
			decoded = append(decoded, '\r')
			i += 2

		case 't':
			decoded = append(decoded, '\t')
			i += 2

		case 'u':
			first, _ := parseJSONHexUint16(raw[i+2 : i+6])
			i += 6
			r := rune(first)
			if 0xd800 <= r && r <= 0xdbff {
				second, _ := parseJSONHexUint16(raw[i+2 : i+6])
				r = utf16.DecodeRune(r, rune(second))
				i += 6
			}
			decoded = utf8.AppendRune(decoded, r)
		}
		start = i
	}
	decoded = append(decoded, raw[start:]...)
	return string(decoded)
}

func parseJSONHexUint16(data []byte) (uint16, bool) {
	var value uint16
	for _, ch := range data {
		value <<= 4
		switch {
		case '0' <= ch && ch <= '9':
			value |= uint16(ch - '0')
		case 'a' <= ch && ch <= 'f':
			value |= uint16(ch-'a') + 10
		case 'A' <= ch && ch <= 'F':
			value |= uint16(ch-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func (decoder *jsonValueDecoder) parseNumberBytes() ([]byte, bool, error) {
	decoder.skipSpace()
	start := decoder.pos
	if decoder.consumeIf('-') && decoder.pos >= len(decoder.data) {
		return nil, false, decoder.errorf("invalid number")
	}
	if decoder.pos >= len(decoder.data) {
		return nil, false, decoder.errorf("invalid number")
	}

	if decoder.data[decoder.pos] == '0' {
		decoder.pos++
		if decoder.pos < len(decoder.data) && decoder.data[decoder.pos] >= '0' && decoder.data[decoder.pos] <= '9' {
			return nil, false, decoder.errorf("invalid leading zero")
		}
	} else {
		if decoder.data[decoder.pos] < '1' || decoder.data[decoder.pos] > '9' {
			return nil, false, decoder.errorf("invalid number")
		}
		for decoder.pos < len(decoder.data) && decoder.data[decoder.pos] >= '0' && decoder.data[decoder.pos] <= '9' {
			decoder.pos++
		}
	}

	fractional := false
	if decoder.consumeIf('.') {
		fractional = true
		fractionStart := decoder.pos
		for decoder.pos < len(decoder.data) && decoder.data[decoder.pos] >= '0' && decoder.data[decoder.pos] <= '9' {
			decoder.pos++
		}
		if decoder.pos == fractionStart {
			return nil, false, decoder.errorf("invalid number fraction")
		}
	}

	if decoder.pos < len(decoder.data) && (decoder.data[decoder.pos] == 'e' || decoder.data[decoder.pos] == 'E') {
		fractional = true
		decoder.pos++
		if decoder.pos < len(decoder.data) && (decoder.data[decoder.pos] == '+' || decoder.data[decoder.pos] == '-') {
			decoder.pos++
		}
		exponentStart := decoder.pos
		for decoder.pos < len(decoder.data) && decoder.data[decoder.pos] >= '0' && decoder.data[decoder.pos] <= '9' {
			decoder.pos++
		}
		if decoder.pos == exponentStart {
			return nil, false, decoder.errorf("invalid number exponent")
		}
	}
	return decoder.data[start:decoder.pos], fractional, nil
}

func (decoder *jsonValueDecoder) consume(expected byte) error {
	decoder.skipSpace()
	if decoder.pos >= len(decoder.data) || decoder.data[decoder.pos] != expected {
		return decoder.errorf("expected %q", expected)
	}
	if (expected == '{' || expected == '[') && decoder.depth >= maxJSONNestingDepth {
		return decoder.errorf("exceeded maximum nesting depth")
	}
	decoder.pos++
	if expected == '{' || expected == '[' {
		decoder.depth++
	}
	return nil
}

func (decoder *jsonValueDecoder) consumeIf(expected byte) bool {
	if decoder.pos < len(decoder.data) && decoder.data[decoder.pos] == expected {
		decoder.pos++
		if (expected == '}' || expected == ']') && decoder.depth > 0 {
			decoder.depth--
		}
		return true
	}
	return false
}

func (decoder *jsonValueDecoder) consumeNull() (bool, error) {
	decoder.skipSpace()
	if decoder.pos >= len(decoder.data) || decoder.data[decoder.pos] != 'n' {
		return false, nil
	}
	if err := decoder.consumeLiteral("null"); err != nil {
		return false, err
	}
	return true, nil
}

func (decoder *jsonValueDecoder) consumeLiteral(literal string) error {
	decoder.skipSpace()
	if len(decoder.data)-decoder.pos < len(literal) || string(decoder.data[decoder.pos:decoder.pos+len(literal)]) != literal {
		return decoder.errorf("expected %s", literal)
	}
	decoder.pos += len(literal)
	return nil
}

func (decoder *jsonValueDecoder) skipSpace() {
	for decoder.pos < len(decoder.data) {
		switch decoder.data[decoder.pos] {
		case ' ', '\t', '\r', '\n':
			decoder.pos++
		default:
			return
		}
	}
}

func (decoder *jsonValueDecoder) errorf(format string, args ...any) error {
	return fmt.Errorf("qx: invalid JSON at byte %d: %s", decoder.pos+1, fmt.Sprintf(format, args...))
}

// Duplicate names are rejected at the object boundary instead of being merged
// or resolved last-wins. Both alternatives make the result depend on decoder
// state and can silently discard data; rejection is unambiguous, cheaper for
// the fixed QX shape, and matches encoding/json/v2's strict default.
func (decoder *jsonValueDecoder) rejectDuplicateField(seen *uint64, field uint64, key jsonObjectKey) error {
	if *seen&field != 0 {
		return decoder.duplicateNameError(key)
	}
	*seen |= field
	return nil
}

func (decoder *jsonValueDecoder) rejectDuplicateName(seen *jsonNameSet, key jsonObjectKey) error {
	if !seen.add(key.transientText()) {
		return decoder.duplicateNameError(key)
	}
	return nil
}

func (decoder *jsonValueDecoder) duplicateNameError(key jsonObjectKey) error {
	return decoder.errorf("duplicate object member name %q", key.transientText())
}

func (key jsonObjectKey) equal(want string) bool {
	// QX wire names are case-sensitive. Besides avoiding ambiguous aliases such
	// as "order" and "ORDER", this matches encoding/json/v2's default lookup.
	if key.raw != nil {
		return bytes.Equal(key.raw, []byte(want))
	}
	return key.decoded == want
}

func (key jsonObjectKey) text() string {
	if key.raw != nil {
		return string(key.raw)
	}
	return key.decoded
}

func (key jsonObjectKey) transientText() string {
	if key.raw != nil {
		return transientBytesToString(key.raw)
	}
	return key.decoded
}
