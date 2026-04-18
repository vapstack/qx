package qx

import (
	"bytes"
	"math"
	"strconv"

	"encoding/json"
)

type jsonMetaEntry struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// MarshalJSON implements json.Marshaler.
func (expr Expr) MarshalJSON() ([]byte, error) {
	dst := make([]byte, 0, estimateExprJSONSize(expr))
	return appendJSONExpr(dst, expr)
}

// UnmarshalJSON implements json.Unmarshaler.
func (expr *Expr) UnmarshalJSON(data []byte) error {
	if expr == nil {
		return nil
	}

	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		*expr = Expr{}
		return nil
	}

	var payload struct {
		Kind  string          `json:"kind"`
		Name  string          `json:"name"`
		Alias string          `json:"alias"`
		Value json.RawMessage `json:"value"`
		Args  []Expr          `json:"args"`
	}

	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	expr.Kind = payload.Kind
	expr.Name = payload.Name
	expr.Alias = payload.Alias
	expr.Args = payload.Args
	expr.Value = nil

	if len(payload.Value) != 0 {
		value, err := decodeJSONValue(payload.Value)
		if err != nil {
			return err
		}
		expr.Value = value
	}

	return nil
}

// MarshalJSON implements json.Marshaler.
func (qx *QX) MarshalJSON() ([]byte, error) {
	if qx == nil {
		return []byte("null"), nil
	}

	dst := make([]byte, 0, estimateJSONSize(qx))
	dst = append(dst, '{')
	first := true

	if !qx.Filter.IsZero() {
		dst = appendJSONFieldName(dst, &first, "filter")
		var err error
		dst, err = appendJSONExpr(dst, qx.Filter)
		if err != nil {
			return nil, err
		}
	}

	if qx.HasReduction() {
		dst = appendJSONFieldName(dst, &first, "reduction")
		var err error
		dst, err = appendJSONReduction(dst, qx.Reduction)
		if err != nil {
			return nil, err
		}
	}

	if len(qx.Order) != 0 {
		dst = appendJSONFieldName(dst, &first, "order")
		var err error
		dst, err = appendJSONOrderSlice(dst, qx.Order)
		if err != nil {
			return nil, err
		}
	}

	if !isZeroWindow(qx.Window) {
		dst = appendJSONFieldName(dst, &first, "window")
		dst = appendJSONWindow(dst, qx.Window)
	}

	if hasNonZeroExpr(qx.Projection) {
		dst = appendJSONFieldName(dst, &first, "projection")
		var err error
		dst, err = appendJSONExprSliceOmitZero(dst, qx.Projection)
		if err != nil {
			return nil, err
		}
	}

	if len(qx.Metadata) != 0 {
		dst = appendJSONFieldName(dst, &first, "meta")
		var err error
		dst, err = appendJSONMetaSlice(dst, qx.Metadata)
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, '}')
	return dst, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (qx *QX) UnmarshalJSON(data []byte) error {
	if qx == nil {
		return nil
	}

	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		*qx = QX{}
		return nil
	}

	var payload struct {
		Filter     Expr            `json:"filter"`
		Reduction  json.RawMessage `json:"reduction"`
		Order      []Order         `json:"order"`
		Window     Window          `json:"window"`
		Projection []Expr          `json:"projection"`
		Meta       []jsonMetaEntry `json:"meta"`
	}

	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	qx.Filter = payload.Filter
	qx.Reduction = nil
	if len(payload.Reduction) != 0 && !bytes.Equal(bytes.TrimSpace(payload.Reduction), []byte("null")) {
		var reduction Reduction
		if err := json.Unmarshal(payload.Reduction, &reduction); err != nil {
			return err
		}
		if !reduction.IsEmpty() {
			qx.Reduction = &reduction
		}
	}
	qx.Order = payload.Order
	qx.Window = payload.Window
	qx.Projection = payload.Projection
	qx.Metadata = nil
	if payload.Meta != nil {
		qx.Metadata = make([]MetaEntry, len(payload.Meta))
		for i := range payload.Meta {
			qx.Metadata[i].Key = payload.Meta[i].Key
			if len(payload.Meta[i].Value) == 0 {
				continue
			}

			value, err := decodeJSONValue(payload.Meta[i].Value)
			if err != nil {
				return err
			}
			qx.Metadata[i].Value = value
		}
	}
	return nil
}

func appendJSONExpr(dst []byte, expr Expr) ([]byte, error) {
	dst = append(dst, '{')
	first := true

	if expr.Kind != "" {
		dst = appendJSONFieldName(dst, &first, "kind")
		dst = strconv.AppendQuote(dst, expr.Kind)
	}
	if expr.Name != "" {
		dst = appendJSONFieldName(dst, &first, "name")
		dst = strconv.AppendQuote(dst, expr.Name)
	}
	if expr.Alias != "" {
		dst = appendJSONFieldName(dst, &first, "alias")
		dst = strconv.AppendQuote(dst, expr.Alias)
	}
	if expr.Value != nil {
		dst = appendJSONFieldName(dst, &first, "value")
		var err error
		dst, err = appendJSONValue(dst, expr.Value)
		if err != nil {
			return nil, err
		}
	}
	if len(expr.Args) != 0 {
		dst = appendJSONFieldName(dst, &first, "args")
		var err error
		dst, err = appendJSONExprSlice(dst, expr.Args)
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, '}')
	return dst, nil
}

func appendJSONReduction(dst []byte, reduction *Reduction) ([]byte, error) {
	dst = append(dst, '{')
	first := true

	if hasNonZeroExpr(reduction.Group) {
		dst = appendJSONFieldName(dst, &first, "group")
		var err error
		dst, err = appendJSONExprSliceOmitZero(dst, reduction.Group)
		if err != nil {
			return nil, err
		}
	}

	if hasNonZeroExpr(reduction.Metrics) {
		dst = appendJSONFieldName(dst, &first, "metrics")
		var err error
		dst, err = appendJSONExprSliceOmitZero(dst, reduction.Metrics)
		if err != nil {
			return nil, err
		}
	}

	if !reduction.Having.IsZero() {
		dst = appendJSONFieldName(dst, &first, "having")
		var err error
		dst, err = appendJSONExpr(dst, reduction.Having)
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, '}')
	return dst, nil
}

func appendJSONOrderSlice(dst []byte, orders []Order) ([]byte, error) {
	dst = append(dst, '[')

	for i := range orders {
		if i != 0 {
			dst = append(dst, ',')
		}

		var err error
		dst, err = appendJSONOrder(dst, orders[i])
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, ']')
	return dst, nil
}

func appendJSONOrder(dst []byte, order Order) ([]byte, error) {
	dst = append(dst, '{')
	first := true

	if !order.By.IsZero() {
		dst = appendJSONFieldName(dst, &first, "by")
		var err error
		dst, err = appendJSONExpr(dst, order.By)
		if err != nil {
			return nil, err
		}
	}

	if order.Desc {
		dst = appendJSONFieldName(dst, &first, "desc")
		dst = strconv.AppendBool(dst, order.Desc)
	}

	dst = append(dst, '}')
	return dst, nil
}

func appendJSONWindow(dst []byte, window Window) []byte {
	dst = append(dst, '{')
	first := true

	if window.Offset != 0 {
		dst = appendJSONFieldName(dst, &first, "offset")
		dst = strconv.AppendUint(dst, window.Offset, 10)
	}
	if window.Limit != 0 {
		dst = appendJSONFieldName(dst, &first, "limit")
		dst = strconv.AppendUint(dst, window.Limit, 10)
	}

	dst = append(dst, '}')
	return dst
}

func appendJSONExprSlice(dst []byte, exprs []Expr) ([]byte, error) {
	dst = append(dst, '[')

	for i := range exprs {
		if i != 0 {
			dst = append(dst, ',')
		}

		var err error
		dst, err = appendJSONExpr(dst, exprs[i])
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, ']')
	return dst, nil
}

func appendJSONExprSliceOmitZero(dst []byte, exprs []Expr) ([]byte, error) {
	dst = append(dst, '[')
	written := 0

	for i := range exprs {
		if exprs[i].IsZero() {
			continue
		}
		if written != 0 {
			dst = append(dst, ',')
		}

		var err error
		dst, err = appendJSONExpr(dst, exprs[i])
		if err != nil {
			return nil, err
		}
		written++
	}

	dst = append(dst, ']')
	return dst, nil
}

func appendJSONMetaSlice(dst []byte, meta []MetaEntry) ([]byte, error) {
	dst = append(dst, '[')

	for i := range meta {
		if i != 0 {
			dst = append(dst, ',')
		}

		var err error
		dst, err = appendJSONMetaEntry(dst, meta[i])
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, ']')
	return dst, nil
}

func appendJSONMetaEntry(dst []byte, entry MetaEntry) ([]byte, error) {
	dst = append(dst, '{')
	first := true

	dst = appendJSONFieldName(dst, &first, "key")
	dst = strconv.AppendQuote(dst, entry.Key)

	if entry.Value != nil {
		dst = appendJSONFieldName(dst, &first, "value")
		var err error
		dst, err = appendJSONValue(dst, entry.Value)
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, '}')
	return dst, nil
}

func appendJSONValue(dst []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, v), nil
	case string:
		return strconv.AppendQuote(dst, v), nil
	case int:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(dst, v, 10), nil
	case uint:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(dst, v, 10), nil
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			_, err := json.Marshal(v)
			return nil, err
		}
		return strconv.AppendFloat(dst, float64(v), 'g', -1, 32), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			_, err := json.Marshal(v)
			return nil, err
		}
		return strconv.AppendFloat(dst, v, 'g', -1, 64), nil
	case Expr:
		return appendJSONExpr(dst, v)
	case *Expr:
		if v == nil {
			return append(dst, "null"...), nil
		}
		return appendJSONExpr(dst, *v)
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	dst = append(dst, raw...)
	return dst, nil
}

func appendJSONFieldName(dst []byte, first *bool, name string) []byte {
	if !*first {
		dst = append(dst, ',')
	} else {
		*first = false
	}
	dst = append(dst, '"')
	dst = append(dst, name...)
	dst = append(dst, '"', ':')
	return dst
}

func decodeJSONValue(data []byte) (any, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}

	switch data[0] {
	case 'n':
		if bytes.Equal(data, []byte("null")) {
			return nil, nil
		}
	case 't':
		if bytes.Equal(data, []byte("true")) {
			return true, nil
		}
	case 'f':
		if bytes.Equal(data, []byte("false")) {
			return false, nil
		}
	case '"':
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case '[', '{':
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()

		var value any
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		return normalizeDecodedJSONValue(value)
	default:
		if isJSONNumberLead(data[0]) {
			return parseJSONNumber(data)
		}
	}

	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func normalizeDecodedJSONValue(value any) (any, error) {
	switch v := value.(type) {
	case json.Number:
		return normalizeJSONNumber(v)
	case []any:
		for i := range v {
			next, err := normalizeDecodedJSONValue(v[i])
			if err != nil {
				return nil, err
			}
			v[i] = next
		}
		return v, nil
	case map[string]any:
		for key, item := range v {
			next, err := normalizeDecodedJSONValue(item)
			if err != nil {
				return nil, err
			}
			v[key] = next
		}
		return v, nil
	default:
		return value, nil
	}
}

func normalizeJSONNumber(value json.Number) (any, error) {
	return parseJSONNumber([]byte(value.String()))
}

func parseJSONNumber(data []byte) (any, error) {
	text := string(data)

	if !bytes.ContainsAny(data, ".eE") {
		if v, err := strconv.ParseInt(text, 10, strconv.IntSize); err == nil {
			return int(v), nil
		}
		if v, err := strconv.ParseInt(text, 10, 64); err == nil {
			return v, nil
		}
		if v, err := strconv.ParseUint(text, 10, strconv.IntSize); err == nil {
			return uint(v), nil
		}
		if v, err := strconv.ParseUint(text, 10, 64); err == nil {
			return v, nil
		}
	}

	return strconv.ParseFloat(text, 64)
}

func hasNonZeroExpr(exprs []Expr) bool {
	for i := range exprs {
		if !exprs[i].IsZero() {
			return true
		}
	}
	return false
}

func isZeroWindow(window Window) bool {
	return window.Offset == 0 && window.Limit == 0
}

func isJSONNumberLead(ch byte) bool {
	return ch == '-' || (ch >= '0' && ch <= '9')
}

func estimateExprJSONSize(expr Expr) int {
	size := 2
	size += len(expr.Kind) + len(expr.Name) + len(expr.Alias)
	size += len(expr.Args) * 2
	if expr.Value != nil {
		size += 16
	}
	if size < 32 {
		return 32
	}
	return size
}

func estimateJSONSize(qx *QX) int {
	size := 2
	size += len(qx.Order) * 24
	size += len(qx.Projection) * 24
	size += len(qx.Metadata) * 32
	if qx.HasReduction() {
		size += len(qx.Reduction.Group)*24 + len(qx.Reduction.Metrics)*24 + 32
	}
	if size < 64 {
		return 64
	}
	return size
}
