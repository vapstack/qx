package qx

import (
	"encoding/base64"
	"errors"
	"math"
	"reflect"
	"sort"
	"strconv"
	"unicode/utf8"
	"unsafe"

	"encoding/json"
)

// MarshalJSON implements json.Marshaler.
func (expr Expr) MarshalJSON() ([]byte, error) {
	dst := make([]byte, 0, estimateExprJSONSize(&expr))
	return appendJSONExpr(dst, &expr)
}

// UnmarshalJSON implements json.Unmarshaler.
func (expr *Expr) UnmarshalJSON(data []byte) error {
	if expr == nil {
		return nil
	}
	return decodeExprJSON(data, expr)
}

// MarshalJSON implements json.Marshaler.
func (qx *QX) MarshalJSON() ([]byte, error) {
	if qx == nil {
		return []byte("null"), nil
	}

	dst := make([]byte, 0, estimateQXJSONSize(qx))
	dst = append(dst, '{')
	first := true

	if !isZeroExpr(&qx.Filter) {
		dst = appendJSONFieldName(dst, &first, "filter")
		var err error
		dst, err = appendJSONExpr(dst, &qx.Filter)
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
	return decodeQXJSON(data, qx)
}

func appendJSONExpr(dst []byte, expr *Expr) ([]byte, error) {
	dst = append(dst, '{')
	first := true

	if expr.Kind != "" {
		dst = appendJSONFieldName(dst, &first, "kind")
		var err error
		dst, err = appendJSONStringBytes(dst, expr.Kind)
		if err != nil {
			return nil, err
		}
	}
	if expr.Name != "" {
		dst = appendJSONFieldName(dst, &first, "name")
		var err error
		dst, err = appendJSONStringBytes(dst, expr.Name)
		if err != nil {
			return nil, err
		}
	}
	if expr.Alias != "" {
		dst = appendJSONFieldName(dst, &first, "alias")
		var err error
		dst, err = appendJSONStringBytes(dst, expr.Alias)
		if err != nil {
			return nil, err
		}
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

	if !isZeroExpr(&reduction.Having) {
		dst = appendJSONFieldName(dst, &first, "having")
		var err error
		dst, err = appendJSONExpr(dst, &reduction.Having)
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
		dst, err = appendJSONOrder(dst, &orders[i])
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, ']')
	return dst, nil
}

func appendJSONOrder(dst []byte, order *Order) ([]byte, error) {
	dst = append(dst, '{')
	first := true

	if !isZeroExpr(&order.By) {
		dst = appendJSONFieldName(dst, &first, "by")
		var err error
		dst, err = appendJSONExpr(dst, &order.By)
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
		dst, err = appendJSONExpr(dst, &exprs[i])
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
		if isZeroExpr(&exprs[i]) {
			continue
		}
		if written != 0 {
			dst = append(dst, ',')
		}

		var err error
		dst, err = appendJSONExpr(dst, &exprs[i])
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
		dst, err = appendJSONMetaEntry(dst, &meta[i])
		if err != nil {
			return nil, err
		}
	}

	dst = append(dst, ']')
	return dst, nil
}

func appendJSONMetaEntry(dst []byte, entry *MetaEntry) ([]byte, error) {
	dst = append(dst, '{')
	first := true

	dst = appendJSONFieldName(dst, &first, "key")
	var err error
	dst, err = appendJSONStringBytes(dst, entry.Key)
	if err != nil {
		return nil, err
	}

	if entry.Value != nil {
		dst = appendJSONFieldName(dst, &first, "value")
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
		return appendJSONStringBytes(dst, v)

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
		return appendJSONFloat(dst, float64(v), 32)

	case float64:
		return appendJSONFloat(dst, v, 64)

	case Expr:
		return appendJSONExpr(dst, &v)

	case *Expr:
		if v == nil {
			return append(dst, "null"...), nil
		}
		return appendJSONExpr(dst, v)

	case []byte:
		return appendJSONBytes(dst, v), nil

	case []string:
		return appendJSONSlice(dst, v, appendJSONString)

	case []int:
		return appendJSONSlice(dst, v, appendJSONInt)

	case []float64:
		return appendJSONSlice(dst, v, appendJSONFloat64)

	case []bool:
		return appendJSONSlice(dst, v, appendJSONBool)

	case []any:
		return appendJSONRecursiveValue(dst, v)

	case map[string]any:
		return appendJSONRecursiveValue(dst, v)

	case map[string]string:
		return appendJSONStringMap(dst, v, appendJSONString)

	case map[string][]byte:
		return appendJSONStringMap(dst, v, func(dst []byte, value []byte) ([]byte, error) {
			return appendJSONBytes(dst, value), nil
		})

	case map[string][]string:
		return appendJSONStringMap(dst, v, func(dst []byte, value []string) ([]byte, error) {
			return appendJSONSlice(dst, value, appendJSONString)
		})

	case map[string][]int:
		return appendJSONStringMap(dst, v, func(dst []byte, value []int) ([]byte, error) {
			return appendJSONSlice(dst, value, appendJSONInt)
		})
	}

	// Values outside QX's fast paths deliberately inherit encoding/json's
	// semantics. In particular, custom marshalers own their output; inspecting
	// it again would defeat their ability to provide an optimized encoding.
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(dst, raw...), nil
}

const appendJSONInlineVisitCount = 16

type appendJSONVisit struct {
	kind byte
	ptr  uintptr
	len  int
}

type appendJSONState struct {
	inline [appendJSONInlineVisitCount]appendJSONVisit
	depth  int
	active map[appendJSONVisit]struct{}
}

func appendJSONRecursiveValue(dst []byte, value any) ([]byte, error) {
	var state appendJSONState
	return state.appendValue(dst, value)
}

func (state *appendJSONState) appendValue(dst []byte, value any) ([]byte, error) {
	switch value := value.(type) {
	case []any:
		if value == nil {
			return append(dst, "null"...), nil
		}
		visit := appendJSONVisit{kind: '[', ptr: uintptr(unsafe.Pointer(unsafe.SliceData(value))), len: len(value)}
		if !state.push(visit) {
			return nil, &json.UnsupportedValueError{Value: reflect.ValueOf(value), Str: "encountered a cycle via []any"}
		}
		dst, err := appendJSONSlice(dst, value, state.appendValue)
		state.pop(visit)
		return dst, err

	case map[string]any:
		if value == nil {
			return append(dst, "null"...), nil
		}
		visit := appendJSONVisit{kind: '{', ptr: reflect.ValueOf(value).Pointer()}
		if !state.push(visit) {
			return nil, &json.UnsupportedValueError{Value: reflect.ValueOf(value), Str: "encountered a cycle via map[string]any"}
		}
		dst, err := appendJSONStringMap(dst, value, state.appendValue)
		state.pop(visit)
		return dst, err

	default:
		return appendJSONValue(dst, value)
	}
}

func (state *appendJSONState) push(visit appendJSONVisit) bool {
	if state.active != nil {
		if _, exists := state.active[visit]; exists {
			return false
		}
	} else {
		for i := 0; i < state.depth; i++ {
			if state.inline[i] == visit {
				return false
			}
		}
		if state.depth == len(state.inline) {
			state.active = make(map[appendJSONVisit]struct{}, len(state.inline)+1)
			for i := range state.inline {
				state.active[state.inline[i]] = struct{}{}
			}
		}
	}

	if state.active != nil {
		state.active[visit] = struct{}{}
	} else {
		state.inline[state.depth] = visit
	}
	state.depth++
	return true
}

func (state *appendJSONState) pop(visit appendJSONVisit) {
	state.depth--
	if state.active != nil {
		delete(state.active, visit)
	}
}

func appendJSONBytes(dst []byte, value []byte) []byte {
	if value == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, '"')
	dst = base64.StdEncoding.AppendEncode(dst, value)
	dst = append(dst, '"')
	return dst
}

func appendJSONString(dst []byte, value string) ([]byte, error) {
	return appendJSONStringBytes(dst, value)
}

func appendJSONInt(dst []byte, value int) ([]byte, error) {
	return strconv.AppendInt(dst, int64(value), 10), nil
}

func appendJSONFloat64(dst []byte, value float64) ([]byte, error) {
	return appendJSONFloat(dst, value, 64)
}

func appendJSONFloat(dst []byte, value float64, bits int) ([]byte, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		_, err := json.Marshal(value)
		return nil, err
	}

	format := byte('f')
	abs := math.Abs(value)
	if abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) ||
			bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	dst = strconv.AppendFloat(dst, value, format, -1, bits)
	if format == 'e' {
		// Match encoding/json's exponent spelling: e-9 instead of e-09.
		if n := len(dst); n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst, nil
}

func appendJSONBool(dst []byte, value bool) ([]byte, error) {
	return strconv.AppendBool(dst, value), nil
}

func appendJSONSlice[T any](dst []byte, values []T, appendValue func([]byte, T) ([]byte, error)) ([]byte, error) {
	if values == nil {
		return append(dst, "null"...), nil
	}
	dst = append(dst, '[')
	for i := range values {
		if i != 0 {
			dst = append(dst, ',')
		}
		var err error
		dst, err = appendValue(dst, values[i])
		if err != nil {
			return nil, err
		}
	}
	dst = append(dst, ']')
	return dst, nil
}

func appendJSONStringMap[V any](dst []byte, values map[string]V, appendValue func([]byte, V) ([]byte, error)) ([]byte, error) {
	if values == nil {
		return append(dst, "null"...), nil
	}

	var inline [16]string
	keys := inline[:0]
	if len(values) > len(inline) {
		keys = make([]string, 0, len(values))
	}
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	dst = append(dst, '{')
	for i, key := range keys {
		if i != 0 {
			dst = append(dst, ',')
		}
		var err error
		dst, err = appendJSONStringBytes(dst, key)
		if err != nil {
			return nil, err
		}
		dst = append(dst, ':')
		dst, err = appendValue(dst, values[key])
		if err != nil {
			return nil, err
		}
	}
	dst = append(dst, '}')
	return dst, nil
}

var errInvalidJSONStringUTF8 = errors.New("qx: cannot marshal string: invalid UTF-8")

// appendJSONStringBytes validates and quotes a Go string in one pass. Invalid
// UTF-8 is rejected instead of being silently replaced, since replacement
// corrupts data and can make different inputs indistinguishable. This follows
// the stricter encoding/json/v2 default and keeps validation allocation-free on
// the normal path. Only characters required by the JSON grammar are escaped;
// U+2028 and U+2029 are valid JSON and therefore remain unescaped.
func appendJSONStringBytes(dst []byte, value string) ([]byte, error) {
	const hex = "0123456789abcdef"

	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(value); {
		ch := value[i]
		if ch >= 0x20 && ch != '\\' && ch != '"' && ch < utf8.RuneSelf {
			i++
			continue
		}
		if ch >= utf8.RuneSelf {
			_, size := utf8.DecodeRuneInString(value[i:])
			if size == 1 {
				return nil, errInvalidJSONStringUTF8
			}
			i += size
			continue
		}

		dst = append(dst, value[start:i]...)
		switch ch {
		case '\\', '"':
			dst = append(dst, '\\', ch)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hex[ch>>4], hex[ch&0xf])
		}
		i++
		start = i
	}
	dst = append(dst, value[start:]...)
	dst = append(dst, '"')
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

func parseJSONNumber(data []byte, fractional bool) (any, error) {
	text := transientBytesToString(data)

	if !fractional {
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

// transientBytesToString avoids allocating while strconv parses data. Callers
// must not retain the returned string after the source byte slice may change.
func transientBytesToString(data []byte) string {
	return unsafe.String(unsafe.SliceData(data), len(data))
}

func hasNonZeroExpr(exprs []Expr) bool {
	for i := range exprs {
		if !isZeroExpr(&exprs[i]) {
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

func estimateExprJSONSize(expr *Expr) int {
	size := 2
	fields := 0
	addField := func(valueSize int) {
		if fields != 0 {
			size++
		}
		size += valueSize
		fields++
	}

	if expr.Kind != "" {
		addField(len(`"kind":`) + estimatedJSONStringSize(expr.Kind))
	}
	if expr.Name != "" {
		addField(len(`"name":`) + estimatedJSONStringSize(expr.Name))
	}
	if expr.Alias != "" {
		addField(len(`"alias":`) + estimatedJSONStringSize(expr.Alias))
	}
	if expr.Value != nil {
		addField(len(`"value":`) + estimateJSONValueSize(expr.Value))
	}
	if len(expr.Args) != 0 {
		argsSize := 2
		for i := range expr.Args {
			if i != 0 {
				argsSize++
			}
			argsSize += estimateExprJSONSize(&expr.Args[i])
		}
		addField(len(`"args":`) + argsSize)
	}
	return size
}

func estimateQXJSONSize(qx *QX) int {
	size := 2
	fields := 0
	addField := func(valueSize int) {
		if fields != 0 {
			size++
		}
		size += valueSize
		fields++
	}

	if !isZeroExpr(&qx.Filter) {
		addField(len(`"filter":`) + estimateExprJSONSize(&qx.Filter))
	}
	if qx.HasReduction() {
		reductionSize := 2
		for i := range qx.Reduction.Group {
			reductionSize += estimateExprJSONSize(&qx.Reduction.Group[i]) + 1
		}
		for i := range qx.Reduction.Metrics {
			reductionSize += estimateExprJSONSize(&qx.Reduction.Metrics[i]) + 1
		}
		if !isZeroExpr(&qx.Reduction.Having) {
			reductionSize += estimateExprJSONSize(&qx.Reduction.Having)
		}
		addField(len(`"reduction":`) + reductionSize + 32)
	}
	if len(qx.Order) != 0 {
		orderSize := 2
		for i := range qx.Order {
			orderSize += estimateExprJSONSize(&qx.Order[i].By) + 16
		}
		addField(len(`"order":`) + orderSize)
	}
	if !isZeroWindow(qx.Window) {
		addField(len(`"window":`) + 48)
	}
	if hasNonZeroExpr(qx.Projection) {
		projectionSize := 2
		for i := range qx.Projection {
			projectionSize += estimateExprJSONSize(&qx.Projection[i]) + 1
		}
		addField(len(`"projection":`) + projectionSize)
	}
	if len(qx.Metadata) != 0 {
		metaSize := 2
		for i := range qx.Metadata {
			metaSize += len(qx.Metadata[i].Key) + estimateJSONValueSize(qx.Metadata[i].Value) + 24
		}
		addField(len(`"meta":`) + metaSize)
	}
	if size < 64 {
		return 64
	}
	return size
}

func estimatedJSONStringSize(value string) int {
	return len(value) + 2
}

func estimateJSONValueSize(value any) int {
	switch v := value.(type) {

	case nil:
		return 4

	case bool:
		return 5

	case string:
		return estimatedJSONStringSize(v)

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 20

	case float32, float64:
		return 24

	case []byte:
		return base64.StdEncoding.EncodedLen(len(v)) + 2

	case []string:
		return 2 + len(v)*12

	case []int:
		return 2 + len(v)*8

	case []float64:
		return 2 + len(v)*16

	case []bool:
		return 2 + len(v)*6

	case []any:
		return 2 + len(v)*16

	case map[string]any:
		return 2 + len(v)*64

	case map[string]string:
		size := 2
		for key, item := range v {
			size += estimatedJSONStringSize(key) + estimatedJSONStringSize(item) + 2
		}
		return size

	case map[string][]byte:
		size := 2
		for key, item := range v {
			size += estimatedJSONStringSize(key) + base64.StdEncoding.EncodedLen(len(item)) + 4
		}
		return size

	case map[string][]string:
		size := 2
		for key, items := range v {
			size += estimatedJSONStringSize(key) + 4 + len(items)*12
		}
		return size

	case map[string][]int:
		size := 2
		for key, items := range v {
			size += estimatedJSONStringSize(key) + 4 + len(items)*8
		}
		return size

	default:
		// The size of a fallback value is controlled by encoding/json and may
		// bear no relation to its Go representation, especially when a custom
		// marshaler is involved. Keep the estimate bounded; once json.Marshal
		// returns, append grows from the exact encoded length if necessary.
		return 64
	}
}
