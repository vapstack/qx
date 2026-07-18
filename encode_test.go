package qx

import (
	"bytes"
	"errors"
	"testing"

	"encoding/json"
)

type compactJSONMarshaler struct {
	Retained []byte
}

func (compactJSONMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`{"compact":true}`), nil
}

func TestExprJSONMarshalStandalone(t *testing.T) {
	expr := EQ("status", "active").AS("is_active")

	data, err := json.Marshal(expr)
	if err != nil {
		t.Fatalf("json.Marshal(expr) error = %v", err)
	}

	wantJSON := `{"kind":"op","name":"eq","alias":"is_active","args":[{"kind":"ref","name":"status"},{"kind":"lit","value":"active"}]}`
	if got := string(data); got != wantJSON {
		t.Fatalf("json.Marshal(expr) = %s, want %s", got, wantJSON)
	}
}

func TestQXJSONMarshalOmitsEmptyExprAndWindow(t *testing.T) {
	q := &QX{
		Reduction: &Reduction{
			Group:   []Expr{{}, REF("country")},
			Metrics: []Expr{{}, ROWCOUNT().AS("rows")},
		},
		Order: []Order{
			{By: Expr{}, Desc: true},
		},
		Projection: []Expr{{}, OUT("rows")},
	}

	data, err := json.Marshal(q)
	if err != nil {
		t.Fatalf("json.Marshal(q) error = %v", err)
	}

	wantJSON := `{"reduction":{"group":[{"kind":"ref","name":"country"}],"metrics":[{"kind":"op","name":"count","alias":"rows"}]},"order":[{"desc":true}],"projection":[{"kind":"out","name":"rows"}]}`
	if got := string(data); got != wantJSON {
		t.Fatalf("json.Marshal(q) = %s, want %s", got, wantJSON)
	}
}

func TestQXJSONMarshalOmitsEmptyReduction(t *testing.T) {
	q := &QX{
		Reduction: &Reduction{},
		Projection: []Expr{
			REF("id"),
		},
	}

	data, err := json.Marshal(q)
	if err != nil {
		t.Fatalf("json.Marshal(q) error = %v", err)
	}

	wantJSON := `{"projection":[{"kind":"ref","name":"id"}]}`
	if got := string(data); got != wantJSON {
		t.Fatalf("json.Marshal(q) = %s, want %s", got, wantJSON)
	}
}

func TestQXJSONMarshalIncludesMeta(t *testing.T) {
	q := Query(EQ("status", "active")).
		Meta("trace.id", "req-9").
		Meta("flags", []any{"portable", 2, true})

	data, err := json.Marshal(q)
	if err != nil {
		t.Fatalf("json.Marshal(q) error = %v", err)
	}

	wantJSON := `{"filter":{"kind":"op","name":"eq","args":[{"kind":"ref","name":"status"},{"kind":"lit","value":"active"}]},"meta":[{"key":"trace.id","value":"req-9"},{"key":"flags","value":["portable",2,true]}]}`
	if got := string(data); got != wantJSON {
		t.Fatalf("json.Marshal(q) = %s, want %s", got, wantJSON)
	}
}

func TestJSONFastLiteralEncodingMatchesStandardLibrary(t *testing.T) {
	values := []any{
		[]byte{0, 1, 2, 253, 254, 255},
		[]string{"alpha", "line\nvalue", "кириллица", "control\x01"},
		[]int{-1, 0, 42},
		[]float64{-1.5, 0, 1e20},
		[]bool{true, false},
		[]any{"value", 3, true, map[string]any{"nested": []any{1, 2}}},
		map[string]string{"z": "last", "a": "first"},
		map[string][]byte{"raw": {1, 2, 3}},
		map[string][]string{"tags": {"a", "b"}},
		map[string][]int{"ids": {1, 2, 3}},
	}

	for _, value := range values {
		got, err := LIT(value).MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON(%T) error = %v", value, err)
		}
		encodedValue, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("json.Marshal(%T) error = %v", value, err)
		}
		want := append([]byte(`{"kind":"lit","value":`), encodedValue...)
		want = append(want, '}')
		if !bytes.Equal(got, want) {
			t.Fatalf("MarshalJSON(%T) = %s, want %s", value, got, want)
		}
	}
}

func TestJSONEncodingUsesMinimalStringEscaping(t *testing.T) {
	value := "<html>&line\u2028paragraph\u2029replacement:\ufffd"
	want := `{"kind":"lit","value":"<html>&line` + "\u2028" + `paragraph` + "\u2029" + `replacement:�"}`
	data, err := LIT(value).MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON(string) error = %v", err)
	}
	if got := string(data); got != want {
		t.Fatalf("MarshalJSON(string) = %q, want %q", got, want)
	}
}

func TestJSONEncodingRejectsInvalidUTF8(t *testing.T) {
	invalid := string([]byte{'x', 0xff})
	tests := []struct {
		name  string
		value any
	}{
		{name: "expression kind", value: Expr{Kind: invalid}},
		{name: "expression name", value: Expr{Kind: KindREF, Name: invalid}},
		{name: "expression alias", value: REF("x").AS(invalid)},
		{name: "literal string", value: LIT(invalid)},
		{name: "string slice", value: LIT([]string{"ok", invalid})},
		{name: "any slice", value: LIT([]any{"ok", invalid})},
		{name: "string map key", value: LIT(map[string]string{invalid: "ok"})},
		{name: "string map value", value: LIT(map[string]string{"key": invalid})},
		{name: "any map key", value: LIT(map[string]any{invalid: "ok"})},
		{name: "any map value", value: LIT(map[string]any{"key": invalid})},
		{name: "metadata key", value: &QX{Metadata: []MetaEntry{{Key: invalid}}}},
		{name: "metadata value", value: &QX{Metadata: []MetaEntry{{Key: "key", Value: invalid}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := json.Marshal(tt.value); !errors.Is(err, errInvalidJSONStringUTF8) {
				t.Fatalf("json.Marshal(%T) error = %v, want invalid UTF-8 error", tt.value, err)
			}
		})
	}
}

func TestJSONSizingDoesNotInspectFallbackRepresentation(t *testing.T) {
	value := compactJSONMarshaler{Retained: make([]byte, 1<<20)}
	tests := []struct {
		name    string
		marshal func() ([]byte, error)
	}{
		{name: "literal", marshal: LIT(value).MarshalJSON},
		{
			name: "metadata",
			marshal: func() ([]byte, error) {
				return (&QX{Metadata: []MetaEntry{{Key: "value", Value: value}}}).MarshalJSON()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.marshal()
			if err != nil {
				t.Fatalf("MarshalJSON() error = %v", err)
			}
			if cap(data) > 4096 {
				t.Fatalf("MarshalJSON() retained capacity = %d for %d-byte output", cap(data), len(data))
			}
		})
	}
}

func TestJSONFastLiteralEncodingRejectsCycles(t *testing.T) {
	cyclicMap := make(map[string]any)
	cyclicMap["self"] = cyclicMap
	if _, err := json.Marshal(LIT(cyclicMap)); err == nil {
		t.Fatal("json.Marshal(cyclic map) error = nil")
	}

	cyclicSlice := make([]any, 1)
	cyclicSlice[0] = cyclicSlice
	if _, err := json.Marshal(LIT(cyclicSlice)); err == nil {
		t.Fatal("json.Marshal(cyclic slice) error = nil")
	}

	shared := []any{"shared"}
	nonCyclic := []any{shared, shared}
	if _, err := json.Marshal(LIT(nonCyclic)); err != nil {
		t.Fatalf("json.Marshal(shared non-cyclic slice) error = %v", err)
	}

	deepCycle := make([]any, 1)
	deepTail := deepCycle
	for range appendJSONInlineVisitCount + 8 {
		next := make([]any, 1)
		deepTail[0] = next
		deepTail = next
	}
	deepTail[0] = deepCycle
	if _, err := json.Marshal(LIT(deepCycle)); err == nil {
		t.Fatal("json.Marshal(deep cyclic slice) error = nil")
	}

	var deepShared any = "shared"
	for range appendJSONInlineVisitCount + 8 {
		deepShared = []any{deepShared}
	}
	if _, err := json.Marshal(LIT([]any{deepShared, deepShared})); err != nil {
		t.Fatalf("json.Marshal(deep shared non-cyclic slice) error = %v", err)
	}
}

var marshalBenchBytesSink []byte

func BenchmarkMarshalExprStructural(b *testing.B) {
	expr := benchmarkMarshalExprStructural()
	data, err := json.Marshal(expr)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshalBenchBytesSink, err = json.Marshal(expr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalQXFilterOnly(b *testing.B) {
	q := benchmarkValidateFilterQuery()
	data, err := json.Marshal(q)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshalBenchBytesSink, err = json.Marshal(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalQXReduction(b *testing.B) {
	q := benchmarkValidateReductionQuery()
	data, err := json.Marshal(q)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshalBenchBytesSink, err = json.Marshal(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalQXLiteralHeavy(b *testing.B) {
	q := benchmarkCloneDeepQuery()
	data, err := json.Marshal(q)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshalBenchBytesSink, err = json.Marshal(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalQXJSONLiterals(b *testing.B) {
	q := benchmarkCloneJSONQuery()
	data, err := json.Marshal(q)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshalBenchBytesSink, err = json.Marshal(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalExprDeepJSONLiteral(b *testing.B) {
	var value any = "leaf"
	for range 256 {
		value = []any{value}
	}
	expr := LIT(value)
	data, err := json.Marshal(expr)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshalBenchBytesSink, err = json.Marshal(expr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMarshalExprStructural() Expr {
	return AND(
		EQ("status", "active"),
		EQ(LOWER("email"), "admin@example.com"),
		NOT(EXISTS("deleted_at")),
		OR(
			PREFIX("country", "u"),
			SUFFIX("email", "@example.com"),
			CONTAINS("role", "admin"),
		),
		GT(LEN("tags"), 2),
		OP("matches", LOWER("name"), LIT("adm.*")),
	).AS("expr_root")
}
