package qx

import (
	"reflect"
	"strconv"
	"testing"

	"encoding/json"
)

func TestExprJSONUnmarshalStandalone(t *testing.T) {
	data := []byte(`{"kind":"op","name":"eq","alias":"is_active","args":[{"kind":"ref","name":"status"},{"kind":"lit","value":"active"}]}`)
	want := EQ("status", "active").AS("is_active")

	var got Expr
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal(expr) error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("json.Unmarshal(expr) = %#v, want %#v", got, want)
	}
}

func TestExprJSONUnmarshalNull(t *testing.T) {
	expr := EQ("status", "active")

	if err := json.Unmarshal([]byte("null"), &expr); err != nil {
		t.Fatalf("json.Unmarshal(null) error = %v", err)
	}
	if !expr.IsZero() {
		t.Fatalf("json.Unmarshal(null) = %#v, want zero Expr", expr)
	}
}

func TestQXJSONUnmarshalRoundTrip(t *testing.T) {
	q := Query(
		AND(
			EQ("status", "active"),
			NOTNULL("email"),
		),
	)
	q.Reduction = &Reduction{
		Group: []Expr{
			REF("country").AS("country"),
		},
		Metrics: []Expr{
			ROWCOUNT().AS("rows"),
		},
		Having: EQ(OUT("country"), "RU"),
	}
	q.Order = []Order{
		{By: OUT("rows"), Desc: true},
	}
	q.Window = Window{Offset: 20, Limit: 10}
	q.Projection = []Expr{
		OUT("country"),
		OUT("rows"),
	}
	q.Metadata = []MetaEntry{
		{Key: "trace.id", Value: "req-7"},
		{
			Key: "transport",
			Value: map[string]any{
				"source": "rpc",
				"tags":   []any{"orders", 3, true},
			},
		},
	}

	data, err := json.Marshal(q)
	if err != nil {
		t.Fatalf("json.Marshal(q) error = %v", err)
	}

	var got QX
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal(q) error = %v", err)
	}

	if !reflect.DeepEqual(got, *q) {
		t.Fatalf("QX round-trip mismatch:\n got: %#v\nwant: %#v", got, *q)
	}
}

func TestQXJSONUnmarshalNull(t *testing.T) {
	q := Query(EQ("status", "active")).Limit(10)

	if err := json.Unmarshal([]byte("null"), q); err != nil {
		t.Fatalf("json.Unmarshal(null) error = %v", err)
	}
	if !reflect.DeepEqual(*q, QX{}) {
		t.Fatalf("json.Unmarshal(null) = %#v, want zero QX", *q)
	}
}

func TestQXJSONUnmarshalEmptyReductionAsNil(t *testing.T) {
	var q QX
	if err := json.Unmarshal([]byte(`{"reduction":{},"projection":[{"kind":"ref","name":"id"}]}`), &q); err != nil {
		t.Fatalf("json.Unmarshal(q) error = %v", err)
	}

	if q.Reduction != nil {
		t.Fatalf("json.Unmarshal(empty reduction) must leave Reduction nil, got %#v", q.Reduction)
	}
	if !reflect.DeepEqual(q.Projection, []Expr{REF("id")}) {
		t.Fatalf("projection = %#v, want %#v", q.Projection, []Expr{REF("id")})
	}
}

func TestExprJSONUnmarshalStrictCases(t *testing.T) {
	data := []byte(`{
		"kind":"op",
		"na\u006de":"first",
		"alias":"root",
		"args":[
			{"kind":"lit","value":[0,-1,9223372036854775807,9223372036854775808,1.5e2]},
			{"kind":"lit","value":{"escaped":"line\nvalue","flag":true}}
		],
		"unknown":{"huge":1e10000,"nested":[{"ignored":true}]}
	}`)

	var expr Expr
	if err := json.Unmarshal(data, &expr); err != nil {
		t.Fatalf("json.Unmarshal(expr) error = %v", err)
	}

	if expr.Kind != KindOP || expr.Name != "first" || expr.Alias != "root" {
		t.Fatalf("decoded expr header = %#v", expr)
	}
	maxSigned64 := int64(9223372036854775807)
	nextUnsigned64 := uint64(9223372036854775808)
	var maxSigned any = maxSigned64
	var nextUnsigned any = nextUnsigned64
	if strconv.IntSize == 64 {
		maxSigned = int(maxSigned64)
		nextUnsigned = uint(nextUnsigned64)
	}
	wantNumbers := []any{int(0), int(-1), maxSigned, nextUnsigned, float64(150)}
	if !reflect.DeepEqual(expr.Args[0].Value, wantNumbers) {
		t.Fatalf("decoded numbers = %#v, want %#v", expr.Args[0].Value, wantNumbers)
	}
	wantObject := map[string]any{"escaped": "line\nvalue", "flag": true}
	if !reflect.DeepEqual(expr.Args[1].Value, wantObject) {
		t.Fatalf("decoded object = %#v, want %#v", expr.Args[1].Value, wantObject)
	}
}

func TestQXJSONUnmarshalStrictCases(t *testing.T) {
	data := []byte(`{
		"filter":{"kind":"op","name":"eq","args":[{"kind":"ref","name":"status"},{"kind":"lit","value":"active"}]},
		"reduction":{},
		"window":{"offset":2,"limit":3},
		"order":[null,{"by":null,"desc":true}],
		"projection":[],
		"meta":[null,{"k\u0065y":"trace.id","value":{"ids":[1,2,3]}}],
		"ignored":{"huge":1e10000}
	}`)

	var q QX
	if err := json.Unmarshal(data, &q); err != nil {
		t.Fatalf("json.Unmarshal(q) error = %v", err)
	}
	if q.Reduction != nil {
		t.Fatalf("empty reduction = %#v, want nil", q.Reduction)
	}
	if q.Window != (Window{Offset: 2, Limit: 3}) {
		t.Fatalf("window = %#v, want offset 2 and limit 3", q.Window)
	}
	if len(q.Order) != 2 || !q.Order[0].By.IsZero() || !q.Order[1].Desc {
		t.Fatalf("order = %#v", q.Order)
	}
	if q.Projection == nil || len(q.Projection) != 0 {
		t.Fatalf("projection = %#v, want non-nil empty slice", q.Projection)
	}
	wantMeta := []MetaEntry{
		{},
		{Key: "trace.id", Value: map[string]any{"ids": []any{int(1), int(2), int(3)}}},
	}
	if !reflect.DeepEqual(q.Metadata, wantMeta) {
		t.Fatalf("metadata = %#v, want %#v", q.Metadata, wantMeta)
	}
}

func TestJSONUnmarshalRejectsDuplicateNames(t *testing.T) {
	tests := []struct {
		name string
		data string
		dst  any
	}{
		{name: "expression field", data: `{"name":"first","name":"last"}`, dst: new(Expr)},
		{name: "escaped expression field", data: `{"name":"first","na\u006de":"last"}`, dst: new(Expr)},
		{name: "expression unknown field", data: `{"extra":0,"extra":1}`, dst: new(Expr)},
		{name: "query order", data: `{"order":[{"by":{"kind":"ref","name":"x"},"desc":true}],"order":[{}]}`, dst: new(QX)},
		{name: "query metadata", data: `{"meta":[{"key":"trace","value":1}],"meta":[{}]}`, dst: new(QX)},
		{name: "query unknown field", data: `{"extra":0,"extra":1}`, dst: new(QX)},
		{name: "query unknown field after another", data: `{"extra":0,"other":1,"extra":2}`, dst: new(QX)},
		{name: "reduction field", data: `{"reduction":{"group":[],"group":[]}}`, dst: new(QX)},
		{name: "order field", data: `{"order":[{"desc":true,"desc":false}]}`, dst: new(QX)},
		{name: "metadata field", data: `{"meta":[{"key":"first","key":"last"}]}`, dst: new(QX)},
		{name: "literal map key", data: `{"value":{"x":0,"x":1}}`, dst: new(Expr)},
		{name: "null literal map key", data: `{"value":{"x":null,"x":1}}`, dst: new(Expr)},
		{name: "escaped literal map key", data: `{"value":{"x":0,"\u0078":1}}`, dst: new(Expr)},
		{name: "ignored nested map key", data: `{"ignored":{"x":0,"y":1,"x":2}}`, dst: new(QX)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(tt.data), tt.dst); err == nil {
				t.Fatalf("json.Unmarshal(%s) error = nil", tt.data)
			}
		})
	}
}

func TestJSONUnmarshalUsesCaseSensitiveFieldNames(t *testing.T) {
	var expr Expr
	if err := json.Unmarshal([]byte(`{
		"KIND":"lit",
		"NAME":"ignored",
		"kind":"ref",
		"name":"kept"
	}`), &expr); err != nil {
		t.Fatalf("json.Unmarshal(expr) error = %v", err)
	}
	if expr.Kind != KindREF || expr.Name != "kept" {
		t.Fatalf("expr = %#v, want exact lowercase fields only", expr)
	}

	var q QX
	data := []byte(`{
		"order":[{"by":{"kind":"ref","name":"x"},"desc":true}],
		"ORDER":[{}],
		"meta":[{"key":"trace","value":1}],
		"META":[{}]
	}`)
	if err := json.Unmarshal(data, &q); err != nil {
		t.Fatalf("json.Unmarshal(q) error = %v", err)
	}
	wantOrder := []Order{{By: REF("x"), Desc: true}}
	if !reflect.DeepEqual(q.Order, wantOrder) {
		t.Fatalf("q.Order = %#v, want %#v", q.Order, wantOrder)
	}
	wantMeta := []MetaEntry{{Key: "trace", Value: int(1)}}
	if !reflect.DeepEqual(q.Metadata, wantMeta) {
		t.Fatalf("q.Metadata = %#v, want %#v", q.Metadata, wantMeta)
	}
}

func TestJSONUnmarshalRejectsWrongFieldTypes(t *testing.T) {
	tests := []struct {
		name string
		data string
		dst  any
	}{
		{name: "expr kind", data: `{"kind":1}`, dst: new(Expr)},
		{name: "expr args", data: `{"args":{}}`, dst: new(Expr)},
		{name: "expr overflowing value", data: `{"kind":"lit","value":1e10000}`, dst: new(Expr)},
		{name: "window offset", data: `{"window":{"offset":-1}}`, dst: new(QX)},
		{name: "order desc", data: `{"order":[{"desc":"yes"}]}`, dst: new(QX)},
		{name: "meta key", data: `{"meta":[{"key":true}]}`, dst: new(QX)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(tt.data), tt.dst); err == nil {
				t.Fatalf("json.Unmarshal(%s) error = nil", tt.data)
			}
		})
	}
}

func TestJSONUnmarshalRejectsInvalidStrings(t *testing.T) {
	invalidUTF8Value := append([]byte(`{"name":"`), 0xff)
	invalidUTF8Value = append(invalidUTF8Value, []byte(`"}`)...)
	invalidUTF8Name := append([]byte(`{"`), 0xff)
	invalidUTF8Name = append(invalidUTF8Name, []byte(`":0}`)...)

	tests := []struct {
		name string
		data []byte
		dst  any
	}{
		{name: "invalid UTF-8 value", data: invalidUTF8Value, dst: new(Expr)},
		{name: "invalid UTF-8 name", data: invalidUTF8Name, dst: new(Expr)},
		{name: "lone high surrogate", data: []byte(`{"name":"\ud800"}`), dst: new(Expr)},
		{name: "lone low surrogate", data: []byte(`{"name":"\udc00"}`), dst: new(Expr)},
		{name: "invalid surrogate pair", data: []byte(`{"name":"\ud800\u0041"}`), dst: new(Expr)},
		{name: "invalid escape", data: []byte(`{"name":"\x00"}`), dst: new(Expr)},
		{name: "invalid nested string", data: []byte(`{"value":{"x":"\ud800"}}`), dst: new(Expr)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := json.Unmarshal(tt.data, tt.dst); err == nil {
				t.Fatalf("json.Unmarshal(%q) error = nil", tt.data)
			}
		})
	}
}

func TestJSONUnmarshalDecodesUnicodeSurrogatePair(t *testing.T) {
	var expr Expr
	data := []byte(`{"name":"smile:\ud83d\ude00","value":{"\ud83d\ude00":"ok"}}`)
	if err := json.Unmarshal(data, &expr); err != nil {
		t.Fatalf("json.Unmarshal(expr) error = %v", err)
	}
	if expr.Name != "smile:😀" {
		t.Fatalf("expr.Name = %q, want %q", expr.Name, "smile:😀")
	}
	want := map[string]any{"😀": "ok"}
	if !reflect.DeepEqual(expr.Value, want) {
		t.Fatalf("expr.Value = %#v, want %#v", expr.Value, want)
	}
}

func TestQXJSONUnmarshalLiteralHeavyReencode(t *testing.T) {
	original, err := json.Marshal(benchmarkCloneDeepQuery())
	if err != nil {
		t.Fatalf("json.Marshal(source) error = %v", err)
	}

	var decoded QX
	if err := json.Unmarshal(original, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(source) error = %v", err)
	}
	reencoded, err := json.Marshal(&decoded)
	if err != nil {
		t.Fatalf("json.Marshal(decoded) error = %v", err)
	}

	var gotValue any
	if err := json.Unmarshal(reencoded, &gotValue); err != nil {
		t.Fatalf("json.Unmarshal(reencoded) error = %v", err)
	}
	var wantValue any
	if err := json.Unmarshal(original, &wantValue); err != nil {
		t.Fatalf("json.Unmarshal(original) error = %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("reencoded query mismatch:\n got: %s\nwant: %s", reencoded, original)
	}
}

var (
	unmarshalBenchExprSink Expr
	unmarshalBenchQXSink   QX
	unmarshalBenchErrSink  error
)

func BenchmarkUnmarshalExprStructural(b *testing.B) {
	data, err := json.Marshal(benchmarkMarshalExprStructural())
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	var expr Expr
	for i := 0; i < b.N; i++ {
		expr = Expr{}
		err = json.Unmarshal(data, &expr)
		if err != nil {
			b.Fatal(err)
		}
	}

	unmarshalBenchExprSink = expr
	unmarshalBenchErrSink = err
}

func BenchmarkUnmarshalQXFilterOnly(b *testing.B) {
	data, err := json.Marshal(benchmarkValidateFilterQuery())
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	var q QX
	for i := 0; i < b.N; i++ {
		q = QX{}
		err = json.Unmarshal(data, &q)
		if err != nil {
			b.Fatal(err)
		}
	}

	unmarshalBenchQXSink = q
	unmarshalBenchErrSink = err
}

func BenchmarkUnmarshalQXReduction(b *testing.B) {
	data, err := json.Marshal(benchmarkValidateReductionQuery())
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	var q QX
	for i := 0; i < b.N; i++ {
		q = QX{}
		err = json.Unmarshal(data, &q)
		if err != nil {
			b.Fatal(err)
		}
	}

	unmarshalBenchQXSink = q
	unmarshalBenchErrSink = err
}

func BenchmarkUnmarshalQXLiteralHeavy(b *testing.B) {
	data, err := json.Marshal(benchmarkCloneDeepQuery())
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	var q QX
	for i := 0; i < b.N; i++ {
		q = QX{}
		err = json.Unmarshal(data, &q)
		if err != nil {
			b.Fatal(err)
		}
	}

	unmarshalBenchQXSink = q
	unmarshalBenchErrSink = err
}

func BenchmarkUnmarshalQXJSONLiterals(b *testing.B) {
	data, err := json.Marshal(benchmarkCloneJSONQuery())
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	var q QX
	for i := 0; i < b.N; i++ {
		q = QX{}
		err = json.Unmarshal(data, &q)
		if err != nil {
			b.Fatal(err)
		}
	}

	unmarshalBenchQXSink = q
	unmarshalBenchErrSink = err
}
