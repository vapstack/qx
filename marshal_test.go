package qx

import (
	"reflect"
	"testing"

	"encoding/json"
)

func TestExprJSONMarshalUnmarshalStandalone(t *testing.T) {
	expr := EQ("status", "active").AS("is_active")

	data, err := json.Marshal(expr)
	if err != nil {
		t.Fatalf("json.Marshal(expr) error = %v", err)
	}

	wantJSON := `{"kind":"op","name":"eq","alias":"is_active","args":[{"kind":"ref","name":"status"},{"kind":"lit","value":"active"}]}`
	if got := string(data); got != wantJSON {
		t.Fatalf("json.Marshal(expr) = %s, want %s", got, wantJSON)
	}

	var got Expr
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal(expr) error = %v", err)
	}

	if !reflect.DeepEqual(got, expr) {
		t.Fatalf("expr round-trip mismatch:\n got: %#v\nwant: %#v", got, expr)
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

func TestQXJSONMarshalUnmarshalRoundTrip(t *testing.T) {
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

var (
	marshalBenchBytesSink []byte
	marshalBenchExprSink  Expr
	marshalBenchQXSink    QX
	marshalBenchErrSink   error
)

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

	marshalBenchExprSink = expr
	marshalBenchErrSink = err
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

	marshalBenchQXSink = q
	marshalBenchErrSink = err
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

	marshalBenchQXSink = q
	marshalBenchErrSink = err
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

	marshalBenchQXSink = q
	marshalBenchErrSink = err
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
