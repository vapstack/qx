package qx

import (
	"encoding/json"
	"testing"
)

func TestExprBuilders(t *testing.T) {
	e := EQ("A", 1)
	if e.Op != OpEQ || e.Field != "A" || e.Value != 1 || e.Not {
		t.Fatalf("EQ builder broken: %+v", e)
	}

	e = NOT(e)
	if !e.Not {
		t.Fatalf("NOT did not flip Not flag")
	}

	and := AND(EQ("A", 1), GT("B", 2))
	if and.Op != OpAND || len(and.Operands) != 2 {
		t.Fatalf("AND builder broken: %+v", and)
	}

	or := OR(EQ("A", 1), EQ("A", 2))
	if or.Op != OpOR || len(or.Operands) != 2 {
		t.Fatalf("OR builder broken: %+v", or)
	}
}

func TestQueryBuilder(t *testing.T) {
	q := Query()
	if q.Expr.Op != 0 {
		t.Fatalf("empty Query should have zero Expr, got %+v", q.Expr)
	}

	e := EQ("A", 1)
	q = Query(e)
	if q.Expr.Op != e.Op || q.Expr.Not != e.Not || q.Expr.Field != e.Field || q.Expr.Value != e.Value || len(q.Expr.Operands) != 0 {
		t.Fatalf("Query(expr) should set Expr directly, got %+v want %+v", q.Expr, e)
	}

	q = Query(EQ("A", 1), EQ("B", 2))
	if q.Expr.Op != OpAND || len(q.Expr.Operands) != 2 {
		t.Fatalf("Query(a,b) should create AND expr")
	}

	q = Where(EQ("A", 1), EQ("B", 2))
	if q.Expr.Op != OpAND || len(q.Expr.Operands) != 2 {
		t.Fatalf("Where(a,b) should alias Query and create AND expr")
	}
}

func TestQXFluentAPI(t *testing.T) {
	q := Query(EQ("A", 1)).
		Where(EQ("B", 2)).
		CacheKey("k").
		By("A", ASC).
		ByArrayCount("B", DESC).
		Skip(10).
		Max(5)

	if q.Key != "k" {
		t.Fatalf("CacheKey not set")
	}
	if q.Offset != 10 || q.Limit != 5 {
		t.Fatalf("Skip/Max broken: offset=%d limit=%d", q.Offset, q.Limit)
	}
	if q.Expr.Op != OpAND || len(q.Expr.Operands) != 2 {
		t.Fatalf("Where should combine expressions using AND, got %+v", q.Expr)
	}
	if len(q.Order) != 2 {
		t.Fatalf("Order not accumulated")
	}
	if !q.Order[1].Desc {
		t.Fatalf("DESC not set correctly")
	}
}

func TestQXPage(t *testing.T) {
	q := Query().Page(2, 20)
	if q.Offset != 20 || q.Limit != 20 {
		t.Fatalf("Page broken: offset=%d limit=%d", q.Offset, q.Limit)
	}
}

func TestQXPagePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on invalid Page()")
		}
	}()
	Query().Page(0, 10)
}

func TestNormalizeNil(t *testing.T) {
	if Normalize(nil) != nil {
		t.Fatalf("Normalize(nil) must return nil")
	}
}

func TestNormalizeFlattensSameLogicalGroups(t *testing.T) {
	q := Query(
		AND(
			EQ("A", 1),
			AND(
				EQ("B", 2),
				AND(EQ("C", 3)),
			),
			AND(),
		),
	).
		CacheKey("k").
		By("A", ASC).
		Skip(5).
		Max(10)

	Normalize(q)

	if q.Key != "k" || q.Offset != 5 || q.Limit != 10 || len(q.Order) != 1 {
		t.Fatalf("Normalize must not touch query metadata: %+v", q)
	}

	if q.Expr.Op != OpAND {
		t.Fatalf("root op = %v, want AND", q.Expr.Op)
	}
	if got := len(q.Expr.Operands); got != 3 {
		t.Fatalf("operand count = %d, want 3", got)
	}
	if q.Expr.Operands[0].Field != "A" || q.Expr.Operands[1].Field != "B" || q.Expr.Operands[2].Field != "C" {
		t.Fatalf("unexpected flattened operands: %+v", q.Expr.Operands)
	}
}

func TestNormalizeCollapsesSingletonLogicalGroup(t *testing.T) {
	q := Query(AND(AND(EQ("A", 1))))

	Normalize(q)

	if q.Expr.Op != OpEQ || q.Expr.Field != "A" || q.Expr.Value != 1 || q.Expr.Not {
		t.Fatalf("unexpected collapsed expression: %+v", q.Expr)
	}
}

func TestNormalizeCollapsesSingletonLogicalGroupWithNot(t *testing.T) {
	q := Query(NOT(AND(NE("A", 1))))

	Normalize(q)

	if q.Expr.Op != OpEQ || q.Expr.Field != "A" || q.Expr.Value != 1 || q.Expr.Not {
		t.Fatalf("expected NOT(AND(NE)) to collapse into EQ, got %+v", q.Expr)
	}
}

func TestNormalizeKeepsINSemanticsUntouched(t *testing.T) {
	var nilInt *int
	q := Query(NOTIN("A", []any{nilInt}))

	Normalize(q)

	if q.Expr.Op != OpIN || !q.Expr.Not || q.Expr.Field != "A" {
		t.Fatalf("unexpected normalized expression: %+v", q.Expr)
	}
	v, ok := q.Expr.Value.([]any)
	if !ok || len(v) != 1 || v[0] != nilInt {
		t.Fatalf("unexpected normalized value: %#v", q.Expr.Value)
	}
}

func TestUsedFields(t *testing.T) {
	q := Query(
		AND(
			EQ("A", 1),
			OR(
				GT("B", 2),
				EQ("C", 3),
			),
		),
	).
		By("D", ASC).
		By("A", DESC)

	fields := q.UsedFields()

	expect := map[string]bool{
		"A": true,
		"B": true,
		"C": true,
		"D": true,
	}

	if len(fields) != len(expect) {
		t.Fatalf("unexpected field count: %v", fields)
	}
	for _, f := range fields {
		if !expect[f] {
			t.Fatalf("unexpected field %q", f)
		}
	}

	fields = q.Expr.UsedFields()

	expect = map[string]bool{
		"A": true,
		"B": true,
		"C": true,
	}

	if len(fields) != len(expect) {
		t.Fatalf("unexpected field count: %v", fields)
	}
	for _, f := range fields {
		if !expect[f] {
			t.Fatalf("unexpected field %q", f)
		}
	}
}

func TestOpJSONRoundTrip(t *testing.T) {
	type T struct {
		Op Op `json:"op"`
	}

	for _, op := range []Op{OpEQ, OpGT, OpIN, OpHASANY} {
		b, err := json.Marshal(T{Op: op})
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		var t2 T
		if err := json.Unmarshal(b, &t2); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		if t2.Op != op {
			t.Fatalf("round-trip failed: %v != %v", t2.Op, op)
		}
	}
}

func TestOpJSONInvalid(t *testing.T) {
	var op Op
	err := json.Unmarshal([]byte(`"UNKNOWN"`), &op)
	if err == nil {
		t.Fatalf("expected error on unknown op")
	}
}
