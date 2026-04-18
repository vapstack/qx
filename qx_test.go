package qx

import (
	"reflect"
	"testing"
)

func TestExprBuilders(t *testing.T) {
	ref := REF("customer.id")
	if want := (Expr{Kind: KindREF, Name: "customer.id"}); !reflect.DeepEqual(ref, want) {
		t.Fatalf("REF() = %#v, want %#v", ref, want)
	}

	out := OUT("total")
	if want := (Expr{Kind: KindOUT, Name: "total"}); !reflect.DeepEqual(out, want) {
		t.Fatalf("OUT() = %#v, want %#v", out, want)
	}

	lit := LIT(42)
	if lit.Kind != KindLIT || lit.Value != 42 {
		t.Fatalf("LIT() = %#v, want literal 42", lit)
	}

	op := OP("  custom  ", ref, lit)
	want := Expr{
		Kind: KindOP,
		Name: "custom",
		Args: []Expr{ref, lit},
	}
	if !reflect.DeepEqual(op, want) {
		t.Fatalf("OP() = %#v, want %#v", op, want)
	}
}

func TestExprMethods(t *testing.T) {
	expr := REF("age")
	aliased := expr.AS("years")

	if expr.Alias != "" {
		t.Fatalf("AS() must not mutate receiver, got %#v", expr)
	}
	if aliased.Alias != "years" {
		t.Fatalf("AS() alias = %q, want %q", aliased.Alias, "years")
	}

	if !(Expr{}).IsZero() {
		t.Fatal("zero Expr must report IsZero()")
	}
	if LIT(nil).IsZero() {
		t.Fatal("literal expression must not report IsZero()")
	}

	lenExpr := OP(OpLEN, REF("name"))
	if !lenExpr.Is(KindOP, OpLEN) {
		t.Fatalf("Is() must match kind/op for %#v", lenExpr)
	}
	if lenExpr.Is(KindREF, OpLEN) {
		t.Fatalf("Is() must reject mismatched kind for %#v", lenExpr)
	}

	if name, ok := REF("age").OutputName(); !ok || name != "age" {
		t.Fatalf("REF output name = (%q, %v), want (%q, true)", name, ok, "age")
	}
	if name, ok := OUT("total").AS("total_amount").OutputName(); !ok || name != "total_amount" {
		t.Fatalf("aliased output name = (%q, %v), want (%q, true)", name, ok, "total_amount")
	}
	if name, ok := SUM("amount").OutputName(); ok || name != "" {
		t.Fatalf("unaliased computed output name = (%q, %v), want empty/false", name, ok)
	}
}

func TestExprUsedFields(t *testing.T) {
	expr := AND(
		EQ("status", "active"),
		PREFIX(LOWER("email"), "adm"),
		EQ(OUT("total"), 1),
		EQ("status", "archived"),
	)

	got := expr.UsedFields()
	want := []string{"status", "email"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UsedFields() = %#v, want %#v", got, want)
	}
}

func TestCountHelpers(t *testing.T) {
	count := COUNT("amount")
	if count.Kind != KindOP || count.Name != OpCOUNT || len(count.Args) != 1 {
		t.Fatalf("COUNT() = %#v, want OpCOUNT with one argument", count)
	}
	if !reflect.DeepEqual(count.Args[0], REF("amount")) {
		t.Fatalf("COUNT() arg = %#v, want %#v", count.Args[0], REF("amount"))
	}

	rowCount := ROWCOUNT()
	if rowCount.Kind != KindOP || rowCount.Name != OpCOUNT || len(rowCount.Args) != 0 {
		t.Fatalf("ROWCOUNT() = %#v, want OpCOUNT without arguments", rowCount)
	}
}

func TestAdditionalScalarHelpers(t *testing.T) {
	nullIf := NULLIF("nickname", "")
	if want := OP(OpNULLIF, REF("nickname"), LIT("")); !reflect.DeepEqual(nullIf, want) {
		t.Fatalf("NULLIF() = %#v, want %#v", nullIf, want)
	}

	ifExpr := IF(EQ("status", "active"), REF("name"), "guest")
	if want := OP(OpIF, EQ("status", "active"), REF("name"), LIT("guest")); !reflect.DeepEqual(ifExpr, want) {
		t.Fatalf("IF() = %#v, want %#v", ifExpr, want)
	}

	concat := CONCAT(REF("first_name"), LIT(" "), REF("last_name"))
	if want := OP(OpCONCAT, REF("first_name"), LIT(" "), REF("last_name")); !reflect.DeepEqual(concat, want) {
		t.Fatalf("CONCAT() = %#v, want %#v", concat, want)
	}

	like := LIKE("email", "adm%")
	if want := OP(OpLIKE, REF("email"), LIT("adm%")); !reflect.DeepEqual(like, want) {
		t.Fatalf("LIKE() = %#v, want %#v", like, want)
	}

	notLike := NOTLIKE("email", "adm%")
	if want := NOT(LIKE("email", "adm%")); !reflect.DeepEqual(notLike, want) {
		t.Fatalf("NOTLIKE() = %#v, want %#v", notLike, want)
	}

	ilike := ILIKE(LOWER("email"), "adm%")
	if want := OP(OpILIKE, LOWER("email"), LIT("adm%")); !reflect.DeepEqual(ilike, want) {
		t.Fatalf("ILIKE() = %#v, want %#v", ilike, want)
	}

	notILike := NOTILIKE("email", "adm%")
	if want := NOT(ILIKE("email", "adm%")); !reflect.DeepEqual(notILike, want) {
		t.Fatalf("NOTILIKE() = %#v, want %#v", notILike, want)
	}

	matches := MATCHES("email", "adm.*")
	if want := OP(OpMATCHES, REF("email"), LIT("adm.*")); !reflect.DeepEqual(matches, want) {
		t.Fatalf("MATCHES() = %#v, want %#v", matches, want)
	}

	notMatches := NOTMATCHES("email", "adm.*")
	if want := NOT(MATCHES("email", "adm.*")); !reflect.DeepEqual(notMatches, want) {
		t.Fatalf("NOTMATCHES() = %#v, want %#v", notMatches, want)
	}

	dateTrunc := DATETRUNC("created_at", "day")
	if want := OP(OpDATETRUNC, REF("created_at"), LIT("day")); !reflect.DeepEqual(dateTrunc, want) {
		t.Fatalf("DATETRUNC() = %#v, want %#v", dateTrunc, want)
	}

	extract := EXTRACT("created_at", "hour")
	if want := OP(OpEXTRACT, REF("created_at"), LIT("hour")); !reflect.DeepEqual(extract, want) {
		t.Fatalf("EXTRACT() = %#v, want %#v", extract, want)
	}

	now := NOW()
	if want := OP(OpNOW); !reflect.DeepEqual(now, want) {
		t.Fatalf("NOW() = %#v, want %#v", now, want)
	}

	dateAdd := DATEADD("created_at", 7, "day")
	if want := OP(OpDATEADD, REF("created_at"), LIT(7), LIT("day")); !reflect.DeepEqual(dateAdd, want) {
		t.Fatalf("DATEADD() = %#v, want %#v", dateAdd, want)
	}

	dateDiff := DATEDIFF("started_at", REF("finished_at"), "day")
	if want := OP(OpDATEDIFF, REF("started_at"), REF("finished_at"), LIT("day")); !reflect.DeepEqual(dateDiff, want) {
		t.Fatalf("DATEDIFF() = %#v, want %#v", dateDiff, want)
	}

	greatest := GREATEST(REF("score"), LIT(100), REF("backup_score"))
	if want := OP(OpGREATEST, REF("score"), LIT(100), REF("backup_score")); !reflect.DeepEqual(greatest, want) {
		t.Fatalf("GREATEST() = %#v, want %#v", greatest, want)
	}

	least := LEAST(REF("score"), LIT(10), REF("backup_score"))
	if want := OP(OpLEAST, REF("score"), LIT(10), REF("backup_score")); !reflect.DeepEqual(least, want) {
		t.Fatalf("LEAST() = %#v, want %#v", least, want)
	}
}

func TestQueryBuilders(t *testing.T) {
	if got := Query(); !reflect.DeepEqual(got, &QX{}) {
		t.Fatalf("Query() = %#v, want empty query", got)
	}

	expr := EQ("status", "active")
	if got := Query(expr); !reflect.DeepEqual(got, &QX{Filter: expr}) {
		t.Fatalf("Query(expr) = %#v, want direct root %#v", got, expr)
	}

	want := &QX{Filter: AND(EQ("status", "active"), GT("age", 18))}
	if got := Query(EQ("status", "active"), GT("age", 18)); !reflect.DeepEqual(got, want) {
		t.Fatalf("Query(a, b) = %#v, want %#v", got, want)
	}

	if got := Where(EQ("status", "active"), GT("age", 18)); !reflect.DeepEqual(got, want) {
		t.Fatalf("Where(a, b) = %#v, want %#v", got, want)
	}
}

func TestReductionBuilders(t *testing.T) {
	metric := SUM("amount").AS("total_amount")
	if got := Aggregate(metric); !reflect.DeepEqual(got, &QX{
		Reduction: &Reduction{Metrics: []Expr{metric}},
	}) {
		t.Fatalf("Aggregate() = %#v", got)
	}

	if got := Metrics(metric); !reflect.DeepEqual(got, &QX{
		Reduction: &Reduction{Metrics: []Expr{metric}},
	}) {
		t.Fatalf("Metrics() = %#v", got)
	}

	if got := Group("country", "city"); !reflect.DeepEqual(got, &QX{
		Reduction: &Reduction{Group: []Expr{REF("country"), REF("city")}},
	}) {
		t.Fatalf("Group() = %#v", got)
	}

	groupExpr := LOWER("country").AS("country_lower")
	if got := GroupBy(groupExpr); !reflect.DeepEqual(got, &QX{
		Reduction: &Reduction{Group: []Expr{groupExpr}},
	}) {
		t.Fatalf("GroupBy() = %#v", got)
	}
}

func TestReductionMethods(t *testing.T) {
	var reduction *Reduction
	if !reduction.IsEmpty() {
		t.Fatal("(*Reduction)(nil).IsEmpty() must report true")
	}

	reduction = &Reduction{}
	if !reduction.IsEmpty() {
		t.Fatal("empty Reduction must report IsEmpty()")
	}

	reduction = &Reduction{Group: make([]Expr, 0, 1), Metrics: make([]Expr, 0, 1)}
	if !reduction.IsEmpty() {
		t.Fatal("Reduction with only empty slices must report IsEmpty()")
	}

	reduction = &Reduction{Group: []Expr{{}}}
	if reduction.IsEmpty() {
		t.Fatal("Reduction with structural entries must not report IsEmpty()")
	}

	var q *QX
	if q.HasReduction() {
		t.Fatal("(*QX)(nil).HasReduction() must report false")
	}

	q = &QX{}
	if q.HasReduction() {
		t.Fatal("empty query must not report HasReduction()")
	}

	q.Reduction = &Reduction{}
	if q.HasReduction() {
		t.Fatal("query with empty reduction must not report HasReduction()")
	}

	q.Reduction = &Reduction{Metrics: []Expr{ROWCOUNT().AS("rows")}}
	if !q.HasReduction() {
		t.Fatal("query with non-empty reduction must report HasReduction()")
	}
}

func TestProjectionBuilders(t *testing.T) {
	projectionExpr := LOWER("email").AS("email_lower")

	if got := Select("id", "email"); !reflect.DeepEqual(got, &QX{
		Projection: []Expr{REF("id"), REF("email")},
	}) {
		t.Fatalf("Select() = %#v", got)
	}

	if got := SelectOut("total_amount"); !reflect.DeepEqual(got, &QX{
		Projection: []Expr{OUT("total_amount")},
	}) {
		t.Fatalf("SelectOut() = %#v", got)
	}

	if got := Select(projectionExpr); !reflect.DeepEqual(got, &QX{
		Projection: []Expr{projectionExpr},
	}) {
		t.Fatalf("Select(expr) = %#v", got)
	}
}

func TestQXWhere(t *testing.T) {
	t.Run("no expressions", func(t *testing.T) {
		q := Query(EQ("status", "active"))
		if got := q.Where(); got != q {
			t.Fatal("Where() must return the receiver")
		}
		if want := EQ("status", "active"); !reflect.DeepEqual(q.Filter, want) {
			t.Fatalf("Where() changed filter to %#v, want %#v", q.Filter, want)
		}
	})

	t.Run("appends to existing AND", func(t *testing.T) {
		q := Query(EQ("status", "active"), GT("age", 18)).Where(LT("age", 65))
		want := AND(EQ("status", "active"), GT("age", 18), LT("age", 65))
		if !reflect.DeepEqual(q.Filter, want) {
			t.Fatalf("Filter = %#v, want %#v", q.Filter, want)
		}
	})

	t.Run("wraps existing non-AND filter", func(t *testing.T) {
		q := Query(EQ("status", "active")).Where(LT("age", 65), GT("age", 18))
		want := AND(EQ("status", "active"), LT("age", 65), GT("age", 18))
		if !reflect.DeepEqual(q.Filter, want) {
			t.Fatalf("Filter = %#v, want %#v", q.Filter, want)
		}
	})
}

func TestQXSort(t *testing.T) {
	q := Query().
		Sort("created_at").
		SortOut("total_amount", DESC).
		SortBy(Expr{}, DESC).
		SortBy(LOWER("email"), DESC)

	want := []Order{
		{By: REF("created_at")},
		{By: OUT("total_amount"), Desc: true},
		{By: LOWER("email"), Desc: true},
	}
	if !reflect.DeepEqual(q.Order, want) {
		t.Fatalf("Order = %#v, want %#v", q.Order, want)
	}
}

func TestQXReductionMethods(t *testing.T) {
	t.Run("metrics and grouping", func(t *testing.T) {
		metric := SUM("amount").AS("total_amount")
		groupExpr := LOWER("country").AS("country_lower")

		q := new(QX).
			Aggregate(metric).
			Group("country").
			GroupBy(groupExpr)

		want := &QX{
			Reduction: &Reduction{
				Group:   []Expr{REF("country"), groupExpr},
				Metrics: []Expr{metric},
			},
		}
		if !reflect.DeepEqual(q, want) {
			t.Fatalf("query = %#v, want %#v", q, want)
		}
	})

	t.Run("having appends to existing AND", func(t *testing.T) {
		q := new(QX).
			Having(GT(OUT("total_amount"), 10), LT(OUT("total_amount"), 100)).
			Having(NE(OUT("country"), ""))

		want := AND(
			GT(OUT("total_amount"), 10),
			LT(OUT("total_amount"), 100),
			NE(OUT("country"), ""),
		)
		if !reflect.DeepEqual(q.Reduction.Having, want) {
			t.Fatalf("Having = %#v, want %#v", q.Reduction.Having, want)
		}
	})

	t.Run("having wraps existing expression", func(t *testing.T) {
		q := new(QX).
			Having(GT(OUT("total_amount"), 10)).
			Having(LT(OUT("total_amount"), 100))

		want := AND(
			GT(OUT("total_amount"), 10),
			LT(OUT("total_amount"), 100),
		)
		if !reflect.DeepEqual(q.Reduction.Having, want) {
			t.Fatalf("Having = %#v, want %#v", q.Reduction.Having, want)
		}
	})
}

func TestQXProjectionMethods(t *testing.T) {
	t.Run("no projection inputs", func(t *testing.T) {
		q := Query(EQ("status", "active"))
		if got := q.Select(); got != q {
			t.Fatal("Select() must return the receiver")
		}
		if got := q.SelectOut(); got != q {
			t.Fatal("SelectOut() must return the receiver")
		}
		if got := q.SelectExpr(); got != q {
			t.Fatal("SelectExpr() must return the receiver")
		}
		if q.Projection != nil {
			t.Fatalf("empty projection calls must not allocate projection, got %#v", q.Projection)
		}
	})

	t.Run("appends to projection", func(t *testing.T) {
		projectedEmail := LOWER("email").AS("email_lower")

		q := new(QX).
			Select("id", "email").
			SelectOut("total_amount").
			SelectExpr(projectedEmail)

		want := []Expr{
			REF("id"),
			REF("email"),
			OUT("total_amount"),
			projectedEmail,
		}
		if !reflect.DeepEqual(q.Projection, want) {
			t.Fatalf("Projection = %#v, want %#v", q.Projection, want)
		}
	})
}

func TestQXMetaMethods(t *testing.T) {
	t.Run("upserts and preserves order", func(t *testing.T) {
		q := Query().
			Meta("trace.id", "req-1").
			Meta("ui.source", "orders.list").
			Meta("trace.id", "req-2")

		want := []MetaEntry{
			{Key: "trace.id", Value: "req-2"},
			{Key: "ui.source", Value: "orders.list"},
		}
		if !reflect.DeepEqual(q.Metadata, want) {
			t.Fatalf("Metadata = %#v, want %#v", q.Metadata, want)
		}

		value, ok := q.MetaValue("trace.id")
		if !ok || value != "req-2" {
			t.Fatalf("MetaValue(trace.id) = (%#v, %v), want (%#v, true)", value, ok, "req-2")
		}

		if value, ok := q.MetaValue("missing"); ok || value != nil {
			t.Fatalf("MetaValue(missing) = (%#v, %v), want (nil, false)", value, ok)
		}
	})

	t.Run("delete removes matching keys", func(t *testing.T) {
		q := Query().
			Meta("trace.id", "req-2").
			Meta("ui.source", "orders.list").
			Meta("debug", true)

		if got := q.DeleteMeta("ui.source", "missing"); got != q {
			t.Fatal("DeleteMeta() must return the receiver")
		}

		want := []MetaEntry{
			{Key: "trace.id", Value: "req-2"},
			{Key: "debug", Value: true},
		}
		if !reflect.DeepEqual(q.Metadata, want) {
			t.Fatalf("Metadata = %#v, want %#v", q.Metadata, want)
		}
	})

	t.Run("nil receiver lookup is safe", func(t *testing.T) {
		var q *QX
		if value, ok := q.MetaValue("trace.id"); ok || value != nil {
			t.Fatalf("nil MetaValue() = (%#v, %v), want (nil, false)", value, ok)
		}
		if got := q.DeleteMeta("trace.id"); got != nil {
			t.Fatalf("nil DeleteMeta() = %#v, want nil", got)
		}
	})
}

func TestQXWindow(t *testing.T) {
	q := Query().Offset(10).Limit(5)
	if q.Window != (Window{Offset: 10, Limit: 5}) {
		t.Fatalf("Window = %#v, want %#v", q.Window, Window{Offset: 10, Limit: 5})
	}

	q = Query().Page(2, 20)
	if q.Window != (Window{Offset: 20, Limit: 20}) {
		t.Fatalf("Page() window = %#v, want %#v", q.Window, Window{Offset: 20, Limit: 20})
	}

	assertPanics(t, "negative offset", func() {
		Query().Offset(-1)
	})
	assertPanics(t, "negative limit", func() {
		Query().Limit(-1)
	})
	assertPanics(t, "invalid page number", func() {
		Query().Page(0, 10)
	})
	assertPanics(t, "invalid page size", func() {
		Query().Page(1, 0)
	})
}

var qxBenchSink *QX

func BenchmarkBuildQXSimpleFilter(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		qxBenchSink = Query(EQ("status", "active"))
	}
}

func BenchmarkBuildQXCompositeFilter(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		qxBenchSink = Query(
			EQ("status", "active"),
			GTE("age", 18),
		).
			Where(
				LT("age", 65),
				OR(
					PREFIX(LOWER("email"), "admin"),
					CONTAINS("role", "ops"),
				),
			).
			Sort("created_at", DESC).
			Page(2, 25)
	}
}

func BenchmarkBuildQXReductionQuery(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		qxBenchSink = Query(
			EQ("status", "active"),
			EQ("tenant_id", "t-1"),
			NOT(EXISTS("deleted_at")),
		).
			Group("country", "city").
			GroupBy(LOWER("currency").AS("currency_lower")).
			Metrics(
				SUM("amount").AS("total_amount"),
				AVG("amount").AS("avg_amount"),
				ROWCOUNT().AS("rows"),
			).
			Having(
				GT(OUT("total_amount"), 100),
				GT(OUT("rows"), 5),
			).
			SortOut("total_amount", DESC).
			SortOut("avg_amount").
			Page(3, 50)
	}
}

func TestQXUsedFields(t *testing.T) {
	var nilQuery *QX
	if got := nilQuery.UsedFields(); got != nil {
		t.Fatalf("nil query UsedFields() = %#v, want nil", got)
	}

	q := Query(
		EQ("status", "active"),
		PREFIX(LOWER("email"), "adm"),
	).
		Group("country").
		GroupBy(LOWER("city").AS("city_lower")).
		Metrics(SUM("amount").AS("total_amount"), ROWCOUNT().AS("rows")).
		Having(GT(OUT("total_amount"), 100), LT("created_at", "2026-01-01")).
		Sort("status", DESC).
		SortOut("total_amount").
		SortBy(LOWER("nickname"))

	want := []string{"status", "email", "country", "city", "amount", "created_at", "nickname"}
	if got := q.UsedFields(); !reflect.DeepEqual(got, want) {
		t.Fatalf("UsedFields() = %#v, want %#v", got, want)
	}
}

func TestQXUsedFieldsIncludesProjection(t *testing.T) {
	q := Query(
		EQ("status", "active"),
	).
		Select("email").
		SelectExpr(
			LOWER("display_name").AS("display_name_lower"),
			CONCAT(REF("country"), LIT("-"), REF("city")).AS("geo"),
		)

	want := []string{"status", "email", "display_name", "country", "city"}
	if got := q.UsedFields(); !reflect.DeepEqual(got, want) {
		t.Fatalf("UsedFields() with projection = %#v, want %#v", got, want)
	}
}

func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatalf("%s: expected panic", name)
		}
	}()
	fn()
}
