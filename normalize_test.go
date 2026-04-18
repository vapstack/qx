package qx

import (
	"reflect"
	"testing"
)

func TestNormalize_NilQuery(t *testing.T) {
	if Normalize(nil) != nil {
		t.Fatalf("Normalize(nil) must return nil")
	}

	var q *QX
	if q.Normalize() != nil {
		t.Fatalf("(*QX)(nil).Normalize() must return nil")
	}
}

func TestNormalizeExpr_StructuralRewrites(t *testing.T) {
	tests := []struct {
		name string
		in   Expr
		want Expr
	}{
		{
			name: "flatten nested and groups and preserve operand order",
			in: AND(
				EQ("a", 1),
				AND(
					EQ("b", 2),
					AND(EQ("c", 3)),
				),
				AND(),
			),
			want: AND(
				EQ("a", 1),
				EQ("b", 2),
				EQ("c", 3),
			),
		},
		{
			name: "flatten nested or groups and drop empty group",
			in: OR(
				EQ("a", 1),
				OR(EQ("b", 2), OR()),
				OR(EQ("c", 3)),
			),
			want: OR(
				EQ("a", 1),
				EQ("b", 2),
				EQ("c", 3),
			),
		},
		{
			name: "collapse singleton logical wrapper",
			in:   AND(AND(EQ("a", 1))),
			want: EQ("a", 1),
		},
		{
			name: "move parent alias onto only child when collapsing",
			in:   AND(EQ("a", 1)).AS("only"),
			want: EQ("a", 1).AS("only"),
		},
		{
			name: "keep singleton wrapper when both parent and child have alias",
			in:   OR(EQ("status", "active").AS("child")).AS("parent"),
			want: OR(EQ("status", "active").AS("child")).AS("parent"),
		},
		{
			name: "do not flatten aliased logical child",
			in: AND(
				EQ("a", 1),
				AND(EQ("b", 2), EQ("c", 3)).AS("nested"),
			),
			want: AND(
				EQ("a", 1),
				AND(EQ("b", 2), EQ("c", 3)).AS("nested"),
			),
		},
		{
			name: "do not flatten different logical operator",
			in: AND(
				EQ("a", 1),
				OR(EQ("b", 2), EQ("c", 3)),
			),
			want: AND(
				EQ("a", 1),
				OR(EQ("b", 2), EQ("c", 3)),
			),
		},
		{
			name: "normalize nested logical args inside non logical parent",
			in: NOT(
				AND(
					EQ("a", 1),
					AND(EQ("b", 2), AND(EQ("c", 3))),
				),
			),
			want: NOT(
				AND(
					EQ("a", 1),
					EQ("b", 2),
					EQ("c", 3),
				),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeExpr(tt.in)
			assertExprEqual(t, got, tt.want)
		})
	}
}

func TestNormalizeExprCOW_ChangeFlag(t *testing.T) {
	t.Run("reports unchanged for already normalized expression", func(t *testing.T) {
		in := AND(EQ("a", 1), EQ("b", 2))

		got, changed := normalizeExprCOW(in)

		if changed {
			t.Fatalf("changed = true, want false")
		}
		assertExprEqual(t, got, in)
	})

	t.Run("reports changed when rewrite happens", func(t *testing.T) {
		in := AND(EQ("a", 1), AND(EQ("b", 2)))
		want := AND(EQ("a", 1), EQ("b", 2))

		got, changed := normalizeExprCOW(in)

		if !changed {
			t.Fatalf("changed = false, want true")
		}
		assertExprEqual(t, got, want)
	})
}

func TestNormalizeQX_RecursesAcrossQuerySections(t *testing.T) {
	q := Query(
		AND(
			EQ("status", "active"),
			AND(EQ("age", 18), AND(EQ("score", 100))),
		),
	)
	q.Reduction = &Reduction{
		Group: []Expr{
			AND(AND(REF("country"))),
			AND(REF("region")).AS("group_region"),
		},
		Metrics: []Expr{
			NOT(AND(AND(EQ("archived", false)))),
			AND(EQ("count", 1)).AS("metric_count"),
		},
		Having: OR(
			OR(GT(OUT("total"), 10)),
			GT(OUT("max_score"), 20),
		),
	}
	q.Order = []Order{
		{By: OR(OR(REF("priority"))), Desc: true},
		{By: AND(EQ("pinned", true)).AS("pin_sort"), Desc: false},
	}
	q.Window = Window{Offset: 10, Limit: 25}
	q.Projection = []Expr{
		OR(OR(OUT("total"))).AS("project_total"),
		AND(AND(EQ(OUT("max_score"), 20))).AS("project_match"),
	}

	got := Normalize(q)

	if got != q {
		t.Fatalf("Normalize must return the original query pointer")
	}
	if q.Window != (Window{Offset: 10, Limit: 25}) {
		t.Fatalf("Normalize must preserve window, got %+v", q.Window)
	}
	if len(q.Order) != 2 || !q.Order[0].Desc || q.Order[1].Desc {
		t.Fatalf("Normalize must preserve order direction flags, got %+v", q.Order)
	}

	assertExprEqual(t, q.Filter, AND(
		EQ("status", "active"),
		EQ("age", 18),
		EQ("score", 100),
	))

	assertExprEqual(t, q.Reduction.Group[0], REF("country"))
	assertExprEqual(t, q.Reduction.Group[1], REF("region").AS("group_region"))

	assertExprEqual(t, q.Reduction.Metrics[0], NOT(EQ("archived", false)))
	assertExprEqual(t, q.Reduction.Metrics[1], EQ("count", 1).AS("metric_count"))

	assertExprEqual(t, q.Reduction.Having, OR(
		GT(OUT("total"), 10),
		GT(OUT("max_score"), 20),
	))

	assertExprEqual(t, q.Order[0].By, REF("priority"))
	assertExprEqual(t, q.Order[1].By, EQ("pinned", true).AS("pin_sort"))
	assertExprEqual(t, q.Projection[0], OUT("total").AS("project_total"))
	assertExprEqual(t, q.Projection[1], EQ(OUT("max_score"), 20).AS("project_match"))
}

func assertExprEqual(t *testing.T, got, want Expr) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	t.Fatalf("unexpected expression:\n got: %#v\nwant: %#v", got, want)
}

var normalizeBenchSink *QX

func BenchmarkNormalizeAlreadyNormalized(b *testing.B) {
	q := benchmarkNormalizeStableQuery()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		normalizeBenchSink = q.Normalize()
	}
}

func BenchmarkNormalizeRewriteHeavy(b *testing.B) {
	source := benchmarkNormalizeRewriteQuery()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q := source.Clone()
		b.StartTimer()

		normalizeBenchSink = q.Normalize()
	}
}

func benchmarkNormalizeStableQuery() *QX {
	return Query(
		AND(
			EQ("status", "active"),
			EQ("tenant_id", "t-1"),
			OR(EQ("country", "us"), EQ("country", "ca")),
		),
	).GroupBy(
		REF("country").AS("country"),
		LOWER("city").AS("city_lower"),
	).Metrics(
		SUM("amount").AS("total_amount"),
		ROWCOUNT().AS("rows"),
	).Having(
		AND(
			GT(OUT("total_amount"), 100),
			GT(OUT("rows"), 1),
		),
	).SortOut("total_amount", DESC).SortBy(REF("created_at")).SelectOut("country", "total_amount")
}

func benchmarkNormalizeRewriteQuery() *QX {
	q := Query(
		AND(
			AND(
				EQ("status", "active"),
				AND(EQ("tenant_id", "t-1")),
			),
			AND(),
			OR(
				OR(EQ("country", "us")),
				EQ("country", "ca"),
			),
		),
	)

	q.Reduction = &Reduction{
		Group: []Expr{
			AND(AND(REF("country"))),
			OR(OR(LOWER("city"))),
		},
		Metrics: []Expr{
			NOT(AND(AND(EQ("archived", false)))),
			AND(EQ("count", 1)).AS("metric_count"),
			OR(OR(SUM("amount"))),
		},
		Having: OR(
			OR(GT(OUT("total_amount"), 100)),
			OR(OR(GT(OUT("rows"), 1))),
		),
	}
	q.Order = []Order{
		{By: OR(OR(REF("priority"))), Desc: true},
		{By: AND(AND(EQ("pinned", true))), Desc: false},
	}
	q.Window = Window{Offset: 50, Limit: 25}
	q.Projection = []Expr{
		OR(OR(OUT("total_amount"))).AS("project_total"),
	}

	return q
}
