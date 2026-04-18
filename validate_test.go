package qx

import (
	"errors"
	"testing"
)

func TestValidateNil(t *testing.T) {
	if err := Validate(nil); err != nil {
		t.Fatalf("Validate(nil) error = %v, want nil", err)
	}

	var q *QX
	if err := q.Validate(); err != nil {
		t.Fatalf("(*QX)(nil).Validate() error = %v, want nil", err)
	}
}

func TestValidateAcceptsValidQueries(t *testing.T) {
	tests := []struct {
		name string
		qx   *QX
	}{
		{
			name: "empty query",
			qx:   &QX{},
		},
		{
			name: "query with metadata",
			qx: Query(
				EQ("status", "active"),
			).
				Meta("trace.id", "req-42").
				Meta("transport", map[string]any{"source": "rpc"}),
		},
		{
			name: "filter only query",
			qx: Query(
				AND(
					EQ("status", "active"),
					EQ(LOWER("email"), "admin@example.com"),
					LIKE("email", "%@example.com"),
					NOTILIKE("role", "ops%"),
					MATCHES("email", "adm.*@example\\.com"),
					NOT(EXISTS("deleted_at")),
				),
			),
		},
		{
			name: "reduction query",
			qx: Query(EQ("status", "active")).
				GroupBy(LOWER("country").AS("country_lower")).
				Metrics(SUM("amount").AS("total_amount"), ROWCOUNT().AS("rows")).
				Having(
					GT(OUT("total_amount"), 100),
					LT("created_at", "2026-01-01"),
				).
				SortOut("total_amount", DESC),
		},
		{
			name: "pre reduction projection query",
			qx: Query(
				EQ("status", "active"),
			).
				Select("id", "email").
				SelectExpr(
					LOWER("display_name").AS("display_name_lower"),
					LIT(true).AS("is_selected"),
				).
				Sort("created_at", DESC),
		},
		{
			name: "post reduction projection query",
			qx: Query(EQ("status", "active")).
				Group("country").
				Metrics(SUM("amount").AS("total_amount"), ROWCOUNT().AS("rows")).
				Having(
					GT(OUT("total_amount"), 100),
				).
				SortOut("total_amount", DESC).
				SelectOut("country", "total_amount").
				SelectExpr(
					LOWER(OUT("country")).AS("country_lower"),
					IF(GT(OUT("rows"), 0), OUT("rows"), LIT(0)).AS("rows_nonzero"),
				),
		},
		{
			name: "query using additional scalar helpers",
			qx: Query(
				EQ(
					CONCAT(LOWER("first_name"), LIT(" "), LOWER("last_name")),
					"john doe",
				),
				ISNULL(NULLIF(TRIM("nickname"), "")),
			).
				GroupBy(NULLIF(LOWER("country"), "").AS("country_key")).
				Metrics(
					GREATEST(SUM("amount"), AVG("amount")).AS("peak_amount"),
					LEAST(SUM("amount"), COUNT("amount")).AS("floor_amount"),
					IF(
						GT(SUM("amount"), 1000),
						SUM("amount"),
						COUNT("amount"),
					).AS("adaptive_amount"),
				).
				Having(
					GT(OUT("peak_amount"), 10),
					GT(
						IF(EQ(OUT("adaptive_amount"), 0), LIT(0), OUT("adaptive_amount")),
						0,
					),
				).
				SortBy(
					DATETRUNC("created_at", "day"),
				),
		},
		{
			name: "query using temporal helpers",
			qx: Query(
				GTE("created_at", DATEADD(NOW(), -30, "day")),
				LT("created_at", NOW()),
			).
				GroupBy(
					DATETRUNC("created_at", "day").AS("created_day"),
					EXTRACT("created_at", "hour").AS("created_hour"),
				).
				Metrics(
					MAX(DATEADD("created_at", 7, "day")).AS("created_plus_week"),
					MIN(DATEDIFF("created_at", NOW(), "day")).AS("age_days"),
				).
				Having(
					GTE(OUT("age_days"), 0),
				).
				SortBy(EXTRACT("created_at", "dow")),
		},
		{
			name: "custom predicate op in filter",
			qx:   Query(OP("glob", LOWER("name"), LIT("adm*"))),
		},
		{
			name: "custom metric op",
			qx:   new(QX).Metrics(OP("custom_metric", REF("amount"))),
		},
		{
			name: "unknown op in group without aggregates",
			qx:   new(QX).GroupBy(OP("bucket", LOWER("country"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.qx.Validate(); err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateTreatsEmptyReductionAsAbsent(t *testing.T) {
	q := &QX{
		Reduction:  &Reduction{},
		Order:      []Order{{By: REF("created_at")}},
		Projection: []Expr{REF("id")},
	}

	if err := q.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestValidateRejectsInvalidQueries(t *testing.T) {
	tests := []struct {
		name string
		qx   *QX
		code string
		path string
	}{
		{
			name: "invalid kind",
			qx:   Query(Expr{Kind: "boom"}),
			code: "invalid_kind",
			path: "filter",
		},
		{
			name: "blank ref name",
			qx:   Query(Expr{Kind: KindREF}),
			code: "invalid_name",
			path: "filter",
		},
		{
			name: "whitespace alias",
			qx:   Query(EQ("status", "active").AS("   ")),
			code: "invalid_alias",
			path: "filter",
		},
		{
			name: "blank meta key",
			qx: &QX{
				Metadata: []MetaEntry{{Key: ""}},
			},
			code: "invalid_meta_key",
			path: "meta[0].key",
		},
		{
			name: "meta key with surrounding whitespace",
			qx: &QX{
				Metadata: []MetaEntry{{Key: " trace.id "}},
			},
			code: "invalid_meta_key",
			path: "meta[0].key",
		},
		{
			name: "duplicate meta key",
			qx: &QX{
				Metadata: []MetaEntry{
					{Key: "trace.id", Value: "req-1"},
					{Key: "trace.id", Value: "req-2"},
				},
			},
			code: "duplicate_meta_key",
			path: "meta[1].key",
		},
		{
			name: "alias with surrounding whitespace",
			qx:   Query(EQ("status", "active").AS(" total ")),
			code: "invalid_alias",
			path: "filter",
		},
		{
			name: "ref with args",
			qx:   Query(Expr{Kind: KindREF, Name: "status", Args: []Expr{LIT("x")}}),
			code: "unexpected_args",
			path: "filter",
		},
		{
			name: "out with value",
			qx:   Query(Expr{Kind: KindOUT, Name: "total", Value: 1}),
			code: "unexpected_value",
			path: "filter",
		},
		{
			name: "literal with name",
			qx:   Query(Expr{Kind: KindLIT, Name: "x", Value: 1}),
			code: "invalid_name",
			path: "filter",
		},
		{
			name: "op with value",
			qx: Query(Expr{
				Kind:  KindOP,
				Name:  OpEQ,
				Value: true,
				Args:  []Expr{REF("status"), LIT("active")},
			}),
			code: "unexpected_value",
			path: "filter",
		},
		{
			name: "untrimmed op name",
			qx: Query(Expr{
				Kind: KindOP,
				Name: " eq ",
				Args: []Expr{REF("status"), LIT("active")},
			}),
			code: "invalid_name",
			path: "filter",
		},
		{
			name: "invalid arity not",
			qx:   Query(Expr{Kind: KindOP, Name: OpNOT}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity eq",
			qx:   Query(Expr{Kind: KindOP, Name: OpEQ, Args: []Expr{REF("status")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity trim",
			qx: Query(Expr{
				Kind: KindOP,
				Name: OpTRIM,
				Args: []Expr{REF("name"), LIT("x"), LIT("y")},
			}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity substr",
			qx:   Query(Expr{Kind: KindOP, Name: OpSUBSTR, Args: []Expr{REF("name")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity replace",
			qx: Query(Expr{
				Kind: KindOP,
				Name: OpREPLACE,
				Args: []Expr{REF("name"), LIT("a")},
			}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity count",
			qx: Query(Expr{
				Kind: KindOP,
				Name: OpCOUNT,
				Args: []Expr{REF("a"), REF("b")},
			}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity rank",
			qx:   Query(Expr{Kind: KindOP, Name: OpRANK}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity coalesce",
			qx:   Query(Expr{Kind: KindOP, Name: OpCOALESCE}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity add",
			qx:   Query(Expr{Kind: KindOP, Name: OpADD}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity sub",
			qx:   Query(Expr{Kind: KindOP, Name: OpSUB}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity mul",
			qx:   Query(Expr{Kind: KindOP, Name: OpMUL}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity div",
			qx:   Query(Expr{Kind: KindOP, Name: OpDIV}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity mod",
			qx:   Query(Expr{Kind: KindOP, Name: OpMOD}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity nullif",
			qx:   Query(Expr{Kind: KindOP, Name: OpNULLIF, Args: []Expr{REF("nickname")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity if",
			qx: Query(Expr{
				Kind: KindOP,
				Name: OpIF,
				Args: []Expr{EQ("status", "active"), LIT(1)},
			}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity concat",
			qx:   Query(Expr{Kind: KindOP, Name: OpCONCAT}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity like",
			qx:   Query(Expr{Kind: KindOP, Name: OpLIKE, Args: []Expr{REF("email")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity ilike",
			qx:   Query(Expr{Kind: KindOP, Name: OpILIKE, Args: []Expr{REF("email")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity matches",
			qx:   Query(Expr{Kind: KindOP, Name: OpMATCHES, Args: []Expr{REF("email")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity date_trunc",
			qx:   Query(Expr{Kind: KindOP, Name: OpDATETRUNC, Args: []Expr{LIT("day")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity extract",
			qx:   Query(Expr{Kind: KindOP, Name: OpEXTRACT, Args: []Expr{LIT("day")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity now",
			qx:   Query(Expr{Kind: KindOP, Name: OpNOW, Args: []Expr{REF("created_at")}}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity dateadd",
			qx: Query(Expr{
				Kind: KindOP,
				Name: OpDATEADD,
				Args: []Expr{LIT("day"), LIT(1)},
			}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity datediff",
			qx: Query(Expr{
				Kind: KindOP,
				Name: OpDATEDIFF,
				Args: []Expr{LIT("day"), REF("created_at")},
			}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity greatest",
			qx:   Query(Expr{Kind: KindOP, Name: OpGREATEST}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "invalid arity least",
			qx:   Query(Expr{Kind: KindOP, Name: OpLEAST}),
			code: "invalid_arity",
			path: "filter",
		},
		{
			name: "zero expr in args",
			qx:   Query(AND(Expr{})),
			code: "empty_expr",
			path: "filter.args[0]",
		},
		{
			name: "zero expr in nested args",
			qx:   Query(AND(NOT(Expr{}))),
			code: "empty_expr",
			path: "filter.args[0].args[0]",
		},
		{
			name: "zero expr in group",
			qx:   new(QX).GroupBy(Expr{}),
			code: "empty_expr",
			path: "reduction.group[0]",
		},
		{
			name: "zero expr in metric",
			qx:   new(QX).Metrics(Expr{}),
			code: "empty_expr",
			path: "reduction.metrics[0]",
		},
		{
			name: "zero expr in order",
			qx:   &QX{Order: []Order{{By: Expr{}}}},
			code: "empty_expr",
			path: "order[0].by",
		},
		{
			name: "zero expr in projection",
			qx:   &QX{Projection: []Expr{{}}},
			code: "empty_expr",
			path: "projection[0]",
		},
		{
			name: "filter requires predicate root",
			qx:   Query(LOWER("name")),
			code: "predicate_required",
			path: "filter",
		},
		{
			name: "having requires predicate root",
			qx:   new(QX).Having(SUM("amount")),
			code: "predicate_required",
			path: "reduction.having",
		},
		{
			name: "aggregate in filter",
			qx:   Query(EQ(SUM("amount"), 1)),
			code: "aggregate_not_allowed",
			path: "filter.args[0]",
		},
		{
			name: "aggregate in group",
			qx:   new(QX).GroupBy(SUM("amount")),
			code: "aggregate_not_allowed",
			path: "reduction.group[0]",
		},
		{
			name: "aggregate in pre reduction order",
			qx:   &QX{Order: []Order{{By: SUM("amount")}}},
			code: "aggregate_not_allowed",
			path: "order[0].by",
		},
		{
			name: "out in filter",
			qx:   Query(EQ(OUT("total"), 1)),
			code: "out_not_allowed",
			path: "filter.args[0]",
		},
		{
			name: "out in group",
			qx:   new(QX).GroupBy(OUT("total")),
			code: "out_not_allowed",
			path: "reduction.group[0]",
		},
		{
			name: "out in metrics",
			qx:   new(QX).Metrics(OUT("total")),
			code: "out_not_allowed",
			path: "reduction.metrics[0]",
		},
		{
			name: "out in order without reduction",
			qx:   &QX{Order: []Order{{By: OUT("total")}}},
			code: "out_not_allowed",
			path: "order[0].by",
		},
		{
			name: "out in pre reduction projection",
			qx:   new(QX).SelectOut("total"),
			code: "out_not_allowed",
			path: "projection[0]",
		},
		{
			name: "scalar only metric",
			qx:   new(QX).Metrics(LOWER("country")),
			code: "aggregate_required",
			path: "reduction.metrics[0]",
		},
		{
			name: "if condition must be predicate",
			qx: Query(
				EQ(
					IF(LOWER("status"), LIT("active"), LIT("inactive")),
					"active",
				),
			),
			code: "predicate_required",
			path: "filter.args[0].args[0]",
		},
		{
			name: "unknown group op containing aggregate",
			qx:   new(QX).GroupBy(OP("custom_group", SUM("amount"))),
			code: "aggregate_not_allowed",
			path: "reduction.group[0].args[0]",
		},
		{
			name: "projection expr requires output name",
			qx:   new(QX).SelectExpr(LOWER("email")),
			code: "output_name_required",
			path: "projection[0]",
		},
		{
			name: "ref in post reduction projection",
			qx: Query(EQ("status", "active")).
				Group("country").
				Metrics(SUM("amount").AS("total_amount")).
				Select("country"),
			code: "ref_not_allowed",
			path: "projection[0]",
		},
		{
			name: "aggregate in pre reduction projection",
			qx:   new(QX).SelectExpr(SUM("amount").AS("total_amount")),
			code: "aggregate_not_allowed",
			path: "projection[0]",
		},
		{
			name: "aggregate in post reduction projection",
			qx: Query(EQ("status", "active")).
				Group("country").
				Metrics(SUM("amount").AS("total_amount")).
				SelectExpr(SUM(OUT("total_amount")).AS("total_sum")),
			code: "aggregate_not_allowed",
			path: "projection[0]",
		},
		{
			name: "unknown output in having",
			qx: Query(EQ("status", "active")).
				Group("country").
				Metrics(SUM("amount").AS("total_amount")).
				Having(GT(OUT("missing"), 1)),
			code: "unknown_output",
			path: "reduction.having.args[0]",
		},
		{
			name: "unknown output in order",
			qx: Query(EQ("status", "active")).
				Group("country").
				Metrics(SUM("amount").AS("total_amount")).
				SortOut("missing"),
			code: "unknown_output",
			path: "order[0].by",
		},
		{
			name: "unknown output in projection",
			qx: Query(EQ("status", "active")).
				Group("country").
				Metrics(SUM("amount").AS("total_amount")).
				SelectOut("missing"),
			code: "unknown_output",
			path: "projection[0]",
		},
		{
			name: "duplicate reduction output name",
			qx: Query(EQ("status", "active")).
				Group("country").
				Metrics(SUM("amount").AS("country")),
			code: "duplicate_output_name",
			path: "reduction.metrics[0]",
		},
		{
			name: "duplicate projection output name",
			qx: new(QX).
				Select("email").
				SelectExpr(LOWER("name").AS("email")),
			code: "duplicate_output_name",
			path: "projection[1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidationError(t, tt.qx.Validate(), tt.code, tt.path)
		})
	}
}

func assertValidationError(t *testing.T, err error, code string, path string) {
	t.Helper()

	if err == nil {
		t.Fatalf("Validate() error = nil, want %s at %s", code, path)
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("Validate() error = %T %v, want *ValidationError", err, err)
	}
	if validationErr.Code != code {
		t.Fatalf("ValidationError.Code = %q, want %q", validationErr.Code, code)
	}
	if validationErr.Path != path {
		t.Fatalf("ValidationError.Path = %q, want %q", validationErr.Path, path)
	}
	if validationErr.Message == "" {
		t.Fatal("ValidationError.Message must not be empty")
	}
}

var validateBenchSink error

func BenchmarkValidateFilterOnly(b *testing.B) {
	q := benchmarkValidateFilterQuery()

	b.ReportAllocs()
	b.ResetTimer()

	var err error
	for i := 0; i < b.N; i++ {
		err = q.Validate()
	}

	validateBenchSink = err
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkValidateReductionQuery(b *testing.B) {
	q := benchmarkValidateReductionQuery()

	b.ReportAllocs()
	b.ResetTimer()

	var err error
	for i := 0; i < b.N; i++ {
		err = q.Validate()
	}

	validateBenchSink = err
	if err != nil {
		b.Fatal(err)
	}
}

func benchmarkValidateFilterQuery() *QX {
	return Query(
		AND(
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
		),
	)
}

func benchmarkValidateReductionQuery() *QX {
	return Query(
		AND(
			EQ("status", "active"),
			EQ("tenant_id", "t-1"),
			NOT(EXISTS("deleted_at")),
		),
	).
		GroupBy(
			LOWER("country").AS("country_lower"),
			OP("bucket", LOWER("city")).AS("city_bucket"),
		).
		Metrics(
			SUM("amount").AS("total_amount"),
			AVG("amount").AS("avg_amount"),
			ROWCOUNT().AS("rows"),
			OP("custom_metric", DIV(SUM("amount"), COUNT("amount"))).AS("weighted"),
		).
		Having(
			AND(
				GT(OUT("total_amount"), 100),
				GT(OUT("rows"), 5),
				LT(OUT("avg_amount"), 500),
			),
		).
		SortOut("total_amount", DESC).
		SortOut("avg_amount").
		SortBy(OUT("rows"), DESC)
}
