package qx

import (
	"slices"
	"strings"
)

// Query creates a new QX and sets the provided expressions as part of the root AND group.
// With zero expressions it returns an empty query. With one expression it becomes the root.
//
// Query and Where functions are completely equivalent.
func Query(expressions ...Expr) *QX {
	return new(QX).Where(expressions...)
}

// Where creates a new QX and sets the provided expressions as part of the root AND group.
// With zero expressions it returns an empty query. With one expression it becomes the root.
//
// Where and Query functions are completely equivalent.
func Where(expressions ...Expr) *QX {
	return new(QX).Where(expressions...)
}

// KindNONE is the zero expression kind.
const KindNONE = ""

// Special forms
const (
	KindREF = "ref"
	KindOUT = "out"
	KindLIT = "lit"
	KindOP  = "op"
)

// Aggregate creates a new QX and appends the provided metrics to its reduction stage.
func Aggregate(metrics ...Expr) *QX { return new(QX).Metrics(metrics...) }

// Metrics creates a new QX and appends the provided metrics to its reduction stage.
func Metrics(metrics ...Expr) *QX { return new(QX).Metrics(metrics...) }

// Group creates a new QX and appends the provided document fields as grouping keys.
// Without metrics this acts as a DISTINCT-style grouping over the selected keys.
func Group(fields ...string) *QX { return new(QX).Group(fields...) }

// GroupBy creates a new QX and appends the provided grouping expressions.
// Without metrics this acts as a DISTINCT-style grouping over the selected keys.
func GroupBy(expressions ...Expr) *QX { return new(QX).GroupBy(expressions...) }

// Fields creates a new QX and appends the provided source fields or expressions
// to its final projection.
func Fields[T string | Expr](expressions ...T) *QX { return Select(expressions...) }

// FieldsOut creates a new QX and appends the provided reduction outputs to its final projection.
func FieldsOut(names ...string) *QX { return new(QX).SelectOut(names...) }

// Select creates a new QX and appends the provided source fields or expressions
// to its final projection.
func Select[T string | Expr](expressions ...T) *QX {
	qx := new(QX)
	if len(expressions) == 0 {
		return qx
	}
	qx.Projection = slices.Grow(qx.Projection, len(expressions))
	for _, expr := range expressions {
		qx.Projection = append(qx.Projection, exprArg(expr))
	}
	return qx
}

// SelectOut creates a new QX and appends the provided reduction outputs to its final projection.
func SelectOut(names ...string) *QX { return new(QX).SelectOut(names...) }

// REF builds a reference to a source document field.
func REF(name string) Expr { return Expr{Kind: KindREF, Name: name} }

// OUT builds a reference to an output name produced by grouping or aggregation.
func OUT(name string) Expr { return Expr{Kind: KindOUT, Name: name} }

// LIT builds a literal term.
func LIT(v any) Expr { return Expr{Kind: KindLIT, Value: v} }

// OP builds a generic function expression.
func OP[T string | Op](name T, args ...Expr) Expr {
	return Expr{
		Kind: KindOP,
		Name: strings.TrimSpace(string(name)),
		Args: args,
	}
}

type (
	// QX represents a query.
	// It combines a filter expression, an optional reduction stage,
	// optional ordering rules, an optional result window, and an optional
	// final projection stage.
	QX struct {
		Filter     Expr        `json:"filter,omitempty"`
		Reduction  *Reduction  `json:"reduction,omitempty"`
		Order      []Order     `json:"order,omitempty"`
		Window     Window      `json:"window,omitempty"`
		Projection []Expr      `json:"projection,omitempty"`
		Metadata   []MetaEntry `json:"meta,omitempty"`
	}

	// MetaEntry carries an opaque user-defined metadata value alongside a query.
	// qx preserves these entries during clone, validation, normalization, and JSON round-trips
	// but does not interpret their meaning.
	MetaEntry struct {
		Key   string `json:"key"`
		Value any    `json:"value,omitempty"`
	}

	// Op identifies an operation name in the query IR.
	Op string

	// Expr is a single query IR node.
	//
	// Expr may represent:
	//   - a reference (Kind: KindREF, KindOUT)
	//   - a literal (Kind: KIndLIT)
	//   - a named function-like expression (Kind: KindOP)
	//
	// Kind represents the node type: REF, OUT, LIT, OP.
	// REF and OUT nodes use Name. LIT nodes use Value.
	// OP expressions use Args to hold child expressions.
	Expr struct {
		Kind  string `json:"kind,omitempty"`
		Name  string `json:"name,omitempty"`
		Alias string `json:"alias,omitempty"`
		Value any    `json:"value,omitempty"`
		Args  []Expr `json:"args,omitempty"`
	}

	// Order describes a single ordering rule applied to query results.
	// If Desc is true, the ordering is descending; otherwise ascending.
	Order struct {
		By   Expr `json:"by,omitempty"`
		Desc bool `json:"desc,omitempty"`
	}

	// Window describes pagination bounds applied to query results.
	Window struct {
		Offset uint64 `json:"offset,omitempty"`
		Limit  uint64 `json:"limit,omitempty"`
	}

	// Reduction describes the optional reduce stage of the query.
	// Group defines grouping expressions, Metrics defines aggregate outputs,
	// and Having filters rows after the reduction has been applied.
	Reduction struct {
		Group   []Expr `json:"group,omitempty"`
		Metrics []Expr `json:"metrics,omitempty"`
		Having  Expr   `json:"having,omitempty"`
	}

	// OrderDirection specifies sort direction.
	OrderDirection byte
)

const (
	// ASC sorts in ascending order.
	ASC OrderDirection = 0
	// DESC sorts in descending order.
	DESC OrderDirection = 1
)

// IsEmpty reports whether reduction has no structural content.
// It is safe to call on a nil receiver.
func (reduction *Reduction) IsEmpty() bool {
	if reduction == nil {
		return true
	}
	return len(reduction.Group) == 0 && len(reduction.Metrics) == 0 && reduction.Having.IsZero()
}

// HasReduction reports whether query contains a non-empty reduction stage.
// It is safe to call on a nil receiver.
func (qx *QX) HasReduction() bool {
	return qx != nil && !qx.Reduction.IsEmpty()
}

// Where combines the current expression with expressions using logical AND.
func (qx *QX) Where(expressions ...Expr) *QX {
	if len(expressions) == 0 {
		return qx
	}
	if qx.Filter.Kind == KindOP && qx.Filter.Name == OpAND {
		qx.Filter.Args = append(qx.Filter.Args, expressions...)
		return qx
	}
	if qx.Filter.Kind == KindNONE {
		if len(expressions) == 1 {
			qx.Filter = expressions[0]
		} else {
			qx.Filter = AND(expressions...)
		}
		return qx
	}
	expr := AND()
	expr.Args = slices.Grow(expr.Args, len(expressions)+1)
	expr.Args = append(expr.Args, qx.Filter)
	expr.Args = append(expr.Args, expressions...)
	qx.Filter = expr
	return qx
}

// Sort adds a sort order by a source field.
// If dir is omitted, ascending order is used.
func (qx *QX) Sort(field string, dir ...OrderDirection) *QX {
	return qx.SortBy(REF(field), dir...)
}

// SortOut adds a sort order by a reduction output.
// If dir is omitted, ascending order is used.
func (qx *QX) SortOut(name string, dir ...OrderDirection) *QX {
	return qx.SortBy(OUT(name), dir...)
}

// SortBy adds a sort order by an expression.
// If dir is omitted, ascending order is used.
func (qx *QX) SortBy(expr Expr, dir ...OrderDirection) *QX {
	if expr.Kind == KindNONE {
		return qx
	}
	desc := false
	if len(dir) > 0 {
		desc = dir[0] == DESC
	}
	qx.Order = append(qx.Order, Order{
		By:   expr,
		Desc: desc,
	})
	return qx
}

// Aggregate appends metrics to the query reduction stage.
func (qx *QX) Aggregate(metrics ...Expr) *QX { return qx.Metrics(metrics...) }

// Metrics appends metrics to the query reduction stage.
func (qx *QX) Metrics(metrics ...Expr) *QX {
	if len(metrics) == 0 {
		return qx
	}
	if qx.Reduction == nil {
		qx.Reduction = new(Reduction)
	}
	qx.Reduction.Metrics = append(qx.Reduction.Metrics, metrics...)
	return qx
}

// Group appends document fields as grouping keys.
// Without metrics this acts as a DISTINCT-style grouping over the selected keys.
func (qx *QX) Group(fields ...string) *QX {
	if len(fields) == 0 {
		return qx
	}
	if qx.Reduction == nil {
		qx.Reduction = new(Reduction)
	}
	qx.Reduction.Group = slices.Grow(qx.Reduction.Group, len(fields))
	for _, field := range fields {
		qx.Reduction.Group = append(qx.Reduction.Group, REF(field))
	}
	return qx
}

// GroupBy appends grouping expressions.
// Without metrics this acts as a DISTINCT-style grouping over the selected keys.
func (qx *QX) GroupBy(expressions ...Expr) *QX {
	if len(expressions) == 0 {
		return qx
	}
	if qx.Reduction == nil {
		qx.Reduction = new(Reduction)
	}
	qx.Reduction.Group = append(qx.Reduction.Group, expressions...)
	return qx
}

// Having adds post-reduction predicates with AND semantics.
// Expressions in Having that refer to reduction outputs should use OUT(...).
func (qx *QX) Having(expressions ...Expr) *QX {
	if len(expressions) == 0 {
		return qx
	}
	if qx.Reduction == nil {
		qx.Reduction = new(Reduction)
	}
	if qx.Reduction.Having.Kind == KindOP && qx.Reduction.Having.Name == OpAND {
		qx.Reduction.Having.Args = append(qx.Reduction.Having.Args, expressions...)
		return qx
	}
	if qx.Reduction.Having.Kind == KindNONE {
		if len(expressions) == 1 {
			qx.Reduction.Having = expressions[0]
		} else {
			qx.Reduction.Having = AND(expressions...)
		}
		return qx
	}
	expr := AND()
	expr.Args = slices.Grow(expr.Args, len(expressions)+1)
	expr.Args = append(expr.Args, qx.Reduction.Having)
	expr.Args = append(expr.Args, expressions...)
	qx.Reduction.Having = expr
	return qx
}

// Fields appends source-field references to the final projection stage.
func (qx *QX) Fields(fields ...string) *QX {
	return qx.Select(fields...)
}

// Select appends source-field references to the final projection stage.
func (qx *QX) Select(fields ...string) *QX {
	if len(fields) == 0 {
		return qx
	}
	qx.Projection = slices.Grow(qx.Projection, len(fields))
	for _, field := range fields {
		qx.Projection = append(qx.Projection, REF(field))
	}
	return qx
}

// FieldsOut appends reduction-output references to the final projection stage.
func (qx *QX) FieldsOut(names ...string) *QX {
	return qx.SelectOut(names...)
}

// SelectOut appends reduction-output references to the final projection stage.
func (qx *QX) SelectOut(names ...string) *QX {
	if len(names) == 0 {
		return qx
	}
	qx.Projection = slices.Grow(qx.Projection, len(names))
	for _, name := range names {
		qx.Projection = append(qx.Projection, OUT(name))
	}
	return qx
}

// FieldsExpr appends expressions to the final projection stage.
func (qx *QX) FieldsExpr(expressions ...Expr) *QX {
	return qx.SelectExpr(expressions...)
}

// SelectExpr appends expressions to the final projection stage.
func (qx *QX) SelectExpr(expressions ...Expr) *QX {
	if len(expressions) == 0 {
		return qx
	}
	qx.Projection = append(qx.Projection, expressions...)
	return qx
}

// Offset sets the query offset (number of items to skip). It panics if offset is negative.
func (qx *QX) Offset(offset int) *QX {
	if offset < 0 {
		panic("qx.Offset: negative value")
	}
	qx.Window.Offset = uint64(offset)
	return qx
}

// Limit sets the query limit (maximum number of items to return). It panics if limit is negative.
func (qx *QX) Limit(limit int) *QX {
	if limit < 0 {
		panic("qx.Limit: negative limit")
	}
	qx.Window.Limit = uint64(limit)
	return qx
}

// Page sets the offset and limit based on 1-based pageNum and perPage.
// It panics if pageNum or perPage are not positive.
func (qx *QX) Page(pageNum, perPage int) *QX {
	if pageNum <= 0 {
		panic("qx.Page: pageNum must be positive")
	}
	if perPage <= 0 {
		panic("qx.Page: perPage must be positive")
	}
	qx.Window.Offset = uint64((pageNum - 1) * perPage)
	qx.Window.Limit = uint64(perPage)
	return qx
}

// UsedFields returns a de-duplicated list of source document fields referenced by the query.
func (qx *QX) UsedFields() []string {
	if qx == nil {
		return nil
	}
	s := appendUsedFields(qx.Filter, make([]string, 0, 8))

	if qx.HasReduction() {
		for _, key := range qx.Reduction.Group {
			s = appendUsedFields(key, s)
		}
		for _, metric := range qx.Reduction.Metrics {
			s = appendUsedFields(metric, s)
		}
		s = appendUsedFields(qx.Reduction.Having, s)
	}
	for _, o := range qx.Order {
		s = appendUsedFields(o.By, s)
	}
	for _, expr := range qx.Projection {
		s = appendUsedFields(expr, s)
	}
	return s
}

// Meta sets or replaces a metadata entry by key while preserving insertion order.
func (qx *QX) Meta(key string, value any) *QX {
	for i := range qx.Metadata {
		if qx.Metadata[i].Key == key {
			qx.Metadata[i].Value = value
			return qx
		}
	}

	qx.Metadata = append(qx.Metadata, MetaEntry{
		Key:   key,
		Value: value,
	})
	return qx
}

// MetaValue reports the metadata value associated with key.
func (qx *QX) MetaValue(key string) (any, bool) {
	if qx == nil {
		return nil, false
	}

	for i := range qx.Metadata {
		if qx.Metadata[i].Key == key {
			return qx.Metadata[i].Value, true
		}
	}

	return nil, false
}

// DeleteMeta removes metadata entries matching any of the provided keys.
func (qx *QX) DeleteMeta(keys ...string) *QX {
	if qx == nil || len(keys) == 0 || len(qx.Metadata) == 0 {
		return qx
	}

	qx.Metadata = slices.DeleteFunc(qx.Metadata, func(entry MetaEntry) bool {
		return slices.Contains(keys, entry.Key)
	})
	return qx
}

// AS sets the expression alias.
func (expr Expr) AS(alias string) Expr {
	expr.Alias = alias
	return expr
}

// OutputName reports the output name implied by the expression root.
// Aliased expressions use Alias. Bare REF and OUT expressions use Name.
func (expr Expr) OutputName() (string, bool) {
	if alias := strings.TrimSpace(expr.Alias); alias != "" {
		return alias, true
	}
	switch expr.Kind {
	case KindREF, KindOUT:
		if name := strings.TrimSpace(expr.Name); name != "" {
			return name, true
		}
	}
	return "", false
}

// IsZero reports whether expression is empty.
func (expr Expr) IsZero() bool {
	return expr.Kind == KindNONE && expr.Name == "" && expr.Alias == "" && expr.Value == nil && len(expr.Args) == 0
}

// Is reports whether expression has the provided kind and operation name.
func (expr Expr) Is(kind string, op string) bool { return expr.Kind == kind && expr.Name == op }

func appendUsedFields(exp Expr, s []string) []string {
	if exp.Kind == KindREF && exp.Name != "" {
		s = appendUnique(s, exp.Name)
	}
	for _, op := range exp.Args {
		s = appendUsedFields(op, s)
	}
	return s
}

func appendUnique(dst []string, field string) []string {
	for _, f := range dst {
		if f == field {
			return dst
		}
	}
	return append(dst, field)
}

// UsedFields returns a de-duplicated list of field names referenced by the expression.
// The returned slice includes fields from nested expressions.
func (expr Expr) UsedFields() []string {
	return appendUsedFields(expr, make([]string, 0, 8))
}

func exprArg[T string | Expr](v T) Expr {
	switch x := any(v).(type) {
	case Expr:
		return x
	case string:
		return REF(x)
	default:
		panic("expected field path or Expr")
	}
}

func valueArg(v any) Expr {
	switch x := v.(type) {
	case Expr:
		return x
	default:
		return LIT(v)
	}
}
