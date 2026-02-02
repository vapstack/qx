package qx

import (
	"fmt"
	"strconv"
	"strings"
)

// Query creates a new QX and sets the provided expressions as part of the root AND group.
// With zero expressions it returns an empty query. With one expression it becomes the root.
func Query(expressions ...Expr) *QX {
	if len(expressions) == 0 {
		return new(QX)
	} else if len(expressions) == 1 {
		return &QX{Expr: expressions[0]}
	}
	return &QX{Expr: AND(expressions...)}
}

// EQ builds an equality expression: field == value.
func EQ(f string, v any) Expr { return Expr{Op: OpEQ, Field: f, Value: v} }

// NE builds a negated equality expression: field != value.
func NE(f string, v any) Expr { return Expr{Op: OpEQ, Not: true, Field: f, Value: v} }

// NOTEQ is an alias of NE and builds a negated equality expression: field != value.
func NOTEQ(f string, v any) Expr { return Expr{Op: OpEQ, Not: true, Field: f, Value: v} }

// GT builds a greater-than expression: field > value.
func GT(f string, v any) Expr { return Expr{Op: OpGT, Field: f, Value: v} }

// GTE builds a greater-than-or-equal expression: field >= value.
func GTE(f string, v any) Expr { return Expr{Op: OpGTE, Field: f, Value: v} }

// LT builds a less-than expression: field < value.
func LT(f string, v any) Expr { return Expr{Op: OpLT, Field: f, Value: v} }

// LTE builds a less-than-or-equal expression: field <= value.
func LTE(f string, v any) Expr { return Expr{Op: OpLTE, Field: f, Value: v} }

// IN builds a set-membership expression: field IN valueSlice.
// The provided value must be a slice; the field is compared against each element of the slice.
func IN(f string, v any) Expr { return Expr{Op: OpIN, Field: f, Value: v} }

// NOTIN builds a negated set-membership expression: field NOT IN valueSlice.
// The provided value must be a slice; the field is compared against each element of the slice.
func NOTIN(f string, v any) Expr { return Expr{Op: OpIN, Not: true, Field: f, Value: v} }

// HAS builds a slice containment expression for slice fields:
// the field slice must contain all elements from the provided value slice.
func HAS(f string, v any) Expr { return Expr{Op: OpHAS, Field: f, Value: v} }

// HASANY builds a slice intersection expression for slice fields:
// the field slice must share at least one element with the provided value slice.
func HASANY(f string, v any) Expr { return Expr{Op: OpHASANY, Field: f, Value: v} }

// HASNOT builds a negated slice containment expression for slice fields.
// It matches when the field slice does not contain all elements from the provided value slice
// (i.e., at least one of the provided values is missing).
// HASNOT is the logical equivalent of NOT(HAS(...)).
func HASNOT(f string, v any) Expr { return Expr{Op: OpHAS, Not: true, Field: f, Value: v} }

// HASNONE builds an expression that matches when the slice field contains none of the provided values.
// It evaluates to true only if the intersection between the field slice and the provided values is empty.
// HASNONE is the logical equivalent of NOT(HASANY(...)).
func HASNONE(f string, v any) Expr { return Expr{Op: OpHASANY, Not: true, Field: f, Value: v} }

// PREFIX builds an expression that matches when the string field starts with the provided prefix.
func PREFIX(f string, v any) Expr { return Expr{Op: OpPREFIX, Field: f, Value: v} }

// SUFFIX builds an expression that matches when the string field ends with the provided suffix.
func SUFFIX(f string, v any) Expr { return Expr{Op: OpSUFFIX, Field: f, Value: v} }

// CONTAINS builds an expression that matches when the string field contains the provided substring.
func CONTAINS(f string, v any) Expr { return Expr{Op: OpCONTAINS, Field: f, Value: v} }

// AND builds a conjunction expression combining all provided expressions.
func AND(exprs ...Expr) Expr { return Expr{Op: OpAND, Operands: exprs} }

// OR builds a disjunction expression combining all provided expressions.
func OR(exprs ...Expr) Expr { return Expr{Op: OpOR, Operands: exprs} }

// NOT negates the provided expression by setting Not flag.
func NOT(exp Expr) Expr {
	exp.Not = !exp.Not
	return exp
}

type (
	// QX represents a compiled query description.
	// It combines a filter expression, optional ordering rules, offset and limit.
	QX struct {
		Key    string // optional cache key
		Expr   Expr
		Order  []Order
		Offset uint64
		Limit  uint64
	}

	Op byte

	// Expr represents a single filter expression or a logical group of expressions.
	// Depending on Op, an Expr may represent:
	//   - a scalar comparison (EQ, GT, LT, etc.),
	//   - a slice operation (IN, HAS, HASANY),
	//   - or a logical operation (AND, OR) combining sub-expressions.
	//
	// For logical operations, Operands contains the nested expressions.
	// For non-logical operations, Field and Value describe the comparison.
	// If Not is set, the result of the expression is logically negated.
	Expr struct {
		Op       Op
		Not      bool
		Field    string
		Value    any
		Operands []Expr
	}

	// Order describes a single ordering rule applied to query results.
	// The meaning of Data depends on the ordering Type.
	// If Desc is true, the ordering is descending; otherwise ascending.
	Order struct {
		Field string
		Type  OrderType
		Data  any
		Desc  bool
	}

	OrderType      byte
	OrderDirection byte
)

const (
	ASC  OrderDirection = 0
	DESC OrderDirection = 1
)

const (
	OrderBasic OrderType = iota
	OrderByArrayPos
	OrderByArrayCount
)

// AND combines the current expression with exprs using logical AND.
func (qx *QX) AND(exprs ...Expr) *QX {
	if qx.Expr.Op == OpNOOP {
		qx.Expr = AND(exprs...)
	} else if qx.Expr.Op == OpAND {
		qx.Expr.Operands = append(qx.Expr.Operands, exprs...)
	} else {
		expr := Expr{Op: OpAND, Operands: make([]Expr, 0, len(exprs)+1)}
		expr.Operands = append(expr.Operands, qx.Expr)
		expr.Operands = append(expr.Operands, exprs...)
		qx.Expr = expr
	}
	return qx
}

// OR combines the current expression with exprs using logical OR.
func (qx *QX) OR(exprs ...Expr) *QX {
	if qx.Expr.Op == OpNOOP {
		qx.Expr = OR(exprs...)
	} else if qx.Expr.Op == OpOR {
		qx.Expr.Operands = append(qx.Expr.Operands, exprs...)
	} else {
		expr := Expr{Op: OpOR, Operands: make([]Expr, 0, len(exprs)+1)}
		expr.Operands = append(expr.Operands, qx.Expr)
		expr.Operands = append(expr.Operands, exprs...)
		qx.Expr = expr
	}
	return qx
}

// CacheKey sets an optional cache key associated with the query.
func (qx *QX) CacheKey(key string) *QX {
	qx.Key = key
	return qx
}

// By adds a basic sort order by field; dir may be ASC or DESC.
func (qx *QX) By(field string, dir OrderDirection) *QX {
	qx.Order = append(qx.Order, Order{
		Field: field,
		Type:  OrderBasic,
		Desc:  dir == DESC,
	})
	return qx
}

// ByArrayPos adds an ordering rule by the position of field's value within the provided slice; dir may be ASC or DESC.
func (qx *QX) ByArrayPos(field string, slice any, dir OrderDirection) *QX {
	qx.Order = append(qx.Order, Order{
		Field: field,
		Type:  OrderByArrayPos,
		Data:  slice,
		Desc:  dir == DESC,
	})
	return qx
}

// ByArrayCount adds an ordering rule by the number of items in a slice field; dir may be ASC or DESC.
func (qx *QX) ByArrayCount(field string, dir OrderDirection) *QX {
	qx.Order = append(qx.Order, Order{
		Field: field,
		Type:  OrderByArrayCount,
		Desc:  dir == DESC,
	})
	return qx
}

// Skip sets the query offset (number of items to skip). It panics if offset is negative.
func (qx *QX) Skip(offset int) *QX {
	if offset < 0 {
		panic("qx.Skip: negative value")
	}
	qx.Offset = uint64(offset)
	return qx
}

// Max sets the query limit (maximum number of items to return). It panics if limit is negative.
func (qx *QX) Max(limit int) *QX {
	if limit < 0 {
		panic("qx.Max: negative limit")
	}
	qx.Limit = uint64(limit)
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
	qx.Offset = uint64((pageNum - 1) * perPage)
	qx.Limit = uint64(perPage)
	return qx
}

// UsedFields returns a de-duplicated list of field names referenced by the query expression
// and sort orders. The returned slice includes fields from nested expressions.
func (qx *QX) UsedFields() []string {
	s := make([]string, 0, 8)

	usedFields(qx.Expr, &s)

	for _, o := range qx.Order {
		add := true
		for _, f := range s {
			if f == o.Field {
				add = false
				break
			}
		}
		if add {
			s = append(s, o.Field)
		}
	}
	return s
}

func usedFields(exp Expr, s *[]string) {
	if exp.Field != "" {
		add := true
		for _, f := range *s {
			if f == exp.Field {
				add = false
				break
			}
		}
		if add {
			*s = append(*s, exp.Field)
		}
	}
	for _, op := range exp.Operands {
		usedFields(op, s)
	}
}

// UsedFields returns a de-duplicated list of field names referenced by the expression.
// The returned slice includes fields from nested expressions.
func (expr Expr) UsedFields() []string {
	s := make([]string, 0, 8)
	usedFields(expr, &s)
	return s
}

const (
	OpNOOP Op = iota

	OpAND
	OpOR

	OpEQ
	OpGT
	OpGTE
	OpLT
	OpLTE
	OpIN

	OpHAS    // for array fields - contains at least all the provided values
	OpHASANY // for array fields - has intersection with the provided values

	OpPREFIX   // has prefix
	OpSUFFIX   // has suffix
	OpCONTAINS // contains substring
)

var QueryOpString = map[Op]string{
	OpNOOP:     "NOOP",
	OpAND:      "AND",
	OpOR:       "OR",
	OpEQ:       "EQ",
	OpGT:       "GT",
	OpGTE:      "GTE",
	OpLT:       "LT",
	OpLTE:      "LTE",
	OpIN:       "IN",
	OpHAS:      "HAS",
	OpHASANY:   "HASANY",
	OpPREFIX:   "PREFIX",
	OpSUFFIX:   "SUFFIX",
	OpCONTAINS: "CONTAINS",
}

var QueryOpValue = map[string]Op{
	"NOOP":     OpNOOP,
	"AND":      OpAND,
	"OR":       OpOR,
	"EQ":       OpEQ,
	"GT":       OpGT,
	"GTE":      OpGTE,
	"LT":       OpLT,
	"LTE":      OpLTE,
	"IN":       OpIN,
	"HAS":      OpHAS,
	"HASANY":   OpHASANY,
	"PREFIX":   OpPREFIX,
	"SUFFIX":   OpSUFFIX,
	"CONTAINS": OpCONTAINS,
}

func (q Op) String() string {
	if s, ok := QueryOpString[q]; ok {
		return s
	}
	return fmt.Sprintf("OP(%d)", byte(q))
}

func (q *Op) UnmarshalJSON(bytes []byte) error {
	str, err := strconv.Unquote(string(bytes))
	if err != nil {
		return err
	}
	op, ok := QueryOpValue[strings.ToUpper(str)]
	if !ok {
		return fmt.Errorf("unknown op: \"%v\"", str)
	}
	*q = op
	return nil
}

func (q Op) MarshalJSON() ([]byte, error) {
	v, ok := QueryOpString[q]
	if !ok {
		return nil, fmt.Errorf("unknown op value: %v", q)
	}
	return []byte(strconv.Quote(v)), nil
}
