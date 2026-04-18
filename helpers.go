package qx

const (
	OpNONE = ""
	OpNOOP = ""
)

/**/

const OpEQ = "eq"

// EQ builds an equality expression: left == right.
func EQ[L string | Expr](left L, right any) Expr {
	return OP(OpEQ, exprArg(left), valueArg(right))
}

/**/

const OpNE = "ne"

// NE builds an inequality expression: left != right.
func NE[L string | Expr](left L, right any) Expr {
	return OP(OpNE, exprArg(left), valueArg(right))
}

// NOTEQ builds an inequality expression: left != right. NOTEQ is an alias of NE.
func NOTEQ[L string | Expr](left L, right any) Expr {
	return OP(OpNE, exprArg(left), valueArg(right))
}

/**/

const OpGT = "gt"

// GT builds a greater-than expression: left > right.
func GT[L string | Expr](left L, right any) Expr {
	return OP(OpGT, exprArg(left), valueArg(right))
}

/**/

const OpGTE = "gte"

// GTE builds a greater-than-or-equal expression: left >= right.
func GTE[L string | Expr](left L, right any) Expr {
	return OP(OpGTE, exprArg(left), valueArg(right))
}

/**/

const OpLT = "lt"

// LT builds a less-than expression: left < right.
func LT[L string | Expr](left L, right any) Expr {
	return OP(OpLT, exprArg(left), valueArg(right))
}

/**/

const OpLTE = "lte"

// LTE builds a less-than-or-equal expression: left <= right.
func LTE[L string | Expr](left L, right any) Expr {
	return OP(OpLTE, exprArg(left), valueArg(right))
}

// BETWEEN builds a range expression equivalent to AND(GTE(expr, low), LTE(expr, high)).
func BETWEEN[L string | Expr](expr L, low, high any) Expr {
	return AND(GTE(expr, low), LTE(expr, high))
}

/**/

const OpIN = "in"

// IN builds a set-membership expression: left IN right slice.
// The right side must be a slice; the left value is compared against its elements.
func IN[L string | Expr, T any](left L, right []T) Expr { return OP(OpIN, exprArg(left), LIT(right)) }

// NOTIN builds a negated set-membership expression: left NOT IN right slice.
// NOTIN is the logical equivalent of NOT(IN(...)).
func NOTIN[L string | Expr, T any](left L, right []T) Expr { return NOT(IN(left, right)) }

/**/

const OpHASALL = "has_all"

// HASALL builds a slice containment expression.
// It matches when the left slice contains all values from the right slice.
func HASALL[L string | Expr, T any](left L, right []T) Expr {
	return OP(OpHASALL, exprArg(left), LIT(right))
}

/**/

const OpHASANY = "has_any"

// HASANY builds a slice intersection expression.
// It matches when the left slice shares at least one value with the right slice.
func HASANY[L string | Expr, T any](left L, right []T) Expr {
	return OP(OpHASANY, exprArg(left), LIT(right))
}

// HASNONE builds an expression that matches when the left slice contains none of the provided values.
// It evaluates to true only if the intersection between left and right slices is empty.
// HASNONE is the logical equivalent of NOT(HASANY(...)).
func HASNONE[L string | Expr, T any](left L, right []T) Expr { return NOT(HASANY(left, right)) }

/**/

const OpEXISTS = "exists"

// EXISTS builds an existence expression: expr is present.
func EXISTS[T string | Expr](expr T) Expr { return OP(OpEXISTS, exprArg(expr)) }

// MISSING builds an absence expression: expr is not present.
func MISSING[T string | Expr](expr T) Expr { return NOT(EXISTS(expr)) }

/**/

const OpISNULL = "is_null"

// ISNULL builds a null-check expression for expr.
func ISNULL[T string | Expr](expr T) Expr { return OP(OpISNULL, exprArg(expr)) }

// NOTNULL builds a negated null-check expression for expr.
// NOTNULL is the logical equivalent of NOT(ISNULL(...)).
func NOTNULL[T string | Expr](expr T) Expr { return NOT(ISNULL(expr)) }

/**/

const OpPREFIX = "prefix"

// PREFIX builds an expression that matches when the left string starts with right.
func PREFIX[L string | Expr](left L, right any) Expr {
	return OP(OpPREFIX, exprArg(left), valueArg(right))
}

/**/

const OpSUFFIX = "suffix"

// SUFFIX builds an expression that matches when the left string ends with right.
func SUFFIX[L string | Expr](left L, right any) Expr {
	return OP(OpSUFFIX, exprArg(left), valueArg(right))
}

/**/

const OpCONTAINS = "contains"

// CONTAINS builds an expression that matches when the left string contains right.
func CONTAINS[L string | Expr](left L, right any) Expr {
	return OP(OpCONTAINS, exprArg(left), valueArg(right))
}

/**/

const OpLIKE = "like"

// LIKE builds a pattern-match expression.
// The exact pattern syntax is backend-defined.
func LIKE[L string | Expr](left L, right any) Expr {
	return OP(OpLIKE, exprArg(left), valueArg(right))
}

// NOTLIKE builds a negated pattern-match expression.
// NOTLIKE is the logical equivalent of NOT(LIKE(...)).
func NOTLIKE[L string | Expr](left L, right any) Expr { return NOT(LIKE(left, right)) }

/**/

const OpILIKE = "ilike"

// ILIKE builds a case-insensitive pattern-match expression.
// The exact pattern syntax and case-folding behavior are backend-defined.
func ILIKE[L string | Expr](left L, right any) Expr {
	return OP(OpILIKE, exprArg(left), valueArg(right))
}

// NOTILIKE builds a negated case-insensitive pattern-match expression.
// NOTILIKE is the logical equivalent of NOT(ILIKE(...)).
func NOTILIKE[L string | Expr](left L, right any) Expr { return NOT(ILIKE(left, right)) }

/**/

const OpMATCHES = "matches"

// MATCHES builds a regex-like pattern-match expression.
// The exact pattern syntax and matching behavior are backend-defined.
func MATCHES[L string | Expr](left L, right any) Expr {
	return OP(OpMATCHES, exprArg(left), valueArg(right))
}

// NOTMATCHES builds a negated regex-like pattern-match expression.
// NOTMATCHES is the logical equivalent of NOT(MATCHES(...)).
func NOTMATCHES[L string | Expr](left L, right any) Expr { return NOT(MATCHES(left, right)) }

/**/

const OpAND = "and"

// AND builds a conjunction expression combining all provided expressions.
func AND(expressions ...Expr) Expr { return OP(OpAND, expressions...) }

/**/

const OpOR = "or"

// OR builds a disjunction expression combining all provided expressions.
func OR(expressions ...Expr) Expr { return OP(OpOR, expressions...) }

/**/

const OpNOT = "not"

// NOT builds a unary negation expression.
func NOT(p Expr) Expr { return OP(OpNOT, p) }

/**/

const OpLEN = "len"

// LEN builds a length expression for expr.
func LEN[T string | Expr](expr T) Expr { return OP(OpLEN, exprArg(expr)) }

/**/

const OpPOS = "pos"

// POS builds a position expression.
// POS(needle, haystack) returns the position of needle in haystack.
// POS(substring, string) returns the position of substring in string.
// POS(ref, collection) returns the position of field value in collection.
//
// If the first argument is string it is treated as REF.
// To pass a literal string as first argument, use LIT.
func POS[T string | Expr](needleExpr T, haystack any) Expr {
	return OP(OpPOS, exprArg(needleExpr), valueArg(haystack))
}

/**/

const OpLOWER = "lower"

// LOWER builds a lowercase string expression for expr.
func LOWER[T string | Expr](expr T) Expr { return OP(OpLOWER, exprArg(expr)) }

/**/

const OpUPPER = "upper"

// UPPER builds an uppercase string expression for expr.
func UPPER[T string | Expr](expr T) Expr { return OP(OpUPPER, exprArg(expr)) }

/**/

const OpTRIM = "trim"

// TRIM builds a string trim expression.
// Without cutoff it trims surrounding whitespace; with cutoff it trims that value instead.
func TRIM[T string | Expr](expr T, cutoff ...any) Expr {
	switch len(cutoff) {
	case 0:
		return OP(OpTRIM, exprArg(expr))
	case 1:
		return OP(OpTRIM, exprArg(expr), valueArg(cutoff[0]))
	default:
		panic("qx.TRIM: expected expr and optional cutoff")
	}
}

/**/

const OpREPLACE = "replace"

// REPLACE builds a string replacement expression.
// It replaces every occurrence of old with new in expr.
func REPLACE[T string | Expr](expr T, old, new any) Expr {
	return OP(OpREPLACE, exprArg(expr), valueArg(old), valueArg(new))
}

/**/

const OpSPLIT = "split"

// SPLIT builds a string split expression.
// It uses sep as the delimiter.
func SPLIT[T string | Expr](expr T, sep any) Expr {
	return OP(OpSPLIT, exprArg(expr), valueArg(sep))
}

/**/

const OpSUBSTR = "substr"

// SUBSTR builds a substring expression.
// It extracts expr starting at start, with optional length.
func SUBSTR[T string | Expr](expr T, start any, length ...any) Expr {
	switch len(length) {
	case 0:
		return OP(OpSUBSTR, exprArg(expr), valueArg(start))
	case 1:
		return OP(OpSUBSTR, exprArg(expr), valueArg(start), valueArg(length[0]))
	default:
		panic("qx.SUBSTR: expected expr, start and optional length")
	}
}

/**/

const OpDATETRUNC = "date_trunc"

// DATETRUNC builds a temporal truncation expression.
// The accepted part values and truncation semantics are backend-defined.
func DATETRUNC[T string | Expr](expr T, part any) Expr {
	return OP(OpDATETRUNC, exprArg(expr), valueArg(part))
}

/**/

const OpEXTRACT = "extract"

// EXTRACT builds a temporal field-extraction expression.
// The accepted part values and extraction semantics are backend-defined.
func EXTRACT[T string | Expr](expr T, part any) Expr {
	return OP(OpEXTRACT, exprArg(expr), valueArg(part))
}

/**/

const OpNOW = "now"

// NOW builds a current-timestamp expression.
// The exact timestamp source and precision are backend-defined.
func NOW() Expr { return OP(OpNOW) }

/**/

const OpDATEADD = "date_add"

// DATEADD builds a temporal offset expression.
// The accepted part values and arithmetic semantics are backend-defined.
func DATEADD[T string | Expr](expr T, amount any, part any) Expr {
	return OP(OpDATEADD, exprArg(expr), valueArg(amount), valueArg(part))
}

/**/

const OpDATEDIFF = "date_diff"

// DATEDIFF builds a temporal difference expression.
// The accepted part values and difference semantics are backend-defined.
func DATEDIFF[L string | Expr](left L, right any, part any) Expr {
	return OP(OpDATEDIFF, exprArg(left), valueArg(right), valueArg(part))
}

/**/

const OpABS = "abs"

// ABS builds an absolute-value expression for expr.
func ABS[T string | Expr](expr T) Expr {
	return OP(OpABS, exprArg(expr))
}

/**/

const OpROUND = "round"

// ROUND builds a rounding expression for expr.
// With precision, it rounds to the specified scale.
func ROUND[T string | Expr](expr T, precision ...any) Expr {
	switch len(precision) {
	case 0:
		return OP(OpROUND, exprArg(expr))
	case 1:
		return OP(OpROUND, exprArg(expr), valueArg(precision[0]))
	default:
		panic("qx.ROUND: expected expr and optional precision")
	}
}

/**/

const OpFLOOR = "floor"

// FLOOR builds a floor expression for expr.
func FLOOR[T string | Expr](expr T) Expr {
	return OP(OpFLOOR, exprArg(expr))
}

/**/

const OpCEIL = "ceil"

// CEIL builds a ceiling expression for expr.
func CEIL[T string | Expr](expr T) Expr {
	return OP(OpCEIL, exprArg(expr))
}

/**/

const OpCOALESCE = "coalesce"

// COALESCE builds a fallback expression.
// It returns the first non-null argument.
func COALESCE(expressions ...Expr) Expr {
	return OP(OpCOALESCE, expressions...)
}

/**/

const OpNULLIF = "nullif"

// NULLIF builds a null-on-equality expression.
// It returns null when left equals right; otherwise it returns left.
func NULLIF[L string | Expr](left L, right any) Expr {
	return OP(OpNULLIF, exprArg(left), valueArg(right))
}

/**/

const OpIF = "if"

// IF builds a conditional expression.
// It returns whenTrue when condition is true; otherwise it returns whenFalse.
// Non-Expr branch values are treated as literals.
func IF(condition Expr, whenTrue any, whenFalse any) Expr {
	return OP(OpIF, condition, valueArg(whenTrue), valueArg(whenFalse))
}

/**/

const OpCONCAT = "concat"

// CONCAT builds a string concatenation expression.
func CONCAT(expressions ...Expr) Expr {
	return OP(OpCONCAT, expressions...)
}

/**/

const OpADD = "add"

// ADD builds an arithmetic sum expression.
func ADD(expressions ...Expr) Expr {
	return OP(OpADD, expressions...)
}

/**/

const OpSUB = "sub"

// SUB builds an arithmetic subtraction expression.
func SUB(expressions ...Expr) Expr {
	return OP(OpSUB, expressions...)
}

/**/

const OpMUL = "mul"

// MUL builds an arithmetic multiplication expression.
func MUL(expressions ...Expr) Expr {
	return OP(OpMUL, expressions...)
}

/**/

const OpDIV = "div"

// DIV builds an arithmetic division expression.
func DIV(expressions ...Expr) Expr {
	return OP(OpDIV, expressions...)
}

/**/

const OpMOD = "mod"

// MOD builds an arithmetic remainder expression.
func MOD(expressions ...Expr) Expr {
	return OP(OpMOD, expressions...)
}

/**/

const OpDISTINCT = "distinct"

// DISTINCT builds a distinct-value expression for expr.
func DISTINCT[T string | Expr](expr T) Expr {
	return OP(OpDISTINCT, exprArg(expr))
}

/**/

const OpRANK = "rank"

// RANK builds a ranking expression for expr against the provided values.
func RANK[T string | Expr](expr T, values ...any) Expr {
	args := make([]Expr, 0, len(values)+1)
	args = append(args, exprArg(expr))
	for _, v := range values {
		args = append(args, valueArg(v))
	}
	return OP(OpRANK, args...)
}

/**/

const OpGREATEST = "greatest"

// GREATEST builds a max-of-values expression.
func GREATEST(expressions ...Expr) Expr {
	return OP(OpGREATEST, expressions...)
}

/**/

const OpLEAST = "least"

// LEAST builds a min-of-values expression.
func LEAST(expressions ...Expr) Expr {
	return OP(OpLEAST, expressions...)
}

/**/

const OpCOUNT = "count"

// COUNT builds COUNT(expr).
// It is the field/value form of the same aggregate operation as ROWCOUNT:
// both produce the same OpCOUNT node, but COUNT includes an explicit argument.
func COUNT[T string | Expr](expr T) Expr {
	return OP(OpCOUNT, exprArg(expr))
}

// ROWCOUNT builds COUNT(*).
// It is functionally the same aggregate operation as COUNT(expr):
// both use OpCOUNT, but ROWCOUNT omits the explicit argument and represents row counting.
func ROWCOUNT() Expr {
	return OP(OpCOUNT)
}

/**/

const OpSUM = "sum"

// SUM builds SUM(expr).
func SUM[T string | Expr](expr T) Expr {
	return OP(OpSUM, exprArg(expr))
}

/**/

const OpAVG = "avg"

// AVG builds AVG(expr).
func AVG[T string | Expr](expr T) Expr {
	return OP(OpAVG, exprArg(expr))
}

/**/

const OpMIN = "min"

// MIN builds MIN(expr).
func MIN[T string | Expr](expr T) Expr {
	return OP(OpMIN, exprArg(expr))
}

/**/

const OpMAX = "max"

// MAX builds MAX(expr).
func MAX[T string | Expr](expr T) Expr {
	return OP(OpMAX, exprArg(expr))
}
