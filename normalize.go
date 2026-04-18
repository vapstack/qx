package qx

// Normalize applies only semantics-preserving structural rewrites to qx in place.
// It avoids backend-specific rewrites such as De Morgan, NNF/DNF/CNF conversion, or reordering.
// The function keeps query metadata, ordering and pagination intact and returns qx.
func Normalize(qx *QX) *QX {
	return qx.Normalize()
}

// Normalize applies only semantics-preserving structural rewrites to qx in place.
// It avoids backend-specific rewrites such as De Morgan, NNF/DNF/CNF conversion, or reordering.
// The function keeps query metadata, ordering and pagination intact and returns qx.
func (qx *QX) Normalize() *QX {
	if qx == nil {
		return nil
	}
	qx.Filter = normalizeExpr(qx.Filter)
	if qx.Reduction != nil {
		for i := range qx.Reduction.Group {
			qx.Reduction.Group[i] = normalizeExpr(qx.Reduction.Group[i])
		}
		for i := range qx.Reduction.Metrics {
			qx.Reduction.Metrics[i] = normalizeExpr(qx.Reduction.Metrics[i])
		}
		qx.Reduction.Having = normalizeExpr(qx.Reduction.Having)
	}
	for i := range qx.Order {
		qx.Order[i].By = normalizeExpr(qx.Order[i].By)
	}
	for i := range qx.Projection {
		qx.Projection[i] = normalizeExpr(qx.Projection[i])
	}
	return qx
}

func normalizeExpr(expr Expr) Expr {
	normalized, _ := normalizeExprCOW(expr)
	return normalized
}

func normalizeExprCOW(expr Expr) (Expr, bool) {
	if expr.IsZero() {
		return expr, false
	}

	args, argsChanged := normalizeExprArgs(expr.Args)

	if expr.Kind != KindOP {
		if !argsChanged {
			return expr, false
		}
		expr.Args = args
		return expr, true
	}
	if expr.Name != OpAND && expr.Name != OpOR {
		if !argsChanged {
			return expr, false
		}
		expr.Args = args
		return expr, true
	}

	expr, logicalChanged := normalizeLogicalExpr(expr, args, argsChanged)
	return expr, argsChanged || logicalChanged
}

func normalizeExprArgs(args []Expr) ([]Expr, bool) {
	var dst []Expr

	for i := range args {
		arg, changed := normalizeExprCOW(args[i])
		if dst != nil {
			dst[i] = args[i]
		}
		if !changed {
			continue
		}
		if dst == nil {
			dst = make([]Expr, len(args))
			copy(dst, args[:i])
		}
		dst[i] = arg
	}

	if dst == nil {
		return args, false
	}
	return dst, true
}

func normalizeLogicalExpr(expr Expr, args []Expr, canReuse bool) (Expr, bool) {
	args, flattened := normalizeLogicalArgs(expr.Name, args, canReuse)
	expr.Args = args

	if len(expr.Args) != 1 {
		return expr, flattened
	}
	if hasExprMetadata(expr) && hasExprMetadata(expr.Args[0]) {
		return expr, flattened
	}
	return collapseLogicalExpr(expr), true
}

func normalizeLogicalArgs(op string, args []Expr, canReuse bool) ([]Expr, bool) {
	total, changed := normalizedLogicalArgsLen(op, args)
	if !changed {
		return args, false
	}

	var dst []Expr
	if canReuse && total <= cap(args) {
		dst = args[:total]
	} else {
		dst = make([]Expr, total)
	}
	return appendNormalizedLogicalArgs(dst, op, args), true
}

func normalizedLogicalArgsLen(op string, args []Expr) (int, bool) {
	total := 0
	changed := false

	for _, arg := range args {
		if isFlattenableLogicalArg(arg, op) {
			changed = true
			total += len(arg.Args)
			continue
		}
		total++
	}

	return total, changed
}

func appendNormalizedLogicalArgs(dst []Expr, op string, args []Expr) []Expr {
	write := len(dst)

	for i := len(args) - 1; i >= 0; i-- {
		arg := args[i]
		if isFlattenableLogicalArg(arg, op) {
			write -= len(arg.Args)
			copy(dst[write:], arg.Args)
			continue
		}
		write--
		dst[write] = arg
	}

	return dst[write:]
}

func collapseLogicalExpr(expr Expr) Expr {
	if len(expr.Args) != 1 {
		return expr
	}

	only := expr.Args[0]
	if hasExprMetadata(expr) && hasExprMetadata(only) {
		return expr
	}
	if expr.Alias != "" && only.Alias == "" {
		only.Alias = expr.Alias
	}
	return only
}

func hasExprMetadata(expr Expr) bool {
	return expr.Alias != ""
}

func isFlattenableLogicalArg(expr Expr, op string) bool {
	return isLogicalOp(expr, op) && !hasExprMetadata(expr)
}

func isLogicalOp(expr Expr, op string) bool {
	return expr.Kind == KindOP && expr.Name == op
}
