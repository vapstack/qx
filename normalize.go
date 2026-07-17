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
	normalizeExpr(&qx.Filter)
	if qx.Reduction != nil {
		for i := range qx.Reduction.Group {
			normalizeExpr(&qx.Reduction.Group[i])
		}
		for i := range qx.Reduction.Metrics {
			normalizeExpr(&qx.Reduction.Metrics[i])
		}
		normalizeExpr(&qx.Reduction.Having)
	}
	for i := range qx.Order {
		normalizeExpr(&qx.Order[i].By)
	}
	for i := range qx.Projection {
		normalizeExpr(&qx.Projection[i])
	}
	return qx
}

func normalizeExpr(expr *Expr) {
	var normalized Expr
	if normalizeExprCOW(expr, &normalized) {
		*expr = normalized
	}
}

// normalizeExprCOW writes a normalized expression to dst only when src changes.
// Keeping the unchanged result in src avoids copying the relatively large Expr
// value at every node on the common already-normalized path.
func normalizeExprCOW(src *Expr, dst *Expr) bool {
	if src.IsZero() {
		return false
	}

	var args []Expr
	argsChanged := normalizeExprArgs(src.Args, &args)
	if !argsChanged {
		args = src.Args
	}

	if src.Kind != KindOP {
		if !argsChanged {
			return false
		}
		*dst = *src
		dst.Args = args
		return true
	}
	if src.Name != OpAND && src.Name != OpOR {
		if !argsChanged {
			return false
		}
		*dst = *src
		dst.Args = args
		return true
	}

	return normalizeLogicalExprCOW(src, args, argsChanged, dst)
}

func normalizeExprArgs(src []Expr, dst *[]Expr) bool {
	var normalized []Expr

	for i := range src {
		var arg Expr
		changed := normalizeExprCOW(&src[i], &arg)
		if normalized != nil {
			normalized[i] = src[i]
		}
		if !changed {
			continue
		}
		if normalized == nil {
			normalized = make([]Expr, len(src))
			copy(normalized, src[:i])
		}
		normalized[i] = arg
	}

	if normalized == nil {
		return false
	}
	*dst = normalized
	return true
}

func normalizeLogicalExprCOW(src *Expr, args []Expr, canReuse bool, dst *Expr) bool {
	args, flattened := normalizeLogicalArgs(src.Name, args, canReuse)

	if len(args) != 1 {
		if !canReuse && !flattened {
			return false
		}
		*dst = *src
		dst.Args = args
		return true
	}
	if hasExprMetadata(*src) && hasExprMetadata(args[0]) {
		if !canReuse && !flattened {
			return false
		}
		*dst = *src
		dst.Args = args
		return true
	}

	*dst = args[0]
	if src.Alias != "" && dst.Alias == "" {
		dst.Alias = src.Alias
	}
	return true
}

func normalizeLogicalArgs(op string, args []Expr, canReuse bool) ([]Expr, bool) {
	total, changed := normalizedLogicalArgsLen(op, args)
	if !changed {
		return args, false
	}

	var dst []Expr
	if canReuse && total <= cap(args) && canFlattenLogicalArgsBackward(args, op, total) {
		dst = args[:total]
	} else {
		dst = make([]Expr, total)
	}
	return appendNormalizedLogicalArgs(dst, op, args), true
}

// canFlattenLogicalArgsBackward reports whether appendNormalizedLogicalArgs
// can reuse args without overwriting an earlier element before reading it.
func canFlattenLogicalArgsBackward(args []Expr, op string, total int) bool {
	write := total
	for i := len(args) - 1; i >= 0; i-- {
		width := 1
		if isFlattenableLogicalArg(args[i], op) {
			width = len(args[i].Args)
		}
		write -= width
		if width != 0 && write < i {
			return false
		}
	}
	return true
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

func hasExprMetadata(expr Expr) bool {
	return expr.Alias != ""
}

func isFlattenableLogicalArg(expr Expr, op string) bool {
	return isLogicalOp(expr, op) && !hasExprMetadata(expr)
}

func isLogicalOp(expr Expr, op string) bool {
	return expr.Kind == KindOP && expr.Name == op
}
