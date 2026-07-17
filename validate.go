package qx

import (
	"fmt"
	"strconv"
	"strings"
)

// ValidationError describes a structural or stage-aware validation failure.
type ValidationError struct {
	Path    string
	Code    string
	Message string
}

func (err *ValidationError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Message == "" {
		return fmt.Sprintf("qx: validation failed at %s (%s)", err.Path, err.Code)
	}
	return fmt.Sprintf("qx: validation failed at %s (%s): %s", err.Path, err.Code, err.Message)
}

// Validate checks whether qx is structurally well-formed and stage-consistent.
// It validates expression node shape, built-in operation arity, predicate-only
// roots in filter and having contexts, and placement rules for OUT references
// and aggregate operations across query sections. Unknown operation names are
// allowed as long as the expression remains structurally valid.
//
// Validate returns nil for a nil query and for an empty query value.
// On failure, it returns the first validation error encountered as a
// *ValidationError with a stable Path and Code.
func Validate(qx *QX) error {
	return qx.Validate()
}

// Validate checks whether qx is structurally well-formed and stage-consistent.
// It validates expression node shape, built-in operation arity, predicate-only
// roots in filter and having contexts, and placement rules for OUT references
// and aggregate operations across query sections. Unknown operation names are
// allowed as long as the expression remains structurally valid.
//
// Validate returns nil for a nil query and for an empty query value.
// On failure, it returns the first validation error encountered as a
// *ValidationError with a stable Path and Code.
func (qx *QX) Validate() error {
	if qx == nil {
		return nil
	}

	var v validator
	hasReduction := qx.HasReduction()

	v.resetPath()
	if err := v.validateMetadata(qx.Metadata); err != nil {
		return err
	}

	v.resetPath()
	if err := v.validateOptionalExpr(&qx.Filter, validationPathSectionFilter, -1, validationContextFilter); err != nil {
		return err
	}

	if hasReduction {
		for i := range qx.Reduction.Group {
			v.resetPath()
			if err := v.validateRequiredExpr(
				&qx.Reduction.Group[i],
				validationPathSectionReductionGroup,
				i,
				validationContextGroup,
			); err != nil {
				return err
			}
		}

		for i := range qx.Reduction.Metrics {
			v.resetPath()
			if err := v.validateRequiredExpr(
				&qx.Reduction.Metrics[i],
				validationPathSectionReductionMetrics,
				i,
				validationContextMetric,
			); err != nil {
				return err
			}
		}

		for i := range qx.Reduction.Group {
			if err := v.recordOutputName(&qx.Reduction.Group[i], validationPathSectionReductionGroup, i, &v.availableOutputs); err != nil {
				return err
			}
		}
		for i := range qx.Reduction.Metrics {
			if err := v.recordOutputName(&qx.Reduction.Metrics[i], validationPathSectionReductionMetrics, i, &v.availableOutputs); err != nil {
				return err
			}
		}

		v.resetPath()
		if err := v.validateOptionalExpr(
			&qx.Reduction.Having,
			validationPathSectionReductionHaving,
			-1,
			validationContextHaving,
		); err != nil {
			return err
		}
	}

	orderCtx := validationContextOrderPreReduction
	if hasReduction {
		orderCtx = validationContextOrderPostReduction
	}
	for i := range qx.Order {
		v.resetPath()
		if err := v.validateRequiredExpr(&qx.Order[i].By, validationPathSectionOrderBy, i, orderCtx); err != nil {
			return err
		}
	}

	projectionCtx := validationContextProjectionPreReduction
	if hasReduction {
		projectionCtx = validationContextProjectionPostReduction
	}
	var projectionOutputs validationNameSet
	for i := range qx.Projection {
		v.resetPath()
		if err := v.validateRequiredExpr(&qx.Projection[i], validationPathSectionProjection, i, projectionCtx); err != nil {
			return err
		}
		if err := v.recordRequiredOutputName(&qx.Projection[i], validationPathSectionProjection, i, &projectionOutputs); err != nil {
			return err
		}
	}

	return nil
}

type validationContext uint8

const (
	validationContextFilter validationContext = iota
	validationContextGroup
	validationContextMetric
	validationContextHaving
	validationContextOrderPreReduction
	validationContextOrderPostReduction
	validationContextProjectionPreReduction
	validationContextProjectionPostReduction
)

type validationPathSection uint8

const (
	validationPathSectionMeta validationPathSection = iota
	validationPathSectionFilter
	validationPathSectionReductionGroup
	validationPathSectionReductionMetrics
	validationPathSectionReductionHaving
	validationPathSectionOrderBy
	validationPathSectionProjection
)

const (
	validationInlineArgDepth  = 16
	validationInlineNameCount = 16
)

// validationNameSet keeps the common small-query path on the stack and only
// creates a map for unusually wide metadata, reduction, or projection lists.
type validationNameSet struct {
	inline   [validationInlineNameCount]string
	count    int
	overflow map[string]struct{}
}

// add reports whether name was newly inserted.
func (s *validationNameSet) add(name string) bool {
	if s.overflow != nil {
		if _, exists := s.overflow[name]; exists {
			return false
		}
		s.overflow[name] = struct{}{}
		return true
	}

	for i := 0; i < s.count; i++ {
		if s.inline[i] == name {
			return false
		}
	}
	if s.count < len(s.inline) {
		s.inline[s.count] = name
		s.count++
		return true
	}

	s.overflow = make(map[string]struct{}, s.count+1)
	for i := 0; i < s.count; i++ {
		s.overflow[s.inline[i]] = struct{}{}
	}
	s.overflow[name] = struct{}{}
	return true
}

func (s *validationNameSet) contains(name string) bool {
	if s.overflow != nil {
		_, exists := s.overflow[name]
		return exists
	}
	for i := 0; i < s.count; i++ {
		if s.inline[i] == name {
			return true
		}
	}
	return false
}

type validator struct {
	argPath          [validationInlineArgDepth]int
	argDepth         int
	argOverflow      []int
	availableOutputs validationNameSet
}

func (v *validator) resetPath() {
	v.argDepth = 0
}

func (v *validator) pushArg(index int) {
	if v.argDepth < len(v.argPath) {
		v.argPath[v.argDepth] = index
	} else {
		overflowIndex := v.argDepth - len(v.argPath)
		if overflowIndex < len(v.argOverflow) {
			v.argOverflow[overflowIndex] = index
		} else {
			v.argOverflow = append(v.argOverflow, index)
		}
	}
	v.argDepth++
}

func (v *validator) popArg() {
	v.argDepth--
}

func (v *validator) argIndex(level int) int {
	if level < len(v.argPath) {
		return v.argPath[level]
	}
	return v.argOverflow[level-len(v.argPath)]
}

func (v *validator) validateMetadata(meta []MetaEntry) error {
	var seen validationNameSet

	for i := range meta {
		key := strings.TrimSpace(meta[i].Key)
		if key == "" {
			return v.validationError(validationPathSectionMeta, i, "invalid_meta_key", "meta key must not be blank")
		}
		if key != meta[i].Key {
			return v.validationError(validationPathSectionMeta, i, "invalid_meta_key", "meta key must not have leading or trailing whitespace")
		}
		if !seen.add(key) {
			return v.validationError(validationPathSectionMeta, i, "duplicate_meta_key", fmt.Sprintf("meta key %q is already defined", key))
		}
	}

	return nil
}

func (v *validator) validateOptionalExpr(expr *Expr, section validationPathSection, itemIndex int, ctx validationContext) error {
	return v.validateExpr(expr, section, itemIndex, ctx, true, true)
}

func (v *validator) validateRequiredExpr(expr *Expr, section validationPathSection, itemIndex int, ctx validationContext) error {
	return v.validateExpr(expr, section, itemIndex, ctx, true, false)
}

func (v *validator) validateExpr(expr *Expr, section validationPathSection, itemIndex int, ctx validationContext, root, allowZero bool) error {
	if isZeroExpr(expr) {
		if allowZero {
			return nil
		}
		return v.validationError(section, itemIndex, "empty_expr", "expression must not be empty")
	}

	if expr.Alias != "" {
		trimmedAlias := strings.TrimSpace(expr.Alias)
		if trimmedAlias == "" {
			return v.validationError(section, itemIndex, "invalid_alias", "alias must not be blank")
		}
		if trimmedAlias != expr.Alias {
			return v.validationError(section, itemIndex, "invalid_alias", "alias must not have leading or trailing whitespace")
		}
	}

	switch expr.Kind {
	case KindREF:
		trimmedName := strings.TrimSpace(expr.Name)
		if trimmedName == "" {
			return v.validationError(section, itemIndex, "invalid_name", "reference name must not be blank")
		}
		if expr.Value != nil {
			return v.validationError(section, itemIndex, "unexpected_value", "reference expression must not carry a value")
		}
		if len(expr.Args) != 0 {
			return v.validationError(section, itemIndex, "unexpected_args", "reference expression must not have arguments")
		}

	case KindOUT:
		trimmedName := strings.TrimSpace(expr.Name)
		if trimmedName == "" {
			return v.validationError(section, itemIndex, "invalid_name", "output name must not be blank")
		}
		if expr.Value != nil {
			return v.validationError(section, itemIndex, "unexpected_value", "output reference must not carry a value")
		}
		if len(expr.Args) != 0 {
			return v.validationError(section, itemIndex, "unexpected_args", "output reference must not have arguments")
		}

	case KindLIT:
		if expr.Name != "" {
			return v.validationError(section, itemIndex, "invalid_name", "literal expression must not define a name")
		}
		if len(expr.Args) != 0 {
			return v.validationError(section, itemIndex, "unexpected_args", "literal expression must not have arguments")
		}

	case KindOP:
		trimmedName := strings.TrimSpace(expr.Name)
		if trimmedName == "" || trimmedName != expr.Name {
			return v.validationError(section, itemIndex, "invalid_name", "operation name must be non-empty and trimmed")
		}
		if expr.Value != nil {
			return v.validationError(section, itemIndex, "unexpected_value", "operation expression must not carry a value")
		}
		if err := v.validateOpArity(section, itemIndex, expr.Name, len(expr.Args)); err != nil {
			return err
		}

	case KindNONE:
		return v.validationError(section, itemIndex, "invalid_kind", "expression kind must not be empty")

	default:
		return v.validationError(section, itemIndex, "invalid_kind", fmt.Sprintf("unsupported expression kind %q", expr.Kind))
	}

	if root && requiresPredicateRoot(ctx) && !isPredicateRoot(expr) {
		return v.validationError(section, itemIndex, "predicate_required", "root expression must be a predicate")
	}
	if root && requiresOutputName(ctx) {
		if _, ok := validationOutputName(expr); !ok {
			return v.validationError(section, itemIndex, "output_name_required", "projection expression must have an output name")
		}
	}
	if expr.Kind == KindREF && forbidsRef(ctx) {
		return v.validationError(section, itemIndex, "ref_not_allowed", "REF is not allowed in this query section")
	}
	if expr.Kind == KindOUT {
		if !allowsOut(ctx) {
			return v.validationError(section, itemIndex, "out_not_allowed", "OUT is not allowed in this query section")
		}
		if !v.availableOutputs.contains(expr.Name) {
			return v.validationError(section, itemIndex, "unknown_output", fmt.Sprintf("output name %q is not defined by reduction", expr.Name))
		}
	}
	if expr.Kind == KindOP && forbidsKnownAggregates(ctx) && isKnownAggregateOp(expr.Name) {
		return v.validationError(section, itemIndex, "aggregate_not_allowed", "aggregate operation is not allowed in this query section")
	}
	if root && ctx == validationContextMetric && !hasMetricSemantics(expr) {
		return v.validationError(section, itemIndex, "aggregate_required", "metric expression must contain aggregate semantics")
	}

	for i := range expr.Args {
		v.pushArg(i)
		err := v.validateExpr(&expr.Args[i], section, itemIndex, ctx, false, false)
		v.popArg()
		if err != nil {
			return err
		}
	}

	if expr.Kind == KindOP && expr.Name == OpIF && !isPredicateExpr(&expr.Args[0]) {
		v.pushArg(0)
		err := v.validationError(section, itemIndex, "predicate_required", "if condition must be a predicate")
		v.popArg()
		return err
	}

	return nil
}

func (v *validator) validateOpArity(section validationPathSection, itemIndex int, op string, argc int) error {
	switch op {
	case OpNOT, OpEXISTS, OpISNULL, OpISEMPTY, OpLEN, OpLOWER, OpUPPER, OpABS, OpFLOOR, OpCEIL, OpDISTINCT, OpSUM, OpAVG, OpMIN, OpMAX:
		if argc != 1 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects exactly 1 argument, got %d", op, argc))
		}
	case OpNOW:
		if argc != 0 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects exactly 0 arguments, got %d", op, argc))
		}
	case OpEQ, OpNE, OpGT, OpGTE, OpLT, OpLTE, OpIN, OpHASALL, OpHASANY, OpPREFIX, OpSUFFIX, OpCONTAINS, OpLIKE, OpILIKE, OpMATCHES, OpPOS, OpSPLIT, OpDATETRUNC, OpEXTRACT:
		if argc != 2 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects exactly 2 arguments, got %d", op, argc))
		}
	case OpNULLIF:
		if argc != 2 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects exactly 2 arguments, got %d", op, argc))
		}
	case OpIF, OpDATEADD, OpDATEDIFF:
		if argc != 3 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects exactly 3 arguments, got %d", op, argc))
		}
	case OpTRIM, OpROUND:
		if argc != 1 && argc != 2 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects 1 or 2 arguments, got %d", op, argc))
		}
	case OpSUBSTR:
		if argc != 2 && argc != 3 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects 2 or 3 arguments, got %d", op, argc))
		}
	case OpREPLACE:
		if argc != 3 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects exactly 3 arguments, got %d", op, argc))
		}
	case OpCOUNT:
		if argc != 0 && argc != 1 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects 0 or 1 arguments, got %d", op, argc))
		}
	case OpRANK:
		if argc < 1 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects at least 1 argument, got %d", op, argc))
		}
	case OpCOALESCE, OpADD, OpSUB, OpMUL, OpDIV, OpMOD:
		if argc < 1 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects at least 1 argument, got %d", op, argc))
		}
	case OpCONCAT, OpGREATEST, OpLEAST:
		if argc < 1 {
			return v.validationError(section, itemIndex, "invalid_arity", fmt.Sprintf("%s expects at least 1 argument, got %d", op, argc))
		}
	}
	return nil
}

func requiresPredicateRoot(ctx validationContext) bool {
	return ctx == validationContextFilter || ctx == validationContextHaving
}

func requiresOutputName(ctx validationContext) bool {
	return ctx == validationContextProjectionPreReduction || ctx == validationContextProjectionPostReduction
}

func allowsOut(ctx validationContext) bool {
	return ctx == validationContextHaving || ctx == validationContextOrderPostReduction || ctx == validationContextProjectionPostReduction
}

func forbidsRef(ctx validationContext) bool {
	return ctx == validationContextProjectionPostReduction
}

func forbidsKnownAggregates(ctx validationContext) bool {
	return ctx == validationContextFilter ||
		ctx == validationContextGroup ||
		ctx == validationContextOrderPreReduction ||
		ctx == validationContextProjectionPreReduction ||
		ctx == validationContextProjectionPostReduction
}

func isPredicateRoot(expr *Expr) bool {
	return isPredicateExpr(expr)
}

func isPredicateExpr(expr *Expr) bool {
	if expr.Kind != KindOP {
		return false
	}
	if !isKnownOp(expr.Name) {
		return true
	}
	return isKnownPredicateOp(expr.Name)
}

func isKnownOp(name string) bool {
	switch name {
	case OpEQ, OpNE, OpGT, OpGTE, OpLT, OpLTE,
		OpIN, OpHASALL, OpHASANY, OpEXISTS, OpISNULL, OpISEMPTY,
		OpPREFIX, OpSUFFIX, OpCONTAINS, OpLIKE, OpILIKE, OpMATCHES,
		OpAND, OpOR, OpNOT,
		OpLEN, OpPOS, OpLOWER, OpUPPER, OpTRIM,
		OpREPLACE, OpSPLIT, OpSUBSTR,
		OpDATETRUNC, OpEXTRACT, OpNOW, OpDATEADD, OpDATEDIFF,
		OpABS, OpROUND, OpFLOOR, OpCEIL,
		OpCOALESCE, OpNULLIF, OpIF, OpCONCAT,
		OpADD, OpSUB, OpMUL, OpDIV, OpMOD,
		OpDISTINCT, OpRANK, OpGREATEST, OpLEAST,
		OpCOUNT, OpSUM, OpAVG, OpMIN, OpMAX:
		return true
	default:
		return false
	}
}

func isKnownPredicateOp(name string) bool {
	switch name {
	case OpAND, OpOR, OpNOT,
		OpEQ, OpNE, OpGT, OpGTE, OpLT, OpLTE,
		OpIN, OpHASALL, OpHASANY,
		OpEXISTS, OpISNULL, OpISEMPTY,
		OpPREFIX, OpSUFFIX, OpCONTAINS, OpLIKE, OpILIKE, OpMATCHES:
		return true
	default:
		return false
	}
}

func isKnownAggregateOp(name string) bool {
	switch name {
	case OpCOUNT, OpSUM, OpAVG, OpMIN, OpMAX:
		return true
	default:
		return false
	}
}

func hasMetricSemantics(expr *Expr) bool {
	if expr.Kind != KindOP {
		return false
	}
	if isKnownAggregateOp(expr.Name) || !isKnownOp(expr.Name) {
		return true
	}
	for i := range expr.Args {
		if hasMetricSemantics(&expr.Args[i]) {
			return true
		}
	}
	return false
}

func isZeroExpr(expr *Expr) bool {
	return expr.Kind == KindNONE && expr.Name == "" && expr.Alias == "" && expr.Value == nil && len(expr.Args) == 0
}

func validationOutputName(expr *Expr) (string, bool) {
	if expr.Alias != "" {
		return expr.Alias, true
	}
	switch expr.Kind {
	case KindREF, KindOUT:
		if name := strings.TrimSpace(expr.Name); name != "" {
			return name, true
		}
	}
	return "", false
}

func (v *validator) recordOutputName(expr *Expr, section validationPathSection, itemIndex int, seen *validationNameSet) error {
	name, ok := validationOutputName(expr)
	if !ok {
		return nil
	}
	if !seen.add(name) {
		return v.validationError(section, itemIndex, "duplicate_output_name", fmt.Sprintf("output name %q is already defined", name))
	}
	return nil
}

func (v *validator) recordRequiredOutputName(expr *Expr, section validationPathSection, itemIndex int, seen *validationNameSet) error {
	name, _ := validationOutputName(expr)
	if !seen.add(name) {
		return v.validationError(section, itemIndex, "duplicate_output_name", fmt.Sprintf("output name %q is already defined", name))
	}
	return nil
}

func (v *validator) validationError(section validationPathSection, itemIndex int, code, message string) error {
	return &ValidationError{
		Path:    v.path(section, itemIndex),
		Code:    code,
		Message: message,
	}
}

func (v *validator) path(section validationPathSection, itemIndex int) string {
	var builder strings.Builder
	builder.Grow(24 + v.argDepth*8)

	switch section {
	case validationPathSectionMeta:
		builder.WriteString("meta[")
		writeValidationIndex(&builder, itemIndex)
		builder.WriteString("].key")

	case validationPathSectionFilter:
		builder.WriteString("filter")

	case validationPathSectionReductionGroup:
		builder.WriteString("reduction.group[")
		writeValidationIndex(&builder, itemIndex)
		builder.WriteByte(']')

	case validationPathSectionReductionMetrics:
		builder.WriteString("reduction.metrics[")
		writeValidationIndex(&builder, itemIndex)
		builder.WriteByte(']')

	case validationPathSectionReductionHaving:
		builder.WriteString("reduction.having")

	case validationPathSectionOrderBy:
		builder.WriteString("order[")
		writeValidationIndex(&builder, itemIndex)
		builder.WriteString("].by")

	case validationPathSectionProjection:
		builder.WriteString("projection[")
		writeValidationIndex(&builder, itemIndex)
		builder.WriteByte(']')
	}

	for i := 0; i < v.argDepth; i++ {
		builder.WriteString(".args[")
		writeValidationIndex(&builder, v.argIndex(i))
		builder.WriteByte(']')
	}

	return builder.String()
}

func writeValidationIndex(builder *strings.Builder, index int) {
	var buf [20]byte
	formatted := strconv.AppendInt(buf[:0], int64(index), 10)
	_, _ = builder.Write(formatted)
}
