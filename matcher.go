package qx

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

type MatchFunc func(v any) (bool, error)

type Matcher struct {
	recMap  map[string]*fieldRec
	srcType reflect.Type
	df      []diffField
}

type diffField struct {
	name     string
	index    []int
	sf       reflect.StructField
	dbName   string
	jsonName string
}

// MatcherFor creates a Matcher for the struct type T.
// T may be a struct or a pointer to a struct (any pointer depth is allowed).
// Reflection data for the type is computed once and cached for reuse.
// The matcher supports addressing fields by Go name and by selected struct tag aliases.
func MatcherFor[T any]() (*Matcher, error) {
	var v T
	t := reflect.TypeOf(v)
	if t == nil {
		t = reflect.TypeOf((*T)(nil)).Elem()
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("value must be a struct or pointer to a struct: %v", t)
	}
	return typeMatcher(t)
}

// NewMatcher creates a matcher for the type of the provided value.
// Pointer types allow faster matching.
func NewMatcher(v any) (*Matcher, error) {
	if v == nil {
		return nil, fmt.Errorf("value must be a struct or pointer to a struct, got nil")
	}
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("value must be a struct or pointer to a struct, got %v", t)
	}
	return typeMatcher(t)
}

// CompileFor is a convenience helper equivalent to MatcherFor[T]().Compile(expr).
// The resulting predicate accepts both T and *T values (including via interfaces).
func CompileFor[T any](expr Expr) (MatchFunc, error) {
	m, err := MatcherFor[T]()
	if err != nil {
		return nil, err
	}
	return m.Compile(expr)
}

// Match evaluates the expression exp against v in one shot.
// v may be provided either as a struct value (T) or as a pointer to a struct (*T);
// interfaces wrapping T or *T are also supported.
// Nil values never match and return (false, nil).
// For repeated evaluations over the same type, prefer creating a Matcher via MatcherFor and reusing it.
func Match(v any, exp Expr) (bool, error) {
	if v == nil {
		return false, nil
	}
	t := reflect.TypeOf(v)

	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false, fmt.Errorf("value must be a struct or pointer to a struct: %v", t)
	}

	m, err := typeMatcher(t)
	if err != nil {
		return false, err
	}
	fn, err := m.Compile(exp)
	if err != nil {
		return false, err
	}
	return fn(v)
}

func typeMatcher(rt reflect.Type) (*Matcher, error) {
	m, err := getRecMap(rt)
	if err != nil {
		return nil, err
	}
	return &Matcher{
		recMap:  m,
		srcType: rt,
		df:      getDiffFields(rt),
	}, nil
}

type matchFunc func(ptr unsafe.Pointer, root reflect.Value) (bool, error)

type fieldRec struct {
	index []int
	// flags byte
}

var recMapCache sync.Map

func getRecMap(rt reflect.Type) (map[string]*fieldRec, error) {
	if rmap, exists := recMapCache.Load(rt); exists {
		return rmap.(map[string]*fieldRec), nil
	}
	recMap := make(map[string]*fieldRec)
	if err := collectFieldRecs(rt, recMap, nil); err != nil {
		return nil, err
	}
	recMapCache.Store(rt, recMap)
	return recMap, nil
}

func collectFieldRecs(rt reflect.Type, recMap map[string]*fieldRec, pos []int) error {
	nf := rt.NumField()
	for i := 0; i < nf; i++ {
		rf := rt.Field(i)

		if !rf.IsExported() {
			continue
		}

		if rf.Anonymous {
			ft := rf.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				nextPos := make([]int, 0, len(pos)+len(rf.Index))
				nextPos = append(nextPos, pos...)
				nextPos = append(nextPos, rf.Index...)
				if err := collectFieldRecs(ft, recMap, nextPos); err != nil {
					return err
				}
			}
			continue
		}

		nextPos := make([]int, 0, len(pos)+len(rf.Index))
		nextPos = append(nextPos, pos...)
		nextPos = append(nextPos, rf.Index...)

		rec := &fieldRec{index: nextPos}

		if _, exists := recMap[rf.Name]; exists {
			return fmt.Errorf("duplicate field identifier \"%v\", field name was taken by db/json tag", rf.Name)
		}
		recMap[rf.Name] = rec

		if jsonTag := rf.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
			name := strings.TrimSpace(strings.Split(jsonTag, ",")[0])
			if name != "" {
				if _, exists := recMap[name]; exists {
					return fmt.Errorf("duplicate field identifier \"%v\" while parsing %v", name, rf.Name)
				}
				recMap[name] = rec
			}
		}
		if dbTag := rf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			name := strings.TrimSpace(strings.Split(dbTag, ",")[0])
			if name != "" {
				if _, exists := recMap[name]; exists {
					return fmt.Errorf("duplicate field identifier \"%v\" while parsing %v", name, rf.Name)
				}
				recMap[name] = rec
			}
		}
	}
	return nil
}

var diffFieldsCache sync.Map

func getDiffFields(rt reflect.Type) []diffField {
	if v, ok := diffFieldsCache.Load(rt); ok {
		return v.([]diffField)
	}
	var out []diffField
	collectDiffFields(rt, nil, &out)
	diffFieldsCache.Store(rt, out)
	return out
}

func collectDiffFields(rt reflect.Type, pos []int, out *[]diffField) {
	nf := rt.NumField()
	for i := 0; i < nf; i++ {
		rf := rt.Field(i)

		if !rf.IsExported() {
			continue
		}

		if rf.Anonymous {
			ft := rf.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				nextPos := make([]int, 0, len(pos)+len(rf.Index))
				nextPos = append(nextPos, pos...)
				nextPos = append(nextPos, rf.Index...)
				collectDiffFields(ft, nextPos, out)
			}
			continue
		}

		nextPos := make([]int, 0, len(pos)+len(rf.Index))
		nextPos = append(nextPos, pos...)
		nextPos = append(nextPos, rf.Index...)

		df := diffField{
			name:  rf.Name,
			index: nextPos,
			sf:    rf,
		}

		if tv := firstTagValue(rf.Tag.Get("json")); tv != "" && tv != "-" {
			df.jsonName = tv
		}
		if tv := firstTagValue(rf.Tag.Get("db")); tv != "" && tv != "-" {
			df.dbName = tv
		}

		*out = append(*out, df)
	}
}

func firstTagValue(tag string) string {
	if i := strings.IndexByte(tag, ','); i >= 0 {
		tag = tag[:i]
	}
	return strings.TrimSpace(tag)
}

func (m *Matcher) normalizeRoot(v any) (reflect.Value, error) {
	if v == nil {
		return reflect.Value{}, fmt.Errorf("nil value")
	}

	rv := reflect.ValueOf(v)

	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}, fmt.Errorf("nil pointer")
		}
		rv = rv.Elem()
	}

	for rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return reflect.Value{}, fmt.Errorf("nil interface")
		}
		rv = rv.Elem()
		for rv.Kind() == reflect.Pointer {
			if rv.IsNil() {
				return reflect.Value{}, fmt.Errorf("nil pointer")
			}
			rv = rv.Elem()
		}
	}

	if rv.Type() != m.srcType {
		if rv.Type().ConvertibleTo(m.srcType) {
			rv = rv.Convert(m.srcType)
		} else {
			return reflect.Value{}, fmt.Errorf("type mismatch, expected %v, got %v", m.srcType, rv.Type())
		}
	}

	return rv, nil
}

// DiffFields returns the names of exported fields whose values differ
// between the provided values.
// Each value may be provided either as T or *T; interfaces wrapping T or *T
// are also supported.
// At least two values must be provided; if fewer are provided, it returns nil, nil.
// Values must be of the matcher's source type or convertible to it.
func (m *Matcher) DiffFields(values ...any) ([]string, error) {
	return m.DiffFieldsTag("", values...)
}

// DiffFieldsTag is like DiffFields but returns field names using the provided struct tag.
// Each value may be provided either as T or *T; interfaces wrapping T or *T
// are also supported.
// For each differing field, the first component of the tag (before a comma) is used;
// if the tag is missing or "-", it falls back to the Go field name.
// If tag is empty, DiffFieldsTag behaves the same as DiffFields.
func (m *Matcher) DiffFieldsTag(tag string, values ...any) ([]string, error) {
	if len(values) < 2 {
		return nil, nil
	}

	// fast
	if len(values) == 2 {
		rLeft, err := m.normalizeRoot(values[0])
		if err != nil {
			return nil, err
		}
		rRight, err := m.normalizeRoot(values[1])
		if err != nil {
			return nil, err
		}

		out := make([]string, 0, len(m.df))

		for _, f := range m.df {
			vLeft, ok := getSafeField(rLeft, f.index)
			if !ok {
				vLeft = reflect.Value{}
			}
			vRight, ok := getSafeField(rRight, f.index)
			if !ok {
				vRight = reflect.Value{}
			}

			if areEqual(vLeft, vRight) {
				continue
			}

			name := f.name
			if tag != "" {
				switch tag {
				case "json":
					if f.jsonName != "" {
						name = f.jsonName
					}
				case "db":
					if f.dbName != "" {
						name = f.dbName
					}
				default:
					tv := firstTagValue(f.sf.Tag.Get(tag))
					if tv != "" && tv != "-" {
						name = tv
					}
				}
			}

			out = append(out, name)
		}

		return out, nil
	}

	// general

	roots := make([]reflect.Value, len(values))
	for i, v := range values {
		rv, err := m.normalizeRoot(v)
		if err != nil {
			return nil, err
		}
		roots[i] = rv
	}

	base := roots[0]
	out := make([]string, 0, len(m.df))

	for _, f := range m.df {
		vBase, ok := getSafeField(base, f.index)
		if !ok {
			vBase = reflect.Value{}
		}

		diff := false
		for i := 1; i < len(roots); i++ {
			v, vok := getSafeField(roots[i], f.index)
			if !vok {
				v = reflect.Value{}
			}
			if !areEqual(vBase, v) {
				diff = true
				break
			}
		}
		if !diff {
			continue
		}

		name := f.name
		if tag != "" {
			switch tag {
			case "json":
				if f.jsonName != "" {
					name = f.jsonName
				}
			case "db":
				if f.dbName != "" {
					name = f.dbName
				}
			default:
				tv := firstTagValue(f.sf.Tag.Get(tag))
				if tv != "" && tv != "-" {
					name = tv
				}
			}
		}

		out = append(out, name)
	}

	return out, nil
}

// Match evaluates the expression expr against v using this Matcher.
// v may be provided either as a struct value (T) or as a pointer to a struct (*T);
// interfaces wrapping T or *T are also supported.
// This method is intended for occasional, one-off evaluations.
// For repeated matching with the same expression, use Compile to obtain
// a reusable predicate.
func (m *Matcher) Match(v any, expr Expr) (bool, error) {
	check, err := m.Compile(expr)
	if err != nil {
		return false, err
	}
	return check(v)
}

// Compile compiles expr into an efficient predicate function.
// The returned function may be called with either a struct value (T)
// or a pointer to a struct (*T); interfaces wrapping T or *T are also supported.
// Passing nil to the predicate returns (false, nil).
// The compiled predicate can be reused safely for repeated evaluations.
func (m *Matcher) Compile(expr Expr) (MatchFunc, error) {

	check, err := m.compileRecursive(expr)
	if err != nil {
		return nil, err
	}

	return func(v any) (bool, error) {
		if v == nil {
			return false, nil
		}

		rv := reflect.ValueOf(v)

		var ptr unsafe.Pointer

		if rv.Kind() == reflect.Pointer {

			for rv.Kind() == reflect.Pointer {
				if rv.IsNil() {
					return false, nil
				}
				ptr = unsafe.Pointer(rv.Pointer())
				rv = rv.Elem()
			}
		}

		for rv.Kind() == reflect.Interface {
			if rv.IsNil() {
				return false, nil
			}
			rv = rv.Elem()

			if rv.Kind() == reflect.Pointer {
				for rv.Kind() == reflect.Pointer {
					if rv.IsNil() {
						return false, nil
					}
					ptr = unsafe.Pointer(rv.Pointer())
					rv = rv.Elem()
				}
			} else {
				ptr = nil
			}
		}

		if rv.Type() != m.srcType {
			if rv.Type().ConvertibleTo(m.srcType) {
				rv = rv.Convert(m.srcType)
				ptr = nil
			} else {
				return false, fmt.Errorf("type mismatch, expected %v, got %v", m.srcType, rv.Type())
			}
		} else if ptr == nil && rv.CanAddr() {
			ptr = unsafe.Pointer(rv.UnsafeAddr())
		}

		return check(ptr, rv)

	}, nil
}

func (m *Matcher) compileRecursive(expr Expr) (matchFunc, error) {

	if expr.Op == OpAND || expr.Op == OpOR {

		checks := make([]matchFunc, 0, len(expr.Operands))
		for _, sub := range expr.Operands {
			c, err := m.compileRecursive(sub)
			if err != nil {
				return nil, err
			}
			checks = append(checks, c)
		}

		if expr.Op == OpAND {
			return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
				for _, check := range checks {
					ok, err := check(ptr, root)
					if err != nil {
						return false, err
					}
					if !ok {
						return false, nil
					}
				}
				return true, nil
			}, nil
		}

		// OpOR
		return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
			for _, check := range checks {
				ok, err := check(ptr, root)
				if err != nil {
					return false, err
				}
				if ok {
					return true, nil
				}
			}
			return false, nil
		}, nil
	}

	rec, ok := m.recMap[expr.Field]
	if !ok {
		return nil, fmt.Errorf("unknown field: %s", expr.Field)
	}
	field := m.srcType.FieldByIndex(rec.index)
	fieldType := field.Type

	var checker matchFunc
	var err error

	switch expr.Op {

	case OpEQ, OpGT, OpGTE, OpLT, OpLTE, OpPREFIX, OpSUFFIX, OpCONTAINS:
		checker, err = compileScalarHybrid(m.srcType, expr.Op, rec.index, fieldType, expr.Value)

	case OpIN, OpHAS, OpHASANY, OpHASNONE:
		checker, err = compileSliceOp(expr.Op, rec.index, fieldType, expr.Value)

	default:
		return nil, fmt.Errorf("unsupported op: %v", expr.Op)
	}

	if err != nil {
		return nil, fmt.Errorf("compile error for field '%s': %w", expr.Field, err)
	}

	if expr.Not {
		original := checker
		checker = func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
			ok, e := original(ptr, root)
			return !ok, e
		}
	}

	return checker, nil
}

func compileScalarHybrid(rootType reflect.Type, op Op, index []int, fieldType reflect.Type, queryVal any) (matchFunc, error) {

	slow, err := compileScalarCmpSlow(op, index, fieldType, queryVal)
	if err != nil {
		return nil, err
	}

	off, leafType, ok := calcFastOffset(rootType, index)
	if !ok {
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			return slow(root)
		}, nil
	}

	fast, err := compileScalarCmpFast(op, off, leafType, queryVal)
	if err != nil {
		// return at least slow
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			return slow(root)
		}, nil
	}

	return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
		if ptr != nil {
			return fast(ptr)
		}
		return slow(root)
	}, nil
}

func compileScalarCmpSlow(op Op, index []int, fieldType reflect.Type, queryVal any) (func(reflect.Value) (bool, error), error) {

	if op == OpEQ && queryVal == nil {
		return func(v reflect.Value) (bool, error) {
			fv, ok := getSafeField(v, index)
			if !ok {
				return true, nil
			}
			for fv.Kind() == reflect.Interface {
				if fv.IsNil() {
					return true, nil
				}
				fv = fv.Elem()
			}
			return isNilableAndNil(fv), nil
		}, nil
	}

	// string operations on interface fields
	if fieldType.Kind() == reflect.Interface && (op == OpPREFIX || op == OpSUFFIX || op == OpCONTAINS) {
		qv, err := prepareValue(reflect.TypeOf(""), queryVal)
		if err != nil {
			return nil, err
		}
		q := qv.String()

		return func(root reflect.Value) (bool, error) {
			fv, ok := getSafeField(root, index)
			if !ok {
				return false, nil
			}
			for fv.Kind() == reflect.Interface {
				if fv.IsNil() {
					return false, nil
				}
				fv = fv.Elem()
			}
			if fv.Kind() == reflect.Pointer {
				if fv.IsNil() {
					return false, nil
				}
				fv = fv.Elem()
			}
			if fv.Kind() != reflect.String {
				return false, nil
			}
			s := fv.String()
			switch op {
			case OpPREFIX:
				return strings.HasPrefix(s, q), nil
			case OpSUFFIX:
				return strings.HasSuffix(s, q), nil
			case OpCONTAINS:
				return strings.Contains(s, q), nil
			default:
				return false, nil
			}
		}, nil
	}

	isPtrField := fieldType.Kind() == reflect.Pointer

	cmpType := fieldType
	if isPtrField {
		cmpType = fieldType.Elem()
	}

	qv, err := prepareValue(cmpType, queryVal)
	if err != nil {
		return nil, err
	}

	getVal := func(root reflect.Value) (reflect.Value, bool) {
		fv, ok := getSafeField(root, index)
		if !ok {
			return reflect.Value{}, false
		}
		for fv.Kind() == reflect.Interface {
			if fv.IsNil() {
				return reflect.Value{}, false
			}
			fv = fv.Elem()
		}
		if isPtrField {
			if fv.Kind() != reflect.Pointer || fv.IsNil() {
				return reflect.Value{}, false
			}
			fv = fv.Elem()
		}
		return fv, true
	}

	switch cmpType.Kind() {

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		q := qv.Int()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return cmpOrderedInt(op, val.Int(), q), nil
		}, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		q := qv.Uint()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return cmpOrderedUint(op, val.Uint(), q), nil
		}, nil

	case reflect.Float32, reflect.Float64:
		q := qv.Float()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return cmpOrderedFloat(op, val.Float(), q), nil
		}, nil

	case reflect.String:
		q := qv.String()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			lv := val.String()
			switch op {
			case OpEQ, OpGT, OpGTE, OpLT, OpLTE:
				return cmpOrderedString(op, lv, q), nil
			case OpPREFIX:
				return strings.HasPrefix(lv, q), nil
			case OpSUFFIX:
				return strings.HasSuffix(lv, q), nil
			case OpCONTAINS:
				return strings.Contains(lv, q), nil
			default:
				return false, fmt.Errorf("operation %v is not supported for string", op)
			}
		}, nil

	case reflect.Bool:
		if op != OpEQ {
			return nil, fmt.Errorf("operation %v is not supported for bool", op)
		}
		q := qv.Bool()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return val.Bool() == q, nil
		}, nil
	}

	// fallback
	if op == OpEQ {
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return areEqual(val, qv), nil
		}, nil
	}

	return nil, fmt.Errorf("unsupported type %v", cmpType)
}

func compileScalarCmpFast(op Op, off uintptr, leafType reflect.Type, queryVal any) (func(unsafe.Pointer) (bool, error), error) {

	if op == OpEQ && queryVal == nil {

		if leafType.Kind() == reflect.Pointer {
			return func(ptr unsafe.Pointer) (bool, error) {
				p := *(*unsafe.Pointer)(unsafe.Add(ptr, off))
				return p == nil, nil
			}, nil
		}

		return nil, fmt.Errorf("fast nil EQ is not supported for %v", leafType)
	}

	isPtrField := leafType.Kind() == reflect.Pointer
	cmpType := leafType
	if isPtrField {
		cmpType = leafType.Elem()
	}

	qv, err := prepareValue(cmpType, queryVal)
	if err != nil {
		return nil, err
	}

	switch cmpType.Kind() {

	case reflect.Int:
		return makeFastInt[int](op, off, qv.Int(), isPtrField), nil
	case reflect.Int8:
		return makeFastInt[int8](op, off, qv.Int(), isPtrField), nil
	case reflect.Int16:
		return makeFastInt[int16](op, off, qv.Int(), isPtrField), nil
	case reflect.Int32:
		return makeFastInt[int32](op, off, qv.Int(), isPtrField), nil
	case reflect.Int64:
		return makeFastInt[int64](op, off, qv.Int(), isPtrField), nil

	case reflect.Uint:
		return makeFastUint[uint](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint8:
		return makeFastUint[uint8](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint16:
		return makeFastUint[uint16](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint32:
		return makeFastUint[uint32](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint64:
		return makeFastUint[uint64](op, off, qv.Uint(), isPtrField), nil

	case reflect.Float32:
		return makeFastFloat[float32](op, off, qv.Float(), isPtrField), nil
	case reflect.Float64:
		return makeFastFloat[float64](op, off, qv.Float(), isPtrField), nil

	case reflect.String:
		q := qv.String()
		if !isPtrField {
			return func(ptr unsafe.Pointer) (bool, error) {
				lv := *(*string)(unsafe.Add(ptr, off))
				switch op {
				case OpEQ, OpGT, OpGTE, OpLT, OpLTE:
					return cmpOrderedString(op, lv, q), nil
				case OpPREFIX:
					return strings.HasPrefix(lv, q), nil
				case OpSUFFIX:
					return strings.HasSuffix(lv, q), nil
				case OpCONTAINS:
					return strings.Contains(lv, q), nil
				default:
					return false, fmt.Errorf("operation %v is not supported for string", op)
				}
			}, nil
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**string)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			switch op {
			case OpEQ, OpGT, OpGTE, OpLT, OpLTE:
				return cmpOrderedString(op, *p, q), nil
			case OpPREFIX:
				return strings.HasPrefix(*p, q), nil
			case OpSUFFIX:
				return strings.HasSuffix(*p, q), nil
			case OpCONTAINS:
				return strings.Contains(*p, q), nil
			default:
				return false, fmt.Errorf("operation %v is not supported for string", op)
			}
		}, nil

	case reflect.Bool:
		if op != OpEQ {
			return nil, fmt.Errorf("operation %v is not supported for bool", op)
		}
		q := qv.Bool()
		if !isPtrField {
			return func(ptr unsafe.Pointer) (bool, error) {
				lv := *(*bool)(unsafe.Add(ptr, off))
				return lv == q, nil
			}, nil
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**bool)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			return *p == q, nil
		}, nil
	}

	return nil, fmt.Errorf("fast path unsupported for %v", cmpType)
}

func makeFastInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](op Op, off uintptr, qVal int64, isPtr bool) func(unsafe.Pointer) (bool, error) {
	q := T(qVal)
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := *(*T)(unsafe.Add(ptr, off))
			return cmpOrderedInt(op, lv, q), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		return cmpOrderedInt(op, *p, q), nil
	}
}

func makeFastUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](op Op, off uintptr, qVal uint64, isPtr bool) func(unsafe.Pointer) (bool, error) {
	q := T(qVal)
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := *(*T)(unsafe.Add(ptr, off))
			return cmpOrderedUint(op, lv, q), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		return cmpOrderedUint(op, *p, q), nil
	}
}

func makeFastFloat[T ~float32 | ~float64](op Op, off uintptr, qVal float64, isPtr bool) func(unsafe.Pointer) (bool, error) {
	q := T(qVal)
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := *(*T)(unsafe.Add(ptr, off))
			return cmpOrderedFloat(op, lv, q), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		return cmpOrderedFloat(op, *p, q), nil
	}
}

func compileSliceOp(op Op, index []int, fieldType reflect.Type, queryVal any) (matchFunc, error) {

	isSliceField := false

	switch fieldType.Kind() {

	case reflect.Slice:
		isSliceField = true

	case reflect.Pointer:
		if fieldType.Elem().Kind() == reflect.Slice {
			isSliceField = true
		}
	}

	switch op {

	case OpHAS, OpHASANY, OpHASNONE:
		if !isSliceField {
			return nil, fmt.Errorf("%v expects a slice (or *slice) field, got %v", op, fieldType)
		}

	case OpIN:
		if isSliceField {
			return nil, fmt.Errorf("IN expects scalar (or *scalar) field, got %v", fieldType)
		}

	default:
		return nil, fmt.Errorf("unsupported op: %v", op)
	}

	if queryVal == nil {
		return nil, fmt.Errorf("value must be a slice for %v, got nil", op)
	}

	qVal := reflect.ValueOf(queryVal)
	for qVal.Kind() == reflect.Interface || qVal.Kind() == reflect.Pointer {
		if qVal.IsNil() {
			return nil, fmt.Errorf("value must be a slice for %v, got nil", op)
		}
		qVal = qVal.Elem()
	}

	if qVal.Kind() == reflect.Slice && qVal.IsNil() {
		qVal = reflect.MakeSlice(qVal.Type(), 0, 0)
	}
	if qVal.Kind() != reflect.Slice {
		return nil, fmt.Errorf("value must be a slice for %v, got %v", op, qVal.Kind())
	}

	// HAS([])     - true
	// HASANY([])  - false
	// HASNONE([]) - true
	// IN([])      - false

	if qVal.Len() == 0 {
		switch op {
		case OpHAS:
			return func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return true, nil }, nil
		case OpHASANY:
			return func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return false, nil }, nil
		case OpHASNONE:
			return func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return true, nil }, nil
		case OpIN:
			return func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return false, nil }, nil
		}
	}

	if op == OpIN {
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			fv, ok := getSafeField(root, index)
			if !ok {
				return false, nil
			}
			for i := 0; i < qVal.Len(); i++ {
				if areEqual(fv, qVal.Index(i)) {
					return true, nil
				}
			}
			return false, nil
		}, nil
	}

	// HAS / HASANY
	return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
		fieldSlice, ok := getSafeField(root, index)
		if !ok {
			return false, nil
		}

		for fieldSlice.Kind() == reflect.Interface {
			if fieldSlice.IsNil() {
				// missing/nil slice cannot satisfy non-empty query for HAS/HASANY
				return false, nil
			}
			fieldSlice = fieldSlice.Elem()

			if fieldSlice.IsNil() {
				if op == OpHASNONE {
					// missing/nil slice always satisfies non-empty query for HASNONE
					return true, nil
				}
				// missing/nil slice cannot satisfy non-empty query for HAS/HASANY
				return false, nil
			}
			fieldSlice = fieldSlice.Elem()
		}
		if fieldSlice.Kind() == reflect.Pointer {
			if fieldSlice.IsNil() {
				// nil *slice treated as empty slice
				if op == OpHASNONE {
					return true, nil
				}
				return false, nil
			}
			fieldSlice = fieldSlice.Elem()
		}
		if fieldSlice.Kind() != reflect.Slice {
			// should not happen with compile-time checks, but keep safe
			return false, nil
		}
		if fieldSlice.IsNil() || fieldSlice.Len() == 0 {
			if op == OpHASNONE {
				// empty field slice always satisfies non-empty query
				return true, nil
			}
			// empty field slice cannot satisfy non-empty query
			return false, nil
		}

		if op == OpHAS {
			for i := 0; i < qVal.Len(); i++ {
				sLen := fieldSlice.Len()
				sItem := qVal.Index(i)
				found := false
				for j := 0; j < sLen; j++ {
					if areEqual(fieldSlice.Index(j), sItem) {
						found = true
						break
					}
				}
				if !found {
					return false, nil
				}
			}
			return true, nil
		}

		if op == OpHASANY {
			l := qVal.Len()
			if l == 0 {
				return false, nil
			}
			for i := 0; i < l; i++ {
				sLen := fieldSlice.Len()
				sItem := qVal.Index(i)
				for j := 0; j < sLen; j++ {
					if areEqual(fieldSlice.Index(j), sItem) {
						return true, nil
					}
				}
			}
		}

		if op == OpHASNONE {
			qvLen := qVal.Len()
			if qvLen == 0 {
				return true, nil
			}
			for i := 0; i < qvLen; i++ {
				sLen := fieldSlice.Len()
				sItem := qVal.Index(i)
				for j := 0; j < sLen; j++ {
					if areEqual(fieldSlice.Index(j), sItem) {
						return false, nil
					}
				}
			}
			return true, nil
		}

		return false, nil
	}, nil
}

func calcFastOffset(t reflect.Type, index []int) (off uintptr, leaf reflect.Type, ok bool) {
	for depth, i := range index {
		if t.Kind() != reflect.Struct {
			return 0, nil, false
		}
		f := t.Field(i)
		off += f.Offset
		t = f.Type
		if depth < len(index)-1 {
			if t.Kind() == reflect.Pointer || t.Kind() == reflect.Interface {
				return 0, nil, false
			}
		}
	}
	return off, t, true
}

func getSafeField(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return reflect.Value{}, false
		}
		v = v.Field(i)
	}
	return v, true
}

func prepareValue(targetType reflect.Type, raw any) (reflect.Value, error) {

	if raw == nil {
		switch targetType.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
			return reflect.Zero(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot use nil for type %v", targetType)
		}
	}

	rv := reflect.ValueOf(raw)

	for rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			switch targetType.Kind() {
			case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
				return reflect.Zero(targetType), nil
			default:
				return reflect.Value{}, fmt.Errorf("cannot use nil for type %v", targetType)
			}
		}
		rv = rv.Elem()
	}

	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			switch targetType.Kind() {
			case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
				return reflect.Zero(targetType), nil
			default:
				return reflect.Value{}, fmt.Errorf("cannot use nil for type %v", targetType)
			}
		}
		rv = rv.Elem()
	}

	if rv.Type() == targetType {
		return rv, nil
	}
	if rv.Type().ConvertibleTo(targetType) {
		return rv.Convert(targetType), nil
	}
	return reflect.Value{}, fmt.Errorf("type mismatch: field is %v, value is %v", targetType, rv.Type())
}

// typeNeedsDeepEqual returns true if comparing values of this type by reflect.Value.Equal
// would likely give "identity" semantics (e.g. pointers inside structs) instead of data semantics.
// Keep it cheap: as soon as we see pointer/interface/slice/map/func/chan we return true.
// Don't descend on pointers (avoids cycles and extra work).
func typeNeedsDeepEqual(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Func, reflect.Chan:
		return true

	case reflect.Struct:
		n := t.NumField()
		for i := 0; i < n; i++ {
			if typeNeedsDeepEqual(t.Field(i).Type) {
				return true
			}
		}
		return false

	case reflect.Array:
		return typeNeedsDeepEqual(t.Elem())

	default:
		return false
	}
}

func areEqual(v1, v2 reflect.Value) bool {

	// maybe combine interface/pointer checks into single if?

	for v1.Kind() == reflect.Interface {
		if v1.IsNil() {
			return isNilableAndNil(v2)
		}
		v1 = v1.Elem()
	}
	for v2.Kind() == reflect.Interface {
		if v2.IsNil() {
			return isNilableAndNil(v1)
		}
		v2 = v2.Elem()
	}

	if v1.Kind() == reflect.Pointer {
		if v1.IsNil() {
			return v2.Kind() == reflect.Pointer && v2.IsNil()
		}
		v1 = v1.Elem()
	}
	if v2.Kind() == reflect.Pointer {
		if v2.IsNil() {
			return false
		}
		v2 = v2.Elem()
	}

	if !v1.IsValid() || !v2.IsValid() {
		return v1.IsValid() == v2.IsValid()
	}

	// same type
	if v1.Type() == v2.Type() {

		switch v1.Kind() {
		case reflect.Struct, reflect.Array:
			// structs/arrays with pointers (or other reference types) should compare by content, not by pointer equality
			if typeNeedsDeepEqual(v1.Type()) {
				return reflect.DeepEqual(v1.Interface(), v2.Interface())
			}
		}

		if v1.Comparable() {
			return v1.Equal(v2)
		}
		return reflect.DeepEqual(v1.Interface(), v2.Interface())
	}

	// numeric cross-type fast paths
	if v1.CanInt() && v2.CanInt() {
		return v1.Int() == v2.Int()
	}
	if v1.CanUint() && v2.CanUint() {
		return v1.Uint() == v2.Uint()
	}
	if v1.CanFloat() && v2.CanFloat() {
		return v1.Float() == v2.Float()
	}

	// convertible types (looks ugly; is it even correct?)
	if v1.Type().ConvertibleTo(v2.Type()) {
		c := v1.Convert(v2.Type())
		if c.Type() == v2.Type() {
			if c.Comparable() && v2.Comparable() {
				return c.Equal(v2)
			}
			return reflect.DeepEqual(c.Interface(), v2.Interface())
		}
	}
	if v2.Type().ConvertibleTo(v1.Type()) {
		c := v2.Convert(v1.Type())
		if c.Type() == v1.Type() {
			if c.Comparable() && v1.Comparable() {
				return v1.Equal(c)
			}
			return reflect.DeepEqual(v1.Interface(), c.Interface())
		}
	}

	return false
}

func isNilableAndNil(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}

func cmpOrderedInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](op Op, lv, q T) bool {
	switch op {
	case OpEQ:
		return lv == q
	case OpGT:
		return lv > q
	case OpGTE:
		return lv >= q
	case OpLT:
		return lv < q
	case OpLTE:
		return lv <= q
	default:
		return false
	}
}

func cmpOrderedUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](op Op, lv, q T) bool {
	switch op {
	case OpEQ:
		return lv == q
	case OpGT:
		return lv > q
	case OpGTE:
		return lv >= q
	case OpLT:
		return lv < q
	case OpLTE:
		return lv <= q
	default:
		return false
	}
}

func cmpOrderedFloat[T ~float32 | ~float64](op Op, lv, q T) bool {
	switch op {
	case OpEQ:
		return lv == q
	case OpGT:
		return lv > q
	case OpGTE:
		return lv >= q
	case OpLT:
		return lv < q
	case OpLTE:
		return lv <= q
	default:
		return false
	}
}

func cmpOrderedString(op Op, lv, q string) bool {
	switch op {
	case OpEQ:
		return lv == q
	case OpGT:
		return lv > q
	case OpGTE:
		return lv >= q
	case OpLT:
		return lv < q
	case OpLTE:
		return lv <= q
	default:
		return false
	}
}
