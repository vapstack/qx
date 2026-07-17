package qx

import (
	"reflect"
	"unsafe"
)

// Clone returns a deep structural copy of qx.
// The cloned query does not reuse source slices or mutable literal values.
func Clone(qx *QX) *QX {
	return qx.Clone()
}

// Clone returns a deep structural copy of qx.
// The cloned query does not reuse source slices or mutable literal values.
func (qx *QX) Clone() *QX {
	if qx == nil {
		return nil
	}

	var cloner valueCloner
	dst := new(QX)
	*dst = *qx
	cloner.cloneExpr(&qx.Filter, &dst.Filter)
	dst.Projection = cloner.cloneExprSlice(qx.Projection)

	if qx.Metadata != nil {
		dst.Metadata = make([]MetaEntry, len(qx.Metadata))
		copy(dst.Metadata, qx.Metadata)
		for i := range qx.Metadata {
			dst.Metadata[i].Value = cloner.cloneValue(qx.Metadata[i].Value)
		}
	}

	if qx.Reduction != nil {
		dst.Reduction = new(Reduction)
		*dst.Reduction = *qx.Reduction
		dst.Reduction.Group = cloner.cloneExprSlice(qx.Reduction.Group)
		dst.Reduction.Metrics = cloner.cloneExprSlice(qx.Reduction.Metrics)
		cloner.cloneExpr(&qx.Reduction.Having, &dst.Reduction.Having)
	}

	if qx.Order != nil {
		dst.Order = make([]Order, len(qx.Order))
		copy(dst.Order, qx.Order)
		for i := range qx.Order {
			cloner.cloneExpr(&qx.Order[i].By, &dst.Order[i].By)
		}
	}

	return dst
}

const cloneInlineVisitCount = 16

type valueCloner struct {
	inline   [cloneInlineVisitCount]cloneSeen
	count    int
	overflow map[cloneVisit]reflect.Value
}

type cloneSeen struct {
	visit cloneVisit
	value reflect.Value
}

type cloneVisit struct {
	typ reflect.Type
	ptr uintptr
	len int
}

func (cloner *valueCloner) cloneExpr(src *Expr, dst *Expr) {
	*dst = *src
	cloner.cloneExprData(src, dst)
}

func (cloner *valueCloner) cloneExprData(src *Expr, dst *Expr) {
	if src.Args != nil {
		dst.Args = cloner.cloneExprSlice(src.Args)
	}
	if src.Value != nil {
		dst.Value = cloner.cloneValue(src.Value)
	}
}

func (cloner *valueCloner) cloneExprSlice(src []Expr) []Expr {
	if src == nil {
		return nil
	}

	dst := make([]Expr, len(src))
	copy(dst, src)

	for i := range dst {
		cloner.cloneExprData(&src[i], &dst[i])
	}
	return dst
}

func (cloner *valueCloner) findSeen(visit cloneVisit) (reflect.Value, bool) {
	if cloner.overflow != nil {
		dst, ok := cloner.overflow[visit]
		return dst, ok
	}
	for i := 0; i < cloner.count; i++ {
		if cloner.inline[i].visit == visit {
			return cloner.inline[i].value, true
		}
	}
	return reflect.Value{}, false
}

func (cloner *valueCloner) remember(visit cloneVisit, dst reflect.Value) {
	if cloner.overflow != nil {
		cloner.overflow[visit] = dst
		return
	}
	if cloner.count < len(cloner.inline) {
		cloner.inline[cloner.count] = cloneSeen{visit: visit, value: dst}
		cloner.count++
		return
	}

	cloner.overflow = make(map[cloneVisit]reflect.Value, cloneInlineVisitCount*2)
	for i := 0; i < cloner.count; i++ {
		seen := cloner.inline[i]
		cloner.overflow[seen.visit] = seen.value
	}
	cloner.overflow[visit] = dst
}

func (cloner *valueCloner) cloneValue(src any) any {
	if src == nil {
		return nil
	}

	switch value := src.(type) {
	case []byte:
		return clonePrimitiveSlice(cloner, value)
	case []string:
		return clonePrimitiveSlice(cloner, value)
	case []int:
		return clonePrimitiveSlice(cloner, value)
	case []float64:
		return clonePrimitiveSlice(cloner, value)
	case []bool:
		return clonePrimitiveSlice(cloner, value)
	case []any:
		return cloner.cloneAnySlice(value)
	case map[string]any:
		return cloner.cloneStringAnyMap(value)
	case map[string]string:
		return clonePrimitiveMap(cloner, value)
	case map[string][]byte:
		return cloneStringSliceMap(cloner, value)
	case map[string][]string:
		return cloneStringSliceMap(cloner, value)
	case map[string][]int:
		return cloneStringSliceMap(cloner, value)
	}

	rv := reflect.ValueOf(src)
	if !typeNeedsDeepClone(rv.Type()) {
		return src
	}

	return cloner.cloneReflectDeep(rv).Interface()
}

func clonePrimitiveSlice[T any](cloner *valueCloner, src []T) any {
	if src == nil {
		return []T(nil)
	}

	visit := cloneVisit{
		typ: reflect.TypeFor[[]T](),
		ptr: uintptr(unsafe.Pointer(unsafe.SliceData(src))),
		len: len(src),
	}
	if seen, ok := cloner.findSeen(visit); ok {
		return seen.Interface()
	}

	dst := make([]T, len(src))
	copy(dst, src)
	value := reflect.ValueOf(dst)
	cloner.remember(visit, value)
	return value.Interface()
}

func clonePrimitiveMap[V any](cloner *valueCloner, src map[string]V) any {
	if src == nil {
		return map[string]V(nil)
	}

	visit := cloneVisit{typ: reflect.TypeFor[map[string]V](), ptr: reflect.ValueOf(src).Pointer()}
	if seen, ok := cloner.findSeen(visit); ok {
		return seen.Interface()
	}

	dst := make(map[string]V, len(src))
	cloner.remember(visit, reflect.ValueOf(dst))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func (cloner *valueCloner) cloneAnySlice(src []any) any {
	if src == nil {
		return []any(nil)
	}

	visit := cloneVisit{
		typ: reflect.TypeFor[[]any](),
		ptr: uintptr(unsafe.Pointer(unsafe.SliceData(src))),
		len: len(src),
	}
	if seen, ok := cloner.findSeen(visit); ok {
		return seen.Interface()
	}

	dst := make([]any, len(src))
	value := reflect.ValueOf(dst)
	cloner.remember(visit, value)
	for i := range src {
		dst[i] = cloner.cloneValue(src[i])
	}
	return value.Interface()
}

func (cloner *valueCloner) cloneStringAnyMap(src map[string]any) any {
	if src == nil {
		return map[string]any(nil)
	}

	visit := cloneVisit{typ: reflect.TypeFor[map[string]any](), ptr: reflect.ValueOf(src).Pointer()}
	if seen, ok := cloner.findSeen(visit); ok {
		return seen.Interface()
	}

	dst := make(map[string]any, len(src))
	cloner.remember(visit, reflect.ValueOf(dst))
	for key, value := range src {
		dst[key] = cloner.cloneValue(value)
	}
	return dst
}

func cloneStringSliceMap[T any](cloner *valueCloner, src map[string][]T) any {
	if src == nil {
		return map[string][]T(nil)
	}

	visit := cloneVisit{typ: reflect.TypeFor[map[string][]T](), ptr: reflect.ValueOf(src).Pointer()}
	if seen, ok := cloner.findSeen(visit); ok {
		return seen.Interface()
	}

	dst := make(map[string][]T, len(src))
	cloner.remember(visit, reflect.ValueOf(dst))
	for key, value := range src {
		dst[key] = clonePrimitiveSlice(cloner, value).([]T)
	}
	return dst
}

func (cloner *valueCloner) cloneReflect(src reflect.Value) reflect.Value {
	if !src.IsValid() {
		return src
	}

	if !typeNeedsDeepClone(src.Type()) {
		return src
	}
	return cloner.cloneReflectDeep(src)
}

func (cloner *valueCloner) cloneReflectDeep(src reflect.Value) reflect.Value {
	typ := src.Type()

	switch typ.Kind() {
	case reflect.Interface:
		if src.IsNil() {
			return reflect.Zero(typ)
		}

		dst := reflect.New(typ).Elem()
		dst.Set(cloner.cloneReflect(src.Elem()))
		return dst

	case reflect.Pointer:
		if src.IsNil() {
			return reflect.Zero(typ)
		}

		key := cloneVisit{typ: typ, ptr: src.Pointer()}
		if dst, ok := cloner.findSeen(key); ok {
			return dst
		}

		dst := reflect.New(typ.Elem())
		cloner.remember(key, dst)
		cloner.cloneReflectInto(src.Elem(), dst.Elem())
		return dst

	case reflect.Slice:
		if src.IsNil() {
			return reflect.Zero(typ)
		}

		key := cloneVisit{typ: typ, ptr: src.Pointer(), len: src.Len()}
		if dst, ok := cloner.findSeen(key); ok {
			return dst
		}

		dst := reflect.MakeSlice(typ, src.Len(), src.Len())
		cloner.remember(key, dst)

		if !typeNeedsDeepClone(typ.Elem()) {
			reflect.Copy(dst, src)
			return dst
		}

		for i := range src.Len() {
			cloner.cloneReflectDeepInto(src.Index(i), dst.Index(i))
		}
		return dst

	case reflect.Array:
		dst := reflect.New(typ).Elem()
		cloner.cloneReflectInto(src, dst)
		return dst

	case reflect.Map:
		if src.IsNil() {
			return reflect.Zero(typ)
		}

		key := cloneVisit{typ: typ, ptr: src.Pointer()}
		if dst, ok := cloner.findSeen(key); ok {
			return dst
		}

		dst := reflect.MakeMapWithSize(typ, src.Len())
		cloner.remember(key, dst)

		keyNeedsDeepClone := typeNeedsDeepClone(typ.Key())
		elemNeedsDeepClone := typeNeedsDeepClone(typ.Elem())

		iter := src.MapRange()
		for iter.Next() {
			mapKey := iter.Key()
			if keyNeedsDeepClone {
				mapKey = cloner.cloneReflectDeep(mapKey)
			}

			mapValue := iter.Value()
			if elemNeedsDeepClone {
				mapValue = cloner.cloneReflectDeep(mapValue)
			}

			dst.SetMapIndex(mapKey, mapValue)
		}
		return dst

	case reflect.Struct:
		dst := reflect.New(typ).Elem()
		cloner.cloneReflectInto(src, dst)
		return dst

	default:
		return src
	}
}

func (cloner *valueCloner) cloneReflectInto(src, dst reflect.Value) {
	if !src.IsValid() {
		return
	}

	typ := src.Type()
	if !typeNeedsDeepClone(typ) {
		setReflectValue(dst, src)
		return
	}
	cloner.cloneReflectDeepInto(src, dst)
}

func (cloner *valueCloner) cloneReflectDeepInto(src, dst reflect.Value) {
	typ := src.Type()
	switch typ.Kind() {
	case reflect.Array:
		for i := range src.Len() {
			cloner.cloneReflectDeepInto(src.Index(i), dst.Index(i))
		}

	case reflect.Struct:
		setReflectValue(dst, src)
		for i := range src.NumField() {
			if !typeNeedsDeepClone(typ.Field(i).Type) {
				continue
			}
			cloner.cloneReflectDeepInto(src.Field(i), dst.Field(i))
		}

	default:
		setReflectValue(dst, cloner.cloneReflectDeep(src))
	}
}

func typeNeedsDeepClone(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String,
		reflect.Func,
		reflect.Chan,
		reflect.UnsafePointer:
		return false

	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
		return true

	case reflect.Array:
		return typeNeedsDeepClone(typ.Elem())

	case reflect.Struct:
		for i := range typ.NumField() {
			if typeNeedsDeepClone(typ.Field(i).Type) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func setReflectValue(dst, src reflect.Value) {
	if dst.CanSet() {
		dst.Set(src)
		return
	}

	reflect.NewAt(dst.Type(), unsafe.Pointer(dst.UnsafeAddr())).Elem().Set(src)
}
