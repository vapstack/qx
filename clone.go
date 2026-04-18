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

	cloner := valueCloner{
		seen: make(map[cloneVisit]reflect.Value, 8),
	}

	dst := &QX{
		Filter:     cloner.cloneExpr(qx.Filter),
		Window:     qx.Window,
		Projection: cloner.cloneExprSlice(qx.Projection),
	}

	if qx.Metadata != nil {
		dst.Metadata = make([]MetaEntry, len(qx.Metadata))
		for i := range qx.Metadata {
			dst.Metadata[i] = MetaEntry{
				Key:   qx.Metadata[i].Key,
				Value: cloner.cloneValue(qx.Metadata[i].Value),
			}
		}
	}

	if qx.Reduction != nil {
		dst.Reduction = &Reduction{
			Group:   cloner.cloneExprSlice(qx.Reduction.Group),
			Metrics: cloner.cloneExprSlice(qx.Reduction.Metrics),
			Having:  cloner.cloneExpr(qx.Reduction.Having),
		}
	}

	if qx.Order != nil {
		dst.Order = make([]Order, len(qx.Order))
		for i := range qx.Order {
			dst.Order[i] = Order{
				By:   cloner.cloneExpr(qx.Order[i].By),
				Desc: qx.Order[i].Desc,
			}
		}
	}

	return dst
}

type valueCloner struct {
	seen map[cloneVisit]reflect.Value
}

type cloneVisit struct {
	typ reflect.Type
	ptr uintptr
	len int
}

func (cloner *valueCloner) cloneExpr(expr Expr) Expr {
	dst := expr

	if expr.Args != nil {
		dst.Args = cloner.cloneExprSlice(expr.Args)
	}

	if expr.Value != nil {
		dst.Value = cloner.cloneValue(expr.Value)
	}

	return dst
}

func (cloner *valueCloner) cloneExprSlice(src []Expr) []Expr {
	if src == nil {
		return nil
	}

	dst := make([]Expr, len(src))
	copy(dst, src)

	for i := range dst {
		if dst[i].Args != nil {
			dst[i].Args = cloner.cloneExprSlice(dst[i].Args)
		}
		if dst[i].Value != nil {
			dst[i].Value = cloner.cloneValue(dst[i].Value)
		}
	}
	return dst
}

func (cloner *valueCloner) cloneValue(src any) any {
	if src == nil {
		return nil
	}

	rv := reflect.ValueOf(src)
	if !typeNeedsDeepClone(rv.Type()) {
		return src
	}

	return cloner.cloneReflect(rv).Interface()
}

func (cloner *valueCloner) cloneReflect(src reflect.Value) reflect.Value {
	if !src.IsValid() {
		return src
	}

	typ := src.Type()
	if !typeNeedsDeepClone(typ) {
		return src
	}

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
		if dst, ok := cloner.seen[key]; ok {
			return dst
		}

		dst := reflect.New(typ.Elem())
		cloner.seen[key] = dst
		setReflectValue(dst.Elem(), cloner.cloneReflect(src.Elem()))
		return dst

	case reflect.Slice:
		if src.IsNil() {
			return reflect.Zero(typ)
		}

		key := cloneVisit{typ: typ, ptr: src.Pointer(), len: src.Len()}
		if dst, ok := cloner.seen[key]; ok {
			return dst
		}

		dst := reflect.MakeSlice(typ, src.Len(), src.Len())
		cloner.seen[key] = dst

		if !typeNeedsDeepClone(typ.Elem()) {
			reflect.Copy(dst, src)
			return dst
		}

		for i := range src.Len() {
			setReflectValue(dst.Index(i), cloner.cloneReflect(src.Index(i)))
		}
		return dst

	case reflect.Array:
		dst := reflect.New(typ).Elem()

		if !typeNeedsDeepClone(typ.Elem()) {
			dst.Set(src)
			return dst
		}

		for i := range src.Len() {
			setReflectValue(dst.Index(i), cloner.cloneReflect(src.Index(i)))
		}
		return dst

	case reflect.Map:
		if src.IsNil() {
			return reflect.Zero(typ)
		}

		key := cloneVisit{typ: typ, ptr: src.Pointer()}
		if dst, ok := cloner.seen[key]; ok {
			return dst
		}

		dst := reflect.MakeMapWithSize(typ, src.Len())
		cloner.seen[key] = dst

		keyNeedsDeepClone := typeNeedsDeepClone(typ.Key())
		elemNeedsDeepClone := typeNeedsDeepClone(typ.Elem())

		iter := src.MapRange()
		for iter.Next() {
			mapKey := iter.Key()
			if keyNeedsDeepClone {
				mapKey = cloner.cloneReflect(mapKey)
			}

			mapValue := iter.Value()
			if elemNeedsDeepClone {
				mapValue = cloner.cloneReflect(mapValue)
			}

			dst.SetMapIndex(mapKey, mapValue)
		}
		return dst

	case reflect.Struct:
		dst := reflect.New(typ).Elem()
		setReflectValue(dst, src)

		for i := range src.NumField() {
			field := typ.Field(i)
			if !typeNeedsDeepClone(field.Type) {
				continue
			}
			setReflectValue(dst.Field(i), cloner.cloneReflect(src.Field(i)))
		}
		return dst

	default:
		return src
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
