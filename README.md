# qx

[![GoDoc](https://godoc.org/github.com/vapstack/qx?status.svg)](https://godoc.org/github.com/vapstack/qx)
[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/vapstack/qx/master/LICENSE)

`qx` is a small, focused package for describing query expressions in a structured,
type-agnostic form.  
Its primary purpose is to act as a minimal **query / filter AST** (expressions, ordering, pagination)
that can be consumed by other packages (databases, indexes, storage engines).

As a secondary feature, it also provides a **high-performance in-memory matcher**:
a fast evaluator that applies expressions to Go structs using reflection and unsafe optimizations.  
This is intended for cases where filtering happens in memory without an index.

You can use the expression layer without ever touching the matcher.

---

## Query Expression

The expression language is intentionally simple and explicit.
It represents filters as a tree of expressions combined with logical operators.

### Expressions

An `Expr` represents either:
- a comparison on a field (`EQ`, `GT`, `IN`, `HAS`, ...)
- a logical combination of other expressions (`AND`, `OR`)

```go
type Expr struct {
    Op       Op
    Not      bool
    Field    string
    Value    any
    Operands []Expr
}
```

### Supported operations

- Scalar comparisons: `EQ`, `GT`, `GTE`, `LT`, `LTE`
- Set / slice operations:
  - `IN` — field value is contained in a provided slice
  - `HAS` — slice field contains all provided values
  - `HASANY` — slice field contains any provided value
- Logical operations: `AND`, `OR`, `NOT`

The semantics are intentionally close to what most query engines provide.

Expressions can be constructed using helper functions with the same names:
```
qx.EQ("age", 30)
qx.GT("score", 100)
qx.IN("status", []string{"active", "pending"})

qx.AND(
    qx.EQ("country", "US"),
    qx.GTE("age", 20),
)

qx.NE("deleted", true)
```

### Queries

`QX` combines:
- a root expression
- optional ordering rules
- pagination (offset / limit)
- an optional cache key

```go
type QX struct {
    Key    string
    Expr   Expr
    Order  []Order
    Offset uint64
    Limit  uint64
}
```

This makes QX suitable as a transport or intermediate representation between
systems.

```go
q := qx.Query(
    qx.EQ("status", "active"),
    qx.GTE("age", 21),
).
By("created_at", DESC).
Page(1, 50)
```

See the [GoDoc](https://godoc.org/github.com/vapstack/qx) for a complete API reference.

---

## In-Memory Matcher

`qx` also provides an optional in-memory matcher that can evaluate expressions against Go structs.

This is useful when:
- there is no index
- data is already in memory
- expressions must be evaluated many times

```go
m := qx.MatcherFor[User]()
```

The matcher:
- inspects the struct type once
- caches reflection metadata
- supports field lookup by Go name and struct tags (json, db, ...)

### One-shot matching

For occasional checks:
```go
match, err := qx.Match(user, EQ("age", 30))
```

or, using a matcher instance:
```go
m, err := qx.MatcherFor[User]()
// ...
match, err := m.Match(user, EQ("age", 30))
```

### Compiled predicates (recommended)

For repeated evaluations of the same expression:

```go
check, err := m.Compile(
    qx.AND(
        GTE("age", 18),
        EQ("active", true),
    )
)
// ...
match, err := check(v)
```

Compiled predicates:
- avoid repeated expression traversal
- use fast paths when possible
- can be safely reused across calls

### Value handling

Matcher methods accept:
- struct values (T)
- pointers to structs (*T)
- interfaces wrapping either

Passing a pointer enables additional fast paths based on unsafe offsets.
Passing `nil` always results in `(false, nil)`.

### Equality semantics

Equality is defined in terms of `data`, not pointer identity:
- pointers are compared by the values they point to
- interfaces are unwrapped
- structs are compared field-by-field
- slices use element-wise comparison where applicable

This makes the matcher suitable for “document-style” filtering.

### Performance characteristics

The matcher is designed with performance in mind:
- No allocations on the fast path for scalar comparisons
- Optional unsafe field access when values are passed as *T
- Reflection metadata cached per type
- Expression compilation amortizes cost over repeated runs

Typical performance characteristics:
- Simple scalar predicates: tens of nanoseconds
- Mixed predicates: tens to hundreds of nanoseconds
- Complex structures (slices, nested structs): higher cost, still allocation-aware
