# qx

[![Go Reference](https://pkg.go.dev/badge/github.com/vapstack/qx.svg)](https://pkg.go.dev/github.com/vapstack/qx)
[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

Backend-agnostic query builder, filter AST and query IR for Go.\
Initially, QX name was from **Q**uery e**X**pression.

It represents query intent as data instead of backend-specific syntax.
`QX` can carry filters, scalar expressions, grouping, aggregates, ordering,
pagination, projection, and caller-defined metadata.

Another layer can validate, normalize, serialize and translate it into SQL,
a document-store query, a search request, or some other backend-specific form.

It does not execute queries.\
It also **does not model joins or subqueries**.

## Use cases

- When it is not known which backend will execute the query.
- Application code should not depend on a PostgreSQL, ClickHouse, Mongo,
  Elasticsearch or other specific builder or syntax.
- The same filtering, ordering, and pagination contract must work across
  multiple backends.
- Queries arrive from HTTP, RPC, config, or scheduled jobs and need to be
  represented in one common format.
- Queries should cross package or service boundaries before translation happens.
- Validation and normalization should happen before a backend-specific layer.

QX is not the right abstraction when:

- the query model depends on joins or subqueries
- the public API is tied to one concrete backend and its native query language

## Quick start

For a plain filtered list, build a `QX` with predicates, sort order, and a window:

```go
q := qx.
    Query(
        qx.EQ("tenant_id", "t-1"),
        qx.EQ("status", "paid"),
        qx.GTE("created_at", "2026-04-01"),
    ).
    Sort("created_at", qx.DESC).
    Limit(50)
```

This describes "paid orders for tenant `t-1`, newest first, first 50 rows".
The result is still plain query data. Translation into SQL or another backend
format happens elsewhere.

## Query model

```text
filter -> reduction -> order -> window -> projection
```

- **Filter**: predicate tree evaluated against source fields
- **Reduction**: optional grouping, metrics, and post-reduction filtering
- **Order**: sort rules before or after reduction, depending on the query shape
- **Window**: offset and limit
- **Projection**: final output shape

Without a reduction stage, the query behaves like a filtered list query.\
With a reduction stage, the query behaves like a grouped or aggregate query.

## Builder and IR

Package has a fluent builder with chaining API.
There is no build phase, objects can be serialized directly.

The main starting points are:

- filter-first: `Query`, `Where`
- reduction-first: `Group`, `GroupBy`, `Metrics`, `Aggregate`
- projection-first: `Select`, `Fields`, `SelectOut`, `FieldsOut`

All of them produce `*QX` object.

Predicate/function helpers produce an `Expr`.
There is only one expression node type, with four forms:

- `REF(name)` for source fields
- `OUT(name)` for reduction outputs
- `LIT(value)` for literals
- `OP(name, args...)` for named operations

Helpers such as `EQ`, `SUM`, `LOWER`, `DATETRUNC`, and `AND` all produce
ordinary `Expr` values. There is no separate DSL or special node type for
built-in helpers.

Builder conventions:

- repeated `Where` calls append predicates with `AND`
- repeated `Having` calls append predicates with `AND`
- in most helpers, the first `string` argument is shorthand for `REF(name)`
- in helpers that accept plain values after the first argument, **non-Expr**
  values become `LIT(value)`
- aliases are attached with `AS(...)`
- computed metric and projection items should have stable output names

Examples:

```go
qx.EQ("status", "paid") // EQ(REF("status"), LIT("paid"))

qx.EQ(qx.LOWER("email"), "ops@example.com")
qx.DATEDIFF("created_at", qx.NOW(), "day")
qx.CONCAT(qx.REF("country"), qx.LIT("-"), qx.REF("city"))
```

## Filters and expressions

Filters are explicit expression trees.
Scalar helpers can be composed anywhere an `Expr` is accepted.

```go
q := qx.
    Query(
        qx.EQ("tenant_id", "t-1"),
    ).
    Where(
        qx.OR(
            qx.EQ("status", "paid"),
            qx.EQ("status", "refunded"),
        ),
        qx.GTE("total_amount", 100),
        qx.EQ("deleted_at", nil), 
        qx.EQ(
            qx.NULLIF(qx.TRIM("promo_code"), ""),
            "spring-2026",
        ),
    )
```

This is still backend-neutral. The query can be built in application code,
received from transport input, or assembled from other query fragments before it
is translated.

## Grouping, metrics, and reduction outputs

For grouped or aggregate results, add a reduction stage:

```go
q := qx.
    Query(
        qx.EQ("tenant_id", "t-1"),
        qx.EQ("status", "paid"),
        qx.GTE("created_at", "2026-04-01"),
        qx.LT("created_at", "2026-05-01"),
    ).
    Group("country").
    GroupBy(qx.DATETRUNC("created_at", "day").AS("day")).
    Metrics(
        qx.ROWCOUNT().AS("orders"),
        qx.SUM("total_amount").AS("revenue"),
        qx.AVG("total_amount").AS("avg_order_value"),
    ).
    Having(
        qx.GTE(qx.OUT("orders"), 20),
        qx.GTE(qx.OUT("revenue"), 10000),
    ).
    SortOut("revenue", qx.DESC).
    SortOut("day")
```

- `Group("country")` is shorthand for grouping by a source field
- `GroupBy` accepts arbitrary grouping expressions
- `Metrics` defines aggregate outputs
- `Having` filters after reduction
- `OUT("name")` refers to a grouping or metric output by name
- after reduction, `Having`, ordering, and projection should refer to outputs
  through `OUT(...)`
- grouping without metrics still has meaning: it acts like a distinct-style
  query over the grouping keys

## Projection, ordering, and pagination

Without an explicit projection, the query keeps its natural output shape:

- before reduction: source rows or documents
- after reduction: grouping keys and metric outputs

Use projection when the caller needs a specific final shape:

```go
q := qx.
    Query(
        qx.EQ("tenant_id", "t-1"),
        qx.EQ("status", "paid"),
    ).
    Fields("id", "customer_email").
    FieldsExpr(
        qx.LOWER("customer_email").AS("customer_email_lower"),
    ).
    Sort("created_at", qx.DESC).
    Page(1, 50)
```

After reduction, projection should use reduction outputs rather than source fields:

```go
q := qx.
    Group("country").
    Metrics(
        qx.SUM("total_amount").AS("revenue"),
        qx.ROWCOUNT().AS("orders"),
    ).
    FieldsOut("country", "revenue").
    FieldsExpr(
        qx.LOWER(qx.OUT("country")).AS("country_lower"),
        qx.IF(qx.GT(qx.OUT("orders"), 0), qx.OUT("orders"), qx.LIT(0)).AS("orders_nonzero"),
    ).
    SortOut("revenue", qx.DESC)
```

Notes:

- `q.Select` and `q.Fields` append source-field references
- `q.SelectOut` and `q.FieldsOut` append reduction-output references
- `q.SelectExpr` and `q.FieldsExpr` append arbitrary expressions
- `q.Sort` orders by a source field
- `q.SortOut` orders by a reduction output
- `q.SortBy` orders by an arbitrary expression
- `q.Page(pageNum, perPage)` is 1-based
- `q.Limit` and `q.Offset` are available when the caller already speaks offsets

## Validation, normalization, and cloning

Package includes structural utilities for working with query values:

- `Validate` checks node shape, helper arity, stage placement rules, output
  names, and duplicate projection or reduction outputs
- `Validate` returns the first failure as `*ValidationError` with
  `Code` and `Path`, for example `filter.args[0]`,
  `reduction.metrics[1].args[0]`, or `order[0].by`
- `Normalize` rewrites only cheap structural cases: it flattens nested `AND`
  and `OR`, and collapses single-item logical groups
- `Normalize` is not a query optimizer and does not reorder predicates or do
  backend-specific rewrites
- `Clone` returns a deep structural copy, including mutable literal payloads
- `UsedFields` returns a de-duplicated list of source fields referenced
  anywhere in the query; `OUT` references are excluded

Validation catches:

- malformed expression nodes
- aggregate operations used in `Filter`, `Group`, or pre-reduction order
- `OUT` used before reduction
- unknown output names after reduction
- post-reduction projection that still uses `REF`
- projection items without stable output names

## JSON and metadata

`QX` and `Expr` implement JSON marshal and unmarshal to speed up some encoding/decoding paths.

Metadata can travel with the query itself:

```go
q := qx.
    Query(
        qx.EQ("tenant_id", "t-1"),
        qx.EQ("status", "paid"),
    ).
    Meta("trace.id", "req-42").
    Meta("transport", map[string]any{
        "source": "rpc",
        "actor":  "ops-ui",
    })
```

- entries are stored in insertion order
- `Meta(key, value)` replaces an existing entry with the same key
- `MetaValue(key)` reads a value by key
- `DeleteMeta(keys...)` removes metadata entries
- metadata is serialized under the top-level `meta` field
- metadata is preserved by `Clone`, `Normalize`, `Validate`, and JSON round-trips

## Custom operations

`OP(name, args...)` allows to carry backend-specific or application-specific 
operations inside the same IR:

```go
q := qx.Query(
    qx.OP("glob", qx.LOWER("customer_email"), qx.LIT("*@example.com")),
)
```

Unknown operation names are allowed by `Validate` as long as the expression is
otherwise well-formed. Support is a translator policy decision, not a property
of the IR itself.

## Helper overview

Predicate and logical helpers:

- comparisons:\
  `EQ`, `NE`, `NOTEQ`, `GT`, `GTE`, `LT`, `LTE`, `BETWEEN`
- sets and null checks:\
  `IN`, `NOTIN`, `HASALL`, `HASANY`, `HASNONE`, `EXISTS`, `MISSING`, `ISNULL`, `NOTNULL`
- string predicates:\
  `PREFIX`, `SUFFIX`, `CONTAINS`, `LIKE`, `NOTLIKE`, `ILIKE`, `NOTILIKE`, `MATCHES`
- logical composition:\
  `AND`, `OR`, `NOT`

Scalar helpers:

- string and collection:\
  `LEN`, `POS`, `LOWER`, `UPPER`, `TRIM`, `REPLACE`,
  `SPLIT`, `SUBSTR`, `CONCAT`
- temporal:\
  `DATETRUNC`, `EXTRACT`, `NOW`, `DATEADD`, `DATEDIFF`
- numeric:\
  `ABS`, `ROUND`, `FLOOR`, `CEIL`
- generic:\
  `COALESCE`, `NULLIF`, `IF`, `DISTINCT`, `RANK`, `GREATEST`, `LEAST`
- arithmetic:\
  `ADD`, `SUB`, `MUL`, `DIV`, `MOD`

Aggregate helpers:
- `COUNT`, `ROWCOUNT`, `SUM`, `AVG`, `MIN`, `MAX`

See [GoDoc](https://pkg.go.dev/github.com/vapstack/qx) for the full API reference
