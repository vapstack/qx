package qx

import (
	"reflect"
	"testing"
)

type cloneLeaf struct {
	Name string
	Tags []string
}

type clonePayload struct {
	Labels []string
	Matrix [][]int
	Meta   map[string][]byte
	Child  *cloneLeaf
	Fixed  [2][]string
}

func TestCloneNil(t *testing.T) {
	if Clone(nil) != nil {
		t.Fatalf("Clone(nil) must return nil")
	}

	var q *QX
	if q.Clone() != nil {
		t.Fatalf("(*QX)(nil).Clone() must return nil")
	}
}

func TestCloneDeepCopy(t *testing.T) {
	filterPayload := clonePayload{
		Labels: []string{"draft", "paid"},
		Matrix: [][]int{{1, 2}, {3, 4}},
		Meta: map[string][]byte{
			"raw": {1, 2, 3},
		},
		Child: &cloneLeaf{
			Name: "filter-child",
			Tags: []string{"child", "primary"},
		},
		Fixed: [2][]string{
			{"north", "south"},
			{"east", "west"},
		},
	}

	groupValue := map[string][]int{
		"a": {1, 2},
		"b": {3, 4},
	}

	metricValue := &clonePayload{
		Labels: []string{"metric"},
		Matrix: [][]int{{10, 20}},
		Meta: map[string][]byte{
			"metric": {9, 8, 7},
		},
		Child: &cloneLeaf{
			Name: "metric-child",
			Tags: []string{"metric-tag"},
		},
		Fixed: [2][]string{
			{"m1"},
			{"m2"},
		},
	}

	havingValue := &cloneLeaf{
		Name: "having-child",
		Tags: []string{"having"},
	}

	orderValue := map[string][]string{
		"bucket": {"gold", "silver"},
	}

	projectionValue := map[string][]int{
		"projection": {5, 6, 7},
	}

	metadataValue := map[string][]int{
		"ids": {7, 8, 9},
	}

	metadataTags := []string{"portable", "ui"}

	q := &QX{
		Filter: Expr{
			Kind: KindOP,
			Name: OpAND,
			Args: []Expr{
				{Kind: KindLIT, Value: filterPayload},
				{
					Kind: KindOP,
					Name: "wrap",
					Args: []Expr{
						{Kind: KindLIT, Value: groupValue},
					},
				},
			},
		},
		Reduction: &Reduction{
			Group: []Expr{
				{Kind: KindLIT, Value: groupValue},
			},
			Metrics: []Expr{
				{Kind: KindLIT, Value: metricValue},
			},
			Having: Expr{
				Kind: KindOP,
				Name: OpOR,
				Args: []Expr{
					{Kind: KindLIT, Value: havingValue},
				},
			},
		},
		Order: []Order{
			{
				By:   Expr{Kind: KindLIT, Value: orderValue},
				Desc: true,
			},
		},
		Window: Window{Offset: 7, Limit: 11},
		Projection: []Expr{
			{Kind: KindLIT, Value: projectionValue},
		},
		Metadata: []MetaEntry{
			{Key: "transport", Value: metadataValue},
			{Key: "tags", Value: metadataTags},
		},
	}

	clone := Clone(q)

	if clone == q {
		t.Fatalf("Clone() must allocate a new query")
	}
	if clone.Reduction == q.Reduction {
		t.Fatalf("Clone() must allocate a new reduction")
	}
	if !reflect.DeepEqual(clone, q) {
		t.Fatalf("Clone() mismatch:\n got: %#v\nwant: %#v", clone, q)
	}

	assertNotSameSliceData(t, q.Filter.Args, clone.Filter.Args, "filter args")
	assertNotSameSliceData(t, q.Filter.Args[1].Args, clone.Filter.Args[1].Args, "nested filter args")
	assertNotSameSliceData(t, q.Reduction.Group, clone.Reduction.Group, "reduction group")
	assertNotSameSliceData(t, q.Reduction.Metrics, clone.Reduction.Metrics, "reduction metrics")
	assertNotSameSliceData(t, q.Reduction.Having.Args, clone.Reduction.Having.Args, "having args")
	assertNotSameSliceData(t, q.Order, clone.Order, "order")
	assertNotSameSliceData(t, q.Projection, clone.Projection, "projection")
	assertNotSameSliceData(t, q.Metadata, clone.Metadata, "metadata")

	origFilter := q.Filter.Args[0].Value.(clonePayload)
	clonedFilter := clone.Filter.Args[0].Value.(clonePayload)
	assertNotSameSliceData(t, origFilter.Labels, clonedFilter.Labels, "filter payload labels")
	assertNotSameSliceData(t, origFilter.Matrix, clonedFilter.Matrix, "filter payload matrix")
	assertNotSameSliceData(t, origFilter.Matrix[0], clonedFilter.Matrix[0], "filter payload matrix row")
	assertNotSameMapData(t, origFilter.Meta, clonedFilter.Meta, "filter payload meta")
	assertNotSameSliceData(t, origFilter.Meta["raw"], clonedFilter.Meta["raw"], "filter payload meta raw")
	if clonedFilter.Child == origFilter.Child {
		t.Fatalf("filter payload child pointer must not alias source")
	}
	assertNotSameSliceData(t, origFilter.Child.Tags, clonedFilter.Child.Tags, "filter payload child tags")
	assertNotSameSliceData(t, origFilter.Fixed[0], clonedFilter.Fixed[0], "filter payload fixed[0]")
	assertNotSameSliceData(t, origFilter.Fixed[1], clonedFilter.Fixed[1], "filter payload fixed[1]")

	origGroup := q.Reduction.Group[0].Value.(map[string][]int)
	clonedGroup := clone.Reduction.Group[0].Value.(map[string][]int)
	assertNotSameMapData(t, origGroup, clonedGroup, "group value")
	assertNotSameSliceData(t, origGroup["a"], clonedGroup["a"], "group value a")
	assertNotSameSliceData(t, origGroup["b"], clonedGroup["b"], "group value b")

	origMetric := q.Reduction.Metrics[0].Value.(*clonePayload)
	clonedMetric := clone.Reduction.Metrics[0].Value.(*clonePayload)
	if clonedMetric == origMetric {
		t.Fatalf("metric payload pointer must not alias source")
	}
	assertNotSameSliceData(t, origMetric.Labels, clonedMetric.Labels, "metric labels")
	assertNotSameSliceData(t, origMetric.Matrix, clonedMetric.Matrix, "metric matrix")
	assertNotSameSliceData(t, origMetric.Matrix[0], clonedMetric.Matrix[0], "metric matrix row")
	assertNotSameMapData(t, origMetric.Meta, clonedMetric.Meta, "metric meta")
	assertNotSameSliceData(t, origMetric.Meta["metric"], clonedMetric.Meta["metric"], "metric meta raw")
	if clonedMetric.Child == origMetric.Child {
		t.Fatalf("metric child pointer must not alias source")
	}
	assertNotSameSliceData(t, origMetric.Fixed[0], clonedMetric.Fixed[0], "metric fixed[0]")
	assertNotSameSliceData(t, origMetric.Fixed[1], clonedMetric.Fixed[1], "metric fixed[1]")

	origHaving := q.Reduction.Having.Args[0].Value.(*cloneLeaf)
	clonedHaving := clone.Reduction.Having.Args[0].Value.(*cloneLeaf)
	if clonedHaving == origHaving {
		t.Fatalf("having payload pointer must not alias source")
	}
	assertNotSameSliceData(t, origHaving.Tags, clonedHaving.Tags, "having tags")

	origOrder := q.Order[0].By.Value.(map[string][]string)
	clonedOrder := clone.Order[0].By.Value.(map[string][]string)
	assertNotSameMapData(t, origOrder, clonedOrder, "order value")
	assertNotSameSliceData(t, origOrder["bucket"], clonedOrder["bucket"], "order bucket")

	origProjection := q.Projection[0].Value.(map[string][]int)
	clonedProjection := clone.Projection[0].Value.(map[string][]int)
	assertNotSameMapData(t, origProjection, clonedProjection, "projection value")
	assertNotSameSliceData(t, origProjection["projection"], clonedProjection["projection"], "projection bucket")

	origMetadata := q.Metadata[0].Value.(map[string][]int)
	clonedMetadata := clone.Metadata[0].Value.(map[string][]int)
	assertNotSameMapData(t, origMetadata, clonedMetadata, "metadata value")
	assertNotSameSliceData(t, origMetadata["ids"], clonedMetadata["ids"], "metadata ids")

	origMetadataTags := q.Metadata[1].Value.([]string)
	clonedMetadataTags := clone.Metadata[1].Value.([]string)
	assertNotSameSliceData(t, origMetadataTags, clonedMetadataTags, "metadata tags")

	clonedFilter.Labels[0] = "changed"
	clonedFilter.Matrix[0][0] = 99
	clonedFilter.Meta["raw"][0] = 42
	clonedFilter.Child.Name = "changed-child"
	clonedFilter.Child.Tags[0] = "changed-tag"
	clonedFilter.Fixed[0][0] = "changed-fixed"

	clonedGroup["a"][0] = 77
	clonedGroup["c"] = []int{55}

	clonedMetric.Labels[0] = "metric-changed"
	clonedMetric.Matrix[0][0] = 123
	clonedMetric.Meta["metric"][0] = 66
	clonedMetric.Child.Name = "metric-child-changed"
	clonedMetric.Fixed[0][0] = "metric-fixed-changed"

	clonedHaving.Name = "having-changed"
	clonedHaving.Tags[0] = "having-tag-changed"

	clonedOrder["bucket"][0] = "platinum"
	clonedOrder["new"] = []string{"new-bucket"}
	clonedProjection["projection"][0] = 88
	clonedProjection["new"] = []int{11}
	clonedMetadata["ids"][0] = 99
	clonedMetadata["extra"] = []int{10}
	clonedMetadataTags[0] = "rewritten"

	if got := origFilter.Labels[0]; got != "draft" {
		t.Fatalf("source filter labels mutated: got %q", got)
	}
	if got := origFilter.Matrix[0][0]; got != 1 {
		t.Fatalf("source filter matrix mutated: got %d", got)
	}
	if got := origFilter.Meta["raw"][0]; got != 1 {
		t.Fatalf("source filter meta mutated: got %d", got)
	}
	if got := origFilter.Child.Name; got != "filter-child" {
		t.Fatalf("source filter child mutated: got %q", got)
	}
	if got := origFilter.Child.Tags[0]; got != "child" {
		t.Fatalf("source filter child tags mutated: got %q", got)
	}
	if got := origFilter.Fixed[0][0]; got != "north" {
		t.Fatalf("source filter fixed mutated: got %q", got)
	}

	if got := origGroup["a"][0]; got != 1 {
		t.Fatalf("source group slice mutated: got %d", got)
	}
	if _, ok := origGroup["c"]; ok {
		t.Fatalf("source group map unexpectedly gained new key")
	}

	if got := origMetric.Labels[0]; got != "metric" {
		t.Fatalf("source metric labels mutated: got %q", got)
	}
	if got := origMetric.Matrix[0][0]; got != 10 {
		t.Fatalf("source metric matrix mutated: got %d", got)
	}
	if got := origMetric.Meta["metric"][0]; got != 9 {
		t.Fatalf("source metric meta mutated: got %d", got)
	}
	if got := origMetric.Child.Name; got != "metric-child" {
		t.Fatalf("source metric child mutated: got %q", got)
	}
	if got := origMetric.Fixed[0][0]; got != "m1" {
		t.Fatalf("source metric fixed mutated: got %q", got)
	}

	if got := origHaving.Name; got != "having-child" {
		t.Fatalf("source having mutated: got %q", got)
	}
	if got := origHaving.Tags[0]; got != "having" {
		t.Fatalf("source having tags mutated: got %q", got)
	}

	if got := origOrder["bucket"][0]; got != "gold" {
		t.Fatalf("source order slice mutated: got %q", got)
	}
	if _, ok := origOrder["new"]; ok {
		t.Fatalf("source order map unexpectedly gained new key")
	}
	if got := origProjection["projection"][0]; got != 5 {
		t.Fatalf("source projection slice mutated: got %d", got)
	}
	if _, ok := origProjection["new"]; ok {
		t.Fatalf("source projection map unexpectedly gained new key")
	}
	if got := origMetadata["ids"][0]; got != 7 {
		t.Fatalf("source metadata slice mutated: got %d", got)
	}
	if _, ok := origMetadata["extra"]; ok {
		t.Fatalf("source metadata map unexpectedly gained new key")
	}
	if got := origMetadataTags[0]; got != "portable" {
		t.Fatalf("source metadata tags mutated: got %q", got)
	}
}

func TestClonePreservesEmptyNonNilSlices(t *testing.T) {
	q := &QX{
		Filter: Expr{
			Kind: KindOP,
			Name: "empty_filter",
			Args: make([]Expr, 0, 1),
		},
		Reduction: &Reduction{
			Group:   make([]Expr, 0, 1),
			Metrics: make([]Expr, 0, 1),
			Having: Expr{
				Kind:  KindLIT,
				Value: make([]int, 0, 1),
			},
		},
		Order:      make([]Order, 0, 1),
		Projection: make([]Expr, 0, 1),
		Metadata:   make([]MetaEntry, 0, 1),
	}

	clone := q.Clone()

	if clone.Filter.Args == nil {
		t.Fatalf("empty filter args must stay non-nil")
	}
	if clone.Reduction.Group == nil {
		t.Fatalf("empty reduction group must stay non-nil")
	}
	if clone.Reduction.Metrics == nil {
		t.Fatalf("empty reduction metrics must stay non-nil")
	}
	if clone.Order == nil {
		t.Fatalf("empty order must stay non-nil")
	}
	if clone.Projection == nil {
		t.Fatalf("empty projection must stay non-nil")
	}
	if clone.Metadata == nil {
		t.Fatalf("empty metadata must stay non-nil")
	}

	clonedLit := clone.Reduction.Having.Value.([]int)
	if clonedLit == nil {
		t.Fatalf("empty literal slice must stay non-nil")
	}

	clone.Filter.Args = append(clone.Filter.Args, LIT("clone"))
	q.Filter.Args = append(q.Filter.Args, LIT("source"))
	if got := clone.Filter.Args[0].Value; got != "clone" {
		t.Fatalf("filter args backing array aliased source, got %#v", got)
	}

	clone.Reduction.Group = append(clone.Reduction.Group, LIT("clone"))
	q.Reduction.Group = append(q.Reduction.Group, LIT("source"))
	if got := clone.Reduction.Group[0].Value; got != "clone" {
		t.Fatalf("group backing array aliased source, got %#v", got)
	}

	clone.Reduction.Metrics = append(clone.Reduction.Metrics, LIT("clone"))
	q.Reduction.Metrics = append(q.Reduction.Metrics, LIT("source"))
	if got := clone.Reduction.Metrics[0].Value; got != "clone" {
		t.Fatalf("metrics backing array aliased source, got %#v", got)
	}

	clone.Order = append(clone.Order, Order{By: LIT("clone")})
	q.Order = append(q.Order, Order{By: LIT("source")})
	if got := clone.Order[0].By.Value; got != "clone" {
		t.Fatalf("order backing array aliased source, got %#v", got)
	}

	clone.Projection = append(clone.Projection, LIT("clone"))
	q.Projection = append(q.Projection, LIT("source"))
	if got := clone.Projection[0].Value; got != "clone" {
		t.Fatalf("projection backing array aliased source, got %#v", got)
	}

	clone.Metadata = append(clone.Metadata, MetaEntry{Key: "clone", Value: "value"})
	q.Metadata = append(q.Metadata, MetaEntry{Key: "source", Value: "value"})
	if got := clone.Metadata[0].Key; got != "clone" {
		t.Fatalf("metadata backing array aliased source, got %#v", got)
	}

	clonedLit = append(clonedLit, 1)
	sourceLit := q.Reduction.Having.Value.([]int)
	sourceLit = append(sourceLit, 2)
	if clonedLit[0] != 1 {
		t.Fatalf("literal slice backing array aliased source, got %d", clonedLit[0])
	}
}

func assertNotSameSliceData[T any](t *testing.T, src, dst []T, what string) {
	t.Helper()

	if len(src) == 0 || len(dst) == 0 {
		return
	}
	if reflect.ValueOf(src).Pointer() == reflect.ValueOf(dst).Pointer() {
		t.Fatalf("%s must not share backing array", what)
	}
}

func assertNotSameMapData[K comparable, V any](t *testing.T, src, dst map[K]V, what string) {
	t.Helper()

	if src == nil || dst == nil {
		return
	}
	if reflect.ValueOf(src).Pointer() == reflect.ValueOf(dst).Pointer() {
		t.Fatalf("%s must not share map storage", what)
	}
}

var cloneBenchSink *QX

func BenchmarkCloneFlatLiterals(b *testing.B) {
	q := benchmarkCloneFlatQuery()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cloneBenchSink = q.Clone()
	}
}

func BenchmarkCloneDeepLiterals(b *testing.B) {
	q := benchmarkCloneDeepQuery()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cloneBenchSink = q.Clone()
	}
}

func benchmarkCloneFlatQuery() *QX {
	return &QX{
		Filter: Expr{
			Kind: KindOP,
			Name: OpAND,
			Args: []Expr{
				{Kind: KindLIT, Value: makeBenchmarkStrings(256)},
				{Kind: KindLIT, Value: makeBenchmarkInts(256)},
				{Kind: KindLIT, Value: makeBenchmarkBytes(1024)},
				{Kind: KindLIT, Value: makeBenchmarkIntArray()},
				{
					Kind: KindOP,
					Name: "flat_nested",
					Args: []Expr{
						{Kind: KindLIT, Value: makeBenchmarkStrings(128)},
						{Kind: KindLIT, Value: makeBenchmarkInts(128)},
					},
				},
			},
		},
		Reduction: &Reduction{
			Group: []Expr{
				{Kind: KindLIT, Value: makeBenchmarkStrings(64)},
			},
			Metrics: []Expr{
				{Kind: KindLIT, Value: makeBenchmarkBytes(512)},
			},
			Having: Expr{
				Kind: KindOP,
				Name: "flat_having",
				Args: []Expr{
					{Kind: KindLIT, Value: makeBenchmarkInts(64)},
				},
			},
		},
		Order: []Order{
			{
				By:   Expr{Kind: KindLIT, Value: makeBenchmarkStrings(32)},
				Desc: true,
			},
		},
		Window: Window{Offset: 100, Limit: 50},
	}
}

func benchmarkCloneDeepQuery() *QX {
	filterPayload := clonePayload{
		Labels: makeBenchmarkStrings(64),
		Matrix: makeBenchmarkMatrix(16, 16),
		Meta: map[string][]byte{
			"raw":    makeBenchmarkBytes(256),
			"packed": makeBenchmarkBytes(128),
		},
		Child: &cloneLeaf{
			Name: "filter-child",
			Tags: makeBenchmarkStrings(16),
		},
		Fixed: [2][]string{
			makeBenchmarkStrings(16),
			makeBenchmarkStrings(16),
		},
	}

	groupValue := map[string][]int{
		"a": makeBenchmarkInts(64),
		"b": makeBenchmarkInts(64),
		"c": makeBenchmarkInts(64),
	}

	metricValue := &clonePayload{
		Labels: makeBenchmarkStrings(32),
		Matrix: makeBenchmarkMatrix(8, 16),
		Meta: map[string][]byte{
			"metric": makeBenchmarkBytes(192),
		},
		Child: &cloneLeaf{
			Name: "metric-child",
			Tags: makeBenchmarkStrings(12),
		},
		Fixed: [2][]string{
			makeBenchmarkStrings(12),
			makeBenchmarkStrings(12),
		},
	}

	havingValue := &cloneLeaf{
		Name: "having-child",
		Tags: makeBenchmarkStrings(10),
	}

	orderValue := map[string][]string{
		"bucket": makeBenchmarkStrings(32),
		"tier":   makeBenchmarkStrings(32),
	}

	return &QX{
		Filter: Expr{
			Kind: KindOP,
			Name: OpAND,
			Args: []Expr{
				{Kind: KindLIT, Value: filterPayload},
				{
					Kind: KindOP,
					Name: "wrap",
					Args: []Expr{
						{Kind: KindLIT, Value: groupValue},
					},
				},
			},
		},
		Reduction: &Reduction{
			Group: []Expr{
				{Kind: KindLIT, Value: groupValue},
			},
			Metrics: []Expr{
				{Kind: KindLIT, Value: metricValue},
			},
			Having: Expr{
				Kind: KindOP,
				Name: OpOR,
				Args: []Expr{
					{Kind: KindLIT, Value: havingValue},
				},
			},
		},
		Order: []Order{
			{
				By:   Expr{Kind: KindLIT, Value: orderValue},
				Desc: true,
			},
		},
		Window: Window{Offset: 7, Limit: 11},
	}
}

func makeBenchmarkStrings(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "value"
	}
	return out
}

func makeBenchmarkInts(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func makeBenchmarkBytes(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i)
	}
	return out
}

func makeBenchmarkMatrix(rows, cols int) [][]int {
	out := make([][]int, rows)
	for i := range out {
		row := make([]int, cols)
		for j := range row {
			row[j] = i + j
		}
		out[i] = row
	}
	return out
}

func makeBenchmarkIntArray() [256]int {
	var out [256]int
	for i := range out {
		out[i] = i
	}
	return out
}
