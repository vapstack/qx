package qx

import (
	"sort"
	"strings"
	"sync"
	"testing"
)

type User struct {
	ID       int     `json:"id" db:"pk"`
	Name     string  `json:"name"`
	Age      int     `json:"age"`
	Score    float64 `db:"score_val"`
	IsActive bool

	Tags    []string
	RolePtr *string
	Meta    any

	private string // must not be accessible
}

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }

func TestMatch_ScalarsAndLogic(t *testing.T) {
	u := User{
		ID:       1,
		Name:     "Alice",
		Age:      30,
		Score:    95.5,
		IsActive: true,
		Tags:     []string{"admin", "editor"},
		RolePtr:  strPtr("superuser"),
		Meta:     100,
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"EQ int", EQ("Age", 30), true},
		{"EQ int mismatch", EQ("Age", 31), false},
		{"GT int", GT("Age", 20), true},
		{"GTE int", GTE("Age", 30), true},
		{"LT float via db tag", LT("score_val", 100.0), true},
		{"EQ string via json tag", EQ("name", "Alice"), true},
		{"EQ bool", EQ("IsActive", true), true},

		{"AND success", AND(EQ("Age", 30), EQ("Name", "Alice")), true},
		{"AND fail", AND(EQ("Age", 30), EQ("Name", "Bob")), false},
		{"OR success", OR(EQ("Age", 100), EQ("Name", "Alice")), true},
		{"NOT success", NOT(EQ("Name", "Bob")), true},

		// pointers
		{"EQ pointer field with scalar", EQ("RolePtr", "superuser"), true},
		{"EQ pointer mismatch", EQ("RolePtr", "user"), false},

		// numeric cross-type (int vs int64 should match by value)
		{"EQ int field with int64", EQ("Age", int64(30)), true},
		{"EQ float strict", EQ("Score", 95.5), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_DataEquality(t *testing.T) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		Profile Profile
	}

	v := &S{
		Profile: Profile{Level: 5, Rank: intPtr(99)},
	}

	query := Profile{Level: 5, Rank: intPtr(99)}

	ok, err := Match(v, EQ("Profile", query))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected data-equality match for struct with pointer fields")
	}
}

func TestMatch_SliceOpsSemantics(t *testing.T) {
	u := User{
		Name: "Alice",
		Tags: []string{"A", "B", "C"},
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		// IN: scalar value in query set
		{"IN success", IN("name", []string{"Alice", "Bob"}), true},
		{"IN fail", IN("name", []string{"Bob", "Charlie"}), false},
		{"IN empty query => false", IN("name", []string{}), false},

		// HAS: contains all query items (subset check)
		{"HAS subset", HAS("Tags", []string{"A", "C"}), true},
		{"HAS fail missing", HAS("Tags", []string{"A", "Z"}), false},
		{"HAS empty query => true", HAS("Tags", []string{}), true},

		// HASANY: intersection exists
		{"HASANY common", HASANY("Tags", []string{"Z", "B"}), true},
		{"HASANY fail", HASANY("Tags", []string{"X", "Y"}), false},
		{"HASANY empty query => false", HASANY("Tags", []string{}), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_SliceOpsWithNilField(t *testing.T) {
	// nil slice field should behave like empty
	u := User{
		Name: "Alice",
		Tags: nil,
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"HAS non-empty on nil field => false", HAS("Tags", []string{"A"}), false},
		{"HASANY non-empty on nil field => false", HASANY("Tags", []string{"A"}), false},
		{"HAS empty on nil field => true", HAS("Tags", []string{}), true},
		{"HASANY empty on nil field => false", HASANY("Tags", []string{}), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_InterfaceUnwrapAndTypedNil(t *testing.T) {
	u1 := User{Meta: intPtr(123)} // interface holds *int
	u2 := User{Meta: 123}         // interface holds int

	var p *int = nil
	uTypedNil := User{Meta: p} // interface is non-nil, but underlying pointer is nil

	cases := []struct {
		name string
		u    User
		expr Expr
		ok   bool
	}{
		{"interface ptr should match scalar", u1, EQ("Meta", 123), true},
		{"interface scalar should match scalar", u2, EQ("Meta", 123), true},
		{"interface ptr should not match other value", u1, EQ("Meta", 124), false},

		// typed nil inside interface counts as nil for EQ(field, nil)
		{"typed nil inside interface should match nil", uTypedNil, EQ("Meta", nil), true},
		{"typed nil inside interface should not match non-nil", uTypedNil, EQ("Meta", 0), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(tc.u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_EqNilOnPointerFields(t *testing.T) {
	u := User{RolePtr: nil}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"EQ nil on nil pointer => true", EQ("RolePtr", nil), true},
		{"NOT(EQ nil) on nil pointer => false", NOT(EQ("RolePtr", nil)), false},
		{"EQ scalar on nil pointer => false", EQ("RolePtr", "admin"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_Errors_TypeAndAccess(t *testing.T) {
	u := User{
		ID:   555,
		Name: "Alice",
		Tags: []string{"A"},
	}

	cases := []struct {
		name string
		expr Expr
	}{
		// private field should not be accessible
		{"private field access must fail", EQ("private", "secret")},

		// scalar comparisons with incompatible types must fail
		{"compare int with string must fail", EQ("ID", "not-an-int")},

		// unsupported op/type combo must fail
		{"GT on slice must fail", GT("Tags", 5)},

		// IN/HAS/HASANY require slice query values
		{"IN with scalar RHS must fail", IN("ID", 123)},
		{"HAS with scalar RHS must fail", HAS("Tags", 123)},
		{"HASANY with scalar RHS must fail", HASANY("Tags", "A")},

		// IN expects scalar field, not slice field
		{"IN on slice field must fail", IN("Tags", []string{"A"})},

		// HAS/HASANY expect slice field, not scalar field
		{"HAS on scalar field must fail", HAS("Age", []int{1})},
		{"HASANY on scalar field must fail", HASANY("Age", []int{1})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Match(u, tc.expr)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestMatch_NilInputs(t *testing.T) {
	ok, err := Match(nil, EQ("ID", 1))
	if err != nil {
		t.Fatalf("Match(nil) must not error, got: %v", err)
	}
	if ok {
		t.Fatalf("Match(nil) must be false")
	}

	fn, err := CompileFor[User](EQ("ID", 1))
	if err != nil {
		t.Fatalf("CompileFor error: %v", err)
	}
	ok, err = fn(nil)
	if err != nil || ok {
		t.Fatalf("fn(nil) expected (false, nil), got (%v, %v)", ok, err)
	}
}

func TestMatch_StringOps(t *testing.T) {
	type S struct {
		Name   string
		Nick   *string
		AnyStr any
	}

	nick := "superuser"
	v := S{
		Name:   "Alice",
		Nick:   &nick,
		AnyStr: "hello world",
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"PREFIX true", PREFIX("Name", "Al"), true},
		{"PREFIX false", PREFIX("Name", "xx"), false},

		{"SUFFIX true", SUFFIX("Name", "ice"), true},
		{"SUFFIX false", SUFFIX("Name", "xx"), false},

		{"CONTAINS true", CONTAINS("Name", "li"), true},
		{"CONTAINS false", CONTAINS("Name", "zz"), false},

		{"PTR PREFIX true", PREFIX("Nick", "sup"), true},
		{"PTR SUFFIX true", SUFFIX("Nick", "user"), true},
		{"PTR CONTAINS true", CONTAINS("Nick", "peru"), true},

		{"ANY CONTAINS true", CONTAINS("AnyStr", "world"), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := Match(v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.ok {
				t.Fatalf("Match()=%v want=%v", ok, tc.ok)
			}
		})
	}
}

func TestMatch_HASNONE(t *testing.T) {
	type S struct {
		Tags []string
	}

	v := S{Tags: []string{"A", "B", "C"}}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"HASNONE true", HASNONE("Tags", []string{"X", "Y"}), true},
		{"HASNONE false", HASNONE("Tags", []string{"X", "B"}), false},
		{"HASNONE empty RHS => true", HASNONE("Tags", []string{}), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := Match(v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.ok {
				t.Fatalf("Match()=%v want=%v", ok, tc.ok)
			}
		})
	}
}

func TestMatch_EmbeddedAndAliases(t *testing.T) {
	type Base struct {
		BaseID int `json:"base_id"`
	}
	type Child struct {
		Base
		Name string `json:"name"`
	}

	c := Child{Base: Base{BaseID: 777}, Name: "Child"}

	ok, err := Match(c, EQ("BaseID", 777))
	if err != nil || !ok {
		t.Fatalf("expected match by promoted field, ok=%v err=%v", ok, err)
	}

	ok, err = Match(c, EQ("base_id", 777))
	if err != nil || !ok {
		t.Fatalf("expected match by json alias, ok=%v err=%v", ok, err)
	}
}

func TestMatch_FastVsSlowPathConsistency(t *testing.T) {

	// behavior should not depend on whether the root value is passed by value or pointer

	type S struct {
		A int
		B string
		C float64
		D bool
	}

	s := S{A: 10, B: "fast", C: 3.14, D: true}

	exps := []Expr{
		EQ("A", 10),
		EQ("B", "fast"),
		GT("C", 3.0),
		EQ("D", true),
	}

	m, err := MatcherFor[S]()
	if err != nil {
		t.Fatalf("MatcherFor error: %v", err)
	}

	for _, e := range exps {
		fn, err := m.Compile(e)
		if err != nil {
			t.Fatalf("compile error for %v: %v", e, err)
		}

		ok1, err1 := fn(&s)
		ok2, err2 := fn(s)

		if err1 != nil || err2 != nil {
			t.Fatalf("unexpected errors: err1=%v err2=%v", err1, err2)
		}
		if ok1 != ok2 {
			t.Fatalf("fast/slow mismatch for %v: ptr=%v val=%v", e, ok1, ok2)
		}
	}
}

func TestAliasCollisions_MustFail(t *testing.T) {

	// alias collisions must be rejected (fail-fast)

	type Bad struct {
		Field1 int `db:"field_1" json:"-"`
		Field2 any `db:"-" json:"field_1"`
	}

	_, err := CompileFor[Bad](EQ("field_1", 1))
	if err == nil {
		t.Fatalf("expected error due to alias collision (field_1), got nil")
	}
}

func TestRace_ConcurrentCompileAndMatch(t *testing.T) {

	t.Parallel()

	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	// shared data for all goroutines

	data := []*S{
		{
			ID:       777,
			Age:      35,
			Name:     "Alice",
			Score:    88.8,
			IsActive: true,
			RolePtr:  strPtr("superuser"),
			Tags:     []string{"admin", "editor", "A", "B"},
			Meta:     intPtr(123),
			Profile:  Profile{Level: 5, Rank: intPtr(99)},
		},
		{
			ID:       1,
			Age:      20,
			Name:     "Bob",
			Score:    12.3,
			IsActive: false,
			RolePtr:  nil,
			Tags:     []string{"user"},
			Meta:     0,
			Profile:  Profile{Level: 1, Rank: nil},
		},
	}

	// expression intentionally touches:
	// - fast scalars
	// - slice ops
	// - interface unwrap

	expr := AND(
		GTE("Age", 18),
		LT("Score", 100.0),
		OR(
			EQ("IsActive", true),
			EQ("Name", "Bob"),
		),
		HASANY("Tags", []string{"admin", "B"}),
		EQ("Meta", 123),
	)

	const workers = 16
	const iters = 100000

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()

			for i := 0; i < iters; i++ {

				// compile concurrently as well (hits caches, reflection, etc.)

				m, err := MatcherFor[S]()
				if err != nil {
					t.Errorf("MatcherFor error: %v", err)
					return
				}
				fn, err := m.Compile(expr)
				if err != nil {
					t.Errorf("compile error: %v", err)
					return
				}

				// run matches on shared objects

				ok0, err0 := fn(data[0])
				if err0 != nil {
					t.Errorf("match error: %v", err0)
					return
				}

				// data[0] should match
				if !ok0 {
					t.Errorf("expected match for data[0], got false")
					return
				}

				ok1, err1 := fn(data[1])
				if err1 != nil {
					t.Errorf("match error: %v", err1)
					return
				}
				// data[1] should not match
				if ok1 {
					t.Errorf("expected no match for data[1], got true")
					return
				}
			}
		}()
	}

	wg.Wait()
}

/**/

func toSet(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

func mustContainAll(t *testing.T, got []string, want []string) {
	t.Helper()
	g := toSet(got)
	for _, w := range want {
		if _, ok := g[w]; !ok {
			t.Fatalf("missing \"%v\" in %v", w, got)
		}
	}
}

func TestMatcher_DiffFields_TwoValues(t *testing.T) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int    `json:"id" db:"pk"`
		Name     string `json:"name"`
		Age      int    `json:"age"`
		IsActive bool
		Tags     []string
		Meta     any
		Profile  Profile `json:"profile"`
	}

	m, err := MatcherFor[S]()
	if err != nil {
		t.Fatalf("MatcherFor error: %v", err)
	}

	a := &S{
		ID:       1,
		Name:     "Alice",
		Age:      30,
		IsActive: true,
		Tags:     []string{"A", "B"},
		Meta:     intPtr(123),
		Profile:  Profile{Level: 5, Rank: intPtr(99)},
	}
	b := &S{
		ID:       1,                                   // same
		Name:     "Alice",                             // same
		Age:      31,                                  // diff
		IsActive: false,                               // diff
		Tags:     []string{"A", "B"},                  // same
		Meta:     123,                                 // same by data equality (unwrap)
		Profile:  Profile{Level: 5, Rank: intPtr(99)}, // same by data equality (deep)
	}

	got, err := m.DiffFields(a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"Age", "IsActive"}
	mustContainAll(t, got, want)

	gotSet := toSet(got)
	if _, ok := gotSet["ID"]; ok {
		t.Fatalf("ID should not be in diff: %v", got)
	}
	if _, ok := gotSet["Name"]; ok {
		t.Fatalf("Name should not be in diff: %v", got)
	}
	if _, ok := gotSet["Meta"]; ok {
		t.Fatalf("Meta should not be in diff (data-equality expected): %v", got)
	}
	if _, ok := gotSet["Profile"]; ok {
		t.Fatalf("Profile should not be in diff (data-equality expected): %v", got)
	}
}

func TestMatcher_DiffFields_ThreeValues(t *testing.T) {
	type S struct {
		A int `json:"a"`
		B int `json:"b"`
		C int `json:"c"`
	}

	m, err := MatcherFor[S]()
	if err != nil {
		t.Fatalf("MatcherFor error: %v", err)
	}

	v1 := &S{A: 1, B: 2, C: 3}
	v2 := &S{A: 1, B: 999, C: 3}
	v3 := &S{A: 1, B: 2, C: 777}

	got, err := m.DiffFields(v1, v2, v3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// B differs vs v2, C differs vs v3
	// A same across all

	sort.Strings(got)
	want := []string{"B", "C"}

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("DiffFields mismatch: got=%v want=%v", got, want)
	}
}

func TestMatcher_DiffFieldsTag_JSON(t *testing.T) {
	type S struct {
		ID       int    `json:"id" db:"pk"`
		Name     string `json:"name"`
		Age      int    `json:"age"`
		IsActive bool   // no json tag -> fallback to field name
	}

	m, err := MatcherFor[S]()
	if err != nil {
		t.Fatalf("MatcherFor error: %v", err)
	}

	a := &S{ID: 1, Name: "Alice", Age: 30, IsActive: true}
	b := &S{ID: 2, Name: "Alice", Age: 31, IsActive: false}

	got, err := m.DiffFieldsTag("json", a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// expect tag names for tagged fields, fallback to field name for untagged
	want := []string{"id", "age", "IsActive"}
	mustContainAll(t, got, want)

	// ensure it does not return the Go field name for tagged ones
	gotSet := toSet(got)
	if _, ok := gotSet["ID"]; ok {
		t.Fatalf("expected json tag 'id' instead of 'ID': %v", got)
	}
	if _, ok := gotSet["Age"]; ok {
		t.Fatalf("expected json tag 'age' instead of 'Age': %v", got)
	}
}

func TestMatcher_DiffFields_TypeMismatchErrors(t *testing.T) {
	type S struct{ A int }

	m, err := MatcherFor[S]()
	if err != nil {
		t.Fatalf("MatcherFor error: %v", err)
	}

	_, err = m.DiffFields(&S{A: 1}, struct{ A int64 }{A: 1})
	if err == nil {
		t.Fatalf("expected error on type mismatch")
	}
}

/**/

var (
	benchSinkBool bool
	benchSinkErr  error
	benchSinkInt  int
)

func BenchmarkMatch_Small(b *testing.B) {

	// simple: few scalar filters, everything should be fast-path

	type S struct {
		A int
		B string
		C float64
		D bool
		E uint64
	}

	v := &S{
		A: 10,
		B: "ok",
		C: 3.14,
		D: true,
		E: 123,
	}

	expr := AND(
		EQ("A", 10),
		EQ("B", "ok"),
		GT("C", 3.0),
		EQ("D", true),
		LT("E", uint64(200)),
	)

	m, err := MatcherFor[S]()
	if err != nil {
		b.Fatalf("MatcherFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkMatch_Mixed(b *testing.B) {

	// mixed: some fast scalar checks + slower bits (interface / slice ops)

	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		Tags     []string
		Meta     any // forces slow-ish comparisons
	}

	v := &S{
		ID:       42,
		Age:      30,
		Name:     "Alice",
		Score:    95.5,
		IsActive: true,
		Tags:     []string{"A", "B", "C"},
		Meta:     intPtr(123), // interface holding pointer
	}

	expr := AND(

		EQ("ID", 42),
		GTE("Age", 18),
		LT("Score", 100.0),
		EQ("IsActive", true),

		IN("Name", []string{"Bob", "Alice", "Charlie"}),
		HASANY("Tags", []string{"Z", "B"}),
		EQ("Meta", 123),
	)

	m, err := MatcherFor[S]()
	if err != nil {
		b.Fatalf("MatcherFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkMatch_Heavy(b *testing.B) {

	// heavy: large expression + more checks (still using *T)

	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	v := &S{
		ID:       777,
		Age:      35,
		Name:     "Alice",
		Score:    88.8,
		IsActive: true,
		RolePtr:  strPtr("superuser"),
		Tags:     []string{"admin", "editor", "A", "B"},
		Meta:     intPtr(123),
		Profile: Profile{
			Level: 5,
			Rank:  intPtr(99),
		},
	}

	// also include an EQ on nested struct (slow path / DeepEqual-like behavior), and a NE on nested struct

	profSame := Profile{Level: 5, Rank: intPtr(99)}
	profOther := Profile{Level: 6, Rank: intPtr(99)}

	expr := AND(
		OR(
			AND(EQ("ID", 777), EQ("Name", "Alice"), GTE("Age", 18)),
			AND(GT("Age", 40), EQ("IsActive", true), LT("Score", 90.0)),
			AND(EQ("RolePtr", "superuser"), NOT(EQ("Name", "Bob"))),
		),

		AND(
			LT("Score", 100.0),
			GTE("Age", 21),
			EQ("IsActive", true),
			NOT(EQ("ID", 0)),
		),

		AND(
			HAS("Tags", []string{"admin", "editor"}),
			HASANY("Tags", []string{"Z", "B"}),
			IN("Name", []string{"Alice", "Charlie", "Dave", "Eve"}),
		),

		AND(
			EQ("Meta", 123),
			NOT(EQ("Meta", 999)),
		),

		AND(
			EQ("Profile", profSame),
			NE("Profile", profOther),
		),
	)

	m, err := MatcherFor[S]()
	if err != nil {
		b.Fatalf("MatcherFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		if e != nil {
			panic(e)
		}
		if !ok {
			panic("aaa")
		}
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkMatch_MixedLoop1000(b *testing.B) {

	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	// xorshift
	var x uint64 = 0x123456789abcdef
	next := func() uint64 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		return x
	}

	names := []string{"Alice", "Bob", "Charlie", "Dave", "Eve"}
	tagPool := []string{"admin", "editor", "user", "A", "B", "C", "X", "Y", "Z"}
	rolePool := []string{"superuser", "staff", "guest"}

	const N = 1000
	data := make([]*S, 0, N)

	for i := 0; i < N; i++ {
		r1 := next()
		r2 := next()

		age := int(r1%70) + 10
		score := float64(r2%10000) / 100.0 // 0..100
		isActive := r1&1 == 0

		name := names[int((r1>>8)%uint64(len(names)))]

		// small tags slice (0..3 items) — realistic, not huge
		tagCount := int((r2 >> 8) % 4)
		tags := make([]string, 0, tagCount)
		for j := 0; j < tagCount; j++ {
			tags = append(tags, tagPool[int((next()>>16)%uint64(len(tagPool)))])
		}

		// RolePtr nil sometimes
		var rolePtr *string
		if (r2 & 3) != 0 {
			s := rolePool[int((r2>>20)%uint64(len(rolePool)))]
			rolePtr = &s
		}

		// Meta sometimes int, sometimes *int
		var meta any
		mv := int((r1 >> 32) % 500)
		if (r1 & 4) == 0 {
			meta = mv
		} else {
			meta = intPtr(mv)
		}

		// Profile (not used in expr; just for shape realism)
		var rank *int
		if (r1 & 8) != 0 {
			rank = intPtr(int((r2 >> 32) % 200))
		}

		data = append(data, &S{
			ID:       i + 1,
			Age:      age,
			Name:     name,
			Score:    score,
			IsActive: isActive,
			RolePtr:  rolePtr,
			Tags:     tags,
			Meta:     meta,
			Profile:  Profile{Level: int((r1 >> 40) % 10), Rank: rank},
		})
	}

	// mix of fast scalars + slice ops + interface unwrap
	// targeting a modest match rate (not too high/low)

	expr := AND(
		GTE("Age", 18),
		LT("Score", 90.0),
		EQ("IsActive", true),
		IN("Name", []string{"Alice", "Bob"}),
		HASANY("Tags", []string{"admin", "editor", "B"}),
		EQ("Meta", 123),
	)

	m, err := MatcherFor[S]()
	if err != nil {
		b.Fatalf("MatcherFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cnt := 0
		for _, v := range data {
			ok, e := fn(v)
			if e != nil {
				benchSinkErr = e
				b.Fatalf("unexpected error: %v", e)
			}
			if ok {
				cnt++
			}
		}
		benchSinkInt = cnt
	}
}

/**/

func BenchmarkDiffFields_Overhead(b *testing.B) {
	type S struct {
		ID       int
		Age      int
		Score    float64
		IsActive bool
		Name     string
		Flag     uint64
	}

	m, err := MatcherFor[S]()
	if err != nil {
		b.Fatalf("MatcherFor error: %v", err)
	}

	left := &S{ID: 1, Age: 30, Score: 88.8, IsActive: true, Name: "Alice", Flag: 10}
	right := &S{ID: 2, Age: 31, Score: 88.8, IsActive: false, Name: "Bob", Flag: 10}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		diff, err := m.DiffFields(left, right)
		if err != nil {
			benchSinkErr = err
			b.Fatalf("unexpected error: %v", err)
		}
		benchSinkInt = len(diff)
	}
}

func BenchmarkDiffFields_Heavy(b *testing.B) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Name     string
		Age      int
		IsActive bool
		Tags     []string
		Meta     any
		Profile  Profile
	}

	m, err := MatcherFor[S]()
	if err != nil {
		b.Fatalf("MatcherFor error: %v", err)
	}

	left := &S{
		ID:       1,
		Name:     "Alice",
		Age:      30,
		IsActive: true,
		Tags:     []string{"A", "B"},
		Meta:     intPtr(123),
		Profile:  Profile{Level: 5, Rank: intPtr(99)},
	}
	right := &S{
		ID:       2,
		Name:     "Bob",
		Age:      31,
		IsActive: false,
		Tags:     []string{"A", "B"},
		Meta:     123,
		Profile:  Profile{Level: 6, Rank: intPtr(99)},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		diff, err := m.DiffFields(left, right)
		if err != nil {
			benchSinkErr = err
			b.Fatalf("unexpected error: %v", err)
		}
		benchSinkInt = len(diff)
	}
}

/**/
