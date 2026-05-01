package completion

import (
	"testing"
)

// In a multi-table query (JOIN), un-aliased column completion is ambiguous —
// suppress it instead of suggesting wrong-table columns.
func TestSQLMultiTableSuppressesUnaliasedColumns(t *testing.T) {
	q := `SELECT  FROM users u JOIN orders o ON u.id = o.user_id`
	cursor := len("SELECT ") // right after "SELECT "
	req := Request{
		Query:         q,
		Cursor:        cursor,
		DBType:        "postgres",
		Tables:        []string{"users", "orders"},
		Schema:        &SchemaInfo{Name: "users", Columns: []ColumnInfo{{Name: "name", Type: "text"}}},
		InferredTable: "users",
	}
	r := Complete(req)
	if r != nil {
		// Either nil or no schema-driven items.
		for _, it := range r.Items {
			if it.Label == "name" {
				t.Fatalf("multi-table query suggested un-aliased column %q", it.Label)
			}
		}
	}
}

// Aliased column reference whose alias matches the inferred table should
// surface its columns. (`u.|` where `u` → `users` and we have `users` schema.)
func TestSQLMultiTableAliasMatchesInferredShowsColumns(t *testing.T) {
	q := `SELECT u. FROM users u JOIN orders o ON u.id = o.user_id`
	cursor := len("SELECT u.")
	req := Request{
		Query:  q,
		Cursor: cursor,
		DBType: "postgres",
		Tables: []string{"users", "orders"},
		Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
			{Name: "id", Type: "int", PrimaryKey: true},
			{Name: "name", Type: "text"},
		}},
		InferredTable: "users",
	}
	r := Complete(req)
	if r == nil {
		t.Fatalf("expected column suggestions for u.|, got nil")
	}
	found := false
	for _, it := range r.Items {
		if it.Label == "name" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'name' from users schema, got %v", r.Items)
	}
}

// Aliased column whose alias maps to a different table must request the
// right schema rather than show wrong-schema columns.
func TestSQLMultiTableAliasMismatchRequestsSchema(t *testing.T) {
	q := `SELECT o. FROM users u JOIN orders o ON u.id = o.user_id`
	cursor := len("SELECT o.")
	req := Request{
		Query:  q,
		Cursor: cursor,
		DBType: "postgres",
		Tables: []string{"users", "orders"},
		Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
			{Name: "id", Type: "int", PrimaryKey: true},
			{Name: "name", Type: "text"},
		}},
		InferredTable: "users",
	}
	r := Complete(req)
	if r == nil {
		t.Fatalf("expected loading-schema result for o.|, got nil")
	}
	if r.NeedSchema != "orders" {
		t.Fatalf("NeedSchema = %q, want orders", r.NeedSchema)
	}
	for _, it := range r.Items {
		if it.Label == "name" {
			t.Fatalf("must not show users column %q for o.| alias", it.Label)
		}
	}
}

// JOIN ... ON activates predicate-column context — without this the picker
// falls back to clause keywords inside the ON clause.
func TestSQLJoinOnTriggersPredicateContext(t *testing.T) {
	q := `SELECT * FROM users u JOIN orders o ON u.`
	cursor := len(q)
	req := Request{
		Query:  q,
		Cursor: cursor,
		DBType: "postgres",
		Tables: []string{"users", "orders"},
		Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
			{Name: "id", Type: "int", PrimaryKey: true},
		}},
		InferredTable: "users",
	}
	r := Complete(req)
	if r == nil {
		t.Fatalf("expected column suggestions inside JOIN ON, got nil")
	}
	if r.Title != "Filter Column" {
		t.Fatalf("Title = %q, want Filter Column", r.Title)
	}
}

// Inserting a value mid-literal must replace the whole literal body so
// trailing characters don't dangle after the inserted value (Mongo parity).
func TestSQLValueCompletionExtendsBoundsToCloseQuote(t *testing.T) {
	q := `SELECT * FROM users WHERE name = 'oldstuff'`
	cursor := len("SELECT * FROM users WHERE name = 'old") // between old and stuff
	req := Request{
		Query:         q,
		Cursor:        cursor,
		DBType:        "postgres",
		Tables:        []string{"users"},
		InferredTable: "users",
	}
	r := Complete(req)
	if r == nil {
		t.Fatalf("expected value-completion result, got nil")
	}
	openQuote := len("SELECT * FROM users WHERE name = '")
	closeQuote := len("SELECT * FROM users WHERE name = 'oldstuff")
	if r.Start != openQuote {
		t.Fatalf("Start = %d, want %d", r.Start, openQuote)
	}
	if r.End != closeQuote {
		t.Fatalf("End = %d, want %d (extend past trailing chars)", r.End, closeQuote)
	}
}

// All JOIN keyword variants must collapse to a single split point so the
// alias parser doesn't keep "left" / "outer" / etc. as a phantom fragment.
func TestParseFromBindingsJoinVariants(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"LEFT JOIN", "select * from users u left join orders o on u.id = o.user_id"},
		{"INNER JOIN", "select * from users u inner join orders o on u.id = o.user_id"},
		{"RIGHT OUTER JOIN", "select * from users u right outer join orders o on u.id = o.user_id"},
		{"CROSS JOIN", "select * from users u cross join orders o"},
		{"FULL OUTER JOIN", "select * from users u full outer join orders o on u.id = o.user_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseFromBindings(tc.in)
			if len(got) != 2 {
				t.Fatalf("got %d bindings for %q, want 2 (%v)", len(got), tc.in, got)
			}
			if got[0].Alias != "u" || got[0].Table != "users" {
				t.Fatalf("first binding = %+v, want {u, users}", got[0])
			}
			if got[1].Alias != "o" || got[1].Table != "orders" {
				t.Fatalf("second binding = %+v, want {o, orders}", got[1])
			}
		})
	}
}

// `WHERE x = 'thing with stuff'` must not false-trigger the multi-table
// guard via a substring match on " with ".
func TestQueryHasMultipleTablesIgnoresLiteralWith(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"select * from users where name = 'thing with stuff'", false},
		{"with t as (select 1) select * from t", true},
		{"select * from users u left join orders o on u.id = o.user_id", true},
		{"select * from users", false},
		{"select * from users u, orders o", true},
	}
	for _, tc := range cases {
		got := queryHasMultipleTables(tc.in)
		if got != tc.want {
			t.Errorf("queryHasMultipleTables(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// FROM-clause alias parsing covers `table alias` and `table AS alias` forms,
// strips quotes, skips derived-table fragments, and ignores ON tails.
func TestParseFromBindingsBasics(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []AliasBinding
	}{
		{
			"single table no alias",
			"select * from users",
			[]AliasBinding{{Alias: "users", Table: "users"}},
		},
		{
			"table with alias",
			"select * from users u",
			[]AliasBinding{{Alias: "u", Table: "users"}},
		},
		{
			"table AS alias",
			"select * from users as u",
			[]AliasBinding{{Alias: "u", Table: "users"}},
		},
		{
			"join with aliases",
			"select * from users u join orders o on u.id = o.user_id",
			[]AliasBinding{{Alias: "u", Table: "users"}, {Alias: "o", Table: "orders"}},
		},
		{
			"comma join",
			"select * from users u, orders o",
			[]AliasBinding{{Alias: "u", Table: "users"}, {Alias: "o", Table: "orders"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseFromBindings(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d bindings, want %d (%v)", len(got), len(tc.want), got)
			}
			for i, w := range tc.want {
				if got[i] != w {
					t.Fatalf("binding %d = %+v, want %+v", i, got[i], w)
				}
			}
		})
	}
}
