package completion

import "testing"

func TestMongoCompleteRejectsWrongSchema(t *testing.T) {
	// Browse panel on "users", query targets "comments" — engine must NOT
	// use the users schema for comments completion.
	req := Request{
		Query:         `db.comments.find({`,
		Cursor:        len(`db.comments.find({`),
		DBType:        "mongo",
		Tables:        []string{"users", "comments"},
		Schema:        &SchemaInfo{Name: "users", Columns: []ColumnInfo{{Name: "name", Type: "string"}}},
		InferredTable: "comments",
	}
	result := Complete(req)
	if result == nil {
		t.Fatal("expected result")
	}
	if result.NeedSchema != "comments" {
		t.Fatalf("NeedSchema = %q, want 'comments'", result.NeedSchema)
	}
	for _, item := range result.Items {
		if item.Label == "name" && item.Detail == "field" {
			t.Fatal("engine used users schema for comments query")
		}
	}
}

func TestMongoCompleteUsesCorrectSchema(t *testing.T) {
	req := Request{
		Query:  `db.comments.find({`,
		Cursor: len(`db.comments.find({`),
		DBType: "mongo",
		Tables: []string{"users", "comments"},
		Schema: &SchemaInfo{Name: "comments", Columns: []ColumnInfo{
			{Name: "body", Type: "string"},
			{Name: "author", Type: "string"},
		}},
		InferredTable: "comments",
	}
	result := Complete(req)
	if result == nil {
		t.Fatal("expected result")
	}
	found := false
	for _, item := range result.Items {
		if item.Label == "body" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected comments schema fields, got %#v", result.Items)
	}
}

func TestMongoCompleteFindProjectionPickerAtSingleArgEnd(t *testing.T) {
	req := Request{
		Query:  `db.users.find({"status":"active"})`,
		Cursor: len(`db.users.find({"status":"active"})`),
		DBType: "mongo",
		Tables: []string{"users"},
		Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
		}},
		InferredTable: "users",
	}
	result := Complete(req)
	if result == nil {
		t.Fatal("expected result")
	}
	if result.Title != "Project Fields" {
		t.Fatalf("Title = %q, want Project Fields", result.Title)
	}
	if !result.Multi || result.MultiPrefix != ", {" || result.MultiSuffix != "}" {
		t.Fatalf("unexpected projection multi config: %+v", result)
	}
	if len(result.Items) == 0 || result.Items[0].Detail != "field" {
		t.Fatalf("expected field items, got %#v", result.Items)
	}
}

func TestMongoCompleteFindProjectionPreservesSelectedFields(t *testing.T) {
	req := Request{
		Query:  `db.users.find({}, {"email":1,"created_at":1})`,
		Cursor: len(`db.users.find({}, {"email":1`),
		DBType: "mongo",
		Tables: []string{"users"},
		Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
			{Name: "status", Type: "string"},
		}},
		InferredTable: "users",
	}
	result := Complete(req)
	if result == nil {
		t.Fatal("expected result")
	}
	if result.Title != "Project Fields" {
		t.Fatalf("Title = %q, want Project Fields", result.Title)
	}
	selected := map[string]bool{}
	for _, item := range result.Items {
		selected[item.Label] = item.Selected
	}
	if !selected["email"] || !selected["created_at"] {
		t.Fatalf("expected existing projection selections to be preserved, got %#v", result.Items)
	}
	if selected["status"] {
		t.Fatalf("status should not be preselected, got %#v", result.Items)
	}
}

func TestMongoCompleteFindOneProjectionPickerAtSingleArgEnd(t *testing.T) {
	req := Request{
		Query:  `db.users.findOne({"status":"active"})`,
		Cursor: len(`db.users.findOne({"status":"active"})`),
		DBType: "mongo",
		Tables: []string{"users"},
		Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
		}},
		InferredTable: "users",
	}
	result := Complete(req)
	if result == nil {
		t.Fatal("expected result")
	}
	if result.Title != "Project Fields" {
		t.Fatalf("Title = %q, want Project Fields", result.Title)
	}
	if !result.Multi || result.MultiPrefix != ", {" || result.MultiSuffix != "}" {
		t.Fatalf("unexpected projection multi config: %+v", result)
	}
}

func TestSQLCompleteRejectsWrongSchema(t *testing.T) {
	req := Request{
		Query:         `SELECT * FROM orders WHERE `,
		Cursor:        len(`SELECT * FROM orders WHERE `),
		DBType:        "sqlite",
		Tables:        []string{"users", "orders"},
		Schema:        &SchemaInfo{Name: "users", Columns: []ColumnInfo{{Name: "email", Type: "text"}}},
		InferredTable: "orders",
	}
	result := Complete(req)
	if result == nil {
		t.Fatal("expected result")
	}
	// Should not show "email" from users schema
	for _, item := range result.Items {
		if item.Label == "email" {
			t.Fatal("engine used users schema for orders query")
		}
	}
}

// A partial identifier touching the cursor must show Filter Column
// (ranked by prefix), not Operator — otherwise tab would overwrite the
// identifier with "=".
func TestSQLCompletePredicateIdentifierTouchingCursor(t *testing.T) {
	schema := &SchemaInfo{Name: "users", Columns: []ColumnInfo{
		{Name: "id", Type: "int", PrimaryKey: true},
		{Name: "name", Type: "text"},
		{Name: "email", Type: "text"},
	}}
	cases := []struct {
		name      string
		query     string
		wantTitle string
		wantFirst string
	}{
		{"whole identifier at cursor", "select * from users where name", "Filter Column", "name"},
		{"partial identifier at cursor", "select * from users where nam", "Filter Column", "name"},
		{"whitespace after identifier", "select * from users where name ", "Operator", "="},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := Request{
				Query:         tc.query,
				Cursor:        len(tc.query),
				DBType:        "sqlite",
				Tables:        []string{"users"},
				Schema:        schema,
				InferredTable: "users",
			}
			r := Complete(req)
			if r == nil {
				t.Fatalf("expected result, got nil")
			}
			if r.Title != tc.wantTitle {
				t.Fatalf("Title = %q, want %q", r.Title, tc.wantTitle)
			}
			if len(r.Items) == 0 || r.Items[0].Label != tc.wantFirst {
				t.Fatalf("first item = %v, want %q", r.Items, tc.wantFirst)
			}
		})
	}
}

// Past-operator positions (= != <> <= >= < > ~ LIKE IN IS …) must suppress
// the column / keyword picker so they don't misfire where a value is
// expected.
func TestSQLCompletePastOperatorSuppressed(t *testing.T) {
	schema := &SchemaInfo{Name: "users", Columns: []ColumnInfo{{Name: "name", Type: "text"}}}
	queries := []string{
		"select * from users where name =",
		"select * from users where name = ",
		"select * from users where name >= ",
		"select * from users where name != ",
		"select * from users where name <> ",
		"select * from users where name like ",
		"select * from users where name in ",
		"select * from users where name is ",
		"select * from users where name ~ ",
		"update users set name = ",
		"select * from users where a = 1 and b = ",
	}
	for _, q := range queries {
		req := Request{
			Query:         q,
			Cursor:        len(q),
			DBType:        "sqlite",
			Tables:        []string{"users"},
			Schema:        schema,
			InferredTable: "users",
		}
		if r := Complete(req); r != nil {
			t.Errorf("Complete(%q) = %+v, want nil (value position)", q, r.Title)
		}
	}
}

// Value-position suppression must not fire once the value literal is closed
// — next-clause keywords (AND / OR / GROUP BY / …) should still appear.
func TestSQLCompleteCompletedPredicateShowsClauses(t *testing.T) {
	schema := &SchemaInfo{Name: "users", Columns: []ColumnInfo{{Name: "name", Type: "text"}}}
	queries := []string{
		"select * from users where name = 'x'",
		"select * from users where name = 'x' ",
	}
	for _, q := range queries {
		req := Request{
			Query:         q,
			Cursor:        len(q),
			DBType:        "sqlite",
			Tables:        []string{"users"},
			Schema:        schema,
			InferredTable: "users",
		}
		r := Complete(req)
		if r == nil {
			t.Errorf("Complete(%q) returned nil, want clause picker", q)
			continue
		}
		foundAnd := false
		for _, it := range r.Items {
			if it.Label == "AND" {
				foundAnd = true
				break
			}
		}
		if !foundAnd {
			t.Errorf("Complete(%q) missing AND in clause picker: %v", q, r.Items)
		}
	}
}

func TestSQLCompleteSuggestsKeywordOnlyForNextClause(t *testing.T) {
	req := Request{
		Query:         `SELECT * FROM users `,
		Cursor:        len(`SELECT * FROM users `),
		DBType:        "sqlite",
		Tables:        []string{"users"},
		InferredTable: "users",
	}
	result := Complete(req)
	if result == nil {
		t.Fatal("expected result")
	}
	foundWhere := false
	for _, item := range result.Items {
		if item.Label == "WHERE" {
			foundWhere = true
			if item.InsertText != "WHERE" {
				t.Fatalf("WHERE InsertText = %q, want keyword only", item.InsertText)
			}
		}
	}
	if !foundWhere {
		t.Fatalf("expected WHERE suggestion, got %#v", result.Items)
	}
}
