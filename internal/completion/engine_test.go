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
