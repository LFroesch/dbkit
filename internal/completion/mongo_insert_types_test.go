package completion

import (
	"encoding/json"
	"strings"
	"testing"
)

// MongoTypedJSONLiteral must emit valid, unquoted JSON for typed literals.
// Strings are the only kind that get JSON-quoted.
func TestMongoTypedJSONLiteralEmitsCorrectTypes(t *testing.T) {
	cases := []struct {
		name      string
		fieldType string
		raw       string
		want      string
	}{
		{"bool true", "bool", "true", "true"},
		{"boolean false", "boolean", "false", "false"},
		{"bool case-insensitive", "bool", "TRUE", "true"},
		{"int", "int", "42", "42"},
		{"uint", "uint", "7", "7"},
		{"float", "float", "3.14", "3.14"},
		{"decimal", "decimal", "-0.5", "-0.5"},
		{"number negative", "number", "-100", "-100"},
		{"null literal", "null", "null", "null"},
		{"null in any type", "string", "NULL", "null"},
		{"objectid hex", "objectid", "65a1b2c3d4e5f6789a0b1c2d", `{"$oid":"65a1b2c3d4e5f6789a0b1c2d"}`},
		{"date rfc3339", "date", "2026-01-15T10:00:00Z", `{"$date":"2026-01-15T10:00:00Z"}`},
		{"string fallback", "string", "hello", `"hello"`},
		{"bool with non-bool raw falls back to quoted", "bool", "yes", `"yes"`},
		{"objectid with bad hex falls back to quoted", "objectid", "not-an-oid", `"not-an-oid"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MongoTypedJSONLiteral(tc.fieldType, tc.raw)
			if got != tc.want {
				t.Fatalf("MongoTypedJSONLiteral(%q, %q) = %q, want %q", tc.fieldType, tc.raw, got, tc.want)
			}
			// Strings are the only quoted form; everything else must be valid JSON
			// unquoted on the value side. Sanity: parse the inserted literal.
			var v any
			if err := json.Unmarshal([]byte(got), &v); err != nil {
				t.Fatalf("emitted literal %q is not valid JSON: %v", got, err)
			}
		})
	}
}

// MongoLiteralCandidates must offer typed literals that round-trip as JSON
// values of the right kind — not string-wrapped numbers/bools/etc.
func TestMongoLiteralCandidatesEmitTypedJSON(t *testing.T) {
	cases := []struct {
		fieldType string
		want      []string
	}{
		{"bool", []string{"true", "false", "null"}},
		{"boolean", []string{"true", "false", "null"}},
		{"int", []string{"0", "1", "-1", "3.14", "null"}},
		{"number", []string{"0", "1", "-1", "3.14", "null"}},
		{"null", []string{"null"}},
		{"objectid", []string{`{"$oid":"000000000000000000000000"}`, "null"}},
	}
	for _, tc := range cases {
		t.Run(tc.fieldType, func(t *testing.T) {
			got := MongoLiteralCandidates(tc.fieldType)
			if len(got) != len(tc.want) {
				t.Fatalf("MongoLiteralCandidates(%q) len = %d, want %d (%v)", tc.fieldType, len(got), len(tc.want), got)
			}
			for i, want := range tc.want {
				if got[i] != want {
					t.Fatalf("MongoLiteralCandidates(%q)[%d] = %q, want %q", tc.fieldType, i, got[i], want)
				}
				var v any
				if err := json.Unmarshal([]byte(want), &v); err != nil {
					t.Fatalf("literal candidate %q is not valid JSON: %v", want, err)
				}
			}
		})
	}
}

// MongoPlaceholderForType drives the field-key insert scaffold
// (`"field":<placeholder>`). The placeholder must be a valid JSON literal of
// the expected kind so the inserted document parses cleanly.
func TestMongoPlaceholderForTypeIsValidJSON(t *testing.T) {
	types := []string{"bool", "boolean", "int", "uint", "float", "decimal", "number", "null", "objectid", "date", "datetime", "timestamp", "array", "object", "document", "map", "mixed", "string", "unknown-type"}
	for _, tp := range types {
		t.Run(tp, func(t *testing.T) {
			got := MongoPlaceholderForType(tp)
			var v any
			if err := json.Unmarshal([]byte(got), &v); err != nil {
				t.Fatalf("placeholder for %q (%q) is not valid JSON: %v", tp, got, err)
			}
		})
	}
}

// In the insert flow, completing a field key must emit a literal of the
// correct JSON type. Specifically: bool/number columns should default to
// false/0, never to a string-wrapped value like "false" or "0".
func TestMongoInsertFieldScaffoldUsesTypedDefaults(t *testing.T) {
	cases := []struct {
		fieldType string
		bad       []string // forbidden insertions
	}{
		{"bool", []string{`"false"`, `"true"`}},
		{"int", []string{`"0"`, `""`}},
		{"number", []string{`"0"`, `""`}},
		{"objectid", []string{`""`}},
	}
	for _, tc := range cases {
		t.Run(tc.fieldType, func(t *testing.T) {
			got := MongoPlaceholderForType(tc.fieldType)
			for _, bad := range tc.bad {
				if got == bad {
					t.Fatalf("placeholder for %q = %q, must not be string-wrapped %q", tc.fieldType, got, bad)
				}
			}
		})
	}
}

// End-to-end: insertOne on a schema with mixed types must surface field
// items as completion options inside the document body, with insertions
// that are valid JSON of the right kind.
func TestMongoCompleteInsertOneFieldKeyInsertions(t *testing.T) {
	q := `db.users.insertOne({`
	req := Request{
		Query:  q,
		Cursor: len(q),
		DBType: "mongo",
		Tables: []string{"users"},
		Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
			{Name: "active", Type: "bool"},
			{Name: "age", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "ref_id", Type: "objectid"},
		}},
		InferredTable: "users",
	}
	r := Complete(req)
	if r == nil {
		t.Fatal("expected result")
	}
	byField := map[string]string{}
	for _, it := range r.Items {
		if it.Detail == "field" {
			byField[it.Label] = it.InsertText
		}
	}
	want := map[string]string{
		"active": `"active":false`,
		"age":    `"age":0`,
		"name":   `"name":""`,
		"ref_id": `"ref_id":{"$oid":"000000000000000000000000"}`,
	}
	for field, expected := range want {
		got, ok := byField[field]
		if !ok {
			t.Errorf("missing field item %q in %v", field, byField)
			continue
		}
		if got != expected {
			t.Errorf("field %q insert = %q, want %q", field, got, expected)
		}
		// The value side (after the colon) must parse as JSON.
		idx := strings.Index(got, ":")
		if idx < 0 {
			t.Errorf("field %q insert %q has no colon", field, got)
			continue
		}
		var v any
		if err := json.Unmarshal([]byte(got[idx+1:]), &v); err != nil {
			t.Errorf("field %q value %q is not valid JSON: %v", field, got[idx+1:], err)
		}
	}
}

// Top-level arg scaffold for insertOne / updateOne / deleteOne /
// countDocuments must be reachable when the cursor sits in the empty
// argument position. A regression here means the picker shows nothing
// when the user tabs from `db.users.insertOne(|)`.
func TestMongoCompleteShellMethodEmptyArgScaffold(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{"insertOne", `db.users.insertOne(`},
		{"updateOne", `db.users.updateOne(`},
		{"deleteOne", `db.users.deleteOne(`},
		{"countDocuments", `db.users.countDocuments(`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := Request{
				Query:  tc.query,
				Cursor: len(tc.query),
				DBType: "mongo",
				Tables: []string{"users"},
				Schema: &SchemaInfo{Name: "users", Columns: []ColumnInfo{
					{Name: "name", Type: "string"},
					{Name: "active", Type: "bool"},
				}},
				InferredTable: "users",
			}
			r := Complete(req)
			if r == nil || len(r.Items) == 0 {
				t.Fatalf("Complete(%q) returned no items", tc.query)
			}
		})
	}
}
