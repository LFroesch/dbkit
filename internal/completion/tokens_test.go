package completion

import "testing"

func TestTokenBounds(t *testing.T) {
	query := []rune("SELECT name FROM users")
	start, end := TokenBounds(query, 10) // cursor in "name"
	if got := string(query[start:end]); got != "name" {
		t.Errorf("TokenBounds = %q, want %q", got, "name")
	}
}

func TestTokenValue(t *testing.T) {
	if got := TokenValue([]rune(`"users"`)); got != "users" {
		t.Errorf("TokenValue = %q, want %q", got, "users")
	}
}

func TestInWhereClause(t *testing.T) {
	tests := []struct {
		before string
		want   bool
	}{
		{"select * from users where ", true},
		{"select * from users where name = 'x' and ", true},
		{"select * from users where name", true},
		{"select * from users where name = 'x'", false},
		{"select * from users ", false},
	}
	for _, tt := range tests {
		if got := InWhereClause(tt.before); got != tt.want {
			t.Errorf("InWhereClause(%q) = %v, want %v", tt.before, got, tt.want)
		}
	}
}

func TestInSelectList(t *testing.T) {
	tests := []struct {
		before string
		want   bool
	}{
		{"select ", true},
		{"select name, ", true},
		{"select name from ", false},
	}
	for _, tt := range tests {
		if got := InSelectList(tt.before); got != tt.want {
			t.Errorf("InSelectList(%q) = %v, want %v", tt.before, got, tt.want)
		}
	}
}

func TestFuzzyMatch(t *testing.T) {
	if !FuzzyMatch("created_at", "cat") {
		t.Error("FuzzyMatch should match 'cat' in 'created_at'")
	}
	if FuzzyMatch("name", "zz") {
		t.Error("FuzzyMatch should not match 'zz' in 'name'")
	}
}
