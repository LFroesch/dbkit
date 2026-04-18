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

func TestResolveSQLContext(t *testing.T) {
	tests := []struct {
		before string
		want   sqlContextKind
	}{
		{"select ", sqlCtxSelectList},
		{"select * from users where ", sqlCtxPredicateColumn},
		{"select * from users where email ", sqlCtxPredicateOperator},
		{"select * from users order by created_at ", sqlCtxOrderDirection},
		{"select * from users limit ", sqlCtxLimitValue},
		{`insert into "users" ("email") values (`, sqlCtxInsertValuesList},
	}
	for _, tt := range tests {
		if got := ResolveSQLContext(tt.before).kind; got != tt.want {
			t.Errorf("ResolveSQLContext(%q) = %v, want %v", tt.before, got, tt.want)
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

// Multi-line queries must resolve clause context even when the keyword
// sits at the start of a line (newline-prefixed instead of space-prefixed).
func TestMultiLineClauseDetection(t *testing.T) {
	tests := []struct {
		name                 string
		before               string
		wantWhere            bool
		wantOrderByList      bool
		wantLimit            bool
		wantOrderByDirection bool
	}{
		{"AND on its own line", "select * from users where a = 1\nand ", true, false, false, false},
		{"WHERE at end of line", "select * from users where\n", true, false, false, false},
		{"WHERE-led line continues predicate", "select * from users\nwhere ", true, false, false, false},
		{"LIMIT-led line", "select * from users\nlimit ", false, false, true, false},
		{"ORDER BY column across newline", "select * from users\norder by ", false, true, false, false},
		{"ORDER BY direction across newline", "select * from users\norder by created_at ", false, false, false, true},
		{"HAVING-led line", "select * from users\ngroup by role\nhaving ", true, false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InWhereClause(tt.before); got != tt.wantWhere {
				t.Errorf("InWhereClause = %v, want %v", got, tt.wantWhere)
			}
			if got := InOrderByList(tt.before); got != tt.wantOrderByList {
				t.Errorf("InOrderByList = %v, want %v", got, tt.wantOrderByList)
			}
			if got := InLimitValue(tt.before); got != tt.wantLimit {
				t.Errorf("InLimitValue = %v, want %v", got, tt.wantLimit)
			}
			if got := OrderByWantsDirection(tt.before); got != tt.wantOrderByDirection {
				t.Errorf("OrderByWantsDirection = %v, want %v", got, tt.wantOrderByDirection)
			}
		})
	}
}

func TestSQLOperatorItemsDoNotInjectNumericPlaceholder(t *testing.T) {
	items := SQLOperatorItems("postgres")
	ops := map[string]string{}
	for _, item := range items {
		ops[item.Label] = item.InsertText
	}

	if got := ops[">="]; got != ">= " {
		t.Fatalf(">= insert text = %q, want %q", got, ">= ")
	}
	if got := ops["<"]; got != "< " {
		t.Fatalf("< insert text = %q, want %q", got, "< ")
	}
	if got := ops["="]; got != "= ''" {
		t.Fatalf("= insert text = %q, want quoted scaffold", got)
	}
}
