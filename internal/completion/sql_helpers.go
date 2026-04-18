package completion

import (
	"regexp"
	"strings"
)

// CursorInsideString reports whether the cursor sits inside an unclosed
// single- or double-quoted string literal.
func CursorInsideString(query string, cursor int) bool {
	runes := []rune(query)
	if cursor > len(runes) {
		cursor = len(runes)
	}
	inQuote := false
	var quote rune
	for i := 0; i < cursor; i++ {
		r := runes[i]
		if inQuote {
			if r == quote {
				if r == '\'' && i+1 < cursor && runes[i+1] == '\'' {
					i++
					continue
				}
				inQuote = false
			}
			continue
		}
		if r == '\'' || r == '"' {
			inQuote = true
			quote = r
		}
	}
	return inQuote
}

// FindOpenQuote returns the position of the unclosed quote rune before the
// cursor, the quote char, and whether one was found.
func FindOpenQuote(runes []rune, cursor int) (int, rune, bool) {
	if cursor > len(runes) {
		cursor = len(runes)
	}
	inQuote := false
	var quote rune
	openIdx := -1
	for i := 0; i < cursor; i++ {
		r := runes[i]
		if inQuote {
			if r == quote {
				if r == '\'' && i+1 < cursor && runes[i+1] == '\'' {
					i++
					continue
				}
				inQuote = false
			}
			continue
		}
		if r == '\'' || r == '"' {
			inQuote = true
			quote = r
			openIdx = i
		}
	}
	if !inQuote {
		return -1, 0, false
	}
	return openIdx, quote, true
}

var valueOpPattern = regexp.MustCompile(`(?i)(?:^|[\s,(])((?:"[^"]+"|[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?))\s*(?:=|!=|<>|<=|>=|<|>|\bLIKE\b|\bILIKE\b|\bIN\s*\(\s*)[^=<>!]*$`)

// ColumnBeforeValueLiteral returns the column name implied by the operator
// sequence immediately preceding the opening quote, or "" if none.
func ColumnBeforeValueLiteral(before string) string {
	match := valueOpPattern.FindStringSubmatch(before)
	if len(match) < 2 {
		return ""
	}
	col := strings.Trim(match[1], `"`)
	if idx := strings.LastIndex(col, "."); idx >= 0 {
		col = col[idx+1:]
	}
	return col
}

// SQLOperatorItems returns the operator/predicate completion menu for SQL.
// Postgres adds ILIKE and POSIX regex operators; SQLite adds GLOB/REGEXP.
func SQLOperatorItems(dbType string) []Item {
	base := []Item{
		{Label: "=", Detail: "equals", InsertText: "= ''"},
		{Label: "!=", Detail: "not equal", InsertText: "!= ''"},
		{Label: ">", Detail: "greater than", InsertText: "> "},
		{Label: ">=", Detail: "greater or equal", InsertText: ">= "},
		{Label: "<", Detail: "less than", InsertText: "< "},
		{Label: "<=", Detail: "less or equal", InsertText: "<= "},
		{Label: "LIKE", Detail: "pattern match", InsertText: "LIKE '%%'"},
		{Label: "IN", Detail: "set membership", InsertText: "IN ('')"},
		{Label: "IS NULL", Detail: "null check", InsertText: "IS NULL"},
		{Label: "IS NOT NULL", Detail: "not null check", InsertText: "IS NOT NULL"},
	}
	switch dbType {
	case "postgres":
		base = append(base,
			Item{Label: "ILIKE", Detail: "case-insensitive pattern", InsertText: "ILIKE '%%'"},
			Item{Label: "SIMILAR TO", Detail: "regex pattern", InsertText: "SIMILAR TO '%%'"},
			Item{Label: "~", Detail: "POSIX regex (case-sensitive)", InsertText: "~ ''"},
			Item{Label: "~*", Detail: "POSIX regex (case-insensitive)", InsertText: "~* ''"},
			Item{Label: "!~", Detail: "not regex (case-sensitive)", InsertText: "!~ ''"},
			Item{Label: "!~*", Detail: "not regex (case-insensitive)", InsertText: "!~* ''"},
		)
	case "sqlite":
		base = append(base,
			Item{Label: "GLOB", Detail: "case-sensitive glob", InsertText: "GLOB '*'"},
			Item{Label: "REGEXP", Detail: "regexp (requires ext)", InsertText: "REGEXP ''"},
		)
	}
	return base
}
