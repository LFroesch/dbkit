package completion

import "strings"

// TokenBounds returns the [start, end) rune offsets of the identifier-like
// token under cursor.
func TokenBounds(query []rune, cursor int) (int, int) {
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(query) {
		cursor = len(query)
	}
	start := cursor
	for start > 0 && IsTokenRune(query[start-1]) {
		start--
	}
	end := cursor
	for end < len(query) && IsTokenRune(query[end]) {
		end++
	}
	return start, end
}

func IsTokenRune(r rune) bool {
	return r == '_' || r == '"' || r == '.' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// TokenValue strips surrounding whitespace and quotes from a token slice.
func TokenValue(token []rune) string {
	return strings.Trim(strings.TrimSpace(string(token)), `"`)
}

// PrefixWithoutAlias strips a leading "alias." from a qualified identifier.
func PrefixWithoutAlias(prefix string) string {
	if idx := strings.LastIndex(prefix, "."); idx >= 0 {
		return prefix[idx+1:]
	}
	return prefix
}

// LastKeyword returns the last index of keyword in lowercased before.
func LastKeyword(before, keyword string) int {
	return strings.LastIndex(strings.ToLower(before), keyword)
}

func SplitLines(query []rune) [][]rune {
	lines := strings.Split(string(query), "\n")
	out := make([][]rune, 0, len(lines))
	for _, line := range lines {
		out = append(out, []rune(line))
	}
	if len(out) == 0 {
		return [][]rune{{}}
	}
	return out
}

func IndexForLineCol(query []rune, line, col int) int {
	lines := SplitLines(query)
	if line < 0 {
		line = 0
	}
	if line >= len(lines) {
		line = len(lines) - 1
	}
	idx := 0
	for i := 0; i < line; i++ {
		idx += len(lines[i]) + 1
	}
	if col < 0 {
		col = 0
	}
	if col > len(lines[line]) {
		col = len(lines[line])
	}
	return idx + col
}

func LineColForIndex(query []rune, idx int) (int, int) {
	if idx < 0 {
		idx = 0
	}
	if idx > len(query) {
		idx = len(query)
	}
	line := 0
	col := 0
	for i := 0; i < idx; i++ {
		if query[i] == '\n' {
			line++
			col = 0
			continue
		}
		col++
	}
	return line, col
}

// FuzzyMatch returns true when every character in query appears in candidate in order.
func FuzzyMatch(candidate, query string) bool {
	if query == "" {
		return true
	}
	pos := 0
	for _, r := range candidate {
		if pos < len(query) && rune(query[pos]) == r {
			pos++
		}
	}
	return pos == len(query)
}
