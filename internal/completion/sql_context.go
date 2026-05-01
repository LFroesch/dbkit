package completion

import (
	"regexp"
	"strings"
)

// sqlValuePositionPattern catches the tail of a predicate when the cursor
// sits right after an operator (= != <> <= >= < > or word operators LIKE /
// ILIKE / IN / IS / SIMILAR TO / GLOB / REGEXP / BETWEEN) so we suppress the
// column / clause pickers that would otherwise misfire where a value is
// expected. Case-insensitive — the caller lowercases before.
var sqlValuePositionPattern = regexp.MustCompile(`(?:=|!=|<>|<=|>=|<|>|~\*?|!~\*?|\s(?:like|ilike|in|is|similar to|glob|regexp|between)\s*)\s*$`)

// normalizeWhitespace collapses runs of ASCII whitespace to single spaces.
// Used by clause-keyword searches so patterns like " where " match even when
// the keyword is newline- or tab-prefixed in a multi-line query.
func normalizeWhitespace(s string) string {
	if !strings.ContainsAny(s, "\n\r\t") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r':
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
		default:
			b.WriteRune(r)
			prevSpace = false
		}
	}
	return b.String()
}

type sqlContextKind int

const (
	sqlCtxNone sqlContextKind = iota
	sqlCtxSelectList
	sqlCtxPredicateColumn
	sqlCtxPredicateOperator
	sqlCtxPredicateValue
	sqlCtxOrderByList
	sqlCtxOrderDirection
	sqlCtxGroupByList
	sqlCtxInsertColumnList
	sqlCtxInsertValuesList
	sqlCtxUpdateSetList
	sqlCtxFromTable
	sqlCtxJoinTable
	sqlCtxUpdateTable
	sqlCtxInsertIntoTable
	sqlCtxDeleteFromTable
	sqlCtxLimitValue
)

type sqlContext struct {
	kind sqlContextKind
}

func ResolveSQLContext(before string) sqlContext {
	lower := normalizeWhitespace(strings.ToLower(before))

	switch {
	case inInsertValuesList(lower):
		return sqlContext{kind: sqlCtxInsertValuesList}
	case wantsOrderByDirection(lower):
		return sqlContext{kind: sqlCtxOrderDirection}
	case inLimitValue(lower):
		return sqlContext{kind: sqlCtxLimitValue}
	case inPredicateValue(lower):
		return sqlContext{kind: sqlCtxPredicateValue}
	case inPredicateOperator(lower):
		return sqlContext{kind: sqlCtxPredicateOperator}
	case inSelectList(lower):
		return sqlContext{kind: sqlCtxSelectList}
	case inPredicateColumn(lower):
		return sqlContext{kind: sqlCtxPredicateColumn}
	case inOrderByList(lower):
		return sqlContext{kind: sqlCtxOrderByList}
	case inGroupByList(lower):
		return sqlContext{kind: sqlCtxGroupByList}
	case inInsertColumnList(lower):
		return sqlContext{kind: sqlCtxInsertColumnList}
	case inUpdateSetList(lower):
		return sqlContext{kind: sqlCtxUpdateSetList}
	case inFromTable(lower):
		return sqlContext{kind: sqlCtxFromTable}
	case inJoinTable(lower):
		return sqlContext{kind: sqlCtxJoinTable}
	case inUpdateTable(lower):
		return sqlContext{kind: sqlCtxUpdateTable}
	case inInsertIntoTable(lower):
		return sqlContext{kind: sqlCtxInsertIntoTable}
	case inDeleteFromTable(lower):
		return sqlContext{kind: sqlCtxDeleteFromTable}
	default:
		return sqlContext{kind: sqlCtxNone}
	}
}

func inSelectList(before string) bool {
	selectIdx := LastKeyword(before, "select")
	if selectIdx < 0 {
		return false
	}
	afterSelect := before[selectIdx:]
	for _, blocker := range []string{" from ", " where ", " group by ", " order by ", " limit ", ";"} {
		if idx := strings.LastIndex(afterSelect, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func inPredicateColumn(before string) bool {
	start := predicateStart(before)
	if start < 0 {
		return false
	}
	for _, blocker := range []string{" = ", " != ", " > ", " < ", " like ", " ilike ", " in ", " is ", " similar to ", " glob ", " regexp ", " ~ ", " ~* ", " !~ ", " !~* "} {
		if idx := strings.LastIndex(before, blocker); idx > start {
			return false
		}
	}
	return true
}

func inPredicateOperator(before string) bool {
	start := predicateStart(before)
	if start < 0 {
		return false
	}
	tail := strings.TrimSpace(before[start:])
	if tail == "" {
		return false
	}
	parts := strings.Fields(strings.ReplaceAll(tail, ",", " "))
	if len(parts) == 0 {
		return false
	}
	switch parts[len(parts)-1] {
	case "where", "and", "or", "having", "set":
		return false
	}
	for _, blocker := range []string{" = ", " != ", " <> ", " > ", " >= ", " < ", " <= ", " like ", " ilike ", " in ", " is ", " similar to ", " glob ", " regexp ", " ~ ", " ~* ", " !~ ", " !~* "} {
		if idx := strings.LastIndex(before, blocker); idx > start {
			return false
		}
	}
	return sqlPredicateOperatorPattern.MatchString(before)
}

// inPredicateValue reports whether the cursor is inside a predicate at a
// position where a value is expected (right after =/</>/LIKE/IN/IS/BETWEEN
// with nothing but whitespace or an unclosed string literal after).
// Suppresses column / keyword pickers so they don't misfire between the
// operator and the value literal.
func inPredicateValue(before string) bool {
	start := predicateStart(before)
	if start < 0 {
		return false
	}
	tail := before[start:]
	if sqlValuePositionPattern.MatchString(tail) {
		return true
	}
	// Detect the "inside an unclosed string literal" tail. sqlValueCompletion
	// handles this via FindOpenQuote, but marking the context suppresses the
	// fallback keyword picker so it doesn't flash "SQL Clauses".
	inStr := false
	var quote rune
	escape := false
	for _, r := range tail {
		if escape {
			escape = false
			continue
		}
		if inStr {
			if r == '\\' {
				escape = true
				continue
			}
			if r == quote {
				inStr = false
			}
			continue
		}
		if r == '\'' || r == '"' {
			inStr = true
			quote = r
		}
	}
	return inStr
}

func predicateStart(before string) int {
	lastWhere := max(LastKeyword(before, " where "), LastKeyword(before, "where "))
	lastAnd := LastKeyword(before, " and ")
	lastOr := LastKeyword(before, " or ")
	lastHaving := LastKeyword(before, " having ")
	// JOIN ... ON is a predicate context too — without this, completing
	// columns inside an ON clause falls through to the keyword picker.
	lastOn := LastKeyword(before, " on ")
	start := max(max(lastWhere, lastAnd), max(lastOr, max(lastHaving, lastOn)))

	updateIdx := LastKeyword(before, "update ")
	setIdx := LastKeyword(before, " set ")
	lastComma := strings.LastIndex(before, ",")
	if updateIdx >= 0 && setIdx > updateIdx {
		lastWhereInUpdate := LastKeyword(before, " where ")
		if lastWhereInUpdate < setIdx {
			start = max(start, max(setIdx, lastComma))
		}
	}
	return start
}

// joinKeywordPattern matches all JOIN keyword variants (LEFT/RIGHT/INNER/
// OUTER/CROSS/FULL/NATURAL/LATERAL [OUTER] JOIN, plain JOIN). Used to
// normalize the FROM body before fragment splitting so multi-word join
// forms collapse to a single ` join ` token.
var joinKeywordPattern = regexp.MustCompile(`\b(?:(?:left|right|inner|outer|cross|full|natural|lateral)\s+)?(?:outer\s+)?join\b`)

func normalizeJoinKeywords(s string) string {
	return joinKeywordPattern.ReplaceAllString(s, "join")
}

// AliasBinding maps a FROM-clause alias to the underlying table name.
// Bare table references map alias=table.
type AliasBinding struct {
	Alias string
	Table string
}

// ParseFromBindings extracts (alias, table) pairs from the FROM/JOIN list of
// the lowercased before string. CTE names and derived tables are skipped —
// they have no resolvable schema. Limited to single-line forms.
func ParseFromBindings(before string) []AliasBinding {
	lower := normalizeWhitespace(before)
	fromIdx := LastKeyword(lower, " from ")
	if fromIdx < 0 && strings.HasPrefix(lower, "from ") {
		fromIdx = 0
	}
	if fromIdx < 0 {
		return nil
	}
	body := lower[fromIdx:]
	for _, kw := range []string{" where ", " group by ", " order by ", " limit ", " having "} {
		if i := strings.Index(body, kw); i >= 0 {
			body = body[:i]
		}
	}
	body = strings.TrimPrefix(body, " from ")
	body = strings.TrimPrefix(body, "from ")
	body = normalizeJoinKeywords(body)

	var bindings []AliasBinding
	// Split on commas (top-level only) and JOIN keywords.
	for _, frag := range splitFromFragments(body) {
		frag = strings.TrimSpace(frag)
		if frag == "" || strings.HasPrefix(frag, "(") {
			continue
		}
		// Drop ON-clause tail if present: `orders o on u.id = o.uid`.
		if i := strings.Index(frag, " on "); i >= 0 {
			frag = frag[:i]
		}
		fields := strings.Fields(frag)
		if len(fields) == 0 {
			continue
		}
		table := strings.Trim(fields[0], `"`)
		alias := table
		if len(fields) >= 3 && fields[1] == "as" {
			alias = strings.Trim(fields[2], `"`)
		} else if len(fields) >= 2 {
			alias = strings.Trim(fields[1], `"`)
		}
		bindings = append(bindings, AliasBinding{Alias: alias, Table: table})
	}
	return bindings
}

// splitFromFragments splits a FROM body on top-level commas and JOIN
// keywords, preserving parenthesized derived-table content as one fragment.
func splitFromFragments(body string) []string {
	var out []string
	depth := 0
	current := strings.Builder{}
	push := func() {
		s := strings.TrimSpace(current.String())
		if s != "" {
			out = append(out, s)
		}
		current.Reset()
	}
	runes := []rune(body)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if r == '(' {
			depth++
		} else if r == ')' {
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && r == ',' {
			push()
			continue
		}
		// Detect top-level " join " (and inner/outer/left/right/full/cross
		// variants collapse to the join keyword).
		if depth == 0 && r == ' ' && i+5 < len(runes) {
			if string(runes[i:i+6]) == " join " {
				push()
				i += 5
				continue
			}
		}
		current.WriteRune(r)
	}
	push()
	return out
}

// ResolveAliasTable returns the underlying table name for an alias in the
// query's FROM clause, or "" when the alias isn't bound or the binding is a
// CTE / derived table.
func ResolveAliasTable(before, alias string) string {
	if alias == "" {
		return ""
	}
	a := strings.ToLower(alias)
	for _, b := range ParseFromBindings(strings.ToLower(before)) {
		if b.Alias == a {
			return b.Table
		}
	}
	return ""
}

// queryHasMultipleTables reports whether the query references more than one
// table source: JOIN, comma-separated FROM list, derived table, or CTE.
// When true, schema-driven column suggestions are ambiguous unless the user
// disambiguates with an alias prefix (`a.col`).
func queryHasMultipleTables(before string) bool {
	normalized := normalizeJoinKeywords(before)
	if strings.Contains(normalized, " join ") {
		return true
	}
	// Only treat WITH at the start of the query as a CTE marker — a
	// substring match would false-trigger on string literals like
	// `WHERE name = 'thing with stuff'`.
	if strings.HasPrefix(strings.TrimSpace(before), "with ") {
		return true
	}
	// Comma in the FROM clause = multiple tables.
	fromIdx := LastKeyword(before, " from ")
	if fromIdx >= 0 {
		afterFrom := before[fromIdx+len(" from "):]
		// Stop at the next clause keyword.
		for _, kw := range []string{" where ", " group by ", " order by ", " limit ", " having "} {
			if i := strings.Index(afterFrom, kw); i >= 0 {
				afterFrom = afterFrom[:i]
			}
		}
		// Strip parenthesized derived tables before checking commas at the
		// top level of the FROM list.
		depth := 0
		topLevel := strings.Builder{}
		topLevel.Grow(len(afterFrom))
		for _, r := range afterFrom {
			switch r {
			case '(':
				depth++
				continue
			case ')':
				if depth > 0 {
					depth--
				}
				continue
			}
			if depth == 0 {
				topLevel.WriteRune(r)
			} else if r == ',' {
				// derived-table content; ignored
			}
		}
		if strings.Contains(topLevel.String(), ",") {
			return true
		}
		// Derived tables (`FROM (SELECT …) sub`) also count as multi-table
		// for column-resolution purposes.
		if strings.Contains(afterFrom, "(") && strings.Contains(strings.ToLower(afterFrom), "select") {
			return true
		}
	}
	return false
}

func inOrderByList(before string) bool {
	orderIdx := LastKeyword(before, " order by ")
	if orderIdx < 0 {
		return false
	}
	after := before[orderIdx:]
	for _, blocker := range []string{" limit ", " where ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func inLimitValue(before string) bool {
	limitIdx := LastKeyword(before, " limit ")
	if limitIdx < 0 {
		return false
	}
	after := before[limitIdx+len(" limit "):]
	return !strings.Contains(after, ";")
}

func wantsOrderByDirection(before string) bool {
	orderIdx := LastKeyword(before, " order by ")
	if orderIdx < 0 {
		return false
	}
	after := strings.TrimSpace(before[orderIdx+len(" order by "):])
	if after == "" {
		return false
	}
	after = strings.TrimRight(after, " \t")
	if strings.HasSuffix(after, ",") {
		return false
	}
	parts := strings.Fields(strings.ReplaceAll(after, ",", " "))
	if len(parts) == 0 {
		return false
	}
	last := strings.ToLower(parts[len(parts)-1])
	switch last {
	case "asc", "desc", "nulls", "first", "last":
		return false
	default:
		return true
	}
}

func inGroupByList(before string) bool {
	groupIdx := LastKeyword(before, " group by ")
	if groupIdx < 0 {
		return false
	}
	after := before[groupIdx:]
	for _, blocker := range []string{" order by ", " limit ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func inInsertColumnList(before string) bool {
	insertIdx := LastKeyword(before, "insert into ")
	valuesIdx := LastKeyword(before, " values")
	openParen := strings.LastIndex(before, "(")
	closeParen := strings.LastIndex(before, ")")
	return insertIdx >= 0 && openParen > insertIdx && openParen > closeParen && valuesIdx < openParen
}

func inInsertValuesList(before string) bool {
	insertIdx := LastKeyword(before, "insert into ")
	valuesIdx := LastKeyword(before, " values")
	openParen := strings.LastIndex(before, "(")
	closeParen := strings.LastIndex(before, ")")
	return insertIdx >= 0 && valuesIdx > insertIdx && openParen > valuesIdx && openParen > closeParen
}

func inUpdateSetList(before string) bool {
	updateIdx := LastKeyword(before, "update ")
	setIdx := LastKeyword(before, " set ")
	if updateIdx < 0 || setIdx < updateIdx {
		return false
	}
	lastWhere := LastKeyword(before, " where ")
	if lastWhere > setIdx {
		return false
	}
	for _, blocker := range []string{" = ", " != ", " > ", " < ", " like "} {
		if idx := strings.LastIndex(before, blocker); idx > setIdx {
			return false
		}
	}
	return true
}

func inFromTable(before string) bool {
	fromIdx := LastKeyword(before, " from ")
	if fromIdx < 0 {
		return false
	}
	after := before[fromIdx:]
	for _, blocker := range []string{" where ", " join ", " group by ", " order by ", " limit ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return strings.TrimSpace(before[fromIdx+len(" from "):]) == ""
}

func inJoinTable(before string) bool {
	joinIdx := LastKeyword(before, " join ")
	if joinIdx < 0 {
		return false
	}
	after := before[joinIdx:]
	for _, blocker := range []string{" on ", " where ", " group by ", " order by ", " limit ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return strings.TrimSpace(before[joinIdx+len(" join "):]) == ""
}

func inUpdateTable(before string) bool {
	updateIdx := LastKeyword(before, "update ")
	if updateIdx < 0 {
		return false
	}
	after := before[updateIdx:]
	for _, blocker := range []string{" set ", " where ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return strings.TrimSpace(before[updateIdx+len("update "):]) == ""
}

func inInsertIntoTable(before string) bool {
	insertIdx := LastKeyword(before, "insert into ")
	if insertIdx < 0 {
		return false
	}
	after := before[insertIdx:]
	for _, blocker := range []string{"(", " values", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return strings.TrimSpace(before[insertIdx+len("insert into "):]) == ""
}

func inDeleteFromTable(before string) bool {
	deleteIdx := LastKeyword(before, "delete from ")
	if deleteIdx < 0 {
		return false
	}
	after := before[deleteIdx:]
	for _, blocker := range []string{" where ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return strings.TrimSpace(before[deleteIdx+len("delete from "):]) == ""
}

func InSelectList(before string) bool { return ResolveSQLContext(before).kind == sqlCtxSelectList }
func InWhereClause(before string) bool {
	return inPredicateColumn(normalizeWhitespace(strings.ToLower(before)))
}
func InHavingClause(before string) bool {
	norm := normalizeWhitespace(strings.ToLower(before))
	return ResolveSQLContext(before).kind == sqlCtxPredicateColumn && LastKeyword(norm, " having ") >= 0
}
func InOrderByList(before string) bool { return ResolveSQLContext(before).kind == sqlCtxOrderByList }
func InLimitValue(before string) bool  { return ResolveSQLContext(before).kind == sqlCtxLimitValue }
func OrderByWantsDirection(before string) bool {
	return ResolveSQLContext(before).kind == sqlCtxOrderDirection
}
func InGroupByList(before string) bool { return ResolveSQLContext(before).kind == sqlCtxGroupByList }
func InInsertColumnList(before string) bool {
	return ResolveSQLContext(before).kind == sqlCtxInsertColumnList
}
func InInsertValuesList(before string) bool {
	return ResolveSQLContext(before).kind == sqlCtxInsertValuesList
}
func InUpdateSetList(before string) bool {
	return ResolveSQLContext(before).kind == sqlCtxUpdateSetList
}
func InFromTable(before string) bool   { return ResolveSQLContext(before).kind == sqlCtxFromTable }
func InJoinTable(before string) bool   { return ResolveSQLContext(before).kind == sqlCtxJoinTable }
func InUpdateTable(before string) bool { return ResolveSQLContext(before).kind == sqlCtxUpdateTable }
func InInsertIntoTable(before string) bool {
	return ResolveSQLContext(before).kind == sqlCtxInsertIntoTable
}
func InDeleteFromTable(before string) bool {
	return ResolveSQLContext(before).kind == sqlCtxDeleteFromTable
}
