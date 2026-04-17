package completion

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

var sqlPredicateOperatorPattern = regexp.MustCompile(`(?i)(?:"[^"]+"|[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)\s*$`)

func sqlComplete(req Request) *Result {
	// Validate schema matches the inferred table to prevent stale data
	if req.Schema != nil && req.InferredTable != "" && !strings.EqualFold(req.Schema.Name, req.InferredTable) {
		req.Schema = nil
	}
	before := strings.ToLower(queryBeforeCursor(req))

	if InInsertValuesList(before) {
		return nil
	}

	if r := sqlValueCompletion(req); r != nil {
		return r
	}

	if CursorInsideString(req.Query, req.Cursor) {
		return nil
	}

	if r := sqlOperatorCompletion(req); r != nil {
		return r
	}

	if r := sqlColumnCompletion(req); r != nil {
		return r
	}

	if r := sqlTableCompletion(req); r != nil {
		return r
	}

	if r := sqlClauseValueCompletion(req); r != nil {
		return r
	}

	return sqlKeywordCompletion(req)
}

func queryBeforeCursor(req Request) string {
	q := []rune(req.Query)
	c := req.Cursor
	if c > len(q) {
		c = len(q)
	}
	if c < 0 {
		c = 0
	}
	return string(q[:c])
}

// --- SQL operator completion (WHERE col |, UPDATE SET col |) ---

func sqlOperatorCompletion(req Request) *Result {
	query := []rune(req.Query)
	start, end := TokenBounds(query, req.Cursor)
	beforeToken := strings.ToLower(string(query[:start]))
	prefix := strings.ToLower(TokenValue(query[start:end]))

	if !InWhereClause(beforeToken) && !InUpdateSetList(beforeToken) && !InHavingClause(beforeToken) {
		return nil
	}
	if prefix != "" {
		for _, r := range prefix {
			if !unicode.IsLetter(r) && r != '!' && r != '<' && r != '>' && r != '=' {
				return nil
			}
		}
	}
	if !sqlPredicateOperatorPattern.MatchString(beforeToken) {
		return nil
	}

	items := SQLOperatorItems(req.DBType)
	return &Result{
		Items:    RankItems(prefix, items),
		Title:    "Operator",
		Start:    start,
		End:      end,
		Fallback: TokenValue(query[start:end]),
	}
}

// --- SQL column completion (SELECT cols, WHERE col, ORDER BY, etc.) ---

func sqlColumnCompletion(req Request) *Result {
	query := []rune(req.Query)
	start, end := TokenBounds(query, req.Cursor)
	// Include * in the replacement range so SELECT * → SELECT col1, col2
	cursorOnStar := false
	if req.Cursor < len(query) && query[req.Cursor] == '*' && start == end {
		end = req.Cursor + 1
		cursorOnStar = true
	} else if req.Cursor > 0 && req.Cursor <= len(query) && query[req.Cursor-1] == '*' && start == end {
		start = req.Cursor - 1
		cursorOnStar = true
	}
	beforeToken := strings.ToLower(string(query[:start]))
	trimmed := strings.TrimSpace(beforeToken)

	type colCtx struct {
		title        string
		multi        bool
		includeStar  bool
		filterSuffix string
	}

	var ctx colCtx
	switch {
	case InSelectList(beforeToken):
		ctx = colCtx{title: "Select Columns", multi: true, includeStar: !cursorOnStar}
	case InWhereClause(beforeToken):
		ctx = colCtx{title: "Filter Column", filterSuffix: " = ''"}
	case InHavingClause(beforeToken):
		ctx = colCtx{title: "Having Column", filterSuffix: " = ''"}
	case InOrderByList(beforeToken):
		if OrderByWantsDirection(beforeToken) {
			return nil
		}
		ctx = colCtx{title: "Order By Columns", multi: true}
	case InGroupByList(beforeToken):
		ctx = colCtx{title: "Group By Columns", multi: true}
	case InInsertColumnList(beforeToken):
		ctx = colCtx{title: "Insert Columns", multi: true}
	case InUpdateSetList(beforeToken):
		ctx = colCtx{title: "Set Columns"}
	case trimmed == "":
		return nil
	default:
		return nil
	}

	prefix := strings.ToLower(TokenValue(query[start:end]))
	aliasPrefix := extractAliasPrefix(string(query[start:end]))

	// Build column items from schema
	items := sqlColumnItems(req, ctx.includeStar, aliasPrefix, prefix)

	// Signal a schema load if we have no real columns (only * or empty)
	var needSchema string
	hasRealCols := false
	for _, item := range items {
		if item.Label != "*" {
			hasRealCols = true
			break
		}
	}
	if !hasRealCols && req.Schema == nil && req.InferredTable != "" {
		needSchema = req.InferredTable
		items = append(items, Item{Label: "loading fields…", Detail: req.InferredTable, InsertText: ""})
	}

	if len(items) == 0 {
		return nil
	}

	return &Result{
		Items:      items,
		Title:      ctx.title,
		Start:      start,
		End:        end,
		Multi:      ctx.multi,
		Fallback:   TokenValue(query[start:end]),
		NeedSchema: needSchema,
	}
}

func sqlColumnItems(req Request, includeStar bool, aliasPrefix, prefix string) []Item {
	var cols []ColumnInfo
	if req.Schema != nil {
		cols = req.Schema.Columns
	}

	items := make([]Item, 0, len(cols)+1)
	if includeStar && len(cols) > 0 {
		items = append(items, Item{Label: "*", Detail: "all", InsertText: "*"})
	}
	for _, col := range cols {
		items = append(items, Item{
			Label:      col.Name,
			Detail:     col.Type,
			InsertText: columnInsertionValue(req.DBType, col.Name, aliasPrefix),
		})
	}
	return RankItems(PrefixWithoutAlias(prefix), items)
}

func columnInsertionValue(dbType, name, aliasPrefix string) string {
	if name == "*" {
		return name
	}
	if dbType == "mongo" {
		if aliasPrefix != "" {
			return aliasPrefix + name
		}
		return name
	}
	value := QuoteIdentifier(dbType, name)
	if aliasPrefix != "" {
		return aliasPrefix + value
	}
	return value
}

func extractAliasPrefix(token string) string {
	t := TokenValue([]rune(token))
	if idx := strings.LastIndex(t, "."); idx >= 0 {
		alias := strings.TrimSpace(t[:idx])
		if alias != "" {
			return alias + "."
		}
	}
	return ""
}

// --- SQL table completion (FROM, JOIN, UPDATE, INSERT INTO, DELETE FROM) ---

func sqlTableCompletion(req Request) *Result {
	query := []rune(req.Query)
	start, end := TokenBounds(query, req.Cursor)
	beforeToken := strings.ToLower(string(query[:start]))

	var title string
	switch {
	case InFromTable(beforeToken):
		title = "From Table"
	case InJoinTable(beforeToken):
		title = "Join Table"
	case InUpdateTable(beforeToken):
		title = "Update Table"
	case InInsertIntoTable(beforeToken):
		title = "Insert Into"
	case InDeleteFromTable(beforeToken):
		title = "Delete From"
	default:
		return nil
	}

	prefix := strings.ToLower(TokenValue(query[start:end]))
	items := make([]Item, 0, len(req.Tables))
	for _, name := range req.Tables {
		items = append(items, Item{
			Label:      name,
			Detail:     DataSourceLabel(req.DBType),
			InsertText: QuoteIdentifier(req.DBType, name),
		})
	}
	items = RankItems(prefix, items)
	if len(items) == 0 {
		return nil
	}

	return &Result{
		Items:    items,
		Title:    title,
		Start:    start,
		End:      end,
		Fallback: TokenValue(query[start:end]),
	}
}

// --- SQL value completion (WHERE col = '|') ---

func sqlValueCompletion(req Request) *Result {
	runes := []rune(req.Query)
	openIdx, _, ok := FindOpenQuote(runes, req.Cursor)
	if !ok {
		return nil
	}
	before := string(runes[:openIdx])
	col := ColumnBeforeValueLiteral(before)
	if col == "" {
		return nil
	}
	table := effectiveTable(req)
	if table == "" {
		return nil
	}

	key := ValueCacheKey(table, col)
	values := req.ValueCache[key]
	cached := req.ValueCache != nil && values != nil

	prefix := strings.ToLower(string(runes[openIdx+1 : req.Cursor]))
	items := make([]Item, 0, len(values)+1)
	for _, v := range values {
		items = append(items, Item{Label: v, Detail: col, InsertText: v})
	}
	items = RankItems(prefix, items)

	var needValues *ValueRequest
	if !cached {
		needValues = &ValueRequest{Table: table, Column: col}
		if len(items) == 0 {
			items = append(items, Item{Label: "loading…", Detail: "fetching samples", InsertText: prefix})
		}
	}
	if len(items) == 0 {
		items = append(items, Item{Label: "(no samples)", Detail: col, InsertText: prefix})
	}

	return &Result{
		Items:      items,
		Title:      "Values for " + col,
		Start:      openIdx + 1,
		End:        req.Cursor,
		Fallback:   prefix,
		ValueMode:  true,
		ValueCol:   col,
		ValueTable: table,
		NeedValues: needValues,
	}
}

// --- SQL clause value completion (LIMIT N, ORDER BY col ASC/DESC) ---

func sqlClauseValueCompletion(req Request) *Result {
	query := []rune(req.Query)
	start, end := TokenBounds(query, req.Cursor)
	beforeToken := strings.ToLower(string(query[:start]))
	prefix := strings.ToLower(TokenValue(query[start:end]))

	if InLimitValue(beforeToken) {
		items := []Item{
			{Label: "10", Detail: "limit", InsertText: "10"},
			{Label: "20", Detail: "limit", InsertText: "20"},
			{Label: "50", Detail: "limit", InsertText: "50"},
			{Label: "100", Detail: "limit", InsertText: "100"},
			{Label: "200", Detail: "limit", InsertText: "200"},
			{Label: "500", Detail: "limit", InsertText: "500"},
			{Label: "1000", Detail: "limit", InsertText: "1000"},
		}
		return &Result{
			Items:    RankItems(prefix, items),
			Title:    "Limit",
			Start:    start,
			End:      end,
			Fallback: TokenValue(query[start:end]),
		}
	}

	if OrderByWantsDirection(beforeToken) {
		items := []Item{
			{Label: "ASC", Detail: "direction", InsertText: "ASC"},
			{Label: "DESC", Detail: "direction", InsertText: "DESC"},
		}
		return &Result{
			Items:    RankItems(prefix, items),
			Title:    "Order Direction",
			Start:    start,
			End:      end,
			Fallback: strings.ToUpper(TokenValue(query[start:end])),
		}
	}

	return nil
}

// --- SQL keyword / starter completion ---

func sqlKeywordCompletion(req Request) *Result {
	query := []rune(req.Query)
	start, end := TokenBounds(query, req.Cursor)
	token := strings.ToLower(TokenValue(query[start:end]))
	beforeToken := strings.ToLower(string(query[:start]))
	trimmed := strings.TrimSpace(beforeToken)

	if token != "" {
		for _, r := range token {
			if !unicode.IsLetter(r) {
				return nil
			}
		}
	}

	// Don't suggest after a completed statement
	if strings.HasSuffix(trimmed, ";") {
		return nil
	}

	title := "SQL Keywords"
	if trimmed == "" {
		title = "SQL Starters"
	} else {
		title = "SQL Clauses"
	}

	items := sqlKeywordItems(req)
	if trimmed == "" {
		for _, name := range req.Tables {
			items = append(items, Item{
				Label:      name,
				Detail:     DataSourceLabel(req.DBType),
				InsertText: fmt.Sprintf("SELECT * FROM %s LIMIT 50;", QuoteIdentifier(req.DBType, name)),
			})
		}
	}

	items = RankItems(token, items)
	if len(items) == 0 {
		return nil
	}

	return &Result{
		Items:    items,
		Title:    title,
		Start:    start,
		End:      end,
		Fallback: TokenValue(query[start:end]),
	}
}

func sqlKeywordItems(req Request) []Item {
	table := fallbackName(effectiveTable(req), "table_name")
	filterCol := fallbackName(preferredFilterColumn(req.Schema), "column_name")
	sortCol := fallbackName(preferredSortColumn(req.Schema), "column_name")
	before := strings.ToLower(queryBeforeCursor(req))
	q := QuoteIdentifier
	trimmed := strings.TrimSpace(before)

	// If there's existing query content, show only contextual next-clause items
	if trimmed != "" {
		var items []Item

		if InSelectList(before) || InHavingClause(before) {
			items = append(items,
				Item{Label: "COUNT(*)", Detail: "aggregate", InsertText: "COUNT(*)"},
				Item{Label: "COUNT(col)", Detail: "aggregate", InsertText: fmt.Sprintf("COUNT(%s)", q(req.DBType, filterCol))},
				Item{Label: "SUM(col)", Detail: "aggregate", InsertText: fmt.Sprintf("SUM(%s)", q(req.DBType, filterCol))},
				Item{Label: "AVG(col)", Detail: "aggregate", InsertText: fmt.Sprintf("AVG(%s)", q(req.DBType, filterCol))},
				Item{Label: "MIN(col)", Detail: "aggregate", InsertText: fmt.Sprintf("MIN(%s)", q(req.DBType, sortCol))},
				Item{Label: "MAX(col)", Detail: "aggregate", InsertText: fmt.Sprintf("MAX(%s)", q(req.DBType, sortCol))},
				Item{Label: "DISTINCT col", Detail: "modifier", InsertText: fmt.Sprintf("DISTINCT %s", q(req.DBType, filterCol))},
			)
		}

		// Next-clause suggestions based on what's already in the query
		hasFrom := strings.Contains(trimmed, " from ")
		hasWhere := strings.Contains(trimmed, " where ")
		hasGroup := strings.Contains(trimmed, " group by ")
		hasOrder := strings.Contains(trimmed, " order by ")
		hasLimit := strings.Contains(trimmed, " limit ")

		if hasFrom && !hasWhere {
			items = append(items, Item{Label: "WHERE", Detail: "clause", InsertText: fmt.Sprintf("\nWHERE %s = ''", q(req.DBType, filterCol))})
		}
		if hasFrom {
			items = append(items, Item{Label: "JOIN", Detail: "clause", InsertText: fmt.Sprintf("\nJOIN %s ON ", q(req.DBType, table))})
		}
		if hasFrom && !hasGroup {
			items = append(items, Item{Label: "GROUP BY", Detail: "clause", InsertText: fmt.Sprintf("\nGROUP BY %s", q(req.DBType, filterCol))})
		}
		if hasFrom && !hasOrder {
			items = append(items, Item{Label: "ORDER BY", Detail: "clause", InsertText: fmt.Sprintf("\nORDER BY %s DESC", q(req.DBType, sortCol))})
		}
		if !hasLimit {
			items = append(items, Item{Label: "LIMIT", Detail: "clause", InsertText: "\nLIMIT 50"})
		}
		items = append(items, Item{Label: "AND", Detail: "keyword", InsertText: "AND"})
		items = append(items, Item{Label: "OR", Detail: "keyword", InsertText: "OR"})

		return items
	}

	// Empty query — show starters
	return []Item{
		{Label: "SELECT starter", Detail: "query", InsertText: fmt.Sprintf("SELECT *\nFROM %s\nLIMIT 50;", q(req.DBType, table))},
		{Label: "INSERT starter", Detail: "query", InsertText: fmt.Sprintf("INSERT INTO %s (%s)\nVALUES ('');", q(req.DBType, table), q(req.DBType, filterCol))},
		{Label: "UPDATE starter", Detail: "query", InsertText: fmt.Sprintf("UPDATE %s\nSET %s = ''\nWHERE %s = '';", q(req.DBType, table), q(req.DBType, filterCol), q(req.DBType, filterCol))},
		{Label: "DELETE starter", Detail: "query", InsertText: fmt.Sprintf("DELETE FROM %s\nWHERE %s = '';", q(req.DBType, table), q(req.DBType, filterCol))},
		{Label: "SELECT", Detail: "keyword", InsertText: "SELECT"},
		{Label: "INSERT INTO", Detail: "keyword", InsertText: "INSERT INTO"},
		{Label: "UPDATE", Detail: "keyword", InsertText: "UPDATE"},
		{Label: "DELETE FROM", Detail: "keyword", InsertText: "DELETE FROM"},
	}
}
