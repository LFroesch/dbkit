package completion

import "strings"

// SchemaInfo is a simplified schema the engine can consume without
// importing the db package.
type SchemaInfo struct {
	Name    string
	Columns []ColumnInfo
}

type ColumnInfo struct {
	Name       string
	Type       string
	PrimaryKey bool
}

// ValueRequest tells the caller which column values need async loading.
type ValueRequest struct {
	Table  string
	Column string
}

// Request contains everything the engine needs to produce completions.
type Request struct {
	Query      string
	Cursor     int
	DBType     string              // "sqlite", "postgres", "mongo"
	Tables     []string            // known table/collection names
	Schema     *SchemaInfo         // schema for the query-inferred table
	ValueCache map[string][]string // "table|col" -> sample values
	// InferredTable is the table/collection the query targets, as resolved
	// by the caller from the query text. Used for cache key lookups and
	// NeedSchema signaling.
	InferredTable string
}

// Result tells the caller what to show in the picker and what async
// work is needed.
type Result struct {
	Items       []Item
	Title       string
	Start, End  int
	Fallback    string
	Multi       bool
	MultiPrefix string
	MultiSuffix string
	MultiSep    string
	ValueMode   bool
	ValueCol    string
	ValueTable  string

	// NeedSchema is non-empty when the engine needs schema for a table
	// that isn't in the request. The caller should load it and retry.
	NeedSchema string
	// NeedValues is non-nil when sample values should be fetched.
	NeedValues *ValueRequest
}

// Complete resolves completion items for the cursor position described
// by req. It is stateless — all context comes from the Request.
func Complete(req Request) *Result {
	if req.DBType == "mongo" {
		return mongoComplete(req)
	}
	return sqlComplete(req)
}

// --- helpers used by sql.go and mongo.go ---

func schemaFields(s *SchemaInfo) ([]string, map[string]string) {
	if s == nil || len(s.Columns) == 0 {
		return nil, map[string]string{}
	}
	fields := make([]string, 0, len(s.Columns))
	types := make(map[string]string, len(s.Columns))
	for _, col := range s.Columns {
		fields = append(fields, col.Name)
		types[col.Name] = strings.ToLower(col.Type)
	}
	return fields, types
}

func primaryKeyColumn(s *SchemaInfo) string {
	if s == nil {
		return ""
	}
	for _, col := range s.Columns {
		if col.PrimaryKey {
			return col.Name
		}
	}
	return ""
}

func preferredFilterColumn(s *SchemaInfo) string {
	if s == nil {
		return ""
	}
	for _, col := range s.Columns {
		name := strings.ToLower(col.Name)
		if strings.Contains(name, "name") || strings.Contains(name, "email") || strings.Contains(name, "status") {
			return col.Name
		}
	}
	if pk := primaryKeyColumn(s); pk != "" {
		return pk
	}
	if len(s.Columns) == 0 {
		return ""
	}
	return s.Columns[0].Name
}

func preferredSortColumn(s *SchemaInfo) string {
	if s == nil {
		return ""
	}
	for _, col := range s.Columns {
		name := strings.ToLower(col.Name)
		if strings.Contains(name, "created") || strings.Contains(name, "updated") || strings.Contains(name, "timestamp") || strings.Contains(name, "date") {
			return col.Name
		}
	}
	return ""
}

func preferredFilterColumnFromFields(fields []string) string {
	for _, name := range fields {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "name") || strings.Contains(lower, "email") || strings.Contains(lower, "status") {
			return name
		}
	}
	if len(fields) > 0 {
		return fields[0]
	}
	return ""
}

func preferredCategoricalColumnFromFields(fields []string, types map[string]string) string {
	for _, name := range fields {
		lower := strings.ToLower(name)
		colType := strings.ToLower(types[name])
		if strings.Contains(lower, "status") || strings.Contains(lower, "type") || strings.Contains(lower, "role") || strings.Contains(lower, "category") {
			return name
		}
		if strings.Contains(colType, "char") || strings.Contains(colType, "text") {
			return name
		}
	}
	if len(fields) > 0 {
		return fields[0]
	}
	return ""
}

func fallbackName(name, def string) string {
	if name == "" {
		return def
	}
	return name
}

// QuoteIdentifier quotes a name for SQL or leaves it unquoted for Mongo.
func QuoteIdentifier(dbType, name string) string {
	if dbType == "mongo" {
		return name
	}
	return `"` + name + `"`
}

// DataSourceLabel returns "table" or "collection" based on DB type.
func DataSourceLabel(dbType string) string {
	if dbType == "mongo" {
		return "collection"
	}
	return "table"
}

// effectiveTable returns the table name parsed from the query text.
// Returns "" when no table is inferred — never falls back to the browse panel.
func effectiveTable(req Request) string {
	return req.InferredTable
}

// ValueCacheKey builds the lookup key for the value cache.
func ValueCacheKey(table, column string) string {
	return table + "|" + column
}
