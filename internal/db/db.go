package db

import "fmt"

// SQLMaxRows caps the number of rows scanned by SQL backends (SQLite, Postgres)
// to avoid OOM on unbounded SELECTs. Mirrors mongoMaxLimit for Mongo.
const SQLMaxRows = 1000

// ColumnInfo describes a single column/field.
type ColumnInfo struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
}

// TableSchema holds schema info for a table/collection.
type TableSchema struct {
	Name     string
	Columns  []ColumnInfo
	RowCount int64
}

// QueryResult holds results from a query execution.
type QueryResult struct {
	Columns  []string
	Rows     [][]string
	Affected int64
	Message  string
}

// DB is the common interface all database backends implement.
type DB interface {
	Connect() error
	Close()
	Ping() error
	GetTables() ([]string, error)
	GetTableSchema(table string) (*TableSchema, error)
	RunQuery(query string) (*QueryResult, error)
	Type() string
	DSN() string
}

// New creates a DB backend by type.
func New(dbType, dsn string) (DB, error) {
	switch dbType {
	case "sqlite":
		return &SQLiteDB{path: dsn}, nil
	case "postgres":
		return &PostgresDB{dsn: dsn}, nil
	case "mongo":
		return &MongoDB{uri: dsn}, nil
	default:
		return nil, fmt.Errorf("unknown database type: %s", dbType)
	}
}
