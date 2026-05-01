package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// SQLiteDB implements DB for SQLite.
type SQLiteDB struct {
	path string
	conn *sql.DB
}

func (d *SQLiteDB) Type() string { return "sqlite" }
func (d *SQLiteDB) DSN() string  { return d.path }

func (d *SQLiteDB) Connect() error {
	conn, err := sql.Open("sqlite", d.path)
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		return err
	}
	d.conn = conn
	return nil
}

func (d *SQLiteDB) Close() {
	if d.conn != nil {
		d.conn.Close()
	}
}

func (d *SQLiteDB) Ping() error {
	if d.conn == nil {
		return fmt.Errorf("not connected")
	}
	return d.conn.Ping()
}

func (d *SQLiteDB) GetTables() ([]string, error) {
	rows, err := d.conn.Query(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

func (d *SQLiteDB) GetTableSchema(table string) (*TableSchema, error) {
	rows, err := d.conn.Query(fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := &TableSchema{Name: table}
	for rows.Next() {
		var cid int
		var name, typeName string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		schema.Columns = append(schema.Columns, ColumnInfo{
			Name:       name,
			Type:       typeName,
			Nullable:   notNull == 0,
			PrimaryKey: pk > 0,
		})
	}

	var count int64
	_ = d.conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %q", table)).Scan(&count)
	schema.RowCount = count
	return schema, nil
}

func (d *SQLiteDB) RunQuery(query string) (*QueryResult, error) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	// Non-SELECT: exec
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") &&
		!strings.HasPrefix(upper, "EXPLAIN") && !strings.HasPrefix(upper, "PRAGMA") {
		res, err := d.conn.Exec(q)
		if err != nil {
			return nil, err
		}
		affected, _ := res.RowsAffected()
		return &QueryResult{
			Message:  fmt.Sprintf("OK — %d row(s) affected", affected),
			Affected: affected,
		}, nil
	}

	rows, err := d.conn.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result QueryResult
	result.Columns = cols

	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	capped := false
	for rows.Next() {
		if int64(len(result.Rows)) >= SQLMaxRows {
			capped = true
			break
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		result.Rows = append(result.Rows, row)
	}
	if capped {
		result.Message = fmt.Sprintf("%d rows (capped — result set larger; add LIMIT/WHERE to narrow)", SQLMaxRows)
	}
	return &result, nil
}
