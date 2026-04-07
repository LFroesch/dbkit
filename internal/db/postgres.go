package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// PostgresDB implements DB for PostgreSQL.
type PostgresDB struct {
	dsn  string
	conn *sql.DB
}

func (d *PostgresDB) Type() string { return "postgres" }
func (d *PostgresDB) DSN() string  { return d.dsn }

func (d *PostgresDB) Connect() error {
	conn, err := sql.Open("postgres", d.dsn)
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		return err
	}
	d.conn = conn
	return nil
}

func (d *PostgresDB) Close() {
	if d.conn != nil {
		d.conn.Close()
	}
}

func (d *PostgresDB) Ping() error {
	if d.conn == nil {
		return fmt.Errorf("not connected")
	}
	return d.conn.Ping()
}

func (d *PostgresDB) GetTables() ([]string, error) {
	rows, err := d.conn.Query(`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		ORDER BY table_name`)
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

func (d *PostgresDB) GetTableSchema(table string) (*TableSchema, error) {
	rows, err := d.conn.Query(`
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := &TableSchema{Name: table}
	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			return nil, err
		}
		schema.Columns = append(schema.Columns, ColumnInfo{
			Name:     colName,
			Type:     dataType,
			Nullable: isNullable == "YES",
		})
	}

	// get primary keys
	pkRows, err := d.conn.Query(`
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1`, table)
	if err == nil {
		defer pkRows.Close()
		pks := map[string]bool{}
		for pkRows.Next() {
			var pk string
			pkRows.Scan(&pk)
			pks[pk] = true
		}
		for i := range schema.Columns {
			if pks[schema.Columns[i].Name] {
				schema.Columns[i].PrimaryKey = true
			}
		}
	}

	var count int64
	_ = d.conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %q", table)).Scan(&count)
	schema.RowCount = count
	return schema, nil
}

func (d *PostgresDB) RunQuery(query string) (*QueryResult, error) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") &&
		!strings.HasPrefix(upper, "EXPLAIN") && !strings.HasPrefix(upper, "SHOW") {
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

	for rows.Next() {
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
	return &result, nil
}
