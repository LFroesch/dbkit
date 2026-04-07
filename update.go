package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"dbkit/internal/config"
	"dbkit/internal/db"
)

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.queryInput.SetWidth(m.rightPanelWidth() - 4)
		return m, nil

	case connectedMsg:
		m.loading = false
		if msg.err != nil {
			m.setStatus("error: " + msg.err.Error())
			return m, nil
		}
		m.activeDB = msg.db
		m.activeConnName = m.cfg.Connections[m.connCursor].Name
		m.setStatus("connected to " + m.activeConnName)
		m.activeTab = tabSchema
		return m, m.loadTables()

	case tablesLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.setStatus("error loading tables: " + msg.err.Error())
			return m, nil
		}
		m.tables = msg.tables
		m.tableCursor = 0
		if len(m.tables) > 0 {
			return m, m.loadSchema(m.tables[0])
		}
		return m, nil

	case schemaLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.tableSchema = nil
			return m, nil
		}
		m.tableSchema = msg.schema
		return m, nil

	case queryDoneMsg:
		m.loading = false
		if msg.err != nil {
			m.queryErr = msg.err.Error()
			m.queryResult = nil
		} else {
			m.queryResult = msg.result
			m.queryErr = ""
			m.resultScroll = 0
			if msg.result.Message != "" {
				m.setStatus(msg.result.Message)
			} else {
				m.setStatus(fmt.Sprintf("%d row(s) returned", len(msg.result.Rows)))
			}
		}
		return m, nil

	case tea.KeyMsg:
		// Help overlay — any key closes
		if m.showHelp {
			m.showHelp = false
			return m, nil
		}

		// New connection modal
		if m.showNewConn {
			return m.updateNewConn(msg)
		}

		// Global keys
		switch msg.String() {
		case "ctrl+c", "q":
			if m.activeTab == tabQuery && m.queryFocus {
				break // let textarea handle
			}
			if m.activeDB != nil {
				m.activeDB.Close()
			}
			return m, tea.Quit
		case "?":
			m.showHelp = true
			return m, nil
		case "1":
			m.activeTab = tabConnections
			return m, nil
		case "2":
			if m.activeDB != nil {
				m.activeTab = tabSchema
			}
			return m, nil
		case "3":
			if m.activeDB != nil {
				m.activeTab = tabQuery
			}
			return m, nil
		case "4":
			m.activeTab = tabHelpers
			return m, nil
		case "tab":
			if m.activeTab == tabQuery && m.queryFocus {
				break // let textarea handle
			}
			m.togglePanel()
			return m, nil
		}

		// Tab-specific handling
		switch m.activeTab {
		case tabConnections:
			return m.updateConnections(msg)
		case tabSchema:
			return m.updateSchema(msg)
		case tabQuery:
			return m.updateQuery(msg)
		case tabHelpers:
			return m.updateHelpers(msg)
		}
	}

	// Propagate to textarea when focused
	if m.activeTab == tabQuery && m.queryFocus {
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		return m, cmd
	}

	return m, nil
}

// --- Connections tab ---

func (m Model) updateConnections(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	conns := m.cfg.Connections
	switch msg.String() {
	case "j", "down":
		if m.connCursor < len(conns)-1 {
			m.connCursor++
		}
	case "k", "up":
		if m.connCursor > 0 {
			m.connCursor--
		}
	case "n":
		m.openNewConnForm()
		return m, nil
	case "d":
		if len(conns) > 0 {
			if m.activeConnIdx == m.connCursor {
				if m.activeDB != nil {
					m.activeDB.Close()
				}
				m.activeDB = nil
				m.activeConnIdx = -1
				m.tables = nil
				m.tableSchema = nil
			}
			m.cfg.DeleteConnection(m.connCursor)
			m.cfg.Save()
			if m.connCursor >= len(m.cfg.Connections) && m.connCursor > 0 {
				m.connCursor--
			}
		}
	case "enter":
		if len(conns) == 0 {
			return m, nil
		}
		conn := conns[m.connCursor]
		m.loading = true
		m.setStatus("connecting to " + conn.Name + "...")
		return m, connectCmd(conn)
	}
	return m, nil
}

// --- Schema tab ---

func (m Model) updateSchema(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "j", "down":
		if m.tableCursor < len(m.tables)-1 {
			m.tableCursor++
			return m, m.loadSchema(m.tables[m.tableCursor])
		}
	case "k", "up":
		if m.tableCursor > 0 {
			m.tableCursor--
			return m, m.loadSchema(m.tables[m.tableCursor])
		}
	case "r":
		return m, m.loadTables()
	case "enter", "q":
		// jump to query with SELECT * prefilled
		if len(m.tables) > 0 {
			table := m.tables[m.tableCursor]
			m.queryInput.SetValue(fmt.Sprintf("SELECT * FROM %q LIMIT 100;", table))
			m.activeTab = tabQuery
		}
	}
	return m, nil
}

// --- Query tab ---

func (m Model) updateQuery(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.queryFocus {
		switch msg.String() {
		case "ctrl+r":
			query := strings.TrimSpace(m.queryInput.Value())
			if query == "" || m.activeDB == nil {
				return m, nil
			}
			m.loading = true
			m.queryErr = ""
			m.queryResult = nil
			return m, runQueryCmd(m.activeDB, query)
		case "esc":
			m.queryFocus = false
			m.queryInput.Blur()
			return m, nil
		}
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		return m, cmd
	}

	// Left panel (table list) focused
	switch msg.String() {
	case "j", "down":
		if m.tableCursor < len(m.tables)-1 {
			m.tableCursor++
		}
	case "k", "up":
		if m.tableCursor > 0 {
			m.tableCursor--
		}
	case "enter":
		if len(m.tables) > 0 {
			table := m.tables[m.tableCursor]
			m.queryInput.SetValue(fmt.Sprintf("SELECT * FROM %q LIMIT 100;", table))
			m.queryFocus = true
			m.queryInput.Focus()
		}
	case "tab", "right", "e":
		m.queryFocus = true
		m.queryInput.Focus()
	case "ctrl+r":
		query := strings.TrimSpace(m.queryInput.Value())
		if query == "" || m.activeDB == nil {
			return m, nil
		}
		m.loading = true
		m.queryErr = ""
		m.queryResult = nil
		return m, runQueryCmd(m.activeDB, query)
	case "ctrl+d", "pgdown":
		m.resultScroll += 5
	case "ctrl+u", "pgup":
		if m.resultScroll > 5 {
			m.resultScroll -= 5
		} else {
			m.resultScroll = 0
		}
	}
	return m, nil
}

// --- Helpers tab ---

var queryHelpers = []struct{ label, template string }{
	{"SELECT * (all rows)", `SELECT * FROM "{table}" LIMIT 100;`},
	{"SELECT * WHERE", `SELECT * FROM "{table}" WHERE {col} = '{val}';`},
	{"COUNT rows", `SELECT COUNT(*) FROM "{table}";`},
	{"INSERT row", `INSERT INTO "{table}" ({col1}, {col2}) VALUES ('{val1}', '{val2}');`},
	{"UPDATE rows", `UPDATE "{table}" SET {col} = '{val}' WHERE {id_col} = {id};`},
	{"DELETE rows", `DELETE FROM "{table}" WHERE {id_col} = {id};`},
	{"CREATE TABLE", `CREATE TABLE IF NOT EXISTS {name} (` + "\n  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL,\n  created_at TIMESTAMP DEFAULT NOW()\n);"},
	{"DROP TABLE", `DROP TABLE IF EXISTS "{table}";`},
	{"BEGIN transaction", "BEGIN;\n-- your statements here\nCOMMIT;"},
	{"EXPLAIN query", `EXPLAIN SELECT * FROM "{table}";`},
	{"Postgres: table sizes", `SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size FROM information_schema.tables WHERE table_schema = 'public' ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;`},
	{"Postgres: running queries", `SELECT pid, now() - pg_stat_activity.query_start AS duration, query, state FROM pg_stat_activity WHERE state != 'idle' AND query_start IS NOT NULL ORDER BY duration DESC;`},
	{"Postgres: indexes", `SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{table}';`},
	{"SQLite: schema", `SELECT sql FROM sqlite_master WHERE type = 'table';`},
	{"SQLite: VACUUM", `VACUUM;`},
}

func (m Model) updateHelpers(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "j", "down":
		if m.helperCursor < len(queryHelpers)-1 {
			m.helperCursor++
		}
	case "k", "up":
		if m.helperCursor > 0 {
			m.helperCursor--
		}
	case "enter":
		tpl := queryHelpers[m.helperCursor].template
		// substitute {table} if we have a selected table
		if len(m.tables) > 0 && m.tableCursor < len(m.tables) {
			tpl = strings.ReplaceAll(tpl, "{table}", m.tables[m.tableCursor])
		}
		m.queryInput.SetValue(tpl)
		m.activeTab = tabQuery
		m.queryFocus = true
		m.queryInput.Focus()
	}
	return m, nil
}

// --- New connection form ---

func (m *Model) openNewConnForm() {
	m.showNewConn = true
	m.newConnFocus = 0
	m.newConnTypeCur = 0
	for i := range m.newConnInputs {
		m.newConnInputs[i].SetValue("")
	}
	m.newConnInputs[fieldName].Focus()
}

func (m Model) updateNewConn(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.showNewConn = false
		return m, nil
	case "tab", "down":
		m.newConnFocus = (m.newConnFocus + 1) % (fieldCount + 2) // +2 for type selector + submit
		m.syncNewConnFocus()
		return m, nil
	case "shift+tab", "up":
		m.newConnFocus = (m.newConnFocus - 1 + fieldCount + 2) % (fieldCount + 2)
		m.syncNewConnFocus()
		return m, nil
	case "left":
		if m.newConnFocus == 2 { // type selector
			if m.newConnTypeCur > 0 {
				m.newConnTypeCur--
				m.updateDSNPlaceholder()
			}
			return m, nil
		}
	case "right":
		if m.newConnFocus == 2 { // type selector
			if m.newConnTypeCur < len(dbTypes)-1 {
				m.newConnTypeCur++
				m.updateDSNPlaceholder()
			}
			return m, nil
		}
	case "enter":
		if m.newConnFocus == fieldCount+1 { // submit button
			return m.submitNewConn()
		}
	}

	// Route key to active input
	var cmd tea.Cmd
	switch m.newConnFocus {
	case 0:
		m.newConnInputs[fieldName], cmd = m.newConnInputs[fieldName].Update(msg)
	case 1:
		// DSN is field index 3 (after type selector at pos 2)
		// handled below
	case 3:
		m.newConnInputs[fieldDSN], cmd = m.newConnInputs[fieldDSN].Update(msg)
	}
	return m, cmd
}

func (m *Model) syncNewConnFocus() {
	for i := range m.newConnInputs {
		m.newConnInputs[i].Blur()
	}
	switch m.newConnFocus {
	case 0:
		m.newConnInputs[fieldName].Focus()
	case 3:
		m.newConnInputs[fieldDSN].Focus()
	}
}

func (m *Model) updateDSNPlaceholder() {
	switch dbTypes[m.newConnTypeCur] {
	case "sqlite":
		m.newConnInputs[fieldDSN].Placeholder = "/path/to/database.sqlite"
	case "postgres":
		m.newConnInputs[fieldDSN].Placeholder = "postgres://user:pass@localhost:5432/dbname"
	case "mongo":
		m.newConnInputs[fieldDSN].Placeholder = "mongodb://localhost:27017/dbname"
	}
}

func (m Model) submitNewConn() (tea.Model, tea.Cmd) {
	name := strings.TrimSpace(m.newConnInputs[fieldName].Value())
	dsn := strings.TrimSpace(m.newConnInputs[fieldDSN].Value())
	dbType := dbTypes[m.newConnTypeCur]

	if name == "" {
		name = dbType + " database"
	}
	if dsn == "" {
		m.setStatus("DSN/path is required")
		return m, nil
	}

	m.cfg.AddConnection(name, dbType, dsn)
	m.cfg.Save()
	m.showNewConn = false
	m.connCursor = len(m.cfg.Connections) - 1
	m.setStatus("connection saved: " + name)
	return m, nil
}

// --- Panel helpers ---

func (m *Model) togglePanel() {
	if m.focus == panelLeft {
		m.focus = panelRight
	} else {
		m.focus = panelLeft
	}
}

func (m Model) leftPanelWidth() int {
	if m.width <= 0 {
		return 30
	}
	return m.width / 3
}

func (m Model) rightPanelWidth() int {
	if m.width <= 0 {
		return 60
	}
	return m.width - m.leftPanelWidth()
}

// --- Async commands ---

func connectCmd(conn config.Connection) tea.Cmd {
	return func() tea.Msg {
		d, err := db.New(conn.Type, conn.DSN)
		if err != nil {
			return connectedMsg{err: err}
		}
		if err := d.Connect(); err != nil {
			return connectedMsg{err: err}
		}
		return connectedMsg{db: d}
	}
}

func (m Model) loadTables() tea.Cmd {
	if m.activeDB == nil {
		return nil
	}
	d := m.activeDB
	return func() tea.Msg {
		tables, err := d.GetTables()
		return tablesLoadedMsg{tables: tables, err: err}
	}
}

func (m Model) loadSchema(table string) tea.Cmd {
	if m.activeDB == nil {
		return nil
	}
	d := m.activeDB
	return func() tea.Msg {
		schema, err := d.GetTableSchema(table)
		return schemaLoadedMsg{schema: schema, err: err}
	}
}

func runQueryCmd(d db.DB, query string) tea.Cmd {
	return func() tea.Msg {
		result, err := d.RunQuery(query)
		return queryDoneMsg{result: result, err: err}
	}
}

// --- Status ---

func (m *Model) setStatus(msg string) {
	m.statusMsg = msg
	m.statusExpiry = time.Now().Add(5 * time.Second)
}

// textinput update helper to avoid unused import
var _ = textinput.New
