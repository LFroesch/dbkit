package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
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
		m.queryInput.SetWidth(max(20, m.rightPanelWidth()-6))
		m.resizeTables()
		return m, nil

	case connectedMsg:
		if msg.reqID != m.connectReqID {
			if msg.db != nil {
				msg.db.Close()
			}
			return m, nil
		}
		m.loading = false
		if msg.err != nil {
			m.setStatus("error: " + msg.err.Error())
			return m, nil
		}
		if m.activeDB != nil {
			m.activeDB.Close()
		}
		m.activeDB = msg.db
		m.activeConnIdx = msg.connIdx
		m.activeConnName = msg.conn.Name
		m.queryHistory = m.cfg.QueriesForConnection(msg.conn.ID)
		m.savedQueries = m.cfg.SavedQueriesForConnection(msg.conn.ID)
		m.historyCursor = 0
		if m.activeDB.Type() == "mongo" {
			m.queryInput.Placeholder = "Write a Mongo command... ctrl+r runs, tab opens completions, help shows syntax"
		} else {
			m.queryInput.Placeholder = "Write SQL... ctrl+r runs, tab opens completions"
		}
		m.setStatus("connected to " + m.activeConnName)
		m.activeTab = tabSchema
		m.focus = panelLeft
		m.tableSchema = nil
		m.schemaTable.SetRows(nil)
		m.queryResult = nil
		m.queryErr = ""
		m.resultTable.SetRows(nil)
		m.resultColOffset = 0
		m.resultVisibleColumn = 0
		m.clearSnippetPlaceholders()
		m.loading = true
		return m, m.loadTables(msg.connIdx)

	case tablesLoadedMsg:
		if msg.reqID != m.tablesReqID {
			return m, nil
		}
		m.loading = false
		if msg.err != nil {
			m.setStatus("error loading tables: " + msg.err.Error())
			return m, nil
		}
		m.tables = msg.tables
		if len(m.tables) == 0 {
			m.tableCursor = 0
			m.tableSchema = nil
			m.schemaTable.SetRows(nil)
			return m, nil
		}
		if m.tableCursor >= len(m.tables) {
			m.tableCursor = len(m.tables) - 1
		}
		if m.tableCursor < 0 {
			m.tableCursor = 0
		}
		if len(m.tables) > 0 {
			m.loading = true
			m.syncHelperCursor()
			return m, m.loadSchema(m.tables[m.tableCursor])
		}
		return m, nil

	case schemaLoadedMsg:
		if msg.reqID != m.schemaReqID || msg.table != m.currentTableName() {
			return m, nil
		}
		m.loading = false
		if msg.err != nil {
			m.tableSchema = nil
			m.schemaTable.SetRows(nil)
			m.setStatus("schema unavailable")
			return m, nil
		}
		m.tableSchema = msg.schema
		m.syncSchemaTable()
		return m, nil

	case queryDoneMsg:
		if msg.reqID != m.queryReqID {
			return m, nil
		}
		m.loading = false
		if msg.err != nil {
			m.queryErr = msg.err.Error()
			m.queryResult = nil
			m.resultTable.SetRows(nil)
			m.setStatus("query failed")
			return m, nil
		}
		m.queryResult = msg.result
		m.queryErr = ""
		m.queryFocus = false
		m.openResultsTab()
		m.queryInput.Blur()
		m.lastRunQuery = msg.query
		m.pushQueryHistory(msg.query)
		m.queryHistoryIdx = -1
		m.clearSnippetPlaceholders()
		m.syncResultTable()
		m.syncTableFocus()
		if msg.result.Message != "" {
			m.setStatus(msg.result.Message)
		} else {
			m.setStatus(fmt.Sprintf("%d row(s) returned", len(msg.result.Rows)))
		}
		return m, nil

	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			if m.activeDB != nil {
				m.activeDB.Close()
			}
			return m, tea.Quit
		}

		if m.showHelp {
			m.showHelp = false
			return m, nil
		}
		if m.showColumnPicker {
			return m.updateColumnPicker(msg)
		}
		if m.showQueryPicker {
			return m.updateQueryPicker(msg)
		}
		if m.showInspect {
			return m.updateInspect(msg)
		}
		if m.showConfirm {
			return m.updateConfirm(msg)
		}
		if m.showNewConn {
			return m.updateNewConn(msg)
		}

		if m.textInputCapturesKeypress() {
			switch m.activeTab {
			case tabQuery:
				return m.updateQuery(msg)
			case tabResults:
				return m.updateResults(msg)
			}
		}

		switch msg.String() {
		case "q":
			if (m.activeTab == tabQuery || m.activeTab == tabResults) && m.queryFocus {
				break
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
			m.focus = panelLeft
			m.queryFocus = false
			m.queryInput.Blur()
			m.syncTableFocus()
			return m, nil
		case "2":
			if m.activeDB != nil {
				m.activeTab = tabSchema
				m.focus = panelLeft
				m.queryFocus = false
				m.queryInput.Blur()
				m.syncTableFocus()
			}
			return m, nil
		case "3":
			if m.activeDB != nil {
				m.activeTab = tabQuery
				m.focus = panelLeft
				m.queryFocus = false
				m.queryInput.Blur()
				m.syncTableFocus()
			}
			return m, nil
		case "4":
			if m.activeDB != nil {
				m.openResultsTab()
			}
			return m, nil
		case "tab":
			if m.activeTab == tabQuery && m.queryFocus {
				break
			}
			m.togglePanel()
			return m, nil
		}

		switch m.activeTab {
		case tabConnections:
			return m.updateConnections(msg)
		case tabSchema:
			return m.updateSchema(msg)
		case tabQuery:
			return m.updateQuery(msg)
		case tabResults:
			return m.updateResults(msg)
		case tabHistory:
			return m.updateHistory(msg)
		case tabHelpers:
			return m.updateHelpers(msg)
		}
	}

	if (m.activeTab == tabQuery || m.activeTab == tabResults) && m.queryFocus {
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
			m.openDeleteConnectionConfirm(m.connCursor)
			return m, nil
		}
	case "enter":
		if len(conns) == 0 {
			return m, nil
		}
		conn := conns[m.connCursor]
		m.connectReqID++
		m.loading = true
		m.setStatus("connecting to " + conn.Name + "...")
		return m, connectCmd(m.connectReqID, m.connCursor, conn)
	case "c":
		if len(conns) == 0 {
			return m, nil
		}
		return m.copyNamedText("dsn", conns[m.connCursor].DSN)
	}
	return m, nil
}

// --- Schema tab ---

func (m Model) updateSchema(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.focus == panelRight {
		switch msg.String() {
		case "j", "down", "k", "up":
			var cmd tea.Cmd
			m.schemaTable, cmd = m.schemaTable.Update(msg)
			return m, cmd
		case "v":
			m.openSchemaInspect()
			return m, nil
		case "enter":
			return m.runDefaultBrowseQuery()
		case "e":
			return m.openDefaultQueryEditor()
		}
		return m, nil
	}

	switch msg.String() {
	case "j", "down":
		return m.moveTableCursor(1)
	case "k", "up":
		return m.moveTableCursor(-1)
	case "r":
		m.loading = true
		return m, m.loadTables(m.activeConnIdx)
	case "enter":
		return m.runDefaultBrowseQuery()
	case "e":
		return m.openDefaultQueryEditor()
	case "c":
		return m.copyNamedText(m.dataSourceLabel(), m.currentTableName())
	default:
		if m.focus == panelRight {
			var cmd tea.Cmd
			m.schemaTable, cmd = m.schemaTable.Update(msg)
			return m, cmd
		}
	}
	return m, nil
}

// --- Query tab ---

func (m Model) updateQuery(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.queryFocus {
		switch msg.String() {
		case "ctrl+r":
			return m.runCurrentQueryWithConfirm()
		case "ctrl+o":
			m.openQueryPicker("Recent Queries", m.queryHistoryPickerItems())
			return m, nil
		case "ctrl+p":
			m.recallPreviousQuery()
			return m, nil
		case "ctrl+n":
			m.recallNextQuery()
			return m, nil
		case "ctrl+y":
			m.recallLastRunQuery()
			return m, nil
		case "ctrl+t":
			m.openQueryPicker("Templates", m.queryHelperPickerItems())
			return m, nil
		case "ctrl+g":
			m.openQueryPicker("Saved Queries", m.querySavedPickerItems())
			return m, nil
		case "ctrl+l":
			m.clearCurrentQuery()
			return m, nil
		case "ctrl+s":
			return m.saveCurrentQuery()
		case "shift+tab":
			if m.jumpSnippetPlaceholder(-1) {
				return m, nil
			}
		case "tab":
			if m.jumpSnippetPlaceholder(1) {
				return m, nil
			}
			if m.openCompletionForCursor(true) {
				return m, nil
			}
			if strings.TrimSpace(m.queryInput.Value()) == "" {
				m.openQueryPicker("Templates", m.queryHelperPickerItems())
				return m, nil
			}
		case "esc":
			m.queryFocus = false
			m.focus = panelRight
			m.queryInput.Blur()
			m.syncTableFocus()
			return m, nil
		}
		if m.handleSnippetPlaceholderEdit(msg) {
			return m, nil
		}
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		m.clearSnippetPlaceholders()
		return m, cmd
	}

	switch msg.String() {
	case "j", "down":
		return m.moveTableCursor(1)
	case "k", "up":
		return m.moveTableCursor(-1)
	case "enter":
		return m.runDefaultBrowseQuery()
	case "e":
		m.queryFocus = true
		m.focus = panelRight
		m.queryInput.Focus()
		m.syncTableFocus()
	case "y":
		m.openQueryPicker("Recent Queries", m.queryHistoryPickerItems())
	case "f":
		m.openQueryPicker("Templates", m.queryHelperPickerItems())
	case "c":
		return m.copyNamedText("query", m.queryInput.Value())
	case "C":
		m.openQueryPicker("Copy Query As", m.queryCopyPickerItems())
	case "g":
		m.openQueryPicker("Saved Queries", m.querySavedPickerItems())
	case "s":
		return m.saveCurrentQuery()
	case "ctrl+l":
		m.clearCurrentQuery()
		return m, nil
	case "ctrl+r":
		return m.runCurrentQueryWithConfirm()
	}
	return m, nil
}

func (m Model) updateResults(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "j", "down":
		if m.focus == panelRight {
			var cmd tea.Cmd
			m.resultTable, cmd = m.resultTable.Update(msg)
			return m, cmd
		}
		return m.moveTableCursor(1)
	case "k", "up":
		if m.focus == panelRight {
			var cmd tea.Cmd
			m.resultTable, cmd = m.resultTable.Update(msg)
			return m, cmd
		}
		return m.moveTableCursor(-1)
	case "e":
		m.activeTab = tabQuery
		m.focus = panelRight
		m.queryFocus = true
		m.queryInput.Focus()
		m.syncTableFocus()
	case "enter":
		if m.focus == panelLeft && len(m.tables) > 0 {
			return m.runDefaultBrowseQuery()
		}
	case "h", "shift+left", "left":
		if m.focus == panelRight {
			m.shiftResultColumns(-1)
			return m, nil
		}
	case "l", "shift+right", "right":
		if m.focus == panelRight {
			m.shiftResultColumns(1)
			return m, nil
		}
	case "v":
		if m.focus == panelRight {
			m.openResultInspect()
			return m, nil
		}
	case "c":
		if m.focus == panelRight {
			return m.copyNamedText("row", m.currentResultRowJSON())
		}
	case "C":
		if m.focus == panelRight {
			m.openQueryPicker("Copy Results As", m.resultCopyPickerItems())
		}
	}
	return m, nil
}

func (m Model) updateHistory(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "j", "down":
		if m.historyCursor < len(m.queryHistory)-1 {
			m.historyCursor++
		}
	case "k", "up":
		if m.historyCursor > 0 {
			m.historyCursor--
		}
	case "enter", "y":
		query := m.currentHistoryQuery()
		if query == "" {
			return m, nil
		}
		m.queryInput.SetValue(query)
		m.queryInput.CursorEnd()
		m.activeTab = tabQuery
		m.focus = panelRight
		m.queryFocus = true
		m.queryInput.Focus()
		m.syncTableFocus()
	case "r":
		query := m.currentHistoryQuery()
		if strings.TrimSpace(query) == "" || m.activeDB == nil {
			return m, nil
		}
		m.queryReqID++
		m.loading = true
		m.queryErr = ""
		m.queryResult = nil
		m.resultTable.SetRows(nil)
		return m, runQueryCmd(m.queryReqID, m.activeDB, query)
	case "c":
		return m.copyNamedText("query", m.currentHistoryQuery())
	}
	return m, nil
}

// --- Helpers tab ---

var sqlQueryHelpers = []queryHelper{
	{label: "SELECT top rows", template: "SELECT *\nFROM \"{table}\"\nLIMIT 100;"},
	{label: "SELECT with filter + order", template: "SELECT *\nFROM \"{table}\"\nWHERE \"{col}\" = 'value'\nORDER BY \"{sort_col}\" DESC\nLIMIT 100;"},
	{label: "Aggregate count by column", template: "SELECT \"{col}\", COUNT(*) AS count\nFROM \"{table}\"\nGROUP BY \"{col}\"\nORDER BY count DESC\nLIMIT 20;"},
	{label: "INSERT row", template: "INSERT INTO \"{table}\" (\"{col1}\", \"{col2}\")\nVALUES ('value1', 'value2');"},
	{label: "UPDATE by key", template: "UPDATE \"{table}\"\nSET \"{col}\" = 'value'\nWHERE \"{id_col}\" = 'id';"},
	{label: "DELETE by key", template: "DELETE FROM \"{table}\"\nWHERE \"{id_col}\" = 'id';"},
	{label: "JOIN starter", template: "SELECT a.*, b.*\nFROM \"{table}\" a\nJOIN \"other_table\" b ON b.\"id\" = a.\"other_id\"\nLIMIT 100;"},
	{label: "CTE starter", template: "WITH recent AS (\n  SELECT *\n  FROM \"{table}\"\n  ORDER BY \"created_at\" DESC\n  LIMIT 100\n)\nSELECT *\nFROM recent;"},
	{label: "BEGIN transaction", template: "BEGIN;\n-- your statements here\nCOMMIT;"},
}

var postgresQueryHelpers = []queryHelper{
	{label: "CREATE table", template: "CREATE TABLE IF NOT EXISTS {name} (\n  id BIGSERIAL PRIMARY KEY,\n  name TEXT NOT NULL,\n  created_at TIMESTAMPTZ DEFAULT NOW()\n);"},
	{label: "EXPLAIN ANALYZE", template: "EXPLAIN ANALYZE\nSELECT *\nFROM \"{table}\"\nLIMIT 100;"},
	{label: "Table sizes", template: `SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size FROM information_schema.tables WHERE table_schema = 'public' ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;`},
	{label: "Active queries", template: `SELECT pid, now() - pg_stat_activity.query_start AS duration, query, state FROM pg_stat_activity WHERE state != 'idle' AND query_start IS NOT NULL ORDER BY duration DESC;`},
	{label: "Indexes", template: `SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{table}';`},
}

var sqliteQueryHelpers = []queryHelper{
	{label: "CREATE table", template: "CREATE TABLE IF NOT EXISTS {name} (\n  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL,\n  created_at TEXT DEFAULT CURRENT_TIMESTAMP\n);"},
	{label: "Schema dump", template: `SELECT sql FROM sqlite_master WHERE type = 'table';`},
	{label: "Table info", template: `PRAGMA table_info("{table}");`},
	{label: "VACUUM", template: `VACUUM;`},
	{label: "ANALYZE", template: `ANALYZE;`},
}

var mongoQueryHelpers = []queryHelper{
	{label: "List collections", template: `collections`},
	{label: "Find all (first 100)", template: `find {table} {} 100`},
	{label: "Find newest N", template: `find {table} {} 50 {"_id":-1}`},
	{label: "Find with filter + sort", template: `find {table} {"status":"active"} 50 {"created_at":-1}`},
	{label: "Find by _id (ObjectID)", template: `find {table} {"_id":{"$oid":"000000000000000000000000"}} 1`},
	{label: "Count all", template: `count {table} {}`},
	{label: "Count by filter", template: `count {table} {"status":"active"}`},
	{label: "Insert document", template: `insert {table} {"name":"example","active":true}`},
	{label: "Update one", template: `update {table} {"_id":{"$oid":"000000000000000000000000"}} {"$set":{"active":false}}`},
	{label: "Update many", template: `update {table} {"status":"pending"} {"$set":{"status":"archived"}} many`},
	{label: "Delete one", template: `delete {table} {"_id":{"$oid":"000000000000000000000000"}}`},
	{label: "Delete many", template: `delete {table} {"active":false} many`},
	{label: "Aggregate top-N by field", template: `aggregate {table} [{"$group":{"_id":"$field","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":25}]`},
	{label: "Aggregate recent + project", template: `aggregate {table} [{"$sort":{"_id":-1}},{"$limit":50},{"$project":{"name":1,"created_at":1}}]`},
}

func (m Model) updateHelpers(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	helpers := m.helperItems()
	switch msg.String() {
	case "j", "down":
		if m.helperCursor < len(helpers)-1 {
			m.helperCursor++
		}
	case "k", "up":
		if m.helperCursor > 0 {
			m.helperCursor--
		}
	case "enter":
		if len(helpers) == 0 {
			return m, nil
		}
		if m.helperCursor >= len(helpers) {
			m.helperCursor = len(helpers) - 1
		}
		tpl := helpers[m.helperCursor].template
		if helpers[m.helperCursor].kind == "prompt" {
			return m.copyNamedText("prompt", tpl)
		}
		m.queryInput.SetValue(tpl)
		m.activeTab = tabQuery
		m.focus = panelRight
		m.queryFocus = true
		m.queryInput.Focus()
		m.syncTableFocus()
	case "c":
		if len(helpers) == 0 || m.helperCursor >= len(helpers) {
			return m, nil
		}
		label := "template"
		if helpers[m.helperCursor].kind == "prompt" {
			label = "prompt"
		}
		return m.copyNamedText(label, helpers[m.helperCursor].template)
	}
	return m, nil
}

func (m Model) updateInspect(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	maxScroll := max(0, len(m.inspectLines)-m.inspectViewportHeight())
	switch msg.String() {
	case "esc", "q", "v", "enter":
		m.showInspect = false
	case "c":
		return m.copyNamedText("detail", m.inspectCopy)
	case "j", "down":
		if m.inspectScroll < maxScroll {
			m.inspectScroll++
		}
	case "k", "up":
		if m.inspectScroll > 0 {
			m.inspectScroll--
		}
	case "pgdown":
		m.inspectScroll = min(maxScroll, m.inspectScroll+m.inspectViewportHeight())
	case "pgup":
		m.inspectScroll = max(0, m.inspectScroll-m.inspectViewportHeight())
	case "g", "home":
		m.inspectScroll = 0
	case "G", "end":
		m.inspectScroll = maxScroll
	}
	return m, nil
}

func (m Model) updateConfirm(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q", "n":
		m.clearConfirm()
		m.setStatus("cancelled")
		return m, nil
	case "y", "enter":
		action := m.confirmAction
		connIdx := m.confirmConnIdx
		query := m.confirmQuery
		m.clearConfirm()
		switch action {
		case confirmDeleteConnection:
			return m.deleteConnectionConfirmed(connIdx)
		case confirmRunQuery:
			return m.runConfirmedQuery(query)
		}
	}
	return m, nil
}

// --- New connection form ---

func (m *Model) openNewConnForm() {
	m.showNewConn = true
	m.newConnFocus = newConnFocusName
	m.newConnTypeCur = 0
	for i := range m.newConnInputs {
		m.newConnInputs[i].SetValue("")
	}
	m.updateDSNPlaceholder()
	m.syncNewConnFocus()
}

func (m Model) updateNewConn(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.showNewConn = false
		return m, nil
	case "tab", "down":
		m.newConnFocus = (m.newConnFocus + 1) % newConnFocusCount
		m.syncNewConnFocus()
		return m, nil
	case "shift+tab", "up":
		m.newConnFocus = (m.newConnFocus - 1 + newConnFocusCount) % newConnFocusCount
		m.syncNewConnFocus()
		return m, nil
	case "left":
		if m.newConnFocus == newConnFocusType {
			if m.newConnTypeCur > 0 {
				m.newConnTypeCur--
				m.updateDSNPlaceholder()
			}
			return m, nil
		}
	case "right":
		if m.newConnFocus == newConnFocusType {
			if m.newConnTypeCur < len(dbTypes)-1 {
				m.newConnTypeCur++
				m.updateDSNPlaceholder()
			}
			return m, nil
		}
	case "enter":
		if m.newConnFocus == newConnFocusSave {
			return m.submitNewConn()
		}
	}

	var cmd tea.Cmd
	switch m.newConnFocus {
	case newConnFocusName:
		m.newConnInputs[fieldName], cmd = m.newConnInputs[fieldName].Update(msg)
	case newConnFocusDSN:
		m.newConnInputs[fieldDSN], cmd = m.newConnInputs[fieldDSN].Update(msg)
	}
	return m, cmd
}

func (m *Model) syncNewConnFocus() {
	for i := range m.newConnInputs {
		m.newConnInputs[i].Blur()
	}
	switch m.newConnFocus {
	case newConnFocusName:
		m.newConnInputs[fieldName].Focus()
	case newConnFocusDSN:
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
		m.setStatus("dsn/path is required")
		return m, nil
	}

	m.cfg.AddConnection(name, dbType, dsn)
	if err := m.cfg.Save(); err != nil {
		m.setStatus("failed to save config: " + err.Error())
		return m, nil
	}
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
	if m.focus == panelLeft {
		m.queryFocus = false
		m.queryInput.Blur()
	}
	m.syncTableFocus()
}

func (m *Model) resetResultViewport() {
	m.resultColOffset = 0
	m.resultVisibleColumn = 0
	m.resultTable.SetCursor(0)
}

func (m *Model) openResultsTab() {
	m.activeTab = tabResults
	m.focus = panelRight
	m.queryFocus = false
	m.queryInput.Blur()
	m.clearSnippetPlaceholders()
	m.resetResultViewport()
	if m.queryResult != nil {
		m.syncResultTable()
	}
	m.syncTableFocus()
}

func (m Model) moveTableCursor(delta int) (tea.Model, tea.Cmd) {
	if len(m.tables) == 0 || delta == 0 {
		return m, nil
	}
	next := m.tableCursor + delta
	if next < 0 {
		next = 0
	}
	if next >= len(m.tables) {
		next = len(m.tables) - 1
	}
	if next == m.tableCursor {
		return m, nil
	}
	m.tableCursor = next
	m.syncHelperCursor()
	m.loading = m.activeTab == tabSchema
	return m, m.loadSchema(m.tables[m.tableCursor])
}

func (m Model) textInputCapturesKeypress() bool {
	return m.queryFocus
}

func (m Model) leftPanelWidth() int {
	if m.width <= 0 {
		return 30
	}
	lw := m.width * 32 / 100
	if lw < 24 {
		lw = 24
	}
	if lw > m.width-30 {
		lw = m.width - 30
	}
	return lw
}

func (m Model) rightPanelWidth() int {
	if m.width <= 0 {
		return 60
	}
	return m.width - m.leftPanelWidth() - 1
}

// --- Async commands ---

func connectCmd(reqID, connIdx int, conn config.Connection) tea.Cmd {
	return func() tea.Msg {
		d, err := db.New(conn.Type, conn.DSN)
		if err != nil {
			return connectedMsg{reqID: reqID, connIdx: connIdx, conn: conn, err: err}
		}
		if err := d.Connect(); err != nil {
			return connectedMsg{reqID: reqID, connIdx: connIdx, conn: conn, err: err}
		}
		return connectedMsg{reqID: reqID, connIdx: connIdx, conn: conn, db: d}
	}
}

func (m *Model) loadTables(_ int) tea.Cmd {
	if m.activeDB == nil {
		return nil
	}
	m.tablesReqID++
	reqID := m.tablesReqID
	d := m.activeDB
	return func() tea.Msg {
		tables, err := d.GetTables()
		return tablesLoadedMsg{reqID: reqID, tables: tables, err: err}
	}
}

func (m *Model) loadSchema(table string) tea.Cmd {
	if m.activeDB == nil {
		return nil
	}
	m.schemaReqID++
	reqID := m.schemaReqID
	d := m.activeDB
	return func() tea.Msg {
		schema, err := d.GetTableSchema(table)
		return schemaLoadedMsg{reqID: reqID, table: table, schema: schema, err: err}
	}
}

func runQueryCmd(reqID int, d db.DB, query string) tea.Cmd {
	return func() tea.Msg {
		result, err := d.RunQuery(query)
		return queryDoneMsg{reqID: reqID, query: query, result: result, err: err}
	}
}

func (m *Model) setStatus(msg string) {
	m.statusMsg = msg
	m.statusExpiry = time.Now().Add(6 * time.Second)
}

func defaultQueryForTable(active db.DB, table string) string {
	if active != nil && active.Type() == "mongo" {
		return fmt.Sprintf("find %s {} 100", table)
	}
	return fmt.Sprintf("SELECT * FROM %q LIMIT 100;", table)
}

func (m Model) openDefaultQueryEditor() (tea.Model, tea.Cmd) {
	if len(m.tables) == 0 || m.activeDB == nil {
		return m, nil
	}
	table := m.tables[m.tableCursor]
	m.queryInput.SetValue(defaultQueryForTable(m.activeDB, table))
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.clearSnippetPlaceholders()
	m.syncTableFocus()
	return m, nil
}

func (m Model) runDefaultBrowseQuery() (tea.Model, tea.Cmd) {
	if len(m.tables) == 0 || m.activeDB == nil {
		return m, nil
	}
	table := m.tables[m.tableCursor]
	query := defaultQueryForTable(m.activeDB, table)
	m.queryInput.SetValue(query)
	m.queryInput.CursorEnd()
	m.queryFocus = false
	m.queryInput.Blur()
	m.clearSnippetPlaceholders()
	m.openResultsTab()
	m.loading = true
	m.queryErr = ""
	m.queryResult = nil
	m.resultTable.SetRows(nil)
	m.queryReqID++
	return m, runQueryCmd(m.queryReqID, m.activeDB, query)
}

func (m Model) helperItems() []queryHelper {
	table := ""
	if len(m.tables) > 0 && m.tableCursor >= 0 && m.tableCursor < len(m.tables) {
		table = m.tables[m.tableCursor]
	}

	items := make([]queryHelper, 0, 16)
	dbType := ""
	if m.activeDB != nil {
		dbType = m.activeDB.Type()
	}

	switch dbType {
	case "mongo":
		items = append(items, m.schemaAwareHelpers(table)...)
		items = append(items, m.generatedMongoMonitorHelpers(table)...)
		if table != "" {
			items = append(items,
				queryHelper{label: "Use current collection", template: fmt.Sprintf("find %s {} 100", table)},
				queryHelper{label: "Count current collection", template: fmt.Sprintf("count %s {}", table)},
			)
		}
		items = append(items, materializeHelpers(mongoQueryHelpers, table)...)
	case "postgres":
		items = append(items, m.schemaAwareHelpers(table)...)
		items = append(items, m.generatedSQLMonitorHelpers(table)...)
		if table != "" {
			items = append(items,
				queryHelper{label: "Use current table", template: fmt.Sprintf("SELECT * FROM %q LIMIT 100;", table)},
				queryHelper{label: "Count current table", template: fmt.Sprintf("SELECT COUNT(*) FROM %q;", table)},
			)
		}
		items = append(items, materializeHelpers(sqlQueryHelpers, table)...)
		items = append(items, materializeHelpers(postgresQueryHelpers, table)...)
	case "sqlite":
		items = append(items, m.schemaAwareHelpers(table)...)
		items = append(items, m.generatedSQLMonitorHelpers(table)...)
		if table != "" {
			items = append(items,
				queryHelper{label: "Use current table", template: fmt.Sprintf("SELECT * FROM %q LIMIT 100;", table)},
				queryHelper{label: "Count current table", template: fmt.Sprintf("SELECT COUNT(*) FROM %q;", table)},
			)
		}
		items = append(items, materializeHelpers(sqlQueryHelpers, table)...)
		items = append(items, materializeHelpers(sqliteQueryHelpers, table)...)
	default:
		items = append(items, m.schemaAwareHelpers(table)...)
		items = append(items, m.generatedSQLMonitorHelpers(table)...)
		if table != "" {
			items = append(items,
				queryHelper{label: "Use current table", template: fmt.Sprintf("SELECT * FROM %q LIMIT 100;", table)},
				queryHelper{label: "Count current table", template: fmt.Sprintf("SELECT COUNT(*) FROM %q;", table)},
			)
		}
		items = append(items, materializeHelpers(sqlQueryHelpers, table)...)
	}
	return items
}

func materializeHelpers(helpers []queryHelper, table string) []queryHelper {
	items := make([]queryHelper, 0, len(helpers))
	for _, helper := range helpers {
		tpl := helper.template
		if table != "" {
			tpl = strings.ReplaceAll(tpl, "{table}", table)
		}
		items = append(items, queryHelper{label: helper.label, template: tpl, kind: helper.kind})
	}
	return items
}

func (m *Model) currentTableName() string {
	if len(m.tables) == 0 || m.tableCursor < 0 || m.tableCursor >= len(m.tables) {
		return ""
	}
	return m.tables[m.tableCursor]
}

func (m *Model) syncHelperCursor() {
	helpers := m.helperItems()
	if len(helpers) == 0 {
		m.helperCursor = 0
		return
	}
	if m.helperCursor >= len(helpers) {
		m.helperCursor = len(helpers) - 1
	}
	if m.helperCursor < 0 {
		m.helperCursor = 0
	}
}

func (m *Model) syncTableFocus() {
	if m.focus == panelRight && !m.queryFocus && m.activeTab == tabSchema {
		m.schemaTable.Focus()
	} else {
		m.schemaTable.Blur()
	}
	if m.focus == panelRight && !m.queryFocus && m.activeTab == tabResults {
		m.resultTable.Focus()
	} else {
		m.resultTable.Blur()
	}
}

func (m *Model) resizeTables() {
	contentW := m.tableViewportWidth()

	schemaHeight := max(3, m.height-10)
	m.schemaTable.SetWidth(contentW)
	m.schemaTable.SetHeight(schemaHeight)

	resultHeight := max(3, m.height-17)
	m.resultTable.SetWidth(contentW)
	m.resultTable.SetHeight(resultHeight)

	m.syncSchemaTable()
	m.syncResultTable()
	m.syncTableFocus()
}

func (m *Model) syncSchemaTable() {
	w := max(12, m.tableViewportWidth())
	const cellChrome = 2
	contentW := max(12, w-(cellChrome*3))
	nameW := clampInt(contentW*34/100, 12, max(12, contentW-20))
	typeW := clampInt(contentW*30/100, 10, max(10, contentW-16))
	flagW := max(10, contentW-(nameW+typeW))
	cursor := m.schemaTable.Cursor()
	cols := []table.Column{
		{Title: "Column", Width: nameW},
		{Title: "Type", Width: typeW},
		{Title: "Flags", Width: flagW},
	}
	m.schemaTable.SetRows(nil)
	m.schemaTable.SetColumns(cols)

	if m.tableSchema == nil {
		return
	}

	rows := make([]table.Row, 0, len(m.tableSchema.Columns))
	for _, col := range m.tableSchema.Columns {
		flags := []string{}
		if col.PrimaryKey {
			flags = append(flags, "PK")
		}
		if !col.Nullable {
			flags = append(flags, "NOT NULL")
		}
		if len(flags) == 0 {
			flags = append(flags, "-")
		}
		rows = append(rows, table.Row{
			fitTableCell(col.Name, nameW),
			fitTableCell(col.Type, typeW),
			fitTableCell(strings.Join(flags, ", "), flagW),
		})
	}
	m.schemaTable.SetRows(rows)
	if len(rows) == 0 {
		m.schemaTable.SetCursor(0)
		return
	}
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(rows) {
		cursor = len(rows) - 1
	}
	m.schemaTable.SetCursor(cursor)
}

func (m *Model) syncResultTable() {
	w := max(12, m.tableViewportWidth())
	if m.queryResult == nil || len(m.queryResult.Columns) == 0 {
		m.resultTable.SetRows(nil)
		m.resultTable.SetColumns(nil)
		m.resultVisibleColumn = 0
		return
	}

	start, cols := visibleResultColumns(m.queryResult, w, m.resultColOffset)
	m.resultColOffset = start
	m.resultVisibleColumn = len(cols)

	rows := make([]table.Row, 0, len(m.queryResult.Rows))
	for _, row := range m.queryResult.Rows {
		visible := make(table.Row, 0, len(cols))
		for i := 0; i < len(cols); i++ {
			colIdx := start + i
			if colIdx < len(row) {
				visible = append(visible, fitTableCell(row[colIdx], cols[i].Width))
			} else {
				visible = append(visible, "")
			}
		}
		rows = append(rows, visible)
	}

	m.resultTable.SetRows(nil)
	m.resultTable.SetColumns(cols)
	m.resultTable.SetRows(rows)
	if len(rows) == 0 {
		m.resultTable.SetCursor(0)
		return
	}
	cursor := m.resultTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(rows) {
		cursor = len(rows) - 1
	}
	m.resultTable.SetCursor(cursor)
}

func (m *Model) shiftResultColumns(delta int) {
	if m.queryResult == nil || len(m.queryResult.Columns) == 0 {
		return
	}
	next := m.resultColOffset + delta
	if next < 0 {
		next = 0
	}
	if next >= len(m.queryResult.Columns) {
		next = len(m.queryResult.Columns) - 1
	}
	if next == m.resultColOffset {
		return
	}
	m.resultColOffset = next
	m.syncResultTable()
}

func (m *Model) openSchemaInspect() {
	if m.tableSchema == nil || len(m.tableSchema.Columns) == 0 {
		return
	}
	cursor := m.schemaTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.tableSchema.Columns) {
		cursor = len(m.tableSchema.Columns) - 1
	}
	col := m.tableSchema.Columns[cursor]
	flags := []string{}
	if col.PrimaryKey {
		flags = append(flags, "PK")
	}
	if !col.Nullable {
		flags = append(flags, "NOT NULL")
	}
	if len(flags) == 0 {
		flags = append(flags, "-")
	}
	m.inspectTitle = "Schema column"
	m.inspectLines = []string{
		accentStyle.Render(m.dataSourceLabel()) + "  " + textStyle.Render(m.currentTableName()),
		accentStyle.Render("column") + "  " + textStyle.Render(col.Name),
		accentStyle.Render("type") + "  " + textStyle.Render(col.Type),
		accentStyle.Render("flags") + "  " + textStyle.Render(strings.Join(flags, ", ")),
	}
	m.inspectCopy = strings.Join([]string{
		m.dataSourceLabel() + "  " + m.currentTableName(),
		"column  " + col.Name,
		"type  " + col.Type,
		"flags  " + strings.Join(flags, ", "),
	}, "\n")
	m.inspectScroll = 0
	m.showInspect = true
}

func (m *Model) openResultInspect() {
	if m.queryResult == nil || len(m.queryResult.Rows) == 0 {
		return
	}
	cursor := m.resultTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.queryResult.Rows) {
		cursor = len(m.queryResult.Rows) - 1
	}
	m.inspectTitle = "Result row"
	m.inspectLines, m.inspectCopy = m.renderResultRowInspect(cursor)
	m.inspectScroll = 0
	m.showInspect = true
}

func (m Model) tableViewportWidth() int {
	if m.isSinglePane() {
		return max(16, m.width-4)
	}
	return max(16, m.rightPanelWidth()-4)
}

func (m *Model) pushQueryHistory(query string) {
	query = strings.TrimSpace(query)
	if query == "" {
		return
	}
	filtered := make([]string, 0, len(m.queryHistory))
	for _, existing := range m.queryHistory {
		if existing == query {
			continue
		}
		filtered = append(filtered, existing)
	}
	m.queryHistory = append([]string{query}, filtered...)
	if len(m.queryHistory) > 50 {
		m.queryHistory = m.queryHistory[:50]
	}
	if connID := m.currentConnectionID(); connID != "" {
		m.cfg.PushQuery(connID, query, 50)
		_ = m.cfg.Save()
	}
	m.historyCursor = 0
}

func (m Model) saveCurrentQuery() (tea.Model, tea.Cmd) {
	query := strings.TrimSpace(m.queryInput.Value())
	if query == "" {
		m.setStatus("nothing to save")
		return m, nil
	}
	connID := m.currentConnectionID()
	if connID == "" {
		m.setStatus("connect first to save queries")
		return m, nil
	}
	label := savedQueryLabel(query)
	m.cfg.SaveQuery(connID, label, query, 50)
	m.savedQueries = m.cfg.SavedQueriesForConnection(connID)
	if err := m.cfg.Save(); err != nil {
		m.setStatus("failed to save query: " + err.Error())
		return m, nil
	}
	m.setStatus("saved query")
	return m, nil
}

func (m *Model) recallPreviousQuery() {
	if len(m.queryHistory) == 0 {
		m.setStatus("no query history yet")
		return
	}
	if m.queryHistoryIdx+1 >= len(m.queryHistory) {
		m.queryHistoryIdx = len(m.queryHistory) - 1
	} else {
		m.queryHistoryIdx++
	}
	m.queryInput.SetValue(m.queryHistory[m.queryHistoryIdx])
	m.queryInput.CursorEnd()
	m.setStatus(fmt.Sprintf("history %d/%d", m.queryHistoryIdx+1, len(m.queryHistory)))
}

func (m *Model) recallNextQuery() {
	if len(m.queryHistory) == 0 {
		m.setStatus("no query history yet")
		return
	}
	if m.queryHistoryIdx <= 0 {
		m.queryHistoryIdx = -1
		m.queryInput.SetValue("")
		m.queryInput.CursorEnd()
		m.setStatus("history cleared")
		return
	}
	m.queryHistoryIdx--
	m.queryInput.SetValue(m.queryHistory[m.queryHistoryIdx])
	m.queryInput.CursorEnd()
	m.setStatus(fmt.Sprintf("history %d/%d", m.queryHistoryIdx+1, len(m.queryHistory)))
}

func (m *Model) recallLastRunQuery() {
	if strings.TrimSpace(m.lastRunQuery) == "" {
		m.setStatus("no query run yet")
		return
	}
	m.queryHistoryIdx = -1
	m.queryInput.SetValue(m.lastRunQuery)
	m.queryInput.CursorEnd()
	m.setStatus("recalled last run query")
}

func (m *Model) clearCurrentQuery() {
	if m.queryInput.Value() == "" && len(m.snippetPlaceholders) == 0 {
		m.setStatus("query editor already clear")
		return
	}
	m.queryInput.SetValue("")
	m.queryInput.Focus()
	m.queryHistoryIdx = -1
	m.clearSnippetPlaceholders()
	m.setStatus("query editor cleared")
}

func (m *Model) openDeleteConnectionConfirm(idx int) {
	if idx < 0 || idx >= len(m.cfg.Connections) {
		return
	}
	conn := m.cfg.Connections[idx]
	body := []string{
		fmt.Sprintf("Delete saved connection %q?", conn.Name),
		"This removes the saved DSN, query history, and saved queries for this connection.",
	}
	m.showConfirm = true
	m.confirmTitle = "Confirm Delete"
	m.confirmBody = body
	m.confirmAccept = "delete connection"
	m.confirmAction = confirmDeleteConnection
	m.confirmConnIdx = idx
	m.confirmQuery = ""
}

func (m *Model) openRunQueryConfirm(query string) {
	label := "Confirm Query"
	body := []string{"Run this write query?"}
	switch m.activeDB.Type() {
	case "mongo":
		body = append(body, "This command appears to change data in the current database.")
	default:
		body = append(body, "This statement appears to change schema or data in the current database.")
	}
	body = append(body, compactInline(query))
	m.showConfirm = true
	m.confirmTitle = label
	m.confirmBody = body
	m.confirmAccept = "run query"
	m.confirmAction = confirmRunQuery
	m.confirmConnIdx = -1
	m.confirmQuery = query
}

func (m *Model) clearConfirm() {
	m.showConfirm = false
	m.confirmTitle = ""
	m.confirmBody = nil
	m.confirmAccept = ""
	m.confirmAction = confirmNone
	m.confirmConnIdx = -1
	m.confirmQuery = ""
}

func (m Model) runCurrentQueryWithConfirm() (tea.Model, tea.Cmd) {
	query := strings.TrimSpace(m.queryInput.Value())
	if query == "" || m.activeDB == nil {
		return m, nil
	}
	if m.queryNeedsConfirmation(query) {
		m.openRunQueryConfirm(query)
		return m, nil
	}
	return m.runConfirmedQuery(query)
}

func (m Model) runConfirmedQuery(query string) (tea.Model, tea.Cmd) {
	if strings.TrimSpace(query) == "" || m.activeDB == nil {
		return m, nil
	}
	m.queryReqID++
	m.loading = true
	m.queryErr = ""
	m.queryResult = nil
	m.resultTable.SetRows(nil)
	return m, runQueryCmd(m.queryReqID, m.activeDB, query)
}

func (m Model) deleteConnectionConfirmed(idx int) (tea.Model, tea.Cmd) {
	if idx < 0 || idx >= len(m.cfg.Connections) {
		return m, nil
	}
	if m.activeConnIdx == idx {
		if m.activeDB != nil {
			m.activeDB.Close()
		}
		m.activeDB = nil
		m.activeConnIdx = -1
		m.activeConnName = ""
		m.tables = nil
		m.tableSchema = nil
		m.queryResult = nil
		m.queryErr = ""
	}
	m.cfg.DeleteConnection(idx)
	if err := m.cfg.Save(); err != nil {
		m.setStatus("failed to save config: " + err.Error())
		return m, nil
	}
	if m.connCursor >= len(m.cfg.Connections) && m.connCursor > 0 {
		m.connCursor--
	}
	if m.activeConnIdx >= len(m.cfg.Connections) {
		m.activeConnIdx = -1
	}
	m.setStatus("connection deleted")
	return m, nil
}

func (m Model) queryNeedsConfirmation(query string) bool {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" || m.activeDB == nil {
		return false
	}
	switch m.activeDB.Type() {
	case "mongo":
		cmd, _ := nextQueryWord(query)
		switch cmd {
		case "insert", "update", "delete", "remove":
			return true
		default:
			return false
		}
	default:
		first := firstSQLKeyword(query)
		switch first {
		case "insert", "update", "delete", "drop", "alter", "truncate", "create", "replace":
			return true
		default:
			return false
		}
	}
}

func firstSQLKeyword(query string) string {
	for _, field := range strings.Fields(query) {
		field = strings.Trim(field, "();")
		if field == "" {
			continue
		}
		return field
	}
	return ""
}

func nextQueryWord(input string) (string, string) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", ""
	}
	for i, r := range input {
		if unicode.IsSpace(r) {
			return input[:i], strings.TrimSpace(input[i+1:])
		}
	}
	return input, ""
}

func (m Model) currentConnectionID() string {
	if m.activeConnIdx < 0 || m.activeConnIdx >= len(m.cfg.Connections) {
		return ""
	}
	return m.cfg.Connections[m.activeConnIdx].ID
}

type queryColumnContext struct {
	start       int
	end         int
	title       string
	multi       bool
	includeStar bool
	fallback    string
}

func (m *Model) openCompletionForCursor(manual bool) bool {
	ctx, items, ok := m.completionItemsForCursor()
	if !ok {
		if manual {
			m.setStatus("completion unavailable here")
		}
		return false
	}
	if len(items) == 0 {
		if manual {
			m.setStatus("no completion items available")
		}
		return false
	}
	m.columnPickerTitle = ctx.title
	m.columnPickerItems = items
	m.columnPickerCursor = 0
	m.columnPickerMulti = ctx.multi
	m.columnPickerStart = ctx.start
	m.columnPickerEnd = ctx.end
	m.columnPickerFallback = ctx.fallback
	m.showColumnPicker = true
	return true
}

func (m *Model) refreshCompletionPicker(manual bool) bool {
	if !m.showColumnPicker {
		return m.openCompletionForCursor(manual)
	}
	ctx, items, ok := m.completionItemsForCursor()
	if !ok {
		m.showColumnPicker = false
		return false
	}
	if len(items) == 0 {
		m.showColumnPicker = false
		return false
	}
	m.columnPickerTitle = ctx.title
	m.columnPickerItems = items
	m.columnPickerCursor = 0
	m.columnPickerMulti = false
	m.columnPickerStart = ctx.start
	m.columnPickerEnd = ctx.end
	m.columnPickerFallback = ctx.fallback
	m.showColumnPicker = true
	return true
}

func (m Model) completionItemsForCursor() (queryColumnContext, []columnPickerItem, bool) {
	if m.activeDB == nil {
		return queryColumnContext{}, nil, false
	}
	if m.activeDB.Type() == "mongo" {
		return m.mongoCompletionContext()
	}
	if ctx, ok := m.queryColumnContext(); ok {
		return ctx, m.columnPickerCandidates(ctx), true
	}
	if ctx, ok := m.queryTableContext(); ok {
		return ctx, m.tablePickerCandidates(ctx), true
	}
	return m.sqlKeywordCompletionContext()
}

func (m Model) tablePickerCandidates(ctx queryColumnContext) []columnPickerItem {
	prefix := strings.ToLower(currentTokenValue([]rune(m.queryInput.Value())[ctx.start:ctx.end]))
	items := make([]columnPickerItem, 0, len(m.tables))
	for _, name := range m.tables {
		items = append(items, columnPickerItem{
			label:      name,
			detail:     m.dataSourceLabel(),
			insertText: quoteIdentifierForDB(m.activeDB, name),
		})
	}
	return rankCompletionItems(prefix, items)
}

func (m Model) sqlKeywordCompletionContext() (queryColumnContext, []columnPickerItem, bool) {
	query := []rune(m.queryInput.Value())
	cursor := m.queryCursorIndex()
	start, end := queryTokenBounds(query, cursor)
	token := strings.ToLower(currentTokenValue(query[start:end]))
	beforeToken := strings.ToLower(string(query[:start]))
	trimmed := strings.TrimSpace(beforeToken)
	title := "SQL Snippets"
	switch {
	case trimmed == "":
		title = "SQL Starters"
	case strings.HasSuffix(trimmed, "from") || strings.HasSuffix(trimmed, "join") || strings.HasSuffix(trimmed, "where") || strings.HasSuffix(trimmed, "group by") || strings.HasSuffix(trimmed, "order by"):
		title = "SQL Clauses"
	default:
		title = "SQL Keywords"
	}
	items := m.sqlKeywordCompletionItems()
	items = rankCompletionItems(token, items)
	if len(items) == 0 {
		return queryColumnContext{}, nil, false
	}
	return queryColumnContext{start: start, end: end, title: title, fallback: currentTokenValue(query[start:end])}, items, true
}

func (m Model) sqlKeywordCompletionItems() []columnPickerItem {
	table := fallbackTableName(m.currentTableName())
	filterCol := fallbackColumnName(m.preferredFilterColumn())
	sortCol := fallbackColumnName(m.preferredSortColumn())
	return []columnPickerItem{
		{label: "SELECT starter", detail: "snippet", insertText: fmt.Sprintf("SELECT ${columns}\nFROM %s\nWHERE %s = ${value}\nLIMIT 50;", quoteIdentifierForDB(m.activeDB, table), quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "INSERT starter", detail: "snippet", insertText: fmt.Sprintf("INSERT INTO %s (${columns})\nVALUES (${values});", quoteIdentifierForDB(m.activeDB, table))},
		{label: "UPDATE starter", detail: "snippet", insertText: fmt.Sprintf("UPDATE %s\nSET %s = ${value}\nWHERE %s = ${id};", quoteIdentifierForDB(m.activeDB, table), quoteIdentifierForDB(m.activeDB, filterCol), quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "DELETE starter", detail: "snippet", insertText: fmt.Sprintf("DELETE FROM %s\nWHERE %s = ${value};", quoteIdentifierForDB(m.activeDB, table), quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "JOIN clause", detail: "snippet", insertText: fmt.Sprintf("JOIN %s ON ${left_column} = ${right_column}", quoteIdentifierForDB(m.activeDB, table))},
		{label: "WHERE clause", detail: "snippet", insertText: fmt.Sprintf("WHERE %s = ${value}", quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "GROUP BY clause", detail: "snippet", insertText: fmt.Sprintf("GROUP BY %s", quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "ORDER BY clause", detail: "snippet", insertText: fmt.Sprintf("ORDER BY %s DESC", quoteIdentifierForDB(m.activeDB, sortCol))},
		{label: "LIMIT clause", detail: "snippet", insertText: "LIMIT ${limit}"},
		{label: "SELECT", detail: "keyword", insertText: "SELECT"},
		{label: "FROM", detail: "keyword", insertText: "FROM"},
		{label: "WHERE", detail: "keyword", insertText: "WHERE"},
		{label: "JOIN", detail: "keyword", insertText: "JOIN"},
		{label: "GROUP BY", detail: "keyword", insertText: "GROUP BY"},
		{label: "ORDER BY", detail: "keyword", insertText: "ORDER BY"},
		{label: "LIMIT", detail: "keyword", insertText: "LIMIT"},
		{label: "INSERT INTO", detail: "keyword", insertText: "INSERT INTO"},
		{label: "UPDATE", detail: "keyword", insertText: "UPDATE"},
		{label: "DELETE FROM", detail: "keyword", insertText: "DELETE FROM"},
	}
}

func (m Model) queryColumnContext() (queryColumnContext, bool) {
	query := []rune(m.queryInput.Value())
	cursor := m.queryCursorIndex()
	start, end := queryTokenBounds(query, cursor)
	beforeToken := strings.ToLower(string(query[:start]))
	trimmed := strings.TrimSpace(beforeToken)

	switch {
	case inSelectList(beforeToken):
		return queryColumnContext{
			start:       start,
			end:         end,
			title:       "Select Columns",
			multi:       true,
			includeStar: true,
			fallback:    "*",
		}, true
	case inWhereClause(beforeToken):
		return queryColumnContext{
			start:    start,
			end:      end,
			title:    "Filter Column",
			multi:    false,
			fallback: currentTokenValue(query[start:end]),
		}, true
	case inOrderByList(beforeToken):
		return queryColumnContext{
			start:    start,
			end:      end,
			title:    "Order By Columns",
			multi:    true,
			fallback: currentTokenValue(query[start:end]),
		}, true
	case inGroupByList(beforeToken):
		return queryColumnContext{
			start:    start,
			end:      end,
			title:    "Group By Columns",
			multi:    true,
			fallback: currentTokenValue(query[start:end]),
		}, true
	case inInsertColumnList(beforeToken):
		return queryColumnContext{
			start:    start,
			end:      end,
			title:    "Insert Columns",
			multi:    true,
			fallback: currentTokenValue(query[start:end]),
		}, true
	case inUpdateSetList(beforeToken):
		return queryColumnContext{
			start:    start,
			end:      end,
			title:    "Set Columns",
			multi:    false,
			fallback: currentTokenValue(query[start:end]),
		}, true
	case trimmed == "":
		return queryColumnContext{}, false
	default:
		return queryColumnContext{}, false
	}
}

func (m Model) queryTableContext() (queryColumnContext, bool) {
	query := []rune(m.queryInput.Value())
	cursor := m.queryCursorIndex()
	start, end := queryTokenBounds(query, cursor)
	beforeToken := strings.ToLower(string(query[:start]))
	switch {
	case inFromTable(beforeToken):
		return queryColumnContext{start: start, end: end, title: "From Table", fallback: currentTokenValue(query[start:end])}, true
	case inJoinTable(beforeToken):
		return queryColumnContext{start: start, end: end, title: "Join Table", fallback: currentTokenValue(query[start:end])}, true
	case inUpdateTable(beforeToken):
		return queryColumnContext{start: start, end: end, title: "Update Table", fallback: currentTokenValue(query[start:end])}, true
	case inInsertIntoTable(beforeToken):
		return queryColumnContext{start: start, end: end, title: "Insert Into", fallback: currentTokenValue(query[start:end])}, true
	case inDeleteFromTable(beforeToken):
		return queryColumnContext{start: start, end: end, title: "Delete From", fallback: currentTokenValue(query[start:end])}, true
	default:
		return queryColumnContext{}, false
	}
}

func (m Model) mongoCompletionContext() (queryColumnContext, []columnPickerItem, bool) {
	query := m.queryInput.Value()
	cursor := m.queryCursorIndex()
	tokens := mongoTokens(query)
	tokenIdx := 0
	start := cursor
	end := cursor
	found := false
	for i, token := range tokens {
		if cursor >= token.start && cursor <= token.end {
			tokenIdx = i
			start = token.start
			end = token.end
			found = true
			break
		}
		if cursor < token.start {
			tokenIdx = i
			start = cursor
			end = cursor
			found = true
			break
		}
	}
	if !found && len(tokens) > 0 {
		tokenIdx = len(tokens)
		start = cursor
		end = cursor
	}
	prefix := strings.ToLower(strings.TrimSpace(query[start:end]))
	command := ""
	if len(tokens) > 0 {
		command = strings.ToLower(tokens[0].value)
	}
	ctx := queryColumnContext{start: start, end: end, title: "Mongo Commands", fallback: strings.TrimSpace(query[start:end])}
	var items []columnPickerItem
	switch tokenIdx {
	case 0:
		ctx.title = "Mongo Commands"
		items = m.mongoCommandItems()
	case 1:
		ctx.title = "Collections"
		items = m.mongoCollectionItems()
	default:
		ctx.title = "Mongo Arguments"
		items = m.mongoArgumentItems(command, tokenIdx)
	}
	items = rankCompletionItems(prefix, items)
	if len(items) == 0 {
		return queryColumnContext{}, nil, false
	}
	return ctx, items, true
}

type mongoToken struct {
	value string
	start int
	end   int
}

func mongoTokens(query string) []mongoToken {
	runes := []rune(query)
	tokens := make([]mongoToken, 0, 8)
	start := -1
	depth := 0
	inQuote := false
	var quote rune
	for i, r := range runes {
		if inQuote {
			if r == quote && (i == 0 || runes[i-1] != '\\') {
				inQuote = false
			}
			continue
		}
		switch r {
		case '"', '\'':
			inQuote = true
			quote = r
			if start == -1 {
				start = i
			}
		case '{', '[':
			depth++
			if start == -1 {
				start = i
			}
		case '}', ']':
			if depth > 0 {
				depth--
			}
		}
		if start == -1 {
			if !unicode.IsSpace(r) {
				start = i
			}
			continue
		}
		if depth == 0 && unicode.IsSpace(r) {
			tokens = append(tokens, mongoToken{value: string(runes[start:i]), start: start, end: i})
			start = -1
		}
	}
	if start >= 0 {
		tokens = append(tokens, mongoToken{value: string(runes[start:]), start: start, end: len(runes)})
	}
	return tokens
}

func (m Model) mongoCommandItems() []columnPickerItem {
	table := fallbackTableName(m.currentTableName())
	return []columnPickerItem{
		{label: "find", detail: "query", insertText: fmt.Sprintf("find %s ${filter} 50 ${sort}", table)},
		{label: "aggregate", detail: "query", insertText: fmt.Sprintf("aggregate %s ${pipeline}", table)},
		{label: "insert", detail: "query", insertText: fmt.Sprintf("insert %s ${document}", table)},
		{label: "update", detail: "query", insertText: fmt.Sprintf("update %s ${filter} ${update}", table)},
		{label: "delete", detail: "query", insertText: fmt.Sprintf("delete %s ${filter}", table)},
		{label: "count", detail: "query", insertText: fmt.Sprintf("count %s ${filter}", table)},
		{label: "help", detail: "command", insertText: "help"},
	}
}

func (m Model) mongoCollectionItems() []columnPickerItem {
	items := make([]columnPickerItem, 0, len(m.tables))
	for _, name := range m.tables {
		items = append(items, columnPickerItem{label: name, detail: "collection", insertText: name})
	}
	return items
}

func (m Model) mongoArgumentItems(command string, tokenIdx int) []columnPickerItem {
	filterField := fallbackColumnName(m.preferredFilterColumn())
	groupField := fallbackColumnName(m.preferredCategoricalColumn())
	switch command {
	case "find":
		if tokenIdx == 2 {
			return []columnPickerItem{
				{label: "empty filter", detail: "json", insertText: "{}"},
				{label: "field filter", detail: "json", insertText: fmt.Sprintf(`{"%s":"${value}"}`, filterField)},
			}
		}
		if tokenIdx == 3 {
			return []columnPickerItem{
				{label: "limit 20", detail: "limit", insertText: "20"},
				{label: "limit 50", detail: "limit", insertText: "50"},
				{label: "limit 100", detail: "limit", insertText: "100"},
			}
		}
		return []columnPickerItem{
			{label: "recent sort", detail: "json", insertText: fmt.Sprintf(`{"%s":-1}`, groupField)},
			{label: "ascending sort", detail: "json", insertText: fmt.Sprintf(`{"%s":1}`, filterField)},
		}
	case "aggregate":
		return []columnPickerItem{
			{label: "match + limit", detail: "pipeline", insertText: fmt.Sprintf(`[{"$match":{"%s":"${value}"}},{"$limit":20}]`, filterField)},
			{label: "group + count", detail: "pipeline", insertText: fmt.Sprintf(`[{"$group":{"_id":"$%s","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}]`, groupField)},
		}
	case "insert":
		return []columnPickerItem{
			{label: "document", detail: "json", insertText: fmt.Sprintf(`{"%s":"${value}"}`, filterField)},
		}
	case "update":
		if tokenIdx == 2 {
			return []columnPickerItem{
				{label: "filter", detail: "json", insertText: fmt.Sprintf(`{"%s":"${value}"}`, filterField)},
			}
		}
		if tokenIdx == 3 {
			return []columnPickerItem{
				{label: "$set", detail: "json", insertText: fmt.Sprintf(`{"$set":{"%s":"${value}"}}`, filterField)},
			}
		}
		return []columnPickerItem{
			{label: "many", detail: "token", insertText: "many"},
		}
	case "delete":
		if tokenIdx == 2 {
			return []columnPickerItem{
				{label: "filter", detail: "json", insertText: fmt.Sprintf(`{"%s":"${value}"}`, filterField)},
			}
		}
		return []columnPickerItem{
			{label: "many", detail: "token", insertText: "many"},
		}
	case "count":
		return []columnPickerItem{
			{label: "empty filter", detail: "json", insertText: "{}"},
			{label: "field filter", detail: "json", insertText: fmt.Sprintf(`{"%s":"${value}"}`, filterField)},
		}
	default:
		return nil
	}
}

func (m Model) columnPickerCandidates(ctx queryColumnContext) []columnPickerItem {
	prefix := strings.ToLower(currentTokenValue([]rune(m.queryInput.Value())[ctx.start:ctx.end]))
	items := make([]columnPickerItem, 0, len(m.tableSchema.Columns)+1)
	if ctx.includeStar {
		items = append(items, columnPickerItem{label: "*", detail: "all", insertText: "*"})
	}
	aliasPrefix := m.currentAliasPrefix(ctx)
	for _, col := range m.tableSchema.Columns {
		items = append(items, columnPickerItem{
			label:      col.Name,
			detail:     col.Type,
			insertText: m.columnInsertionValue(col.Name, aliasPrefix),
		})
	}
	return rankCompletionItems(prefixWithoutAlias(prefix), items)
}

func (m Model) queryCursorIndex() int {
	query := []rune(m.queryInput.Value())
	line := clampInt(m.queryInput.Line(), 0, len(splitQueryLines(query))-1)
	col := m.queryInput.LineInfo().ColumnOffset
	return queryIndexForLineCol(query, line, col)
}

func (m *Model) applyCompletionInsertion(ctx queryColumnContext, items []columnPickerItem) {
	if len(items) == 0 && ctx.fallback != "" {
		items = []columnPickerItem{{insertText: ctx.fallback}}
	}
	if len(items) == 0 {
		return
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		insertText := item.insertText
		if insertText == "" {
			insertText = item.label
		}
		parts = append(parts, insertText)
	}
	insert := strings.Join(parts, ", ")
	if !ctx.multi && len(parts) == 1 {
		insert = parts[0]
	}
	query := []rune(m.queryInput.Value())
	if ctx.start < 0 || ctx.start > len(query) {
		ctx.start = len(query)
	}
	if ctx.end < ctx.start || ctx.end > len(query) {
		ctx.end = ctx.start
	}
	updated := string(query[:ctx.start]) + insert + string(query[ctx.end:])
	line, col := queryLineColForIndex([]rune(updated), ctx.start+len([]rune(insert)))
	m.queryInput.SetValue(updated)
	m.queryInput.Focus()
	setTextareaCursor(&m.queryInput, line, col)
	m.activateSnippetPlaceholders(insert, ctx.start)
	m.queryFocus = true
	m.focus = panelRight
	m.syncTableFocus()
}

func (m Model) columnInsertionValue(name, aliasPrefix string) string {
	if name == "*" {
		return name
	}
	if m.activeDB != nil && m.activeDB.Type() == "mongo" {
		if aliasPrefix != "" {
			return aliasPrefix + name
		}
		return name
	}
	value := fmt.Sprintf("%q", name)
	if aliasPrefix != "" {
		return aliasPrefix + value
	}
	return value
}

func queryTokenBounds(query []rune, cursor int) (int, int) {
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(query) {
		cursor = len(query)
	}
	start := cursor
	for start > 0 && isQueryTokenRune(query[start-1]) {
		start--
	}
	end := cursor
	for end < len(query) && isQueryTokenRune(query[end]) {
		end++
	}
	return start, end
}

func isQueryTokenRune(r rune) bool {
	return r == '_' || r == '"' || r == '.' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func currentTokenValue(token []rune) string {
	return strings.Trim(strings.TrimSpace(string(token)), `"`)
}

func splitQueryLines(query []rune) [][]rune {
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

func queryIndexForLineCol(query []rune, line, col int) int {
	lines := splitQueryLines(query)
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

func queryLineColForIndex(query []rune, idx int) (int, int) {
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

func prefixWithoutAlias(prefix string) string {
	if idx := strings.LastIndex(prefix, "."); idx >= 0 {
		return prefix[idx+1:]
	}
	return prefix
}

func rankCompletionItems(prefix string, items []columnPickerItem) []columnPickerItem {
	if len(items) == 0 {
		return nil
	}
	if prefix == "" {
		return append([]columnPickerItem(nil), items...)
	}
	type ranked struct {
		item  columnPickerItem
		score int
	}
	rankedItems := make([]ranked, 0, len(items))
	for _, item := range items {
		label := strings.ToLower(item.label)
		insert := strings.ToLower(item.insertText)
		score := -1
		switch {
		case strings.HasPrefix(label, prefix), strings.HasPrefix(insert, prefix):
			score = 0
		case fuzzyMatch(label, prefix), fuzzyMatch(insert, prefix):
			score = 1
		}
		if score >= 0 {
			rankedItems = append(rankedItems, ranked{item: item, score: score})
		}
	}
	if len(rankedItems) == 0 {
		return rankCompletionItems("", items)
	}
	sort.SliceStable(rankedItems, func(i, j int) bool {
		if rankedItems[i].score != rankedItems[j].score {
			return rankedItems[i].score < rankedItems[j].score
		}
		return strings.ToLower(rankedItems[i].item.label) < strings.ToLower(rankedItems[j].item.label)
	})
	out := make([]columnPickerItem, 0, len(rankedItems))
	for _, item := range rankedItems {
		out = append(out, item.item)
	}
	return out
}

func fuzzyMatch(candidate, query string) bool {
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

func quoteIdentifierForDB(active db.DB, name string) string {
	if active != nil && active.Type() == "mongo" {
		return name
	}
	return fmt.Sprintf("%q", name)
}

func setTextareaCursor(input *textarea.Model, line, col int) {
	input.CursorStart()
	for i := 0; i < line; i++ {
		input.CursorDown()
	}
	input.SetCursor(col)
}

func lastKeyword(before, keyword string) int {
	return strings.LastIndex(strings.ToLower(before), keyword)
}

func inSelectList(before string) bool {
	selectIdx := lastKeyword(before, "select")
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

func inWhereClause(before string) bool {
	lastWhere := max(lastKeyword(before, " where "), lastKeyword(before, "where "))
	lastAnd := lastKeyword(before, " and ")
	lastOr := lastKeyword(before, " or ")
	lastOn := lastKeyword(before, " on ")
	start := max(max(lastWhere, lastAnd), max(lastOr, lastOn))
	if start < 0 {
		return false
	}
	for _, blocker := range []string{" = ", " != ", " > ", " < ", " like ", " in ", " is ", "\n"} {
		if idx := strings.LastIndex(before, blocker); idx > start {
			return false
		}
	}
	return true
}

func inOrderByList(before string) bool {
	orderIdx := lastKeyword(before, " order by ")
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

func inGroupByList(before string) bool {
	groupIdx := lastKeyword(before, " group by ")
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
	insertIdx := lastKeyword(before, "insert into ")
	valuesIdx := lastKeyword(before, " values")
	openParen := strings.LastIndex(before, "(")
	closeParen := strings.LastIndex(before, ")")
	return insertIdx >= 0 && openParen > insertIdx && openParen > closeParen && valuesIdx < openParen
}

func inUpdateSetList(before string) bool {
	updateIdx := lastKeyword(before, "update ")
	setIdx := lastKeyword(before, " set ")
	if updateIdx < 0 || setIdx < updateIdx {
		return false
	}
	lastWhere := lastKeyword(before, " where ")
	if lastWhere > setIdx {
		return false
	}
	for _, blocker := range []string{" = ", " != ", " > ", " < ", " like ", "\n"} {
		if idx := strings.LastIndex(before, blocker); idx > setIdx {
			return false
		}
	}
	return true
}

func inFromTable(before string) bool {
	fromIdx := lastKeyword(before, " from ")
	if fromIdx < 0 {
		return false
	}
	after := before[fromIdx:]
	for _, blocker := range []string{" where ", " join ", " group by ", " order by ", " limit ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func inJoinTable(before string) bool {
	joinIdx := lastKeyword(before, " join ")
	if joinIdx < 0 {
		return false
	}
	after := before[joinIdx:]
	for _, blocker := range []string{" on ", " where ", " group by ", " order by ", " limit ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func inUpdateTable(before string) bool {
	updateIdx := lastKeyword(before, "update ")
	if updateIdx < 0 {
		return false
	}
	after := before[updateIdx:]
	for _, blocker := range []string{" set ", " where ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func inInsertIntoTable(before string) bool {
	insertIdx := lastKeyword(before, "insert into ")
	if insertIdx < 0 {
		return false
	}
	after := before[insertIdx:]
	for _, blocker := range []string{"(", " values", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func inDeleteFromTable(before string) bool {
	deleteIdx := lastKeyword(before, "delete from ")
	if deleteIdx < 0 {
		return false
	}
	after := before[deleteIdx:]
	for _, blocker := range []string{" where ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func (m Model) currentHistoryQuery() string {
	if m.historyCursor < 0 || m.historyCursor >= len(m.queryHistory) {
		return ""
	}
	return m.queryHistory[m.historyCursor]
}

func (m Model) currentResultRowText() string {
	if m.queryResult == nil || len(m.queryResult.Rows) == 0 {
		return ""
	}
	cursor := m.resultTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.queryResult.Rows) {
		cursor = len(m.queryResult.Rows) - 1
	}
	var parts []string
	row := m.queryResult.Rows[cursor]
	for i, col := range m.queryResult.Columns {
		val := "(empty)"
		if i < len(row) && strings.TrimSpace(row[i]) != "" {
			val = row[i]
		}
		parts = append(parts, fmt.Sprintf("%s: %s", col, val))
	}
	return strings.Join(parts, "\n")
}

func (m Model) currentResultRowJSON() string {
	data, err := json.MarshalIndent(m.currentResultRowObject(), "", "  ")
	if err != nil {
		return ""
	}
	return string(data)
}

func (m Model) currentResultRowPrettyText() string {
	_, copyText := m.renderResultRowInspect(m.currentResultCursor())
	return copyText
}

func (m Model) copyNamedText(label, text string) (tea.Model, tea.Cmd) {
	text = strings.TrimSpace(text)
	if text == "" {
		m.setStatus("nothing to copy")
		return m, nil
	}
	if err := clipboard.WriteAll(text); err != nil {
		m.lastCopied = text
		if label == "" {
			label = "text"
		}
		m.setStatus("saved " + label + " locally")
		return m, nil
	}
	m.lastCopied = text
	if label == "" {
		label = "text"
	}
	m.setStatus("copied " + label)
	return m, nil
}

func (m *Model) openQueryPicker(title string, items []queryPickerItem) {
	if len(items) == 0 {
		m.setStatus("nothing available yet")
		return
	}
	m.queryPickerTitle = title
	m.queryPickerItems = items
	m.queryPickerCursor = 0
	m.showQueryPicker = true
}

func (m Model) updateQueryPicker(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.showQueryPicker = false
		return m, nil
	case "j", "down":
		if m.queryPickerCursor < len(m.queryPickerItems)-1 {
			m.queryPickerCursor++
		}
	case "k", "up":
		if m.queryPickerCursor > 0 {
			m.queryPickerCursor--
		}
	case "c":
		if item := m.currentQueryPickerItem(); item.value != "" {
			return m.copyNamedText("query", item.value)
		}
	case "enter":
		item := m.currentQueryPickerItem()
		if item.value == "" {
			return m, nil
		}
		m.showQueryPicker = false
		switch item.kind {
		case "prompt":
			return m.copyNamedText("prompt", item.value)
		case "copy":
			return m.copyNamedText(item.detail, item.value)
		default:
			m.queryInput.SetValue(item.value)
			m.queryInput.CursorEnd()
			m.activeTab = tabQuery
			m.focus = panelRight
			m.queryFocus = true
			m.queryInput.Focus()
			m.clearSnippetPlaceholders()
			m.syncTableFocus()
			m.setStatus("loaded into query editor")
		}
	}
	return m, nil
}

func (m Model) updateColumnPicker(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.showColumnPicker = false
		return m, nil
	case "j", "down":
		if m.columnPickerCursor < len(m.columnPickerItems)-1 {
			m.columnPickerCursor++
		}
	case "k", "up":
		if m.columnPickerCursor > 0 {
			m.columnPickerCursor--
		}
	case " ":
		if len(m.columnPickerItems) == 0 {
			return m, nil
		}
		if !m.columnPickerMulti {
			var cmd tea.Cmd
			m.queryInput, cmd = m.queryInput.Update(msg)
			m.refreshCompletionPicker(false)
			return m, cmd
		}
		m.columnPickerItems[m.columnPickerCursor].selected = !m.columnPickerItems[m.columnPickerCursor].selected
	case "enter":
		items := make([]columnPickerItem, 0, len(m.columnPickerItems))
		for _, item := range m.columnPickerItems {
			if item.selected {
				items = append(items, item)
			}
		}
		if len(items) == 0 {
			if m.columnPickerMulti && m.columnPickerFallback != "" {
				items = append(items, columnPickerItem{insertText: m.columnPickerFallback})
			} else if len(m.columnPickerItems) > 0 {
				items = append(items, m.columnPickerItems[m.columnPickerCursor])
			}
		}
		m.showColumnPicker = false
		m.applyCompletionInsertion(queryColumnContext{
			start:    m.columnPickerStart,
			end:      m.columnPickerEnd,
			fallback: m.columnPickerFallback,
			multi:    m.columnPickerMulti,
		}, items)
		m.setStatus("inserted completion")
	default:
		if m.completionPickerCapturesTyping(msg) {
			var cmd tea.Cmd
			m.queryInput, cmd = m.queryInput.Update(msg)
			m.clearSnippetPlaceholders()
			m.refreshCompletionPicker(false)
			return m, cmd
		}
	}
	return m, nil
}

func (m Model) currentQueryPickerItem() queryPickerItem {
	if m.queryPickerCursor < 0 || m.queryPickerCursor >= len(m.queryPickerItems) {
		return queryPickerItem{}
	}
	return m.queryPickerItems[m.queryPickerCursor]
}

func (m Model) queryHistoryPickerItems() []queryPickerItem {
	items := make([]queryPickerItem, 0, len(m.queryHistory))
	for i, query := range m.queryHistory {
		items = append(items, queryPickerItem{
			label:  truncate(compactInline(query), 68),
			detail: fmt.Sprintf("recent #%d", i+1),
			value:  query,
			kind:   "history",
		})
	}
	return items
}

func (m Model) querySavedPickerItems() []queryPickerItem {
	items := make([]queryPickerItem, 0, len(m.savedQueries))
	for i, saved := range m.savedQueries {
		items = append(items, queryPickerItem{
			label:  saved.Label,
			detail: fmt.Sprintf("saved #%d", i+1),
			value:  saved.Query,
			kind:   "saved",
		})
	}
	return items
}

func (m Model) queryHelperPickerItems() []queryPickerItem {
	helpers := m.helperItems()
	items := make([]queryPickerItem, 0, len(helpers))
	for _, helper := range helpers {
		detail := "template"
		if helper.kind == "prompt" {
			detail = "copyable prompt"
		}
		items = append(items, queryPickerItem{
			label:  helper.label,
			detail: detail,
			value:  helper.template,
			kind:   helper.kind,
		})
	}
	return items
}

func (m Model) queryCopyPickerItems() []queryPickerItem {
	query := strings.TrimSpace(m.queryInput.Value())
	if query == "" {
		return nil
	}
	return []queryPickerItem{
		{label: "Raw query", detail: "query", value: query, kind: "copy"},
		{label: "Go string literal", detail: "go string", value: goStringLiteral(query), kind: "copy"},
		{label: "JavaScript string", detail: "js string", value: strconv.Quote(query), kind: "copy"},
		{label: "Python string", detail: "python string", value: pythonStringLiteral(query), kind: "copy"},
		{label: "JSON string", detail: "json string", value: strconv.Quote(query), kind: "copy"},
	}
}

func (m Model) currentAliasPrefix(ctx queryColumnContext) string {
	token := currentTokenValue([]rune(m.queryInput.Value())[ctx.start:ctx.end])
	if idx := strings.LastIndex(token, "."); idx >= 0 {
		alias := strings.TrimSpace(token[:idx])
		if alias != "" {
			return alias + "."
		}
	}
	return ""
}

func (m Model) completionPickerCapturesTyping(msg tea.KeyMsg) bool {
	switch msg.Type {
	case tea.KeyRunes, tea.KeyBackspace, tea.KeyDelete:
		return true
	case tea.KeySpace:
		return !m.columnPickerMulti
	default:
		return false
	}
}

func (m *Model) activateSnippetPlaceholders(insert string, base int) {
	placeholders := findSnippetPlaceholders(insert, base)
	m.snippetPlaceholders = placeholders
	m.snippetIndex = 0
	if len(placeholders) > 0 {
		m.focusSnippetPlaceholder(0)
		return
	}
	m.clearSnippetPlaceholders()
}

func (m *Model) clearSnippetPlaceholders() {
	m.snippetPlaceholders = nil
	m.snippetIndex = 0
}

func findSnippetPlaceholders(insert string, base int) []snippetPlaceholder {
	runes := []rune(insert)
	items := make([]snippetPlaceholder, 0, 4)
	for i := 0; i < len(runes)-2; i++ {
		if runes[i] != '$' || runes[i+1] != '{' {
			continue
		}
		for j := i + 2; j < len(runes); j++ {
			if runes[j] == '}' {
				name := string(runes[i+2 : j])
				items = append(items, snippetPlaceholder{name: name, start: base + i, end: base + j + 1, fresh: true})
				i = j
				break
			}
		}
	}
	return items
}

func (m *Model) focusSnippetPlaceholder(idx int) bool {
	if idx < 0 || idx >= len(m.snippetPlaceholders) {
		return false
	}
	m.snippetIndex = idx
	line, col := queryLineColForIndex([]rune(m.queryInput.Value()), m.snippetPlaceholders[idx].start)
	setTextareaCursor(&m.queryInput, line, col)
	return true
}

func (m *Model) jumpSnippetPlaceholder(delta int) bool {
	if len(m.snippetPlaceholders) == 0 {
		return false
	}
	next := m.snippetIndex + delta
	if next < 0 {
		next = len(m.snippetPlaceholders) - 1
	}
	if next >= len(m.snippetPlaceholders) {
		next = 0
	}
	return m.focusSnippetPlaceholder(next)
}

func (m *Model) handleSnippetPlaceholderEdit(msg tea.KeyMsg) bool {
	if len(m.snippetPlaceholders) == 0 || m.snippetIndex >= len(m.snippetPlaceholders) {
		return false
	}
	switch msg.Type {
	case tea.KeyRunes:
		if len(msg.Runes) == 0 {
			return false
		}
		m.applySnippetEdit(string(msg.Runes))
		return true
	case tea.KeySpace:
		m.applySnippetEdit(" ")
		return true
	case tea.KeyBackspace:
		m.applySnippetBackspace()
		return true
	case tea.KeyDelete:
		m.applySnippetDelete()
		return true
	default:
		return false
	}
}

func (m *Model) applySnippetEdit(text string) {
	if m.snippetIndex >= len(m.snippetPlaceholders) {
		return
	}
	ph := m.snippetPlaceholders[m.snippetIndex]
	cursor := m.queryCursorIndex()
	if ph.fresh {
		oldLen := ph.end - ph.start
		m.replaceQueryRange(ph.start, ph.end, text)
		m.shiftSnippetPlaceholders(ph.start, oldLen, len([]rune(text)))
		m.snippetPlaceholders[m.snippetIndex].start = ph.start
		m.snippetPlaceholders[m.snippetIndex].end = ph.start + len([]rune(text))
		m.snippetPlaceholders[m.snippetIndex].fresh = false
		m.focusCursorAtIndex(ph.start + len([]rune(text)))
		return
	}
	insertAt := clampInt(cursor, ph.start, ph.end)
	m.replaceQueryRange(insertAt, insertAt, text)
	m.shiftSnippetPlaceholders(insertAt, 0, len([]rune(text)))
	m.focusCursorAtIndex(insertAt + len([]rune(text)))
}

func (m *Model) applySnippetBackspace() {
	if m.snippetIndex >= len(m.snippetPlaceholders) {
		return
	}
	ph := m.snippetPlaceholders[m.snippetIndex]
	cursor := m.queryCursorIndex()
	if ph.fresh {
		oldLen := ph.end - ph.start
		m.replaceQueryRange(ph.start, ph.end, "")
		m.shiftSnippetPlaceholders(ph.start, oldLen, 0)
		m.snippetPlaceholders[m.snippetIndex].end = ph.start
		m.snippetPlaceholders[m.snippetIndex].fresh = false
		m.focusCursorAtIndex(ph.start)
		return
	}
	if cursor <= ph.start {
		return
	}
	m.replaceQueryRange(cursor-1, cursor, "")
	m.shiftSnippetPlaceholders(cursor-1, 1, 0)
	m.focusCursorAtIndex(cursor - 1)
}

func (m *Model) applySnippetDelete() {
	if m.snippetIndex >= len(m.snippetPlaceholders) {
		return
	}
	ph := m.snippetPlaceholders[m.snippetIndex]
	cursor := clampInt(m.queryCursorIndex(), ph.start, ph.end)
	if ph.fresh {
		oldLen := ph.end - ph.start
		m.replaceQueryRange(ph.start, ph.end, "")
		m.shiftSnippetPlaceholders(ph.start, oldLen, 0)
		m.snippetPlaceholders[m.snippetIndex].end = ph.start
		m.snippetPlaceholders[m.snippetIndex].fresh = false
		m.focusCursorAtIndex(ph.start)
		return
	}
	if cursor >= ph.end {
		return
	}
	m.replaceQueryRange(cursor, cursor+1, "")
	m.shiftSnippetPlaceholders(cursor, 1, 0)
	m.focusCursorAtIndex(cursor)
}

func (m *Model) replaceQueryRange(start, end int, text string) {
	query := []rune(m.queryInput.Value())
	start = clampInt(start, 0, len(query))
	end = clampInt(end, start, len(query))
	updated := string(query[:start]) + text + string(query[end:])
	m.queryInput.SetValue(updated)
	m.queryInput.Focus()
}

func (m *Model) shiftSnippetPlaceholders(changeStart, removed, added int) {
	delta := added - removed
	for i := range m.snippetPlaceholders {
		ph := &m.snippetPlaceholders[i]
		switch {
		case changeStart <= ph.start:
			ph.start += delta
			ph.end += delta
		case changeStart < ph.end:
			ph.end += delta
		}
		if ph.end < ph.start {
			ph.end = ph.start
		}
	}
}

func (m *Model) focusCursorAtIndex(idx int) {
	line, col := queryLineColForIndex([]rune(m.queryInput.Value()), idx)
	setTextareaCursor(&m.queryInput, line, col)
}

func (m Model) resultCopyPickerItems() []queryPickerItem {
	if m.queryResult == nil {
		return nil
	}
	items := make([]queryPickerItem, 0, 5)
	if row := m.currentResultRowJSON(); row != "" {
		items = append(items, queryPickerItem{label: "Current row JSON", detail: "row json", value: row, kind: "copy"})
	}
	if pretty := m.currentResultRowPrettyText(); pretty != "" {
		items = append(items, queryPickerItem{label: "Current row inspect view", detail: "row detail", value: pretty, kind: "copy"})
	}
	if data := m.allResultRowsJSON(); data != "" {
		items = append(items, queryPickerItem{label: "All rows JSON", detail: "rows json", value: data, kind: "copy"})
	}
	if data := m.allResultRowsCSV(); data != "" {
		items = append(items, queryPickerItem{label: "All rows CSV", detail: "rows csv", value: data, kind: "copy"})
	}
	return items
}

func (m Model) currentResultCursor() int {
	if m.queryResult == nil || len(m.queryResult.Rows) == 0 {
		return 0
	}
	cursor := m.resultTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.queryResult.Rows) {
		cursor = len(m.queryResult.Rows) - 1
	}
	return cursor
}

func (m Model) renderResultRowInspect(cursor int) ([]string, string) {
	if m.queryResult == nil || len(m.queryResult.Rows) == 0 {
		return nil, ""
	}
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.queryResult.Rows) {
		cursor = len(m.queryResult.Rows) - 1
	}

	row := m.queryResult.Rows[cursor]
	wrapW := max(40, min(96, m.width-14))
	viewLines := []string{dimStyle.Render(fmt.Sprintf("row %d", cursor+1)), "", textStyle.Render("{")}
	copyLines := []string{fmt.Sprintf("row %d", cursor+1), "", "{"}
	for i, col := range m.queryResult.Columns {
		raw := ""
		if i < len(row) {
			raw = row[i]
		}
		parts := structuredValueLines(raw)
		suffix := ""
		if i < len(m.queryResult.Columns)-1 {
			suffix = ","
		}
		copyPrefix := fmt.Sprintf("  %q: ", col)
		prefixW := len([]rune(copyPrefix))
		if len(parts) == 1 {
			value := parts[0]
			viewWrapped := wrapTextPreservingRuns(value, max(20, wrapW-prefixW))
			copyWrapped := wrapTextPreservingRuns(value, max(20, wrapW-prefixW))
			for j, part := range viewWrapped {
				lineSuffix := ""
				if j == len(viewWrapped)-1 {
					lineSuffix = suffix
				}
				if j == 0 {
					viewLines = append(viewLines, "  "+accentStyle.Render(fmt.Sprintf("%q", col))+textStyle.Render(": "+part+lineSuffix))
					copyLines = append(copyLines, copyPrefix+copyWrapped[j]+lineSuffix)
					continue
				}
				viewLines = append(viewLines, strings.Repeat(" ", prefixW)+textStyle.Render(part+lineSuffix))
				copyLines = append(copyLines, strings.Repeat(" ", prefixW)+copyWrapped[j]+lineSuffix)
			}
			continue
		}

		viewLines = append(viewLines, "  "+accentStyle.Render(fmt.Sprintf("%q", col))+textStyle.Render(":"))
		copyLines = append(copyLines, copyPrefix)
		for j, part := range parts {
			lineSuffix := ""
			if j == len(parts)-1 {
				lineSuffix = suffix
			}
			viewLines = append(viewLines, "    "+textStyle.Render(part+lineSuffix))
			copyLines = append(copyLines, "    "+part+lineSuffix)
		}
	}
	viewLines = append(viewLines, textStyle.Render("}"))
	copyLines = append(copyLines, "}")
	return viewLines, strings.Join(copyLines, "\n")
}

func (m Model) currentResultRowObject() map[string]any {
	cursor := m.currentResultCursor()
	if m.queryResult == nil || len(m.queryResult.Rows) == 0 {
		return nil
	}
	row := m.queryResult.Rows[cursor]
	return m.resultRowObject(row)
}

func (m Model) resultRowObject(row []string) map[string]any {
	obj := make(map[string]any, len(m.queryResult.Columns))
	for i, col := range m.queryResult.Columns {
		raw := ""
		if i < len(row) {
			raw = row[i]
		}
		obj[col] = structuredCopyValue(raw)
	}
	return obj
}

func structuredCopyValue(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if (strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}")) || (strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]")) {
		var decoded any
		if json.Unmarshal([]byte(raw), &decoded) == nil {
			return decoded
		}
	}
	return raw
}

func (m Model) allResultRowsJSON() string {
	if m.queryResult == nil || len(m.queryResult.Rows) == 0 {
		return ""
	}
	rows := make([]map[string]any, 0, len(m.queryResult.Rows))
	for _, row := range m.queryResult.Rows {
		rows = append(rows, m.resultRowObject(row))
	}
	data, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return ""
	}
	return string(data)
}

func (m Model) allResultRowsCSV() string {
	if m.queryResult == nil {
		return ""
	}
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(m.queryResult.Columns); err != nil {
		return ""
	}
	for _, row := range m.queryResult.Rows {
		record := make([]string, len(m.queryResult.Columns))
		copy(record, row)
		if err := writer.Write(record); err != nil {
			return ""
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return ""
	}
	return strings.TrimRight(buf.String(), "\n")
}

func structuredValueLines(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{"(empty)"}
	}
	if (strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}")) || (strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]")) {
		var decoded any
		if json.Unmarshal([]byte(raw), &decoded) == nil {
			data, err := json.MarshalIndent(decoded, "", "  ")
			if err == nil {
				return strings.Split(string(data), "\n")
			}
		}
	}
	return []string{compactInline(raw)}
}

func (m Model) schemaAwareHelpers(table string) []queryHelper {
	if table == "" || m.tableSchema == nil || m.tableSchema.Name != table {
		return nil
	}

	var items []queryHelper
	cols := m.schemaColumnNames()
	if len(cols) == 0 {
		return nil
	}

	if m.activeDB != nil && m.activeDB.Type() == "mongo" {
		lookupField := m.preferredFilterColumn()
		if lookupField == "" {
			lookupField = "_id"
		}
		groupField := m.preferredCategoricalColumn()
		if groupField == "" {
			groupField = lookupField
		}
		items = append(items,
			queryHelper{label: "Find using schema fields", template: fmt.Sprintf("find %s {} 100", table)},
			queryHelper{label: "Lookup by key field", template: fmt.Sprintf(`find %s {"%s":"value"} 20`, table, lookupField)},
			queryHelper{label: "Recent documents", template: fmt.Sprintf(`find %s {} 50`, table)},
			queryHelper{label: "Count by field", template: fmt.Sprintf(`count %s {"%s":"value"}`, table, lookupField)},
			queryHelper{label: "Group by categorical field", template: fmt.Sprintf(`aggregate %s [{"$group":{"_id":"$%s","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}]`, table, groupField)},
			queryHelper{label: "Ask Ollama from schema", template: m.ollamaPrompt(table), kind: "prompt"},
		)
		return items
	}

	columnList := strings.Join(m.quotedColumns(max(1, min(5, len(cols)))), ", ")
	filterCol := m.preferredFilterColumn()
	if filterCol == "" {
		filterCol = cols[0]
	}
	idCol := m.primaryKeyColumn()
	if idCol == "" {
		idCol = filterCol
	}
	insertCols := m.preferredWriteColumns(4)
	if len(insertCols) == 0 {
		insertCols = cols[:min(4, len(cols))]
	}
	updateCols := m.preferredUpdateColumns(3)
	if len(updateCols) == 0 {
		updateCols = []string{filterCol}
	}
	items = append(items,
		queryHelper{label: "Select all rows", template: fmt.Sprintf("SELECT * FROM %q LIMIT 100;", table)},
		queryHelper{label: "Select top schema columns", template: fmt.Sprintf("SELECT %s FROM %q LIMIT 100;", columnList, table)},
		queryHelper{label: "Lookup by key field", template: fmt.Sprintf("SELECT * FROM %q WHERE %q = 'value' LIMIT 20;", table, idCol)},
		queryHelper{label: "Filter by likely search column", template: fmt.Sprintf("SELECT * FROM %q WHERE %q = 'value' LIMIT 50;", table, filterCol)},
		queryHelper{label: "Search with LIKE scaffold", template: fmt.Sprintf("SELECT %s FROM %q WHERE %q LIKE '%%value%%' ORDER BY %q LIMIT 50;", columnList, table, filterCol, filterCol)},
		queryHelper{label: "Count by populated field", template: fmt.Sprintf("SELECT COUNT(*) FROM %q WHERE %q IS NOT NULL;", table, filterCol)},
		queryHelper{label: "Insert row with schema columns", template: fmt.Sprintf("INSERT INTO %q (%s) VALUES (%s);", table, quoteColumns(insertCols), placeholderValues(len(insertCols)))},
		queryHelper{label: "Update row by key field", template: fmt.Sprintf("UPDATE %q SET %s WHERE %q = 'value';", table, updateAssignments(updateCols), idCol)},
		queryHelper{label: "Delete row by key field", template: fmt.Sprintf("DELETE FROM %q WHERE %q = 'value';", table, idCol)},
		queryHelper{label: "Null audit for filter column", template: fmt.Sprintf("SELECT %s FROM %q WHERE %q IS NULL LIMIT 50;", columnList, table, filterCol)},
		queryHelper{label: "Ask Ollama from schema", template: m.ollamaPrompt(table), kind: "prompt"},
	)
	if sortCol := m.preferredSortColumn(); sortCol != "" {
		items = append(items, queryHelper{
			label:    "Recent rows by timestamp",
			template: fmt.Sprintf("SELECT * FROM %q ORDER BY %q DESC LIMIT 50;", table, sortCol),
		}, queryHelper{
			label:    "Recent rows with top columns",
			template: fmt.Sprintf("SELECT %s FROM %q ORDER BY %q DESC LIMIT 50;", columnList, table, sortCol),
		})
	}
	if catCol := m.preferredCategoricalColumn(); catCol != "" {
		items = append(items,
			queryHelper{label: "Group by categorical column", template: fmt.Sprintf("SELECT %q, COUNT(*) AS count FROM %q GROUP BY %q ORDER BY count DESC LIMIT 20;", catCol, table, catCol)},
			queryHelper{label: "Distinct categorical values", template: fmt.Sprintf("SELECT DISTINCT %q FROM %q ORDER BY %q LIMIT 50;", catCol, table, catCol)},
		)
	}
	return items
}

func (m Model) generatedSQLMonitorHelpers(table string) []queryHelper {
	if table == "" || m.tableSchema == nil || m.tableSchema.Name != table {
		return nil
	}
	filterCol := m.preferredFilterColumn()
	if filterCol == "" {
		return nil
	}
	idCol := m.primaryKeyColumn()
	if idCol == "" {
		idCol = filterCol
	}
	columnList := strings.Join(m.quotedColumns(max(1, min(6, len(m.schemaColumnNames())))), ", ")
	items := []queryHelper{
		{label: "Debug one row by key", template: fmt.Sprintf("SELECT %s FROM %q WHERE %q = 'value' LIMIT 1;", columnList, table, idCol)},
		{label: "Compare duplicate values", template: fmt.Sprintf("SELECT %q, COUNT(*) AS count FROM %q GROUP BY %q HAVING COUNT(*) > 1 ORDER BY count DESC LIMIT 20;", filterCol, table, filterCol)},
		{label: "Find blanks or nulls", template: fmt.Sprintf("SELECT %s FROM %q WHERE %q IS NULL OR TRIM(CAST(%q AS TEXT)) = '' LIMIT 50;", columnList, table, filterCol, filterCol)},
	}
	if sortCol := m.preferredSortColumn(); sortCol != "" {
		items = append(items,
			queryHelper{label: "Recent failures to inspect", template: fmt.Sprintf("SELECT %s FROM %q ORDER BY %q DESC LIMIT 20;", columnList, table, sortCol)},
			queryHelper{label: "Count by day", template: fmt.Sprintf("SELECT DATE(%q) AS day, COUNT(*) AS count FROM %q GROUP BY DATE(%q) ORDER BY day DESC LIMIT 30;", sortCol, table, sortCol)},
		)
	}
	return items
}

func (m Model) generatedMongoMonitorHelpers(table string) []queryHelper {
	if table == "" || m.tableSchema == nil || m.tableSchema.Name != table {
		return nil
	}
	filterField := m.preferredFilterColumn()
	if filterField == "" {
		filterField = "_id"
	}
	groupField := m.preferredCategoricalColumn()
	if groupField == "" {
		groupField = filterField
	}
	items := []queryHelper{
		{label: "Project top schema fields", template: fmt.Sprintf(`aggregate %s [{"$limit":50},{"$project":{"%s":1,"%s":1}}]`, table, filterField, groupField)},
		{label: "Match likely field + sort recent", template: fmt.Sprintf(`find %s {"%s":"value"} 50 {"_id":-1}`, table, filterField)},
		{label: "Count grouped values", template: fmt.Sprintf(`aggregate %s [{"$group":{"_id":"$%s","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}]`, table, groupField)},
		{label: "Documents missing field", template: fmt.Sprintf(`find %s {"%s":{"$exists":false}} 50`, table, filterField)},
	}
	return items
}

func (m Model) schemaColumnNames() []string {
	if m.tableSchema == nil {
		return nil
	}
	cols := make([]string, 0, len(m.tableSchema.Columns))
	for _, col := range m.tableSchema.Columns {
		cols = append(cols, col.Name)
	}
	return cols
}

func (m Model) quotedColumns(limit int) []string {
	cols := m.schemaColumnNames()
	if len(cols) == 0 {
		return []string{"*"}
	}
	if limit > 0 && len(cols) > limit {
		cols = cols[:limit]
	}
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		out = append(out, fmt.Sprintf("%q", col))
	}
	return out
}

func quoteColumns(cols []string) string {
	if len(cols) == 0 {
		return "*"
	}
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		out = append(out, fmt.Sprintf("%q", col))
	}
	return strings.Join(out, ", ")
}

func placeholderValues(count int) string {
	if count <= 0 {
		return ""
	}
	values := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		values = append(values, fmt.Sprintf("'value%d'", i))
	}
	return strings.Join(values, ", ")
}

func updateAssignments(cols []string) string {
	if len(cols) == 0 {
		return `"{col}" = '{value}'`
	}
	assignments := make([]string, 0, len(cols))
	for i, col := range cols {
		assignments = append(assignments, fmt.Sprintf("%q = 'value%d'", col, i+1))
	}
	return strings.Join(assignments, ", ")
}

func (m Model) primaryKeyColumn() string {
	if m.tableSchema == nil {
		return ""
	}
	for _, col := range m.tableSchema.Columns {
		if col.PrimaryKey {
			return col.Name
		}
	}
	return ""
}

func (m Model) preferredFilterColumn() string {
	if m.tableSchema == nil {
		return ""
	}
	for _, col := range m.tableSchema.Columns {
		name := strings.ToLower(col.Name)
		if strings.Contains(name, "name") || strings.Contains(name, "email") || strings.Contains(name, "status") {
			return col.Name
		}
	}
	if pk := m.primaryKeyColumn(); pk != "" {
		return pk
	}
	if len(m.tableSchema.Columns) == 0 {
		return ""
	}
	return m.tableSchema.Columns[0].Name
}

func (m Model) preferredSortColumn() string {
	if m.tableSchema == nil {
		return ""
	}
	for _, col := range m.tableSchema.Columns {
		name := strings.ToLower(col.Name)
		if strings.Contains(name, "created") || strings.Contains(name, "updated") || strings.Contains(name, "timestamp") || strings.Contains(name, "date") {
			return col.Name
		}
	}
	return ""
}

func (m Model) preferredCategoricalColumn() string {
	if m.tableSchema == nil {
		return ""
	}
	for _, col := range m.tableSchema.Columns {
		colType := strings.ToLower(col.Type)
		name := strings.ToLower(col.Name)
		if strings.Contains(name, "status") || strings.Contains(name, "type") || strings.Contains(name, "role") || strings.Contains(name, "category") {
			return col.Name
		}
		if strings.Contains(colType, "char") || strings.Contains(colType, "text") {
			return col.Name
		}
	}
	return ""
}

func (m Model) preferredWriteColumns(limit int) []string {
	if m.tableSchema == nil {
		return nil
	}
	out := make([]string, 0, limit)
	for _, col := range m.tableSchema.Columns {
		if col.PrimaryKey && len(m.tableSchema.Columns) > 1 {
			continue
		}
		out = append(out, col.Name)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (m Model) preferredUpdateColumns(limit int) []string {
	if m.tableSchema == nil {
		return nil
	}
	out := make([]string, 0, limit)
	for _, col := range m.tableSchema.Columns {
		if col.PrimaryKey {
			continue
		}
		out = append(out, col.Name)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (m Model) ollamaPrompt(table string) string {
	var schemaLines []string
	if m.tableSchema != nil {
		for _, col := range m.tableSchema.Columns {
			flags := []string{}
			if col.PrimaryKey {
				flags = append(flags, "primary key")
			}
			if !col.Nullable {
				flags = append(flags, "not null")
			}
			meta := col.Type
			if len(flags) > 0 {
				meta += ", " + strings.Join(flags, ", ")
			}
			schemaLines = append(schemaLines, fmt.Sprintf("- %s: %s", col.Name, meta))
		}
	}
	if len(schemaLines) == 0 {
		schemaLines = append(schemaLines, "- schema unavailable")
	}
	engine := "SQL"
	if m.activeDB != nil && m.activeDB.Type() == "mongo" {
		engine = "Mongo"
	}
	return fmt.Sprintf("Write a %s query for the %q dataset.\nSchema:\n%s\n\nReturn only the query.", engine, table, strings.Join(schemaLines, "\n"))
}

func savedQueryLabel(query string) string {
	for _, line := range strings.Split(query, "\n") {
		line = compactInline(line)
		if line != "" {
			return truncate(line, 72)
		}
	}
	return "saved query"
}

func goStringLiteral(query string) string {
	if !strings.Contains(query, "`") {
		return "`" + query + "`"
	}
	return strconv.Quote(query)
}

func pythonStringLiteral(query string) string {
	if strings.Contains(query, "\n") && !strings.Contains(query, `"""`) {
		return `"""` + query + `"""`
	}
	return strconv.Quote(query)
}

// textinput update helper to avoid unused import
var _ = textinput.New
