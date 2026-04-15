package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"regexp"
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
	"dbkit/internal/ollama"
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
			m.queryInput.Placeholder = "db.collection.method({}) — ctrl+r runs · tab completes"
		} else {
			m.queryInput.Placeholder = "Write SQL — ctrl+r runs · tab completes"
		}
		m.setStatus("connected to " + m.activeConnName)
		m.activeTab = tabBrowse
		m.focus = panelLeft
		m.browseView = browseViewData
		m.browseData = nil
		m.browseDataTableName = ""
		m.browseDataTable.SetRows(nil)
		m.browseColOffset = 0
		m.browseVisibleColumn = 0
		m.querySourceTable = ""
		m.tableSchema = nil
		m.schemaTable.SetRows(nil)
		m.schemaCache = make(map[string]*db.TableSchema)
		m.queryResult = nil
		m.queryErr = ""
		m.resultTable.SetRows(nil)
		m.resultColOffset = 0
		m.resultVisibleColumn = 0
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
			cmds := []tea.Cmd{m.loadSchema(m.tables[m.tableCursor])}
			if m.browseView == browseViewData {
				cmds = append(cmds, m.loadBrowseData(m.tables[m.tableCursor]))
			}
			return m, tea.Batch(cmds...)
		}
		return m, nil

	case schemaLoadedMsg:
		if msg.err == nil && msg.schema != nil && msg.table != "" {
			m.schemaCache[schemaCacheKey(m.activeConnIdx, msg.table)] = msg.schema
		}
		// Clear the cache-load pending flag regardless of success.
		if msg.table != "" {
			delete(m.schemaPending, schemaCacheKey(m.activeConnIdx, msg.table))
		}
		if msg.reqID != m.schemaReqID {
			// Stale or cache-only load (reqID=-1): refresh the completion picker
			// if it's looking at this table, but never touch m.tableSchema.
			if m.showColumnPicker && m.activeTab == tabQuery && m.queryFocus {
				if inferred := m.queryInferredTable(); inferred != "" && strings.EqualFold(inferred, msg.table) {
					_, cmd := m.refreshCompletionPicker(false)
					return m, cmd
				}
			}
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
		// Refresh completion picker if it's open in the query editor.
		if m.showColumnPicker && m.activeTab == tabQuery && m.queryFocus {
			_, cmd := m.refreshCompletionPicker(false)
			return m, cmd
		}
		return m, nil

	case browseDataLoadedMsg:
		if msg.reqID != m.browseDataReqID {
			return m, nil
		}
		m.loading = false
		if msg.err != nil {
			m.browseData = nil
			m.browseDataTableName = ""
			m.browseDataTable.SetRows(nil)
			m.setStatus("data preview failed: " + msg.err.Error())
			return m, nil
		}
		m.browseData = msg.result
		m.browseDataTableName = msg.table
		m.browseColOffset = 0
		m.browseDataTable.SetCursor(0)
		m.syncBrowseDataTable()
		return m, nil

	case ollamaQueryDoneMsg:
		m.ollamaGenerating = false
		if msg.err != nil {
			m.ollamaErr = msg.err.Error()
		} else {
			m.ollamaResult = msg.query
			m.ollamaErr = ""
		}
		return m, nil

	case columnValuesMsg:
		key := columnValueKey(msg.connIdx, msg.table, msg.column)
		delete(m.columnValuePending, key)
		if msg.err != nil || msg.connIdx != m.activeConnIdx {
			return m, nil
		}
		m.columnValueCache[key] = msg.values
		if m.showColumnPicker && m.activeTab == tabQuery && m.queryFocus {
			_, cmd := m.refreshCompletionPicker(false)
			return m, cmd
		}
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
		m.syncResultTable()
		m.syncTableFocus()
		if msg.autoRefresh {
			// Keep the write-query status message visible; don't pollute history
			m.setStatus(fmt.Sprintf("%s — showing updated data", m.statusMsg))
		} else {
			m.lastRunQuery = msg.query
			m.pushQueryHistory(msg.query)
			m.queryHistoryIdx = -1
			if msg.result.Message != "" {
				m.setStatus(msg.result.Message)
			} else {
				m.setStatus(fmt.Sprintf("%d row(s) returned", len(msg.result.Rows)))
			}
		}
		// After a write query, invalidate browse data so it re-fetches
		if msg.result.Affected > 0 {
			m.browseData = nil
			m.browseDataTableName = ""
			m.browseDataTable.SetRows(nil)
			// Auto-refresh: run a follow-up SELECT to show updated data in results
			table := extractTableFromQuery(msg.query)
			if table != "" && m.activeDB != nil {
				selectQuery := targetedSelectFromWriteQuery(m.activeDB, msg.query)
				m.querySourceTable = table
				m.queryReqID++
				m.loading = true
				return m, runAutoRefreshCmd(m.queryReqID, m.activeDB, selectQuery)
			}
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
		if m.showOllamaGen {
			return m.updateOllamaGen(msg)
		}
		if m.showColumnPicker && m.activeTab == tabQuery && m.queryFocus && queryEditorShortcutWhileCompletionOpen(msg) {
			return m.updateQuery(msg)
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
			if m.activeTab == tabQuery && m.queryFocus {
				break
			}
			return m.navigateBack()
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
				m.activeTab = tabBrowse
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
		case tabBrowse:
			return m.updateBrowse(msg)
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
	case "e":
		if len(conns) > 0 {
			m.openEditConnForm(m.connCursor)
			return m, nil
		}
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

// --- Browse tab ---

func (m Model) updateBrowse(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.focus == panelRight && m.browseView == browseViewData {
		return m.updateBrowseData(msg)
	}
	if m.focus == panelRight && m.browseView == browseViewSchema {
		return m.updateBrowseSchema(msg)
	}

	// Left panel — table list
	switch msg.String() {
	case "j", "down":
		return m.moveTableCursor(1)
	case "k", "up":
		return m.moveTableCursor(-1)
	case "r":
		m.browseData = nil
		m.browseDataTableName = ""
		m.browseDataTable.SetRows(nil)
		m.loading = true
		return m, m.loadTables(m.activeConnIdx)
	case "enter":
		return m.switchBrowseToData()
	case "e":
		return m.openContextualEdit()
	case "E":
		return m.openContextualEditEmpty()
	case "c":
		return m.copyNamedText(m.dataSourceLabel(), m.currentTableName())
	}
	return m, nil
}

func (m Model) updateBrowseSchema(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "j", "down", "k", "up":
		var cmd tea.Cmd
		m.schemaTable, cmd = m.schemaTable.Update(msg)
		return m, cmd
	case "v":
		m.openSchemaInspect()
		return m, nil
	case "enter":
		return m.switchBrowseToData()
	case "e":
		return m.openContextualEdit()
	case "E":
		return m.openContextualEditEmpty()
	}
	return m, nil
}

func (m Model) updateBrowseData(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "j", "down", "k", "up":
		var cmd tea.Cmd
		m.browseDataTable, cmd = m.browseDataTable.Update(msg)
		return m, cmd
	case "h", "left":
		m.shiftBrowseColumns(-1)
		return m, nil
	case "l", "right":
		m.shiftBrowseColumns(1)
		return m, nil
	case "enter":
		m.browseView = browseViewSchema
		m.syncTableFocus()
		return m, nil
	case "e":
		return m.openContextualEdit()
	case "E":
		return m.openContextualEditEmpty()
	case "v":
		m.openBrowseDataInspect()
		return m, nil
	case "c":
		return m.copyNamedText("row", m.currentBrowseRowJSON())
	}
	return m, nil
}

func (m Model) switchBrowseToData() (tea.Model, tea.Cmd) {
	m.browseView = browseViewData
	m.focus = panelRight
	m.syncTableFocus()
	table := m.currentTableName()
	if table == "" || m.activeDB == nil {
		return m, nil
	}
	if m.browseDataTableName == table && m.browseData != nil {
		return m, nil
	}
	m.loading = true
	return m, m.loadBrowseData(table)
}

func (m *Model) openBrowseDataInspect() {
	if m.browseData == nil || len(m.browseData.Rows) == 0 {
		return
	}
	cursor := m.browseDataTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.browseData.Rows) {
		cursor = len(m.browseData.Rows) - 1
	}
	m.inspectTitle = "Data row"
	m.inspectLines, m.inspectCopy = m.renderRowInspect(m.browseData, cursor)
	m.inspectScroll = 0
	m.showInspect = true
}

func (m Model) currentBrowseRowJSON() string {
	if m.browseData == nil || len(m.browseData.Rows) == 0 {
		return ""
	}
	cursor := m.browseDataTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.browseData.Rows) {
		cursor = len(m.browseData.Rows) - 1
	}
	data, err := json.MarshalIndent(rowObject(m.browseData, m.browseData.Rows[cursor]), "", "  ")
	if err != nil {
		return ""
	}
	return string(data)
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
		case "ctrl+e":
			m.openQueryPicker("Examples", m.queryExamplePickerItems())
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
		case "ctrl+u":
			m.openQueryPicker("Saved Queries", m.querySavedPickerItems())
			return m, nil
		case "ctrl+g":
			return m.openOllamaGen()
		case "ctrl+l":
			m.clearCurrentQuery()
			return m, nil
		case "ctrl+s":
			return m.saveCurrentQuery()
		case "tab":
			if ok, cmd := m.openCompletionForCursor(false); ok {
				return m, cmd
			}
			if strings.TrimSpace(m.queryInput.Value()) == "" {
				m.openQueryPicker("Templates", m.queryHelperPickerItems())
				return m, nil
			}
			m.openCompletionForCursor(true)
		case "esc":
			m.queryFocus = false
			m.focus = panelRight
			m.queryInput.Blur()
			m.syncTableFocus()
			return m, nil
		}
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		prefetchCmd := m.prefetchInferredSchema()
		if m.shouldAutoTriggerCompletion(msg) {
			_, refreshCmd := m.refreshCompletionPicker(false)
			return m, tea.Batch(cmd, prefetchCmd, refreshCmd)
		}
		if prefetchCmd != nil {
			return m, tea.Batch(cmd, prefetchCmd)
		}
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
	case "x":
		m.openQueryPicker("Examples", m.queryExamplePickerItems())
	case "c":
		return m.copyNamedText("query", m.queryInput.Value())
	case "C":
		m.openQueryPicker("Copy Query As", m.queryCopyPickerItems())
	case "g":
		return m.openOllamaGen()
	case "u":
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

func queryEditorShortcutWhileCompletionOpen(msg tea.KeyMsg) bool {
	switch msg.String() {
	case "ctrl+r", "ctrl+o", "ctrl+e", "ctrl+p", "ctrl+n", "ctrl+y", "ctrl+t", "ctrl+g", "ctrl+u", "ctrl+l", "ctrl+s":
		return true
	default:
		return false
	}
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
		return m.openContextualEdit()
	case "E":
		return m.openContextualEditEmpty()
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
	{label: "SELECT with filter + order", template: "SELECT *\nFROM \"{table}\"\nWHERE \"column\" = ''\nORDER BY \"column\" DESC\nLIMIT 50;"},
	{label: "Aggregate count by column", template: "SELECT \"column\", COUNT(*) AS count\nFROM \"{table}\"\nGROUP BY \"column\"\nORDER BY count DESC\nLIMIT 20;"},
	{label: "INSERT row", template: "INSERT INTO \"{table}\" (\"column\")\nVALUES ('');"},
	{label: "UPDATE by key", template: "UPDATE \"{table}\"\nSET \"column\" = ''\nWHERE \"column\" = '';"},
	{label: "DELETE by key", template: "DELETE FROM \"{table}\"\nWHERE \"column\" = '';"},
	{label: "JOIN starter", template: "SELECT a.*, b.*\nFROM \"{table}\" a\nJOIN \"other_table\" b ON a.\"id\" = b.\"id\"\nLIMIT 50;"},
	{label: "CTE starter", template: "WITH recent AS (\n  SELECT *\n  FROM \"{table}\"\n  ORDER BY \"created_at\" DESC\n  LIMIT 50\n)\nSELECT *\nFROM recent;"},
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
	{label: "Find all (first 100)", template: `db.{table}.find({})`},
	{label: "Find with filter", template: `db.{table}.find({"status":"active"})`},
	{label: "Find by _id (ObjectID)", template: `db.{table}.findOne({"_id":{"$oid":"000000000000000000000000"}})`},
	{label: "Count all", template: `db.{table}.countDocuments({})`},
	{label: "Count by filter", template: `db.{table}.countDocuments({"status":"active"})`},
	{label: "Insert document", template: `db.{table}.insertOne({"name":"example","active":true})`},
	{label: "Update one", template: `db.{table}.updateOne({"_id":{"$oid":"000000000000000000000000"}},{"$set":{"active":false}})`},
	{label: "Update many", template: `db.{table}.updateMany({"status":"pending"},{"$set":{"status":"archived"}})`},
	{label: "Delete one", template: `db.{table}.deleteOne({"_id":{"$oid":"000000000000000000000000"}})`},
	{label: "Delete many", template: `db.{table}.deleteMany({"active":false})`},
	{label: "Aggregate top-N by field", template: `db.{table}.aggregate([{"$group":{"_id":"$field","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":25}])`},
	{label: "Aggregate recent + project", template: `db.{table}.aggregate([{"$sort":{"_id":-1}},{"$limit":50},{"$project":{"name":1,"created_at":1}}])`},
}

var sqlQueryExamples = []queryHelper{
	{label: "Read: select top rows", template: "SELECT *\nFROM \"{table}\"\nLIMIT 50;"},
	{label: "Filter: exact match", template: "SELECT *\nFROM \"{table}\"\nWHERE \"email\" = 'alice@example.com'\nLIMIT 20;"},
	{label: "Filter: contains text", template: "SELECT *\nFROM \"{table}\"\nWHERE \"name\" LIKE '%alice%'\nLIMIT 20;"},
	{label: "Sort: recent first", template: "SELECT *\nFROM \"{table}\"\nORDER BY \"created_at\" DESC\nLIMIT 50;"},
	{label: "Aggregate: count by field", template: "SELECT \"status\", COUNT(*) AS count\nFROM \"{table}\"\nGROUP BY \"status\"\nORDER BY count DESC\nLIMIT 20;"},
	{label: "Write: update rows", template: "UPDATE \"{table}\"\nSET \"status\" = 'archived'\nWHERE \"status\" = 'pending';"},
	{label: "Write: delete rows", template: "DELETE FROM \"{table}\"\nWHERE \"deleted_at\" IS NOT NULL;"},
}

var postgresQueryExamples = []queryHelper{
	{label: "Postgres: explain analyze", template: "EXPLAIN ANALYZE\nSELECT *\nFROM \"{table}\"\nLIMIT 50;"},
	{label: "Postgres: recent active queries", template: `SELECT pid, now() - query_start AS duration, state, query FROM pg_stat_activity WHERE state <> 'idle' AND query_start IS NOT NULL ORDER BY query_start ASC;`},
	{label: "Postgres: table size", template: `SELECT pg_size_pretty(pg_total_relation_size('"public"."{table}"')) AS total_size;`},
}

var sqliteQueryExamples = []queryHelper{
	{label: "SQLite: table info", template: `PRAGMA table_info("{table}");`},
	{label: "SQLite: list indexes", template: `PRAGMA index_list("{table}");`},
	{label: "SQLite: schema SQL", template: `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{table}';`},
}

var mongoQueryExamples = []queryHelper{
	{label: "Read: find top documents", template: `db.{table}.find({})`},
	{label: "Filter: exact field match", template: `db.{table}.find({"status":"active"})`},
	{label: "Filter: nested operator", template: `db.{table}.find({"age":{"$gte":18}})`},
	{label: "Filter: regex", template: `db.{table}.find({"email":{"$regex":"@example.com"}})`},
	{label: "Aggregate: group and count", template: `db.{table}.aggregate([{"$group":{"_id":"$status","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}])`},
	{label: "Write: update many", template: `db.{table}.updateMany({"status":"pending"},{"$set":{"status":"archived"}})`},
	{label: "Write: delete many", template: `db.{table}.deleteMany({"deleted_at":{"$exists":true}})`},
	{label: "Reference: lookup by ObjectID", template: `db.{table}.findOne({"_id":{"$oid":"000000000000000000000000"}})`},
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
	m.newConnEditIdx = -1
	m.newConnFocus = newConnFocusName
	m.newConnTypeCur = 0
	for i := range m.newConnInputs {
		m.newConnInputs[i].SetValue("")
	}
	m.updateDSNPlaceholder()
	m.syncNewConnFocus()
}

func (m *Model) openEditConnForm(idx int) {
	if idx < 0 || idx >= len(m.cfg.Connections) {
		return
	}
	conn := m.cfg.Connections[idx]
	m.showNewConn = true
	m.newConnEditIdx = idx
	m.newConnFocus = newConnFocusName
	m.newConnTypeCur = indexOfString(dbTypes, conn.Type)
	m.newConnInputs[fieldName].SetValue(conn.Name)
	m.newConnInputs[fieldDSN].SetValue(conn.DSN)
	m.updateDSNPlaceholder()
	m.syncNewConnFocus()
}

func (m Model) updateNewConn(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.showNewConn = false
		m.newConnEditIdx = -1
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

	status := "connection saved: " + name
	if m.newConnEditIdx >= 0 {
		if !m.cfg.UpdateConnection(m.newConnEditIdx, name, dbType, dsn) {
			m.setStatus("failed to update connection")
			return m, nil
		}
		m.connCursor = m.newConnEditIdx
		status = "connection updated: " + name
	} else {
		m.cfg.AddConnection(name, dbType, dsn)
		m.connCursor = len(m.cfg.Connections) - 1
	}
	if err := m.cfg.Save(); err != nil {
		m.setStatus("failed to save config: " + err.Error())
		return m, nil
	}
	m.showNewConn = false
	m.newConnEditIdx = -1
	m.setStatus(status)
	return m, nil
}

func (m Model) navigateBack() (tea.Model, tea.Cmd) {
	switch m.activeTab {
	case tabResults:
		m.activeTab = tabQuery
		m.focus = panelRight
		m.queryFocus = true
		m.queryInput.Focus()
	case tabQuery:
		m.activeTab = tabBrowse
		m.focus = panelLeft
		m.queryFocus = false
		m.queryInput.Blur()
	case tabBrowse:
		m.activeTab = tabConnections
		m.focus = panelLeft
		m.queryFocus = false
		m.queryInput.Blur()
	case tabConnections:
		if m.activeDB != nil {
			m.activeDB.Close()
		}
		return m, tea.Quit
	default:
		m.activeTab = tabConnections
		m.focus = panelLeft
	}
	m.syncTableFocus()
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
	m.showColumnPicker = false
	m.showQueryPicker = false
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
	m.browseData = nil
	m.browseDataTableName = ""
	m.browseColOffset = 0
	m.browseVisibleColumn = 0
	m.browseDataTable.SetRows(nil)
	m.loading = m.activeTab == tabBrowse
	cmds := []tea.Cmd{m.loadSchema(m.tables[m.tableCursor])}
	if m.browseView == browseViewData {
		cmds = append(cmds, m.loadBrowseData(m.tables[m.tableCursor]))
	}
	return m, tea.Batch(cmds...)
}

func (m Model) textInputCapturesKeypress() bool {
	return m.activeTab == tabQuery && m.queryFocus
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

// prefetchInferredSchema inspects the current query text, figures out which
// collection/table it targets, and kicks off a background schema load if that
// schema isn't already cached or currently displayed. Runs on every keystroke
// in the Query editor so switching `db.comments.find(...)` -> `db.users.find(...)`
// populates the `users` schema before the user even reaches the `{`.
func (m *Model) prefetchInferredSchema() tea.Cmd {
	if m.activeDB == nil {
		return nil
	}
	queryText := m.queryInput.Value()
	inferred := strings.TrimSpace(extractSelectTable(queryText))
	if inferred == "" {
		inferred = strings.TrimSpace(extractTableFromQuery(queryText))
	}
	if inferred == "" {
		return nil
	}
	// Already the left-panel schema — nothing to do.
	if m.tableSchema != nil && strings.EqualFold(m.tableSchema.Name, inferred) {
		return nil
	}
	key := schemaCacheKey(m.activeConnIdx, inferred)
	if m.schemaCache[key] != nil {
		return nil
	}
	// Only prefetch if the inferred name actually matches a known table — avoid
	// firing requests for half-typed names like "us" in `db.us`.
	known := false
	for _, name := range m.tables {
		if strings.EqualFold(name, inferred) {
			known = true
			break
		}
	}
	if !known {
		return nil
	}
	return m.loadSchemaForCache(inferred)
}

// loadSchemaForCache fetches a schema without updating the left-panel tableSchema.
// Used when autocomplete needs fields for a different collection than the one
// currently displayed in the browse pane. The reqID is deliberately left stale
// so the schemaLoadedMsg handler only caches the result and refreshes the picker.
func (m *Model) loadSchemaForCache(table string) tea.Cmd {
	if m.activeDB == nil {
		return nil
	}
	if m.schemaPending == nil {
		m.schemaPending = make(map[string]bool)
	}
	key := schemaCacheKey(m.activeConnIdx, table)
	if m.schemaPending[key] {
		return nil
	}
	m.schemaPending[key] = true
	d := m.activeDB
	return func() tea.Msg {
		schema, err := d.GetTableSchema(table)
		// reqID=-1 is intentionally stale so the handler only updates the cache.
		return schemaLoadedMsg{reqID: -1, table: table, schema: schema, err: err}
	}
}

func (m *Model) loadColumnValues(table, column string) tea.Cmd {
	if m.activeDB == nil || table == "" || column == "" {
		return nil
	}
	key := columnValueKey(m.activeConnIdx, table, column)
	if m.columnValuePending[key] {
		return nil
	}
	m.columnValuePending[key] = true
	m.valuesReqID++
	reqID := m.valuesReqID
	connIdx := m.activeConnIdx
	d := m.activeDB
	query := columnValuesQuery(d.Type(), table, column)
	return func() tea.Msg {
		if query == "" {
			return columnValuesMsg{reqID: reqID, connIdx: connIdx, table: table, column: column}
		}
		result, err := d.RunQuery(query)
		if err != nil {
			return columnValuesMsg{reqID: reqID, connIdx: connIdx, table: table, column: column, err: err}
		}
		values := make([]string, 0, len(result.Rows))
		for _, row := range result.Rows {
			if len(row) == 0 {
				continue
			}
			v := strings.TrimSpace(row[0])
			if v == "" {
				continue
			}
			values = append(values, v)
		}
		return columnValuesMsg{reqID: reqID, connIdx: connIdx, table: table, column: column, values: values}
	}
}

func columnValueKey(connIdx int, table, column string) string {
	return fmt.Sprintf("%d|%s|%s", connIdx, table, column)
}

func schemaCacheKey(connIdx int, table string) string {
	return fmt.Sprintf("%d|%s", connIdx, table)
}

func columnValuesQuery(dbType, table, column string) string {
	switch dbType {
	case "sqlite", "postgres":
		return fmt.Sprintf(`SELECT DISTINCT %q FROM %q WHERE %q IS NOT NULL ORDER BY %q LIMIT 20`, column, table, column, column)
	case "mongo":
		field := strings.ReplaceAll(column, `"`, `\"`)
		return fmt.Sprintf(`db.%s.aggregate([{"$match":{"%s":{"$exists":true}}},{"$group":{"_id":"$%s"}},{"$limit":20}])`, table, field, field)
	default:
		return ""
	}
}

func runQueryCmd(reqID int, d db.DB, query string) tea.Cmd {
	return func() tea.Msg {
		result, err := d.RunQuery(query)
		return queryDoneMsg{reqID: reqID, query: query, result: result, err: err}
	}
}

func runAutoRefreshCmd(reqID int, d db.DB, query string) tea.Cmd {
	return func() tea.Msg {
		result, err := d.RunQuery(query)
		return queryDoneMsg{reqID: reqID, query: query, result: result, err: err, autoRefresh: true}
	}
}

func (m *Model) setStatus(msg string) {
	m.statusMsg = msg
	m.statusExpiry = time.Now().Add(6 * time.Second)
}

func defaultQueryForTable(active db.DB, table string) string {
	if active != nil && active.Type() == "mongo" {
		return fmt.Sprintf("db.%s.find({})", table)
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
	m.syncTableFocus()
	return m, nil
}

func (m Model) openContextualEdit() (tea.Model, tea.Cmd) {
	return m.openContextualEditPrefill(true)
}

func (m Model) openContextualEditEmpty() (tea.Model, tea.Cmd) {
	return m.openContextualEditPrefill(false)
}

func (m Model) openContextualEditPrefill(prefill bool) (tea.Model, tea.Cmd) {
	if m.activeDB == nil {
		return m, nil
	}

	tableName := ""
	focusedCol := ""
	var rowCols []string
	var rowValues []string

	switch {
	case m.activeTab == tabBrowse && m.browseView == browseViewData && m.browseData != nil:
		tableName = m.currentTableName()
		rowCols = m.browseData.Columns
		cursor := m.browseDataTable.Cursor()
		if cursor >= 0 && cursor < len(m.browseData.Rows) {
			rowValues = m.browseData.Rows[cursor]
		}
		focusedCol = m.focusedBrowseColumn()

	case m.activeTab == tabBrowse && m.browseView == browseViewSchema:
		tableName = m.currentTableName()
		if m.tableSchema != nil && len(m.tableSchema.Columns) > 0 {
			cursor := m.schemaTable.Cursor()
			if cursor >= 0 && cursor < len(m.tableSchema.Columns) {
				focusedCol = m.tableSchema.Columns[cursor].Name
			}
		}

	case m.activeTab == tabResults && m.focus == panelRight && m.queryResult != nil && m.querySourceTable != "":
		tableName = m.querySourceTable
		rowCols = m.queryResult.Columns
		cursor := m.resultTable.Cursor()
		if cursor >= 0 && cursor < len(m.queryResult.Rows) {
			rowValues = m.queryResult.Rows[cursor]
		}
		focusedCol = m.focusedResultColumn()

	default:
		return m.openDefaultQueryEditor()
	}

	if tableName == "" {
		return m.openDefaultQueryEditor()
	}

	query := m.buildContextualUpdate(tableName, focusedCol, rowCols, rowValues, prefill)
	m.queryInput.SetValue(query)
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	// Position cursor inside empty quotes when value is blank
	if idx := strings.Index(query, "= ''"); idx >= 0 {
		m.focusCursorAtIndex(idx + 3)
	} else if idx := strings.Index(query, `:""`); idx >= 0 {
		m.focusCursorAtIndex(idx + 2)
	} else {
		m.queryInput.CursorEnd()
	}
	m.syncTableFocus()
	m.setStatus("edit value and ctrl+r to run")
	return m, nil
}

func (m Model) focusedBrowseColumn() string {
	if m.browseData == nil || len(m.browseData.Columns) == 0 {
		return ""
	}
	colIdx := m.browseColOffset
	if colIdx < 0 {
		colIdx = 0
	}
	if colIdx >= len(m.browseData.Columns) {
		colIdx = len(m.browseData.Columns) - 1
	}
	return m.browseData.Columns[colIdx]
}

func (m Model) focusedResultColumn() string {
	if m.queryResult == nil || len(m.queryResult.Columns) == 0 {
		return ""
	}
	colIdx := m.resultColOffset
	if colIdx < 0 {
		colIdx = 0
	}
	if colIdx >= len(m.queryResult.Columns) {
		colIdx = len(m.queryResult.Columns) - 1
	}
	return m.queryResult.Columns[colIdx]
}

func (m Model) buildContextualUpdate(tableName, focusedCol string, rowCols, rowValues []string, prefill bool) string {
	isMongo := m.activeDB != nil && m.activeDB.Type() == "mongo"
	pkCol := m.primaryKeyColumn()
	if pkCol == "" {
		pkCol = m.preferredFilterColumn()
	}
	if pkCol == "" && len(rowCols) > 0 {
		pkCol = rowCols[0]
	}
	if isMongo {
		pkCol = "_id"
	}

	if focusedCol == "" && len(rowCols) > 0 {
		focusedCol = rowCols[0]
		if focusedCol == pkCol && len(rowCols) > 1 {
			focusedCol = rowCols[1]
		}
	}
	if focusedCol == "" {
		focusedCol = "column"
	}

	// Resolve PK value from row data
	pkValue := ""
	if len(rowCols) > 0 && len(rowValues) > 0 {
		for i, col := range rowCols {
			if col == pkCol && i < len(rowValues) {
				pkValue = rowValues[i]
				break
			}
		}
	}

	// Resolve current value for the focused column (only when pre-filling)
	currentVal := ""
	if prefill {
		for i, col := range rowCols {
			if col == focusedCol && i < len(rowValues) {
				currentVal = rowValues[i]
				break
			}
		}
	}

	if isMongo {
		filter := `{}`
		if pkValue != "" {
			// Use the actual _id value; try to detect ObjectID format
			if len(pkValue) == 24 && isHexString(pkValue) {
				filter = fmt.Sprintf(`{"_id":{"$oid":"%s"}}`, pkValue)
			} else {
				filter = fmt.Sprintf(`{"_id":"%s"}`, pkValue)
			}
		}
		// Use raw JSON for JSON/null/bool values, quoted string otherwise
		setVal := strconv.Quote(currentVal)
		trimmedVal := strings.TrimSpace(currentVal)
		if trimmedVal == "null" || trimmedVal == "true" || trimmedVal == "false" ||
			((strings.HasPrefix(trimmedVal, "{") || strings.HasPrefix(trimmedVal, "[")) && json.Valid([]byte(trimmedVal))) {
			setVal = trimmedVal
		}
		return fmt.Sprintf(`db.%s.updateOne(%s,{"$set":{"%s":%s}})`, tableName, filter, focusedCol, setVal)
	}

	// SQL path
	whereClause := ""
	if pkValue != "" {
		whereClause = fmt.Sprintf(`WHERE %q = '%s'`, pkCol, escapeSQLString(pkValue))
	} else if pkCol != "" {
		whereClause = fmt.Sprintf(`WHERE %q = ''`, pkCol)
	} else {
		whereClause = "WHERE 1 = 1"
	}

	setExpr := fmt.Sprintf(`%q = '%s'`, focusedCol, escapeSQLString(currentVal))
	if len(currentVal) > 40 || strings.Contains(currentVal, "\n") {
		return fmt.Sprintf("UPDATE %q\nSET %s\n%s;", tableName, setExpr, whereClause)
	}
	return fmt.Sprintf(`UPDATE %q SET %s %s;`, tableName, setExpr, whereClause)
}

func isHexString(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// extractSelectTable parses the primary table name from a SELECT query's FROM clause.
func extractSelectTable(query string) string {
	upper := strings.ToUpper(query)
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return ""
	}
	rest := strings.TrimSpace(query[fromIdx+6:])
	end := strings.IndexAny(rest, " \t\n,();")
	if end < 0 {
		end = len(rest)
	}
	return unquoteIdentifier(rest[:end])
}

// queryInferredTable returns the table most relevant to the current query editor context:
// the FROM-clause table if parseable, the write-query target table, otherwise the left-panel cursor table.
func (m Model) queryInferredTable() string {
	if table := extractSelectTable(m.queryInput.Value()); table != "" {
		return table
	}
	if table := extractTableFromQuery(m.queryInput.Value()); table != "" {
		return table
	}
	return m.currentTableName()
}

// extractTableFromQuery parses the target table/collection from supported SQL and Mongo commands.
func extractTableFromQuery(query string) string {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)
	words := strings.Fields(q)
	// Standard MongoDB shell syntax: db.collection.method(...)
	if strings.HasPrefix(q, "db.") {
		rest := q[3:]
		if dot := strings.Index(rest, "."); dot > 0 {
			return rest[:dot]
		}
		return ""
	}
	if len(words) < 2 {
		return ""
	}
	// Internal mongo command format: find/count/aggregate/insert/update/delete <collection> ...
	switch strings.ToLower(words[0]) {
	case "find", "count", "aggregate", "agg", "insert", "update", "delete":
		return words[1]
	}
	// SQL
	switch {
	case strings.HasPrefix(upper, "UPDATE"):
		return unquoteIdentifier(words[1])
	case strings.HasPrefix(upper, "INSERT INTO") && len(words) >= 3:
		return unquoteIdentifier(words[2])
	case strings.HasPrefix(upper, "DELETE FROM") && len(words) >= 3:
		return unquoteIdentifier(words[2])
	}
	return ""
}

// targetedSelectFromWriteQuery builds a SELECT that shows the affected row(s) after an UPDATE/DELETE.
// For UPDATE/DELETE with a WHERE clause, it reuses that clause for a focused SELECT.
func targetedSelectFromWriteQuery(active db.DB, query string) string {
	table := extractTableFromQuery(query)
	if table == "" {
		return ""
	}
	if active != nil && active.Type() == "mongo" {
		if filter := extractMongoFilter(query); filter != "" {
			return fmt.Sprintf("db.%s.findOne(%s)", table, filter)
		}
		return defaultQueryForTable(active, table)
	}
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)
	if strings.HasPrefix(upper, "UPDATE") || strings.HasPrefix(upper, "DELETE") {
		whereIdx := strings.Index(upper, "WHERE")
		if whereIdx >= 0 {
			whereClause := strings.TrimSuffix(strings.TrimSpace(q[whereIdx:]), ";")
			return fmt.Sprintf("SELECT * FROM %q %s LIMIT 1;", table, whereClause)
		}
	}
	return defaultQueryForTable(active, table)
}

// extractMongoFilter pulls the filter JSON from a mongo shell query.
// Works for both db.coll.updateOne({filter},...) and internal format.
// Returns the first JSON object in the query string.
func extractMongoFilter(query string) string {
	q := strings.TrimSpace(query)
	start := strings.Index(q, "{")
	if start < 0 {
		return ""
	}
	// Walk balanced braces to find the end of the filter object
	depth := 0
	inStr := false
	escape := false
	for i := start; i < len(q); i++ {
		c := q[i]
		if escape {
			escape = false
			continue
		}
		if c == '\\' && inStr {
			escape = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 {
				return q[start : i+1]
			}
		}
	}
	return ""
}

func unquoteIdentifier(s string) string {
	s = strings.TrimSuffix(strings.TrimPrefix(s, `"`), `"`)
	s = strings.TrimSuffix(strings.TrimPrefix(s, "`"), "`")
	return s
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
	m.openResultsTab()
	m.loading = true
	m.queryErr = ""
	m.queryResult = nil
	m.querySourceTable = table
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
				queryHelper{label: "Use current collection", template: fmt.Sprintf("db.%s.find({})", table)},
				queryHelper{label: "Count current collection", template: fmt.Sprintf("db.%s.countDocuments({})", table)},
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

func (m Model) exampleItems() []queryHelper {
	table := ""
	if len(m.tables) > 0 && m.tableCursor >= 0 && m.tableCursor < len(m.tables) {
		table = m.tables[m.tableCursor]
	}

	dbType := ""
	if m.activeDB != nil {
		dbType = m.activeDB.Type()
	}

	switch dbType {
	case "mongo":
		items := []queryHelper{{label: "Reference: list collections", template: "collections"}}
		if table != "" {
			items = append(items, queryHelper{label: "Reference: use current collection", template: fmt.Sprintf("db.%s.find({})", table)})
		}
		items = append(items, materializeHelpers(mongoQueryExamples, table)...)
		return items
	case "postgres":
		items := materializeHelpers(sqlQueryExamples, table)
		items = append(items, materializeHelpers(postgresQueryExamples, table)...)
		return items
	case "sqlite":
		items := materializeHelpers(sqlQueryExamples, table)
		items = append(items, materializeHelpers(sqliteQueryExamples, table)...)
		return items
	default:
		return materializeHelpers(sqlQueryExamples, table)
	}
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
	browseRight := m.focus == panelRight && !m.queryFocus && m.activeTab == tabBrowse
	if browseRight && m.browseView == browseViewSchema {
		m.schemaTable.Focus()
	} else {
		m.schemaTable.Blur()
	}
	if browseRight && m.browseView == browseViewData {
		m.browseDataTable.Focus()
	} else {
		m.browseDataTable.Blur()
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

	m.browseDataTable.SetWidth(contentW)
	m.browseDataTable.SetHeight(schemaHeight)

	m.syncSchemaTable()
	m.syncBrowseDataTable()
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

func (m *Model) syncBrowseDataTable() {
	w := max(12, m.tableViewportWidth())
	if m.browseData == nil || len(m.browseData.Columns) == 0 {
		m.browseDataTable.SetRows(nil)
		m.browseDataTable.SetColumns(nil)
		m.browseVisibleColumn = 0
		return
	}

	start, cols := visibleResultColumns(m.browseData, w, m.browseColOffset)
	m.browseColOffset = start
	m.browseVisibleColumn = len(cols)

	rows := make([]table.Row, 0, len(m.browseData.Rows))
	for _, row := range m.browseData.Rows {
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

	m.browseDataTable.SetRows(nil)
	m.browseDataTable.SetColumns(cols)
	m.browseDataTable.SetRows(rows)
	if len(rows) == 0 {
		m.browseDataTable.SetCursor(0)
		return
	}
	cursor := m.browseDataTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(rows) {
		cursor = len(rows) - 1
	}
	m.browseDataTable.SetCursor(cursor)
}

func (m *Model) shiftBrowseColumns(delta int) {
	if m.browseData == nil || len(m.browseData.Columns) == 0 {
		return
	}
	next := m.browseColOffset + delta
	if next < 0 {
		next = 0
	}
	if next >= len(m.browseData.Columns) {
		next = len(m.browseData.Columns) - 1
	}
	if next == m.browseColOffset {
		return
	}
	m.browseColOffset = next
	m.syncBrowseDataTable()
}

func (m *Model) loadBrowseData(table string) tea.Cmd {
	if m.activeDB == nil || table == "" {
		return nil
	}
	m.browseDataReqID++
	reqID := m.browseDataReqID
	d := m.activeDB
	query := defaultQueryForTable(d, table)
	return func() tea.Msg {
		result, err := d.RunQuery(query)
		return browseDataLoadedMsg{reqID: reqID, table: table, result: result, err: err}
	}
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
	if m.queryInput.Value() == "" {
		m.setStatus("query editor already clear")
		return
	}
	m.showColumnPicker = false
	m.columnPickerValueMode = false
	m.columnPickerValuePrefix = ""
	m.columnPickerValueCursor = 0
	m.queryInput.SetValue("")
	m.queryInput.Focus()
	m.queryHistoryIdx = -1
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

func indexOfString(items []string, needle string) int {
	for i, item := range items {
		if item == needle {
			return i
		}
	}
	return 0
}

type queryColumnContext struct {
	start        int
	end          int
	title        string
	multi        bool
	includeStar  bool
	fallback     string
	filterSuffix string // appended after insertion (e.g. WHERE column -> " = ''")
	valueMode    bool   // value completion: typing filters list, does NOT insert into query
	valueCol     string // column whose values are being completed (value mode)
	valueTable   string // table whose values are being completed (value mode)
}

var sqlPredicateOperatorPattern = regexp.MustCompile(`(?i)(?:"[^"]+"|[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)\s*$`)

func (m *Model) openCompletionForCursor(manual bool) (bool, tea.Cmd) {
	ctx, items, cmd, ok := m.completionItemsForCursor()
	if !ok {
		if manual {
			m.setStatus("completion unavailable here")
		}
		return false, nil
	}
	if len(items) == 0 {
		// If an async load (schema or values) is in flight, show a "loading"
		// row so the picker opens and refreshes when the data arrives.
		if cmd != nil {
			token := currentTokenValue([]rune(m.queryInput.Value())[ctx.start:ctx.end])
			items = []columnPickerItem{{label: "loading…", detail: "fetching schema / values", insertText: token}}
		} else {
			if manual {
				m.setStatus("no completion items available")
			}
			return false, nil
		}
	}
	m.columnPickerTitle = ctx.title
	m.columnPickerItems = items
	m.columnPickerCursor = 0
	m.columnPickerMulti = ctx.multi
	m.columnPickerStart = ctx.start
	m.columnPickerEnd = ctx.end
	m.columnPickerFallback = ctx.fallback
	m.columnPickerValueMode = ctx.valueMode
	m.columnPickerValuePrefix = ""
	m.columnPickerValueCursor = 0
	m.columnPickerValueCol = ctx.valueCol
	m.columnPickerValueTable = ctx.valueTable
	m.showColumnPicker = true
	return true, cmd
}

func (m *Model) refreshCompletionPicker(manual bool) (bool, tea.Cmd) {
	if !m.showColumnPicker {
		return m.openCompletionForCursor(manual)
	}
	// In value mode, only refresh the item list from cache and keep the current
	// replacement range stable.
	if m.columnPickerValueMode {
		m.refilterValuePicker()
		return true, nil
	}
	ctx, items, cmd, ok := m.completionItemsForCursor()
	if !ok {
		m.showColumnPicker = false
		return false, nil
	}
	if len(items) == 0 {
		m.showColumnPicker = false
		return false, nil
	}
	m.columnPickerTitle = ctx.title
	m.columnPickerItems = items
	m.columnPickerCursor = 0
	m.columnPickerMulti = false
	m.columnPickerStart = ctx.start
	m.columnPickerEnd = ctx.end
	m.columnPickerFallback = ctx.fallback
	m.showColumnPicker = true
	return true, cmd
}

// refilterValuePicker re-fetches cached values and applies the current prefix
// filter, without changing the picker's insertion range (start/end).
func (m *Model) refilterValuePicker() {
	if m.columnPickerValueCol == "" {
		return
	}
	m.columnPickerValueCursor = clampInt(m.columnPickerValueCursor, 0, len([]rune(m.columnPickerValuePrefix)))
	key := columnValueKey(m.activeConnIdx, m.columnPickerValueTable, m.columnPickerValueCol)
	values := m.columnValueCache[key]
	items := make([]columnPickerItem, 0, len(values))
	for _, v := range values {
		items = append(items, columnPickerItem{label: v, detail: m.columnPickerValueCol, insertText: v})
	}
	items = rankCompletionItems(m.columnPickerValuePrefix, items)
	if len(items) == 0 {
		if values == nil {
			items = []columnPickerItem{{label: "loading…", detail: "fetching samples", insertText: m.columnPickerValuePrefix}}
		} else {
			items = []columnPickerItem{{label: "(no samples)", detail: m.columnPickerValueCol, insertText: m.columnPickerValuePrefix}}
		}
	}
	m.columnPickerItems = items
	if m.columnPickerCursor >= len(items) {
		m.columnPickerCursor = 0
	}
}

func (m *Model) completionItemsForCursor() (queryColumnContext, []columnPickerItem, tea.Cmd, bool) {
	if m.activeDB == nil {
		return queryColumnContext{}, nil, nil, false
	}
	before := strings.ToLower(m.queryBeforeCursor())
	if m.activeDB.Type() == "mongo" {
		ctx, items, cmd, ok := m.mongoCompletionContext()
		return ctx, items, cmd, ok
	}
	if inInsertValuesList(before) {
		return queryColumnContext{}, nil, nil, false
	}
	if ctx, items, cmd, ok := m.queryValueCompletionContext(); ok {
		return ctx, items, cmd, true
	}
	if cursorInsideString(m.queryInput.Value(), m.queryCursorIndex()) {
		return queryColumnContext{}, nil, nil, false
	}
	if ctx, items, ok := m.sqlOperatorCompletionContext(); ok {
		return ctx, items, nil, true
	}
	if ctx, ok := m.queryColumnContext(); ok {
		// If the schema doesn't match the table in the query, trigger a cache-only
		// load so autocomplete gets fresh fields without clobbering the left-panel
		// tableSchema the user has pinned to a different collection.
		var schemaCmd tea.Cmd
		inferred := m.queryInferredTable()
		if inferred != "" {
			if m.tableSchema == nil || !strings.EqualFold(m.tableSchema.Name, inferred) {
				if m.schemaCache[schemaCacheKey(m.activeConnIdx, inferred)] == nil {
					schemaCmd = m.loadSchemaForCache(inferred)
				}
			}
		}
		items := m.columnPickerCandidates(ctx)
		if len(items) == 0 && schemaCmd != nil {
			items = []columnPickerItem{{label: "loading fields…", detail: inferred, insertText: ""}}
		}
		return ctx, items, schemaCmd, true
	}
	if ctx, ok := m.queryTableContext(); ok {
		return ctx, m.tablePickerCandidates(ctx), nil, true
	}
	if ctx, items, ok := m.sqlClauseValueCompletionContext(); ok {
		return ctx, items, nil, true
	}
	ctx, items, ok := m.sqlKeywordCompletionContext()
	return ctx, items, nil, ok
}

func (m Model) sqlOperatorCompletionContext() (queryColumnContext, []columnPickerItem, bool) {
	query := []rune(m.queryInput.Value())
	cursor := m.queryCursorIndex()
	start, end := queryTokenBounds(query, cursor)
	beforeToken := strings.ToLower(string(query[:start]))
	prefix := strings.ToLower(currentTokenValue(query[start:end]))

	if !inWhereClause(beforeToken) && !inUpdateSetList(beforeToken) {
		return queryColumnContext{}, nil, false
	}
	if prefix != "" {
		for _, r := range prefix {
			if !unicode.IsLetter(r) && r != '!' && r != '<' && r != '>' && r != '=' {
				return queryColumnContext{}, nil, false
			}
		}
	}
	if !sqlPredicateOperatorPattern.MatchString(beforeToken) {
		return queryColumnContext{}, nil, false
	}

	items := []columnPickerItem{
		{label: "=", detail: "equals", insertText: "= ''"},
		{label: "!=", detail: "not equal", insertText: "!= ''"},
		{label: ">", detail: "greater than", insertText: "> 0"},
		{label: ">=", detail: "greater or equal", insertText: ">= 0"},
		{label: "<", detail: "less than", insertText: "< 0"},
		{label: "<=", detail: "less or equal", insertText: "<= 0"},
		{label: "LIKE", detail: "pattern match", insertText: "LIKE '%%'"},
		{label: "IN", detail: "set membership", insertText: "IN ('')"},
		{label: "IS NULL", detail: "null check", insertText: "IS NULL"},
		{label: "IS NOT NULL", detail: "not null check", insertText: "IS NOT NULL"},
	}
	return queryColumnContext{
		start:    start,
		end:      end,
		title:    "Operator",
		fallback: currentTokenValue(query[start:end]),
	}, rankCompletionItems(prefix, items), true
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
	if trimmed != "" && token == "" {
		return queryColumnContext{}, nil, false
	}
	if token != "" {
		for _, r := range token {
			if !unicode.IsLetter(r) {
				return queryColumnContext{}, nil, false
			}
		}
	}
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
	if trimmed == "" {
		for _, name := range m.tables {
			items = append(items, columnPickerItem{
				label:      name,
				detail:     m.dataSourceLabel(),
				insertText: fmt.Sprintf("SELECT * FROM %s LIMIT 50;", quoteIdentifierForDB(m.activeDB, name)),
			})
		}
	}
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
	before := strings.ToLower(m.queryBeforeCursor())
	items := []columnPickerItem{
		{label: "SELECT starter", detail: "query", insertText: fmt.Sprintf("SELECT *\nFROM %s\nLIMIT 50;", quoteIdentifierForDB(m.activeDB, table))},
		{label: "INSERT starter", detail: "query", insertText: fmt.Sprintf("INSERT INTO %s (%s)\nVALUES ('');", quoteIdentifierForDB(m.activeDB, table), quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "UPDATE starter", detail: "query", insertText: fmt.Sprintf("UPDATE %s\nSET %s = ''\nWHERE %s = '';", quoteIdentifierForDB(m.activeDB, table), quoteIdentifierForDB(m.activeDB, filterCol), quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "DELETE starter", detail: "query", insertText: fmt.Sprintf("DELETE FROM %s\nWHERE %s = '';", quoteIdentifierForDB(m.activeDB, table), quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "JOIN clause", detail: "query", insertText: fmt.Sprintf("JOIN %s ON ", quoteIdentifierForDB(m.activeDB, table))},
		{label: "WHERE clause", detail: "query", insertText: fmt.Sprintf("WHERE %s = ''", quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "GROUP BY clause", detail: "query", insertText: fmt.Sprintf("GROUP BY %s", quoteIdentifierForDB(m.activeDB, filterCol))},
		{label: "ORDER BY clause", detail: "query", insertText: fmt.Sprintf("ORDER BY %s DESC", quoteIdentifierForDB(m.activeDB, sortCol))},
		{label: "LIMIT clause", detail: "query", insertText: "LIMIT 50"},
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
	if inSelectList(before) {
		items = append(items,
			columnPickerItem{label: "COUNT(*)", detail: "aggregate", insertText: "COUNT(*)"},
			columnPickerItem{label: "COUNT(col)", detail: "aggregate", insertText: fmt.Sprintf("COUNT(%s)", quoteIdentifierForDB(m.activeDB, filterCol))},
			columnPickerItem{label: "SUM(col)", detail: "aggregate", insertText: fmt.Sprintf("SUM(%s)", quoteIdentifierForDB(m.activeDB, filterCol))},
			columnPickerItem{label: "AVG(col)", detail: "aggregate", insertText: fmt.Sprintf("AVG(%s)", quoteIdentifierForDB(m.activeDB, filterCol))},
			columnPickerItem{label: "MIN(col)", detail: "aggregate", insertText: fmt.Sprintf("MIN(%s)", quoteIdentifierForDB(m.activeDB, sortCol))},
			columnPickerItem{label: "MAX(col)", detail: "aggregate", insertText: fmt.Sprintf("MAX(%s)", quoteIdentifierForDB(m.activeDB, sortCol))},
			columnPickerItem{label: "DISTINCT col", detail: "modifier", insertText: fmt.Sprintf("DISTINCT %s", quoteIdentifierForDB(m.activeDB, filterCol))},
		)
	}
	return items
}

func (m Model) queryBeforeCursor() string {
	q := []rune(m.queryInput.Value())
	c := m.queryCursorIndex()
	if c > len(q) {
		c = len(q)
	}
	if c < 0 {
		c = 0
	}
	return string(q[:c])
}

// cursorInsideString reports whether the cursor sits inside an unclosed
// single- or double-quoted string literal. Used to suppress keyword/column
// completion inside literal values (which are handled by the value-completion
// path when preceded by a comparison).
func cursorInsideString(query string, cursor int) bool {
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
				// Handle SQL-style escape ('')
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

// findOpenQuote returns the position of the unclosed quote rune before the
// cursor, the quote char, and whether one was found.
func findOpenQuote(runes []rune, cursor int) (int, rune, bool) {
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

// columnBeforeValueLiteral returns the column name implied by the operator
// sequence immediately preceding the opening quote, or "" if none.
func columnBeforeValueLiteral(before string) string {
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

func (m *Model) queryValueCompletionContext() (queryColumnContext, []columnPickerItem, tea.Cmd, bool) {
	runes := []rune(m.queryInput.Value())
	cursor := m.queryCursorIndex()
	openIdx, _, ok := findOpenQuote(runes, cursor)
	if !ok {
		return queryColumnContext{}, nil, nil, false
	}
	before := string(runes[:openIdx])
	col := columnBeforeValueLiteral(before)
	if col == "" {
		return queryColumnContext{}, nil, nil, false
	}
	table := m.queryInferredTable()
	if table == "" {
		return queryColumnContext{}, nil, nil, false
	}
	key := columnValueKey(m.activeConnIdx, table, col)
	values, cached := m.columnValueCache[key]
	var cmd tea.Cmd
	if !cached {
		cmd = m.loadColumnValues(table, col)
	}
	prefix := strings.ToLower(string(runes[openIdx+1 : cursor]))
	items := make([]columnPickerItem, 0, len(values)+1)
	for _, v := range values {
		items = append(items, columnPickerItem{label: v, detail: col, insertText: v})
	}
	items = rankCompletionItems(prefix, items)
	if !cached && len(items) == 0 {
		items = append(items, columnPickerItem{label: "loading…", detail: "fetching samples", insertText: prefix})
	}
	if len(items) == 0 {
		items = append(items, columnPickerItem{label: "(no samples)", detail: col, insertText: prefix})
	}
	ctx := queryColumnContext{
		start:      openIdx + 1,
		end:        cursor,
		title:      "Values for " + col,
		fallback:   prefix,
		valueMode:  true,
		valueCol:   col,
		valueTable: m.queryInferredTable(),
	}
	return ctx, items, cmd, true
}

func (m Model) schemaHasColumn(name string) bool {
	lower := strings.ToLower(name)
	if m.tableSchema != nil {
		for _, col := range m.tableSchema.Columns {
			if strings.ToLower(col.Name) == lower {
				return true
			}
		}
	}
	// Fall back to result columns when schema isn't loaded.
	if m.queryResult != nil {
		for _, col := range m.queryResult.Columns {
			if strings.ToLower(col) == lower {
				return true
			}
		}
	}
	return false
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
			start:        start,
			end:          end,
			title:        "Filter Column",
			multi:        false,
			fallback:     currentTokenValue(query[start:end]),
			filterSuffix: " = ''",
		}, true
	case inOrderByList(beforeToken):
		if orderByWantsDirection(beforeToken) {
			return queryColumnContext{}, false
		}
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

func (m Model) sqlClauseValueCompletionContext() (queryColumnContext, []columnPickerItem, bool) {
	query := []rune(m.queryInput.Value())
	cursor := m.queryCursorIndex()
	start, end := queryTokenBounds(query, cursor)
	beforeToken := strings.ToLower(string(query[:start]))
	prefix := strings.ToLower(currentTokenValue(query[start:end]))

	if inLimitValue(beforeToken) {
		items := []columnPickerItem{
			{label: "10", detail: "limit", insertText: "10"},
			{label: "20", detail: "limit", insertText: "20"},
			{label: "50", detail: "limit", insertText: "50"},
			{label: "100", detail: "limit", insertText: "100"},
			{label: "200", detail: "limit", insertText: "200"},
			{label: "500", detail: "limit", insertText: "500"},
			{label: "1000", detail: "limit", insertText: "1000"},
		}
		return queryColumnContext{
			start:    start,
			end:      end,
			title:    "Limit",
			fallback: currentTokenValue(query[start:end]),
		}, rankCompletionItems(prefix, items), true
	}
	if orderByWantsDirection(beforeToken) {
		items := []columnPickerItem{
			{label: "ASC", detail: "direction", insertText: "ASC"},
			{label: "DESC", detail: "direction", insertText: "DESC"},
		}
		return queryColumnContext{
			start:    start,
			end:      end,
			title:    "Order Direction",
			fallback: strings.ToUpper(currentTokenValue(query[start:end])),
		}, rankCompletionItems(prefix, items), true
	}
	return queryColumnContext{}, nil, false
}

func (m Model) mongoCompletionContext() (queryColumnContext, []columnPickerItem, tea.Cmd, bool) {
	query := m.queryInput.Value()
	cursor := m.queryCursorIndex()
	isShell := strings.HasPrefix(strings.TrimSpace(query), "db.")
	tokens := mongoTokens(query)
	tokenIdx := 0
	start := cursor
	end := cursor
	found := false
	// Find which token the cursor is inside. In shell format, tokens may not be
	// position-ordered (method token comes first semantically but later in the string),
	// so check all tokens instead of short-circuiting.
	for i, token := range tokens {
		if cursor >= token.start && cursor <= token.end {
			tokenIdx = i
			start = token.start
			end = token.end
			found = true
			break
		}
	}
	if !found {
		// Cursor is between or after tokens — find the next token by position
		bestIdx := -1
		for i, token := range tokens {
			if token.start > cursor {
				if bestIdx < 0 || token.start < tokens[bestIdx].start {
					bestIdx = i
				}
			}
		}
		if bestIdx >= 0 {
			tokenIdx = bestIdx
		} else if len(tokens) > 0 {
			tokenIdx = len(tokens)
		}
	}
	// Prefix is only the text from token start up to cursor — so when the cursor
	// is at the start of a token, no prefix filter is applied.
	prefix := ""
	if start < cursor {
		prefix = mongoCompletionPrefix(query[start:cursor])
	}
	command := ""
	if len(tokens) > 0 {
		command = strings.ToLower(tokens[0].value)
	}
	ctx := queryColumnContext{start: start, end: end, title: "Mongo Commands", fallback: strings.TrimSpace(query[start:end])}
	var cmd tea.Cmd
	var items []columnPickerItem
	switch tokenIdx {
	case 0:
		ctx.title = "Mongo Commands"
		// In shell format, prefer the collection already typed in the query
		// (e.g. typing `db.users.find` + tab should keep `users`, not overwrite
		// with the left-panel cursor's collection).
		table := ""
		if isShell {
			table = mongoCollectionFromTokens(tokens)
		}
		if table == "" {
			table = fallbackTableName(m.currentTableName())
		}
		items = mongoCommandItemsForCollection(table)
		// In shell format, command items replace the entire query
		if isShell {
			ctx.start = 0
			ctx.end = len([]rune(query))
			ctx.fallback = query
		}
	case 1:
		ctx.title = "Collections"
		items = m.mongoCollectionItems()
		if isShell {
			// In shell format, collection is embedded in db.COLLECTION.method(...)
			// The token positions already map to the right place, but we need to
			// rebuild the surrounding shell expression when inserting.
			// Use method-aware collection items that rebuild the query.
			items = m.mongoShellCollectionItems(command, tokens)
			ctx.start = 0
			ctx.end = len([]rune(query))
			ctx.fallback = query
		}
	default:
		ctx.title = "Mongo Arguments"
		collection := mongoCollectionFromTokens(tokens)
		tokenText := strings.TrimSpace(query[start:end])
		items, cmd = m.mongoArgumentItems(command, collection, tokenIdx, tokenText, cursor-start)
		if strings.HasPrefix(tokenText, "{") && len(tokenText) > 1 {
			if valueStart, valueEnd, ok := mongoJSONValueBounds(tokenText, cursor-start); ok {
				ctx.title = "Mongo Value"
				ctx.start = start + valueStart
				ctx.end = start + valueEnd
				ctx.fallback = string([]rune(query)[ctx.start:ctx.end])
			}
		}
		if command == "find" && tokenIdx >= 4 {
			ctx.title = "Sort"
		}
		if strings.HasPrefix(tokenText, "{") && len(tokenText) > 1 {
			if field, ok := mongoJSONComparisonFieldContext(tokenText, cursor-start); ok && field != "" {
				if opStart, opEnd, _, opOK := mongoJSONOperatorPairBounds(tokenText, cursor-start); opOK {
					ctx.title = "Mongo Operator"
					ctx.start = start + opStart
					ctx.end = start + opEnd
					ctx.fallback = strings.Trim(string([]rune(query)[ctx.start:ctx.end]), `"`)
					prefix = ""
				}
			} else if keyStart, keyEnd, ok := mongoJSONKeyBounds(tokenText, cursor-start); ok {
				ctx.title = "Mongo Field"
				ctx.start = start + keyStart
				ctx.end = start + keyEnd
				ctx.fallback = strings.Trim(string([]rune(query)[ctx.start:ctx.end]), `"`)
			}
		}
	}
	items = rankCompletionItems(prefix, items)
	if len(items) == 0 {
		return queryColumnContext{}, nil, cmd, false
	}
	return ctx, items, cmd, true
}

// mongoShellCollectionItems produces collection picker items that rebuild
// the shell expression with the new collection name.
func (m Model) mongoShellCollectionItems(method string, tokens []mongoToken) []columnPickerItem {
	if method == "" {
		method = "find"
	}
	// Map internal method names to shell method names
	shellMethod := method
	switch method {
	case "find":
		shellMethod = "find"
	case "aggregate":
		shellMethod = "aggregate"
	case "insert":
		shellMethod = "insertOne"
	case "update":
		shellMethod = "updateOne"
	case "delete":
		shellMethod = "deleteOne"
	case "count":
		shellMethod = "countDocuments"
	}
	// Reconstruct the args portion from existing tokens (if any)
	args := ""
	if len(tokens) > 2 {
		argParts := make([]string, 0, len(tokens)-2)
		for _, t := range tokens[2:] {
			argParts = append(argParts, t.value)
		}
		args = strings.Join(argParts, ", ")
	}
	if args == "" {
		args = "{}"
	}

	items := make([]columnPickerItem, 0, len(m.tables))
	for _, name := range m.tables {
		items = append(items, columnPickerItem{
			label:      name,
			detail:     "collection",
			insertText: fmt.Sprintf("db.%s.%s(%s)", name, shellMethod, args),
		})
	}
	return items
}

func mongoCollectionFromTokens(tokens []mongoToken) string {
	if len(tokens) < 2 {
		return ""
	}
	return strings.Trim(tokens[1].value, `"'`)
}

type mongoToken struct {
	value string
	start int
	end   int
}

func mongoTokens(query string) []mongoToken {
	runes := []rune(query)

	// Detect shell format: db.collection.method(args...)
	// Convert to virtual tokens: [method, collection, arg0, arg1, ...]
	// with positions mapping back to the original query.
	if shellTokens, ok := mongoShellTokens(runes); ok {
		return shellTokens
	}

	// Fallback: whitespace-separated internal format (find collection {filter} ...)
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

// mongoShellTokens parses db.collection.method(arg0, arg1, ...) into virtual tokens:
//   token 0 = method name (e.g. "find")
//   token 1 = collection name (e.g. "users")
//   token 2+ = comma-separated arguments inside parens
// Positions map back to the original query so completion insertion works correctly.
func mongoShellTokens(runes []rune) ([]mongoToken, bool) {
	s := string(runes)
	if !strings.HasPrefix(s, "db.") {
		return nil, false
	}
	rest := s[3:]
	dotIdx := strings.Index(rest, ".")
	if dotIdx < 0 {
		// Typing "db." or "db.use" — still in collection position.
		// token 0 = empty command placeholder, token 1 = what's after "db."
		return []mongoToken{
			{value: "", start: 0, end: 0},
			{value: rest, start: 3, end: len(runes)},
		}, true
	}
	collection := rest[:dotIdx]
	afterCollection := rest[dotIdx+1:]

	parenIdx := strings.Index(afterCollection, "(")
	if parenIdx < 0 {
		// Typing method name, e.g. "db.users.fi"
		methodStart := 3 + dotIdx + 1
		return []mongoToken{
			{value: afterCollection, start: methodStart, end: len(runes)},
			{value: collection, start: 3, end: 3 + dotIdx},
		}, true
	}
	method := strings.ToLower(afterCollection[:parenIdx])
	methodStart := 3 + dotIdx + 1
	// The args portion starts after '(' and ends before the final ')'
	argsStart := methodStart + parenIdx + 1 // index in runes right after '('
	tokens := []mongoToken{
		{value: method, start: methodStart, end: methodStart + parenIdx},
		{value: collection, start: 3, end: 3 + dotIdx},
	}

	// Split args by top-level commas (not inside {}, [], or strings)
	argsRunes := runes[argsStart:]
	// Strip trailing ')' if present
	argsEnd := len(argsRunes)
	if argsEnd > 0 && argsRunes[argsEnd-1] == ')' {
		argsEnd--
	}
	argsRunes = argsRunes[:argsEnd]

	if len(argsRunes) == 0 {
		// Empty parens: db.users.find() — add a virtual empty arg token at cursor
		tokens = append(tokens, mongoToken{value: "", start: argsStart, end: argsStart})
		return tokens, true
	}

	depth := 0
	inStr := false
	escape := false
	argStart := 0
	for i := 0; i < len(argsRunes); i++ {
		ch := argsRunes[i]
		if escape {
			escape = false
			continue
		}
		if ch == '\\' && inStr {
			escape = true
			continue
		}
		if ch == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		switch ch {
		case '{', '[', '(':
			depth++
		case '}', ']', ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				argVal := strings.TrimSpace(string(argsRunes[argStart:i]))
				tokens = append(tokens, mongoToken{
					value: argVal,
					start: argsStart + argStart,
					end:   argsStart + i,
				})
				argStart = i + 1
			}
		}
	}
	// Last arg
	argVal := strings.TrimSpace(string(argsRunes[argStart:]))
	tokens = append(tokens, mongoToken{
		value: argVal,
		start: argsStart + argStart,
		end:   argsStart + len(argsRunes),
	})

	return tokens, true
}

func (m Model) mongoCommandItems() []columnPickerItem {
	return mongoCommandItemsForCollection(fallbackTableName(m.currentTableName()))
}

func mongoCommandItemsForCollection(table string) []columnPickerItem {
	if table == "" {
		table = "collection"
	}
	return []columnPickerItem{
		{label: "find", detail: "query", insertText: fmt.Sprintf("db.%s.find({})", table)},
		{label: "aggregate", detail: "query", insertText: fmt.Sprintf("db.%s.aggregate([])", table)},
		{label: "insertOne", detail: "query", insertText: fmt.Sprintf("db.%s.insertOne({})", table)},
		{label: "updateOne", detail: "query", insertText: fmt.Sprintf("db.%s.updateOne({},{\"$set\":{}})", table)},
		{label: "updateMany", detail: "query", insertText: fmt.Sprintf("db.%s.updateMany({},{\"$set\":{}})", table)},
		{label: "deleteOne", detail: "query", insertText: fmt.Sprintf("db.%s.deleteOne({})", table)},
		{label: "deleteMany", detail: "query", insertText: fmt.Sprintf("db.%s.deleteMany({})", table)},
		{label: "countDocuments", detail: "query", insertText: fmt.Sprintf("db.%s.countDocuments({})", table)},
		{label: "collections", detail: "command", insertText: "collections"},
	}
}

func (m Model) mongoCollectionItems() []columnPickerItem {
	items := make([]columnPickerItem, 0, len(m.tables))
	for _, name := range m.tables {
		items = append(items, columnPickerItem{label: name, detail: "collection", insertText: name})
	}
	return items
}

func (m Model) mongoArgumentItems(command, collection string, tokenIdx int, token string, tokenCursor int) ([]columnPickerItem, tea.Cmd) {
	schemaFields, schemaTypes, schemaCmd := m.mongoSchemaFields(collection)
	filterField := fallbackColumnName(preferredFilterColumnFromFields(schemaFields))
	groupField := fallbackColumnName(preferredCategoricalColumnFromFields(schemaFields, schemaTypes))

	filterItems := func() []columnPickerItem {
		items := []columnPickerItem{{label: "empty filter", detail: "json", insertText: "{}"}}
		items = append(items, mongoJSONTopLevelOperatorItems()...)
		if len(schemaFields) == 0 {
			items = append(items, columnPickerItem{label: "field filter", detail: "json", insertText: fmt.Sprintf(`{"%s":%s}`, filterField, mongoPlaceholderForType(""))})
			return items
		}
		for _, field := range schemaFields {
			items = append(items, columnPickerItem{
				label:      field,
				detail:     "field",
				insertText: fmt.Sprintf(`{"%s":%s}`, field, mongoPlaceholderForType(schemaTypes[field])),
			})
		}
		return items
	}
	sortItems := func() []columnPickerItem {
		if len(schemaFields) == 0 {
			return []columnPickerItem{
				{label: "recent sort", detail: "json", insertText: fmt.Sprintf(`{"%s":-1}`, groupField)},
				{label: "ascending sort", detail: "json", insertText: fmt.Sprintf(`{"%s":1}`, filterField)},
			}
		}
		items := make([]columnPickerItem, 0, len(schemaFields)*2)
		for _, field := range schemaFields {
			items = append(items,
				columnPickerItem{label: field + " desc", detail: "sort", insertText: fmt.Sprintf(`{"%s":-1}`, field)},
				columnPickerItem{label: field + " asc", detail: "sort", insertText: fmt.Sprintf(`{"%s":1}`, field)},
			)
		}
		return items
	}

	trimmedToken := strings.TrimSpace(token)
	if strings.HasPrefix(trimmedToken, "{") && len(trimmedToken) > 1 {
		if valueItems, valueCmd, ok := m.mongoJSONValueItems(collection, token, tokenCursor); ok {
			return valueItems, valueCmd
		}
		if keyItems, ok := m.mongoJSONObjectItems(command, schemaFields, schemaTypes, token, tokenCursor); ok {
			// If we have no schema fields yet but a load is pending, show a hint
			// at the top so the user knows fields are on their way.
			if len(schemaFields) == 0 && schemaCmd != nil {
				hint := columnPickerItem{label: "loading fields…", detail: collection, insertText: ""}
				keyItems = append([]columnPickerItem{hint}, keyItems...)
			}
			return keyItems, schemaCmd
		}
	}

	switch command {
	case "find":
		if tokenIdx == 2 {
			return filterItems(), schemaCmd
		}
		if tokenIdx == 3 {
			if strings.HasPrefix(strings.TrimSpace(token), "{") {
				return sortItems(), schemaCmd
			}
			return []columnPickerItem{
				{label: "limit 20", detail: "limit", insertText: "20"},
				{label: "limit 50", detail: "limit", insertText: "50"},
				{label: "limit 100", detail: "limit", insertText: "100"},
			}, nil
		}
		return sortItems(), schemaCmd
	case "aggregate":
		return []columnPickerItem{
			{label: "match + limit", detail: "pipeline", insertText: fmt.Sprintf(`[{"$match":{"%s":%s}},{"$limit":20}]`, filterField, mongoPlaceholderForType(schemaTypes[filterField]))},
			{label: "group + count", detail: "pipeline", insertText: fmt.Sprintf(`[{"$group":{"_id":"$%s","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}]`, groupField)},
		}, nil
	case "insert":
		return []columnPickerItem{
			{label: "document", detail: "json", insertText: fmt.Sprintf(`{"%s":%s}`, filterField, mongoPlaceholderForType(schemaTypes[filterField]))},
		}, nil
	case "update":
		if tokenIdx == 2 {
			return filterItems(), schemaCmd
		}
		if tokenIdx == 3 {
			return []columnPickerItem{
				{label: "$set", detail: "json", insertText: fmt.Sprintf(`{"$set":{"%s":%s}}`, filterField, mongoPlaceholderForType(schemaTypes[filterField]))},
			}, nil
		}
		return []columnPickerItem{
			{label: "many", detail: "token", insertText: "many"},
		}, nil
	case "delete":
		if tokenIdx == 2 {
			return filterItems(), schemaCmd
		}
		return []columnPickerItem{
			{label: "many", detail: "token", insertText: "many"},
		}, nil
	case "count":
		return filterItems(), schemaCmd
	default:
		return nil, schemaCmd
	}
}

func (m Model) mongoSchemaFields(collection string) ([]string, map[string]string, tea.Cmd) {
	types := map[string]string{}
	if collection == "" {
		fields, fieldTypes := m.currentSchemaFieldNames()
		return fields, fieldTypes, nil
	}
	if m.tableSchema != nil && strings.EqualFold(m.tableSchema.Name, collection) {
		fields, fieldTypes := m.currentSchemaFieldNames()
		return fields, fieldTypes, nil
	}
	if cached := m.schemaCache[schemaCacheKey(m.activeConnIdx, collection)]; cached != nil {
		fields := make([]string, 0, len(cached.Columns))
		for _, col := range cached.Columns {
			fields = append(fields, col.Name)
			types[col.Name] = strings.ToLower(col.Type)
		}
		return fields, types, nil
	}
	return nil, types, m.loadSchemaForCache(collection)
}

func (m Model) currentSchemaFieldNames() ([]string, map[string]string) {
	if m.tableSchema == nil || len(m.tableSchema.Columns) == 0 {
		return nil, map[string]string{}
	}
	fields := make([]string, 0, len(m.tableSchema.Columns))
	types := make(map[string]string, len(m.tableSchema.Columns))
	for _, col := range m.tableSchema.Columns {
		fields = append(fields, col.Name)
		types[col.Name] = strings.ToLower(col.Type)
	}
	return fields, types
}

func mongoJSONFieldItems(fields []string, fieldTypes map[string]string, token string, cursor int) ([]columnPickerItem, bool) {
	if len(fields) == 0 {
		return nil, false
	}
	if !mongoLooksLikeFieldKeyContext(token, cursor) {
		return nil, false
	}
	items := make([]columnPickerItem, 0, len(fields))
	for _, field := range fields {
		items = append(items, columnPickerItem{
			label:      field,
			detail:     "field",
			insertText: fmt.Sprintf(`"%s":%s`, field, mongoPlaceholderForType(fieldTypes[field])),
		})
	}
	return items, true
}

func (m Model) mongoJSONObjectItems(command string, fields []string, fieldTypes map[string]string, token string, cursor int) ([]columnPickerItem, bool) {
	if items, ok := mongoJSONUpdateFieldItems(fields, fieldTypes, token, cursor); ok {
		return items, true
	}
	if field, ok := mongoJSONComparisonFieldContext(token, cursor); ok {
		return mongoJSONComparisonOperatorItems(field, fieldTypes[field], token, cursor), true
	}
	if !mongoLooksLikeFieldKeyContext(token, cursor) {
		return nil, false
	}
	items, _ := mongoJSONFieldItems(fields, fieldTypes, token, cursor)
	switch command {
	case "update":
		items = append(mongoJSONUpdateOperatorItems(), items...)
	default:
		items = append(items, mongoJSONTopLevelOperatorItems()...)
	}
	return items, len(items) > 0
}

func mongoJSONTopLevelOperatorItems() []columnPickerItem {
	return []columnPickerItem{
		{label: "$or", detail: "operator", insertText: `"$or":[{}]`},
		{label: "$and", detail: "operator", insertText: `"$and":[{}]`},
		{label: "$nor", detail: "operator", insertText: `"$nor":[{}]`},
		{label: "$expr", detail: "operator", insertText: `"$expr":{}`},
	}
}

func mongoJSONUpdateOperatorItems() []columnPickerItem {
	return []columnPickerItem{
		{label: "$set", detail: "operator", insertText: `"$set":{}`},
		{label: "$unset", detail: "operator", insertText: `"$unset":{"field":""}`},
		{label: "$inc", detail: "operator", insertText: `"$inc":{"field":1}`},
		{label: "$push", detail: "operator", insertText: `"$push":{"field":""}`},
		{label: "$pull", detail: "operator", insertText: `"$pull":{"field":""}`},
		{label: "$addToSet", detail: "operator", insertText: `"$addToSet":{"field":""}`},
	}
}

func mongoJSONComparisonOperatorItems(field, fieldType, token string, cursor int) []columnPickerItem {
	_, _, rawValue, preserveValue := mongoJSONOperatorPairBounds(token, cursor)
	operatorInsert := func(op, fallback string) string {
		if preserveValue {
			return fmt.Sprintf(`"%s":%s`, op, mongoTransformOperatorValue(op, rawValue, fieldType))
		}
		return fallback
	}
	value := mongoPlaceholderForType(fieldType)
	items := []columnPickerItem{
		{label: "$eq", detail: "operator", insertText: operatorInsert(`$eq`, fmt.Sprintf(`"$eq":%s`, value))},
		{label: "$ne", detail: "operator", insertText: operatorInsert(`$ne`, fmt.Sprintf(`"$ne":%s`, value))},
		{label: "$exists", detail: "operator", insertText: operatorInsert(`$exists`, `"$exists":true`)},
	}
	switch strings.ToLower(fieldType) {
	case "int", "uint", "float", "decimal", "number", "date", "datetime", "timestamp":
		items = append(items,
			columnPickerItem{label: "$gt", detail: "operator", insertText: operatorInsert(`$gt`, fmt.Sprintf(`"$gt":%s`, value))},
			columnPickerItem{label: "$gte", detail: "operator", insertText: operatorInsert(`$gte`, fmt.Sprintf(`"$gte":%s`, value))},
			columnPickerItem{label: "$lt", detail: "operator", insertText: operatorInsert(`$lt`, fmt.Sprintf(`"$lt":%s`, value))},
			columnPickerItem{label: "$lte", detail: "operator", insertText: operatorInsert(`$lte`, fmt.Sprintf(`"$lte":%s`, value))},
			columnPickerItem{label: "$in", detail: "operator", insertText: operatorInsert(`$in`, fmt.Sprintf(`"$in":[%s]`, value))},
			columnPickerItem{label: "$nin", detail: "operator", insertText: operatorInsert(`$nin`, fmt.Sprintf(`"$nin":[%s]`, value))},
		)
	case "string":
		items = append(items,
			columnPickerItem{label: "$regex", detail: "operator", insertText: operatorInsert(`$regex`, `"$regex":""`)},
			columnPickerItem{label: "$in", detail: "operator", insertText: operatorInsert(`$in`, `"$in":[""]`)},
		)
	}
	return items
}

func mongoJSONComparisonFieldContext(token string, cursor int) (string, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	match := regexp.MustCompile(`(?s)"([^"]+)"\s*:\s*\{\s*(?:"\$?[A-Za-z_]*)?$`).FindStringSubmatch(before)
	if len(match) != 2 {
		return "", false
	}
	if strings.HasPrefix(match[1], "$") {
		return "", false
	}
	return match[1], true
}

func mongoJSONOperatorBounds(token string, cursor int) (int, int, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	start := cursor
	for start > 0 {
		if runes[start-1] == '"' {
			break
		}
		if !(unicode.IsLetter(runes[start-1]) || runes[start-1] == '$' || runes[start-1] == '_') {
			return 0, 0, false
		}
		start--
	}
	if start >= len(runes) || runes[start] != '$' {
		if start+1 >= len(runes) || runes[start] != '"' || runes[start+1] != '$' {
			return 0, 0, false
		}
		start++
	}
	end := start
	for end < len(runes) && (unicode.IsLetter(runes[end]) || runes[end] == '$' || runes[end] == '_') {
		end++
	}
	if start == end {
		return 0, 0, false
	}
	return start - 1, min(len(runes), end+1), true
}

func mongoJSONOperatorPairBounds(token string, cursor int) (int, int, string, bool) {
	runes := []rune(token)
	start, end, ok := mongoJSONOperatorBounds(token, cursor)
	if !ok {
		return 0, 0, "", false
	}
	if end > len(runes) {
		end = len(runes)
	}
	i := end
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	if i >= len(runes) || runes[i] != ':' {
		return 0, 0, "", false
	}
	i++
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	if i >= len(runes) {
		return start, end, "", false
	}
	valueStart := i
	valueEnd := mongoJSONLiteralEnd(runes, valueStart)
	raw := strings.TrimSpace(string(runes[valueStart:valueEnd]))
	return start, valueEnd, raw, raw != ""
}

func mongoTransformOperatorValue(op, rawValue, fieldType string) string {
	raw := strings.TrimSpace(rawValue)
	if raw == "" {
		switch op {
		case "$exists":
			return "true"
		case "$in", "$nin":
			value := mongoPlaceholderForType(fieldType)
			return "[" + value + "]"
		default:
			return mongoPlaceholderForType(fieldType)
		}
	}
	switch op {
	case "$exists":
		if strings.EqualFold(raw, "true") || strings.EqualFold(raw, "false") {
			return strings.ToLower(raw)
		}
		return "true"
	case "$in", "$nin":
		if strings.HasPrefix(raw, "[") {
			return raw
		}
		return "[" + raw + "]"
	default:
		return raw
	}
}

func mongoJSONUpdateFieldItems(fields []string, fieldTypes map[string]string, token string, cursor int) ([]columnPickerItem, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	if !regexp.MustCompile(`(?s)"\$(?:set|unset|inc|push|pull|addToSet)"\s*:\s*\{\s*(?:"[^"]*)?$`).MatchString(before) {
		return nil, false
	}
	return mongoJSONFieldItems(fields, fieldTypes, token, cursor)
}

func mongoLooksLikeFieldKeyContext(token string, cursor int) bool {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	segment := strings.TrimSpace(string(runes[:cursor]))
	if segment == "" {
		return false
	}
	segment = strings.TrimRight(segment, " \t")
	if strings.HasSuffix(segment, "{") || strings.HasSuffix(segment, ",") || strings.HasSuffix(segment, `"`) {
		return true
	}
	if regexp.MustCompile(`(?s)[{,]\s*"[^"]*$`).MatchString(segment) {
		return true
	}
	return false
}

func mongoJSONKeyBounds(token string, cursor int) (int, int, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	idxs := regexp.MustCompile(`(?s)(^|[{,]\s*)("?[A-Za-z0-9_$]*)$`).FindStringSubmatchIndex(before)
	if len(idxs) != 6 {
		return 0, 0, false
	}
	start := idxs[4]
	end := cursor
	for end < len(runes) {
		r := runes[end]
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '$' {
			end++
			continue
		}
		if r == '"' {
			end++
		}
		break
	}
	return start, end, true
}

func (m Model) mongoJSONValueItems(collection, token string, cursor int) ([]columnPickerItem, tea.Cmd, bool) {
	if _, ok := mongoJSONComparisonFieldContext(token, cursor); ok {
		return nil, nil, false
	}
	col, prefix, ok := mongoFieldAndValuePrefix(token, cursor)
	if !ok || col == "" || strings.HasPrefix(col, "$") || strings.HasPrefix(strings.TrimSpace(prefix), "$") {
		return nil, nil, false
	}
	if collection == "" {
		return nil, nil, false
	}
	fieldType := m.mongoFieldType(collection, col)
	key := columnValueKey(m.activeConnIdx, collection, col)
	values, cached := m.columnValueCache[key]
	var cmd tea.Cmd
	if !cached {
		cmd = m.loadColumnValues(collection, col)
	}

	literals := mongoLiteralCandidates(fieldType)
	items := make([]columnPickerItem, 0, len(values)+len(literals)+1)
	for _, literal := range literals {
		items = append(items, columnPickerItem{
			label:      literal,
			detail:     col,
			insertText: literal,
		})
	}
	for _, v := range values {
		items = append(items, columnPickerItem{
			label:      v,
			detail:     col,
			insertText: mongoTypedJSONLiteral(fieldType, v),
		})
	}
	items = rankCompletionItems(strings.ToLower(prefix), items)
	if !cached && len(items) == 0 {
		items = append(items, columnPickerItem{label: "loading…", detail: "fetching samples", insertText: token})
	}
	if len(items) == 0 {
		items = append(items, columnPickerItem{label: "(no samples)", detail: col, insertText: token})
	}
	return items, cmd, true
}

func (m Model) mongoFieldType(collection, field string) string {
	if field == "" {
		return ""
	}
	if m.tableSchema != nil && strings.EqualFold(m.tableSchema.Name, collection) {
		for _, col := range m.tableSchema.Columns {
			if strings.EqualFold(col.Name, field) {
				return strings.ToLower(col.Type)
			}
		}
	}
	if cached := m.schemaCache[schemaCacheKey(m.activeConnIdx, collection)]; cached != nil {
		for _, col := range cached.Columns {
			if strings.EqualFold(col.Name, field) {
				return strings.ToLower(col.Type)
			}
		}
	}
	return ""
}

func mongoPlaceholderForType(fieldType string) string {
	switch strings.ToLower(fieldType) {
	case "objectid":
		return `{"$oid":"000000000000000000000000"}`
	case "date", "datetime", "timestamp":
		return `{"$date":"2026-01-01T00:00:00Z"}`
	case "array":
		return "[]"
	case "object", "document", "map", "mixed":
		return "{}"
	case "bool", "boolean", "int", "uint", "float", "decimal", "number", "null":
		return "null"
	default:
		return `""`
	}
}

func mongoTypedJSONLiteral(fieldType, raw string) string {
	trimmed := strings.TrimSpace(raw)
	kind := strings.ToLower(fieldType)

	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		if json.Valid([]byte(trimmed)) {
			return trimmed
		}
	}

	switch kind {
	case "objectid":
		if looksLikeObjectIDHex(trimmed) {
			return fmt.Sprintf(`{"$oid":"%s"}`, trimmed)
		}
	case "date", "datetime", "timestamp":
		if t, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
			return fmt.Sprintf(`{"$date":"%s"}`, t.UTC().Format(time.RFC3339Nano))
		}
		if t, err := time.Parse(time.RFC3339, trimmed); err == nil {
			return fmt.Sprintf(`{"$date":"%s"}`, t.UTC().Format(time.RFC3339))
		}
	case "array":
		if strings.HasPrefix(trimmed, "[") && json.Valid([]byte(trimmed)) {
			return trimmed
		}
	case "object", "document", "map":
		if strings.HasPrefix(trimmed, "{") && json.Valid([]byte(trimmed)) {
			return trimmed
		}
	}

	switch strings.ToLower(fieldType) {
	case "bool", "boolean":
		if strings.EqualFold(trimmed, "true") {
			return "true"
		}
		if strings.EqualFold(trimmed, "false") {
			return "false"
		}
	case "int", "uint", "float", "decimal", "number":
		if _, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return trimmed
		}
	case "null":
		if strings.EqualFold(trimmed, "null") || strings.EqualFold(trimmed, "NULL") {
			return "null"
		}
	}
	if strings.EqualFold(trimmed, "null") || strings.EqualFold(trimmed, "NULL") {
		return "null"
	}
	return strconv.Quote(raw)
}

func looksLikeObjectIDHex(s string) bool {
	if len(s) != 24 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

func mongoLiteralCandidates(fieldType string) []string {
	switch strings.ToLower(fieldType) {
	case "bool", "boolean":
		return []string{"true", "false", "null"}
	case "int", "uint", "float", "decimal", "number":
		return []string{"0", "1", "-1", "3.14", "null"}
	case "objectid":
		return []string{`{"$oid":"000000000000000000000000"}`, "null"}
	case "date", "datetime", "timestamp":
		return []string{`{"$date":"2026-01-01T00:00:00Z"}`, "null"}
	case "array":
		return []string{"[]", "[1,2,3]", "null"}
	case "object", "document", "map":
		return []string{"{}", `{"key":"value"}`, "null"}
	case "mixed":
		return []string{"true", "false", "0", "null", "{}", "[]"}
	default:
		return []string{"null"}
	}
}

func mongoFieldAndValuePrefix(token string, cursor int) (field string, valuePrefix string, ok bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	quoted := regexp.MustCompile(`"([^"]+)"\s*:\s*"([^"]*)$`).FindStringSubmatch(before)
	if len(quoted) == 3 {
		return quoted[1], quoted[2], true
	}
	bare := regexp.MustCompile(`"([^"]+)"\s*:\s*([^,\}\]\s]*)$`).FindStringSubmatch(before)
	if len(bare) == 3 {
		return bare[1], strings.TrimSpace(strings.TrimPrefix(bare[2], `"`)), true
	}
	return "", "", false
}

func mongoJSONValueBounds(token string, cursor int) (int, int, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	if idxs := regexp.MustCompile(`"([^"]+)"\s*:\s*"([^"]*)$`).FindStringSubmatchIndex(before); len(idxs) == 6 {
		contentStart := idxs[4]
		start := contentStart - 1
		end := cursor
		for end < len(runes) {
			if runes[end] == '"' && (end == 0 || runes[end-1] != '\\') {
				end++
				break
			}
			end++
		}
		return start, end, true
	}
	if idxs := regexp.MustCompile(`"([^"]+)"\s*:\s*([^,\}\]\s]*)$`).FindStringSubmatchIndex(before); len(idxs) == 6 {
		start := idxs[4]
		end := cursor
		for end < len(runes) && !strings.ContainsRune(",}]", runes[end]) && !unicode.IsSpace(runes[end]) {
			end++
		}
		return start, end, true
	}
	return 0, 0, false
}

func mongoJSONLiteralEnd(runes []rune, start int) int {
	if start >= len(runes) {
		return start
	}
	if runes[start] == '"' {
		end := start + 1
		for end < len(runes) {
			if runes[end] == '"' && runes[end-1] != '\\' {
				return end + 1
			}
			end++
		}
		return len(runes)
	}
	depthBrace := 0
	depthBracket := 0
	inQuote := false
	for end := start; end < len(runes); end++ {
		r := runes[end]
		if inQuote {
			if r == '"' && runes[end-1] != '\\' {
				inQuote = false
			}
			continue
		}
		switch r {
		case '"':
			inQuote = true
		case '{':
			depthBrace++
		case '}':
			if depthBrace == 0 && depthBracket == 0 {
				return end
			}
			if depthBrace > 0 {
				depthBrace--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case ',':
			if depthBrace == 0 && depthBracket == 0 {
				return end
			}
		}
	}
	return len(runes)
}

func mongoCompletionPrefix(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if idx := strings.LastIndex(token, `"`); idx >= 0 && idx+1 <= len(token) {
		return strings.ToLower(strings.TrimSpace(token[idx+1:]))
	}
	return strings.ToLower(strings.Trim(token, `"'`))
}

func (m Model) columnPickerCandidates(ctx queryColumnContext) []columnPickerItem {
	prefix := strings.ToLower(currentTokenValue([]rune(m.queryInput.Value())[ctx.start:ctx.end]))
	aliasPrefix := m.currentAliasPrefix(ctx)

	// Prefer schema for the query-inferred table; fall back to result columns.
	// Don't use schema from a different table — return empty so "loading…" shows instead.
	var colNames []struct{ name, typ string }
	inferred := m.queryInferredTable()
	if m.tableSchema != nil && (inferred == "" || strings.EqualFold(m.tableSchema.Name, inferred)) {
		for _, col := range m.tableSchema.Columns {
			colNames = append(colNames, struct{ name, typ string }{col.Name, col.Type})
		}
	} else if inferred != "" {
		if cached := m.schemaCache[schemaCacheKey(m.activeConnIdx, inferred)]; cached != nil {
			for _, col := range cached.Columns {
				colNames = append(colNames, struct{ name, typ string }{col.Name, col.Type})
			}
		}
	} else if m.tableSchema == nil {
		if m.queryResult != nil {
			for _, col := range m.queryResult.Columns {
				colNames = append(colNames, struct{ name, typ string }{col, ""})
			}
		}
	}

	items := make([]columnPickerItem, 0, len(colNames)+1)
	// Only offer '*' when we have actual column info; a lone '*' with no schema
	// isn't useful on its own.
	if ctx.includeStar && len(colNames) > 0 {
		items = append(items, columnPickerItem{label: "*", detail: "all", insertText: "*"})
	}
	for _, col := range colNames {
		items = append(items, columnPickerItem{
			label:      col.name,
			detail:     col.typ,
			insertText: m.columnInsertionValue(col.name, aliasPrefix),
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
	// Only append filterSuffix when no operator already follows the insertion
	// point, so contextual insertion doesn't duplicate an existing predicate.
	if ctx.filterSuffix != "" && !ctx.multi && len(parts) == 1 {
		queryAfter := strings.TrimLeft(string(query[ctx.end:]), " \t\n")
		if len(queryAfter) == 0 || !strings.ContainsAny(string(queryAfter[0]), "=!<>") {
			insert += ctx.filterSuffix
		}
	}

	insertRunes := []rune(insert)

	updated := string(query[:ctx.start]) + insert + string(query[ctx.end:])
	m.queryInput.SetValue(updated)
	m.queryInput.Focus()

	endPos := ctx.start + len(insertRunes)
	line, col := queryLineColForIndex([]rune(updated), endPos)
	setTextareaCursor(&m.queryInput, line, col)
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
		case strings.Contains(label, prefix), strings.Contains(insert, prefix):
			score = 1
		case fuzzyMatch(label, prefix), fuzzyMatch(insert, prefix):
			score = 2
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

func inLimitValue(before string) bool {
	limitIdx := lastKeyword(before, " limit ")
	if limitIdx < 0 {
		return false
	}
	after := before[limitIdx+len(" limit "):]
	for _, blocker := range []string{";", "\n"} {
		if strings.Contains(after, blocker) {
			return false
		}
	}
	return true
}

func orderByWantsDirection(before string) bool {
	orderIdx := lastKeyword(before, " order by ")
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

func inInsertValuesList(before string) bool {
	insertIdx := lastKeyword(before, "insert into ")
	valuesIdx := lastKeyword(before, " values")
	openParen := strings.LastIndex(before, "(")
	closeParen := strings.LastIndex(before, ")")
	return insertIdx >= 0 && valuesIdx > insertIdx && openParen > valuesIdx && openParen > closeParen
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
	for i, item := range m.queryPickerItems {
		if !item.sectionRow {
			m.queryPickerCursor = i
			break
		}
	}
	m.showQueryPicker = true
}

func (m Model) updateQueryPicker(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.showQueryPicker = false
		return m, nil
	case "j", "down":
		m.moveQueryPickerCursor(1)
	case "k", "up":
		m.moveQueryPickerCursor(-1)
	case "c":
		if item := m.currentQueryPickerItem(); item.value != "" {
			return m.copyNamedText("query", item.value)
		}
	case "enter":
		item := m.currentQueryPickerItem()
		if item.value == "" || item.sectionRow {
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
			m.syncTableFocus()
			m.setStatus("loaded into query editor")
			if ok, cmd := m.openCompletionForCursor(false); ok {
				return m, cmd
			}
		}
	}
	return m, nil
}

func (m *Model) moveQueryPickerCursor(delta int) {
	if len(m.queryPickerItems) == 0 || delta == 0 {
		return
	}
	next := m.queryPickerCursor
	for {
		next += delta
		if next < 0 || next >= len(m.queryPickerItems) {
			return
		}
		if !m.queryPickerItems[next].sectionRow {
			m.queryPickerCursor = next
			return
		}
	}
}

func (m Model) updateColumnPicker(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.showColumnPicker = false
		m.columnPickerValueMode = false
		m.columnPickerValuePrefix = ""
		m.columnPickerValueCursor = 0
		return m, nil
	case "tab":
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
		valueMode := m.columnPickerValueMode
		m.columnPickerValueMode = false
		m.columnPickerValuePrefix = ""
		m.columnPickerValueCursor = 0
		m.applyCompletionInsertion(queryColumnContext{
			start:    m.columnPickerStart,
			end:      m.columnPickerEnd,
			fallback: m.columnPickerFallback,
			multi:    m.columnPickerMulti,
		}, items)
		if !valueMode {
			m.setStatus("inserted completion")
		}
		return m, nil
	case "j", "down":
		if m.columnPickerCursor < len(m.columnPickerItems)-1 {
			m.columnPickerCursor++
		}
	case "k", "up":
		if m.columnPickerCursor > 0 {
			m.columnPickerCursor--
		}
	case "left", "ctrl+b":
		if m.columnPickerValueMode {
			m.columnPickerValueCursor = max(0, m.columnPickerValueCursor-1)
			return m, nil
		}
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		_, refreshCmd := m.refreshCompletionPicker(false)
		return m, tea.Batch(cmd, refreshCmd)
	case "right", "ctrl+f":
		if m.columnPickerValueMode {
			runes := []rune(m.columnPickerValuePrefix)
			m.columnPickerValueCursor = min(len(runes), m.columnPickerValueCursor+1)
			return m, nil
		}
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		_, refreshCmd := m.refreshCompletionPicker(false)
		return m, tea.Batch(cmd, refreshCmd)
	case "home":
		if m.columnPickerValueMode {
			m.columnPickerValueCursor = 0
			return m, nil
		}
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		_, refreshCmd := m.refreshCompletionPicker(false)
		return m, tea.Batch(cmd, refreshCmd)
	case "end":
		if m.columnPickerValueMode {
			m.columnPickerValueCursor = len([]rune(m.columnPickerValuePrefix))
			return m, nil
		}
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		_, refreshCmd := m.refreshCompletionPicker(false)
		return m, tea.Batch(cmd, refreshCmd)
	case " ":
		if len(m.columnPickerItems) == 0 {
			return m, nil
		}
		if m.columnPickerValueMode {
			m.editValueFilterPrefix(msg)
			m.refilterValuePicker()
			return m, nil
		}
		if !m.columnPickerMulti {
			var cmd tea.Cmd
			m.queryInput, cmd = m.queryInput.Update(msg)
			_, refreshCmd := m.refreshCompletionPicker(false)
			return m, tea.Batch(cmd, refreshCmd)
		}
		m.columnPickerItems[m.columnPickerCursor].selected = !m.columnPickerItems[m.columnPickerCursor].selected
	case "enter":
		var cmd tea.Cmd
		m.queryInput, cmd = m.queryInput.Update(msg)
		_, refreshCmd := m.refreshCompletionPicker(false)
		return m, tea.Batch(cmd, refreshCmd)
	default:
		if m.completionPickerCapturesTyping(msg) {
			if m.columnPickerValueMode {
				m.editValueFilterPrefix(msg)
				m.refilterValuePicker()
				return m, nil
			}
			var cmd tea.Cmd
			m.queryInput, cmd = m.queryInput.Update(msg)
			_, refreshCmd := m.refreshCompletionPicker(false)
			return m, tea.Batch(cmd, refreshCmd)
		}
	}
	return m, nil
}

func (m *Model) editValueFilterPrefix(msg tea.KeyMsg) {
	runes := []rune(m.columnPickerValuePrefix)
	cursor := clampInt(m.columnPickerValueCursor, 0, len(runes))

	switch msg.Type {
	case tea.KeyRunes:
		if len(msg.Runes) == 0 {
			return
		}
		insert := msg.Runes
		updated := append([]rune{}, runes[:cursor]...)
		updated = append(updated, insert...)
		updated = append(updated, runes[cursor:]...)
		m.columnPickerValuePrefix = string(updated)
		m.columnPickerValueCursor = cursor + len(insert)
	case tea.KeySpace:
		updated := append([]rune{}, runes[:cursor]...)
		updated = append(updated, ' ')
		updated = append(updated, runes[cursor:]...)
		m.columnPickerValuePrefix = string(updated)
		m.columnPickerValueCursor = cursor + 1
	case tea.KeyBackspace:
		if cursor == 0 {
			return
		}
		updated := append([]rune{}, runes[:cursor-1]...)
		updated = append(updated, runes[cursor:]...)
		m.columnPickerValuePrefix = string(updated)
		m.columnPickerValueCursor = cursor - 1
	case tea.KeyDelete:
		if cursor >= len(runes) {
			return
		}
		updated := append([]rune{}, runes[:cursor]...)
		updated = append(updated, runes[cursor+1:]...)
		m.columnPickerValuePrefix = string(updated)
		m.columnPickerValueCursor = cursor
	}
}

func (m Model) currentQueryPickerItem() queryPickerItem {
	if m.queryPickerCursor < 0 || m.queryPickerCursor >= len(m.queryPickerItems) {
		return queryPickerItem{}
	}
	return m.queryPickerItems[m.queryPickerCursor]
}

func (m Model) queryHistoryPickerItems() []queryPickerItem {
	items := make([]queryPickerItem, 0, len(m.queryHistory))
	for _, query := range m.queryHistory {
		items = append(items, queryPickerItem{
			label: truncate(compactInline(query), 100),
			value: query,
			kind:  "history",
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
		detail := ""
		if helper.kind == "prompt" {
			detail = ""
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

func (m Model) queryExamplePickerItems() []queryPickerItem {
	examples := m.exampleItems()
	items := make([]queryPickerItem, 0, len(examples)+4)
	lastSection := ""
	for _, example := range examples {
		if example.kind == "prompt" {
			continue
		}
		section := queryExampleSection(example.label)
		if section != "" && section != lastSection {
			items = append(items, queryPickerItem{
				label:      section,
				kind:       "example-section",
				sectionRow: true,
			})
			lastSection = section
		}
		items = append(items, queryPickerItem{
			label: example.label,
			value: example.template,
			kind:  "example",
		})
	}
	return items
}

func queryExampleSection(label string) string {
	head, _, ok := strings.Cut(label, ":")
	if !ok {
		return ""
	}
	return strings.TrimSpace(head)
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

func (m Model) shouldAutoTriggerCompletion(msg tea.KeyMsg) bool {
	switch msg.Type {
	case tea.KeyRunes, tea.KeySpace, tea.KeyBackspace, tea.KeyDelete:
		return true
	default:
		return false
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
	return m.renderRowInspect(m.queryResult, cursor)
}

func (m Model) renderRowInspect(result *db.QueryResult, cursor int) ([]string, string) {
	if result == nil || len(result.Rows) == 0 {
		return nil, ""
	}
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(result.Rows) {
		cursor = len(result.Rows) - 1
	}

	row := result.Rows[cursor]
	wrapW := max(40, min(96, m.width-14))
	viewLines := []string{dimStyle.Render(fmt.Sprintf("row %d", cursor+1)), "", textStyle.Render("{")}
	copyLines := []string{fmt.Sprintf("row %d", cursor+1), "", "{"}
	for i, col := range result.Columns {
		raw := ""
		if i < len(row) {
			raw = row[i]
		}
		parts := structuredValueLines(raw)
		suffix := ""
		if i < len(result.Columns)-1 {
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
			viewLines = append(viewLines, "    "+colorizeJSONLine(part)+dimStyle.Render(lineSuffix))
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
	return rowObject(m.queryResult, row)
}

func (m Model) resultRowObject(row []string) map[string]any {
	return rowObject(m.queryResult, row)
}

func rowObject(result *db.QueryResult, row []string) map[string]any {
	if result == nil {
		return nil
	}
	obj := make(map[string]any, len(result.Columns))
	for i, col := range result.Columns {
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
			queryHelper{label: "Find using schema fields", template: fmt.Sprintf("db.%s.find({})", table)},
			queryHelper{label: "Lookup by key field", template: fmt.Sprintf(`db.%s.find({"%s":"value"})`, table, lookupField)},
			queryHelper{label: "Recent documents", template: fmt.Sprintf(`db.%s.find({})`, table)},
			queryHelper{label: "Count by field", template: fmt.Sprintf(`db.%s.countDocuments({"%s":"value"})`, table, lookupField)},
			queryHelper{label: "Group by categorical field", template: fmt.Sprintf(`db.%s.aggregate([{"$group":{"_id":"$%s","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}])`, table, groupField)},
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
		queryHelper{label: "Insert row with schema columns", template: fmt.Sprintf("INSERT INTO %q (%s) VALUES (%s);", table, quoteColumns(insertCols), literalValues(len(insertCols)))},
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
		{label: "Project top schema fields", template: fmt.Sprintf(`db.%s.aggregate([{"$limit":50},{"$project":{"%s":1,"%s":1}}])`, table, filterField, groupField)},
		{label: "Match likely field + sort recent", template: fmt.Sprintf(`db.%s.find({"%s":"value"})`, table, filterField)},
		{label: "Count grouped values", template: fmt.Sprintf(`db.%s.aggregate([{"$group":{"_id":"$%s","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}])`, table, groupField)},
		{label: "Documents missing field", template: fmt.Sprintf(`db.%s.find({"%s":{"$exists":false}})`, table, filterField)},
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

func literalValues(count int) string {
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

// preferredFilterColumnFromFields picks the best filter column from an explicit
// field list, mirroring preferredFilterColumn but without reading m.tableSchema.
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

// preferredCategoricalColumnFromFields picks the best categorical column from
// an explicit field list, mirroring preferredCategoricalColumn.
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

// --- Ollama query generator ---

// openOllamaGen opens the ollama NLP query modal.
func (m Model) openOllamaGen() (tea.Model, tea.Cmd) {
	m.showOllamaGen = true
	m.ollamaResult = ""
	m.ollamaErr = ""
	m.ollamaGenerating = false
	m.ollamaInput.SetValue("")
	m.ollamaInput.Focus()
	return m, nil
}

// updateOllamaGen handles keypresses while the ollama modal is open.
func (m Model) updateOllamaGen(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.showOllamaGen = false
		m.ollamaInput.Blur()
		return m, nil
	case "enter":
		if m.ollamaGenerating {
			return m, nil
		}
		// Result phase: accept generated query into editor
		if m.ollamaResult != "" {
			m.queryInput.SetValue(m.ollamaResult)
			m.queryFocus = true
			m.focus = panelRight
			m.queryInput.Focus()
			m.syncTableFocus()
			m.showOllamaGen = false
			m.ollamaInput.Blur()
			m.activeTab = tabQuery
			m.setStatus("query loaded from ollama")
			return m, nil
		}
		// Input phase: fire generation
		prompt := strings.TrimSpace(m.ollamaInput.Value())
		if prompt == "" {
			return m, nil
		}
		m.ollamaGenerating = true
		m.ollamaErr = ""
		dbType := "sqlite"
		if m.activeDB != nil {
			dbType = m.activeDB.Type()
		}
		schema := m.ollamaSchemaContext()
		return m, runOllamaGenCmd(prompt, dbType, schema)
	case "r":
		// Retry: go back to input phase
		if !m.ollamaGenerating {
			m.ollamaResult = ""
			m.ollamaErr = ""
			m.ollamaInput.Focus()
		}
		return m, nil
	}

	// In result/error phase, don't forward to textinput
	if m.ollamaResult != "" || m.ollamaGenerating {
		return m, nil
	}

	var cmd tea.Cmd
	m.ollamaInput, cmd = m.ollamaInput.Update(msg)
	return m, cmd
}

// ollamaSchemaContext builds a schema string from all known collections/tables.
// Uses cached schemas for field details; falls back to just the name for uncached ones.
func (m Model) ollamaSchemaContext() string {
	seen := make(map[string]bool)
	var lines []string

	// Build lookup from cached schemas
	schemaByName := make(map[string]*db.TableSchema)
	for _, schema := range m.schemaCache {
		schemaByName[schema.Name] = schema
	}
	if m.tableSchema != nil {
		schemaByName[m.tableSchema.Name] = m.tableSchema
	}

	// All known tables first (so every collection/table is represented)
	for _, name := range m.tables {
		if seen[name] {
			continue
		}
		seen[name] = true
		if schema, ok := schemaByName[name]; ok {
			cols := make([]string, 0, len(schema.Columns))
			for _, c := range schema.Columns {
				cols = append(cols, c.Name)
			}
			lines = append(lines, name+"("+strings.Join(cols, ", ")+")")
		} else {
			lines = append(lines, name) // no schema detail yet
		}
	}
	return strings.Join(lines, "\n")
}

// runOllamaGenCmd fires an async ollama request and returns the result as ollamaQueryDoneMsg.
func runOllamaGenCmd(prompt, dbType, schemaContext string) tea.Cmd {
	return func() tea.Msg {
		client := ollama.New()
		query, err := client.GenerateQuery(context.Background(), prompt, dbType, schemaContext)
		return ollamaQueryDoneMsg{query: query, err: err}
	}
}
