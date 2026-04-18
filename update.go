package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"bobdb/internal/completion"
	"bobdb/internal/config"
	"bobdb/internal/db"
	"bobdb/internal/ollama"
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
		if m.activeTab == tabQuery && m.queryPageScrollKey(msg) {
			return m.updateQueryPageScroll(msg)
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
		case "/":
			if m.activeDB != nil {
				m.openQueryTab()
			}
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
				m.openQueryTab()
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

func (m *Model) openQueryTab() {
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.syncTableFocus()
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
	case "C":
		m.openQueryPicker("Copy Browse Row As", m.browseCopyPickerItems())
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

func (m Model) currentBrowseCursor() int {
	if m.browseData == nil || len(m.browseData.Rows) == 0 {
		return 0
	}
	cursor := m.browseDataTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.browseData.Rows) {
		cursor = len(m.browseData.Rows) - 1
	}
	return cursor
}

func (m Model) currentBrowseRowPrettyText() string {
	_, copyText := m.renderRowInspect(m.browseData, m.currentBrowseCursor())
	return copyText
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
			m.openQueryPicker("Recent Queries", m.queryHistoryPickerItems())
			return m, nil
		case "ctrl+n":
			m.openQueryPicker("Recent Queries", m.queryHistoryPickerItems())
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
			if m.shouldOpenTableFirstPicker() {
				m.openTableFirstPicker()
				return m, nil
			}
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
	case "e", "enter", "j", "k", "down", "up":
		// Left panel is a static cheat sheet — any nav key focuses the editor
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

func (m Model) shouldOpenTableFirstPicker() bool {
	if m.activeDB == nil || m.activeDB.Type() == "mongo" || len(m.tables) == 0 {
		return false
	}
	trimmed := strings.TrimSpace(m.queryInput.Value())
	return trimmed == "" || strings.EqualFold(trimmed, "select")
}

func (m Model) queryPageScrollKey(msg tea.KeyMsg) bool {
	if m.showColumnPicker || m.showQueryPicker || m.showInspect || m.showConfirm || m.showNewConn || m.showOllamaGen {
		return false
	}
	switch msg.String() {
	case "pgup", "pgdown", "home", "end":
		return true
	default:
		return false
	}
}

func (m Model) updateQueryPageScroll(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	maxScroll := max(0, len(m.queryReferenceLinesForWidth(m.queryReferenceWidth()))-m.queryReferenceViewportHeight())
	switch msg.String() {
	case "pgup":
		m.queryRefScroll = max(0, m.queryRefScroll-m.queryReferencePageStep())
	case "pgdown":
		m.queryRefScroll = min(maxScroll, m.queryRefScroll+m.queryReferencePageStep())
	case "home":
		m.queryRefScroll = 0
	case "end":
		m.queryRefScroll = maxScroll
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
	{label: "Find with projection", template: `db.{table}.find({}, {"name":1, "email":1})`},
	{label: "Find one with projection", template: `db.{table}.findOne({}, {"name":1, "email":1})`},
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
	{label: "Read: project selected fields", template: `db.{table}.find({}, {"name":1, "email":1})`},
	{label: "Read: findOne projected document", template: `db.{table}.findOne({"status":"active"}, {"name":1, "email":1})`},
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

// queryInferredTable returns the table referenced in the query editor text:
// the FROM-clause table if parseable, or the write-query target table.
// Returns "" when no table can be parsed — never falls back to the browse panel.
func (m Model) queryInferredTable() string {
	if table := extractSelectTable(m.queryInput.Value()); table != "" {
		return table
	}
	return extractTableFromQuery(m.queryInput.Value())
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
		return mongoQueryIsWrite(query)
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

// mongoQueryIsWrite detects mutation commands in either shell syntax
// (db.coll.insertOne(...)) or the internal whitespace-separated form
// (insert coll ...). Lowercased query expected.
func mongoQueryIsWrite(query string) bool {
	if strings.HasPrefix(query, "db.") {
		// Extract the method name between the second '.' and the first '('.
		rest := query[3:]
		dot := strings.Index(rest, ".")
		if dot < 0 {
			return false
		}
		rest = rest[dot+1:]
		paren := strings.Index(rest, "(")
		if paren < 0 {
			return false
		}
		method := rest[:paren]
		switch method {
		case "insertone", "insertmany",
			"updateone", "updatemany", "replaceone",
			"deleteone", "deletemany", "remove",
			"findoneandupdate", "findoneandreplace", "findoneanddelete",
			"bulkwrite", "drop", "renamecollection":
			return true
		}
		return false
	}
	cmd, _ := nextQueryWord(query)
	switch cmd {
	case "insert", "update", "delete", "remove":
		return true
	}
	return false
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

// --- Completion engine adapter ---

// buildCompletionRequest builds a completion.Request from the current Model state.
func (m *Model) buildCompletionRequest() completion.Request {
	req := completion.Request{
		Query:         m.queryInput.Value(),
		Cursor:        m.queryCursorIndex(),
		DBType:        "",
		Tables:        m.tables,
		Schema:        m.resolveSchemaForCompletion(),
		ValueCache:    m.completionValueCache(),
		InferredTable: m.queryInferredTable(),
	}
	if m.activeDB != nil {
		req.DBType = m.activeDB.Type()
	}
	return req
}

// resolveSchemaForCompletion returns the schema for the query-inferred table,
// preferring the cache (keyed by connIdx|table) over the browse panel's
// tableSchema. This is the fix for the stale collection bug — we always
// resolve based on what the query text targets, not the browse cursor.
func (m *Model) resolveSchemaForCompletion() *completion.SchemaInfo {
	inferred := m.queryInferredTable()
	if inferred == "" {
		return nil
	}
	// Always prefer cache — keyed by connIdx|table, always correct
	key := schemaCacheKey(m.activeConnIdx, inferred)
	if cached := m.schemaCache[key]; cached != nil {
		return m.dbSchemaToCompletionSchema(cached)
	}
	// Only use browse panel schema if it matches the inferred table
	if m.tableSchema != nil && strings.EqualFold(m.tableSchema.Name, inferred) {
		return m.dbSchemaToCompletionSchema(m.tableSchema)
	}
	return nil
}

func (m *Model) toSchemaInfo(s *db.TableSchema) *completion.SchemaInfo {
	if s == nil {
		return nil
	}
	return m.dbSchemaToCompletionSchema(s)
}

func (m *Model) dbSchemaToCompletionSchema(s *db.TableSchema) *completion.SchemaInfo {
	if s == nil {
		return nil
	}
	cols := make([]completion.ColumnInfo, len(s.Columns))
	for i, c := range s.Columns {
		cols[i] = completion.ColumnInfo{Name: c.Name, Type: c.Type, PrimaryKey: c.PrimaryKey}
	}
	return &completion.SchemaInfo{Name: s.Name, Columns: cols}
}

// completionValueCache returns a value cache keyed by "table|col" (no connIdx
// prefix) so the engine doesn't need to know about connection indices.
func (m *Model) completionValueCache() map[string][]string {
	out := make(map[string][]string, len(m.columnValueCache))
	prefix := fmt.Sprintf("%d|", m.activeConnIdx)
	for k, v := range m.columnValueCache {
		if strings.HasPrefix(k, prefix) {
			out[k[len(prefix):]] = v
		}
	}
	return out
}

func (m *Model) openCompletionForCursor(manual bool) (bool, tea.Cmd) {
	if m.activeDB == nil {
		if manual {
			m.setStatus("completion unavailable here")
		}
		return false, nil
	}
	result := completion.Complete(m.buildCompletionRequest())
	if result == nil {
		if manual {
			m.setStatus("no completion items available")
		}
		return false, nil
	}
	// Handle async needs
	var cmd tea.Cmd
	if result.NeedSchema != "" {
		cmd = m.loadSchemaForCache(result.NeedSchema)
	}
	if result.NeedValues != nil {
		valCmd := m.loadColumnValues(result.NeedValues.Table, result.NeedValues.Column)
		if cmd != nil {
			cmd = tea.Batch(cmd, valCmd)
		} else {
			cmd = valCmd
		}
	}
	if len(result.Items) == 0 {
		if cmd != nil {
			// Async load in flight — show loading placeholder
			token := completion.TokenValue([]rune(m.queryInput.Value())[result.Start:result.End])
			result.Items = []completion.Item{{Label: "loading…", Detail: "fetching schema / values", InsertText: token}}
		} else {
			if manual {
				m.setStatus("no completion items available")
			}
			return false, nil
		}
	}
	m.columnPickerTitle = result.Title
	m.columnPickerItems = result.Items
	m.columnPickerCursor = 0
	m.columnPickerMulti = result.Multi
	m.columnPickerMultiPrefix = result.MultiPrefix
	m.columnPickerMultiSuffix = result.MultiSuffix
	m.columnPickerMultiSep = result.MultiSep
	m.columnPickerStart = result.Start
	m.columnPickerEnd = result.End
	m.columnPickerFallback = result.Fallback
	m.columnPickerTableFirst = false
	m.columnPickerValueMode = result.ValueMode
	m.columnPickerValuePrefix = ""
	m.columnPickerValueCursor = 0
	m.columnPickerValueCol = result.ValueCol
	m.columnPickerValueTable = result.ValueTable
	m.showColumnPicker = true
	return true, cmd
}

// openTableFirstPicker opens a table picker for the table-first SQL flow.
// Selecting a table scaffolds SELECT * FROM <table>\nWHERE with cursor on *.
func (m *Model) openTableFirstPicker() {
	items := make([]completion.Item, 0, len(m.tables))
	for _, name := range m.tables {
		items = append(items, completion.Item{
			Label:      name,
			Detail:     "table",
			InsertText: name,
		})
	}
	m.columnPickerTitle = "Select Table"
	m.columnPickerItems = items
	m.columnPickerCursor = 0
	m.columnPickerMulti = false
	m.columnPickerMultiPrefix = ""
	m.columnPickerMultiSuffix = ""
	m.columnPickerMultiSep = ""
	m.columnPickerStart = 0
	m.columnPickerEnd = 0
	m.columnPickerFallback = ""
	m.columnPickerTableFirst = true
	m.columnPickerValueMode = false
	m.columnPickerValuePrefix = ""
	m.columnPickerValueCursor = 0
	m.showColumnPicker = true
}

func (m *Model) refreshCompletionPicker(manual bool) (bool, tea.Cmd) {
	if !m.showColumnPicker {
		return m.openCompletionForCursor(manual)
	}
	if m.columnPickerValueMode {
		m.refilterValuePicker()
		return true, nil
	}
	result := completion.Complete(m.buildCompletionRequest())
	if result == nil || len(result.Items) == 0 {
		m.showColumnPicker = false
		m.columnPickerTableFirst = false
		return false, nil
	}
	m.columnPickerTableFirst = false
	var cmd tea.Cmd
	if result.NeedSchema != "" {
		cmd = m.loadSchemaForCache(result.NeedSchema)
	}
	if result.NeedValues != nil {
		valCmd := m.loadColumnValues(result.NeedValues.Table, result.NeedValues.Column)
		if cmd != nil {
			cmd = tea.Batch(cmd, valCmd)
		} else {
			cmd = valCmd
		}
	}
	m.columnPickerTitle = result.Title
	m.columnPickerItems = result.Items
	m.columnPickerCursor = 0
	m.columnPickerMulti = result.Multi
	m.columnPickerMultiPrefix = result.MultiPrefix
	m.columnPickerMultiSuffix = result.MultiSuffix
	m.columnPickerMultiSep = result.MultiSep
	m.columnPickerStart = result.Start
	m.columnPickerEnd = result.End
	m.columnPickerFallback = result.Fallback
	m.showColumnPicker = true
	return true, cmd
}

func (m *Model) refilterValuePicker() {
	if m.columnPickerValueCol == "" {
		return
	}
	m.columnPickerValueCursor = clampInt(m.columnPickerValueCursor, 0, len([]rune(m.columnPickerValuePrefix)))
	key := columnValueKey(m.activeConnIdx, m.columnPickerValueTable, m.columnPickerValueCol)
	values := m.columnValueCache[key]
	items := make([]completion.Item, 0, len(values))
	for _, v := range values {
		items = append(items, completion.Item{Label: v, Detail: m.columnPickerValueCol, InsertText: v})
	}
	items = completion.RankItemsKeepAll(strings.ToLower(m.columnPickerValuePrefix), items)
	if len(items) == 0 {
		if values == nil {
			items = []completion.Item{{Label: "loading…", Detail: "fetching samples", InsertText: m.columnPickerValuePrefix}}
		} else {
			items = []completion.Item{{Label: "(no samples)", Detail: m.columnPickerValueCol, InsertText: m.columnPickerValuePrefix}}
		}
	}
	m.columnPickerItems = items
	if m.columnPickerCursor >= len(items) {
		m.columnPickerCursor = 0
	}
}

func (m Model) queryCursorIndex() int {
	query := []rune(m.queryInput.Value())
	line := clampInt(m.queryInput.Line(), 0, len(completion.SplitLines(query))-1)
	col := m.queryInput.LineInfo().ColumnOffset
	return completion.IndexForLineCol(query, line, col)
}

func (m *Model) applyCompletionInsertion(start, end int, fallback string, multi bool, items []completion.Item) {
	if len(items) == 0 && fallback != "" {
		items = []completion.Item{{InsertText: fallback}}
	}
	if len(items) == 0 {
		return
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		insertText := item.InsertText
		if insertText == "" {
			insertText = item.Label
		}
		parts = append(parts, insertText)
	}
	sep := ", "
	if m.columnPickerMultiSep != "" {
		sep = m.columnPickerMultiSep
	}
	insert := strings.Join(parts, sep)
	if !multi && len(parts) == 1 {
		insert = parts[0]
	} else if multi {
		insert = m.columnPickerMultiPrefix + insert + m.columnPickerMultiSuffix
	}
	query := []rune(m.queryInput.Value())
	if start < 0 || start > len(query) {
		start = len(query)
	}
	if end < start || end > len(query) {
		end = start
	}

	insertRunes := []rune(insert)
	updated := string(query[:start]) + insert + string(query[end:])
	m.queryInput.SetValue(updated)
	m.queryInput.Focus()

	endPos := start + len(insertRunes)
	line, col := completion.LineColForIndex([]rune(updated), endPos)
	setTextareaCursor(&m.queryInput, line, col)
	m.queryFocus = true
	m.focus = panelRight
	m.syncTableFocus()
}

func setTextareaCursor(input *textarea.Model, line, col int) {
	// Go to absolute top-left first
	for input.Line() > 0 {
		input.CursorUp()
	}
	input.CursorStart()
	for i := 0; i < line; i++ {
		input.CursorDown()
	}
	input.SetCursor(col)
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
		m.columnPickerTableFirst = false
		m.columnPickerValueMode = false
		m.columnPickerMultiPrefix = ""
		m.columnPickerMultiSuffix = ""
		m.columnPickerMultiSep = ""
		m.columnPickerValuePrefix = ""
		m.columnPickerValueCursor = 0
		return m, nil
	case "tab":
		if m.columnPickerTableFirst {
			// Table-first flow: scaffold SELECT * FROM <table>\nWHERE
			m.showColumnPicker = false
			m.columnPickerTableFirst = false
			table := ""
			if len(m.columnPickerItems) > 0 {
				item := m.columnPickerItems[m.columnPickerCursor]
				table = item.InsertText
				if table == "" {
					table = item.Label
				}
			}
			if table == "" {
				return m, nil
			}
			dbType := ""
			if m.activeDB != nil {
				dbType = m.activeDB.Type()
			}
			quoted := completion.QuoteIdentifier(dbType, table)
			query := fmt.Sprintf("SELECT * FROM %s\nWHERE ", quoted)
			m.queryInput.SetValue(query)
			m.queryInput.Focus()
			// Position cursor on the * (index 7)
			setTextareaCursor(&m.queryInput, 0, 7)
			m.queryFocus = true
			m.focus = panelRight
			m.syncTableFocus()
			// Prefetch schema for the selected table
			cmd := m.prefetchInferredSchema()
			return m, cmd
		}
		items := make([]completion.Item, 0, len(m.columnPickerItems))
		for _, item := range m.columnPickerItems {
			if item.Selected {
				items = append(items, item)
			}
		}
		if len(items) == 0 {
			if m.columnPickerMulti && m.columnPickerFallback != "" {
				items = append(items, completion.Item{InsertText: m.columnPickerFallback})
			} else if len(m.columnPickerItems) > 0 {
				items = append(items, m.columnPickerItems[m.columnPickerCursor])
			}
		}
		m.showColumnPicker = false
		valueMode := m.columnPickerValueMode
		m.columnPickerValueMode = false
		m.columnPickerValuePrefix = ""
		m.columnPickerValueCursor = 0
		m.applyCompletionInsertion(m.columnPickerStart, m.columnPickerEnd, m.columnPickerFallback, m.columnPickerMulti, items)
		m.columnPickerMultiPrefix = ""
		m.columnPickerMultiSuffix = ""
		m.columnPickerMultiSep = ""
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
		m.columnPickerItems[m.columnPickerCursor].Selected = !m.columnPickerItems[m.columnPickerCursor].Selected
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

func (m Model) completionPickerCapturesTyping(msg tea.KeyMsg) bool {
	switch msg.Type {
	case tea.KeyRunes, tea.KeyBackspace, tea.KeyDelete:
		return true
	case tea.KeySpace:
		return !m.columnPickerMulti || m.columnPickerValueMode
	case tea.KeyLeft, tea.KeyRight, tea.KeyHome, tea.KeyEnd:
		return m.columnPickerValueMode
	default:
		return false
	}
}

func (m Model) shouldAutoTriggerCompletion(msg tea.KeyMsg) bool {
	if m.activeDB != nil && m.activeDB.Type() != "mongo" && msg.Type == tea.KeyRunes && len(msg.Runes) == 1 {
		switch msg.Runes[0] {
		case '\'', '"':
			cursor := m.queryCursorIndex()
			query := m.queryInput.Value()
			if cursor > 0 && completion.CursorInsideString(query, cursor-1) && !completion.CursorInsideString(query, cursor) {
				return false
			}
		}
	}
	switch msg.Type {
	case tea.KeyRunes, tea.KeySpace, tea.KeyBackspace, tea.KeyDelete:
		return true
	default:
		return false
	}
}

func (m *Model) focusCursorAtIndex(idx int) {
	line, col := completion.LineColForIndex([]rune(m.queryInput.Value()), idx)
	setTextareaCursor(&m.queryInput, line, col)
}

func (m Model) resultCopyPickerItems() []queryPickerItem {
	return m.copyPickerItemsForResult(
		m.queryResult,
		m.currentResultRowJSON(),
		m.currentResultRowPrettyText(),
	)
}

func (m Model) browseCopyPickerItems() []queryPickerItem {
	return m.copyPickerItemsForResult(
		m.browseData,
		m.currentBrowseRowJSON(),
		m.currentBrowseRowPrettyText(),
	)
}

func (m Model) copyPickerItemsForResult(result *db.QueryResult, currentRowJSON, currentRowPretty string) []queryPickerItem {
	if result == nil {
		return nil
	}
	items := make([]queryPickerItem, 0, 5)
	if currentRowJSON != "" {
		items = append(items, queryPickerItem{label: "Current row JSON", detail: "row json", value: currentRowJSON, kind: "copy"})
	}
	if currentRowPretty != "" {
		items = append(items, queryPickerItem{label: "Current row inspect view", detail: "row detail", value: currentRowPretty, kind: "copy"})
	}
	if data := allRowsJSON(result); data != "" {
		items = append(items, queryPickerItem{label: "All rows JSON", detail: "rows json", value: data, kind: "copy"})
	}
	if data := allRowsCSV(result); data != "" {
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
		displayRaw := formatDisplayValue(raw)
		parts := structuredValueLines(displayRaw)
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
	return allRowsJSON(m.queryResult)
}

func allRowsJSON(result *db.QueryResult) string {
	if result == nil || len(result.Rows) == 0 {
		return ""
	}
	rows := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, rowObject(result, row))
	}
	data, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return ""
	}
	return string(data)
}

func (m Model) allResultRowsCSV() string {
	return allRowsCSV(m.queryResult)
}

func allRowsCSV(result *db.QueryResult) string {
	if result == nil {
		return ""
	}
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(result.Columns); err != nil {
		return ""
	}
	for _, row := range result.Rows {
		record := make([]string, len(result.Columns))
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
			queryHelper{label: "Project top schema fields", template: fmt.Sprintf(`db.%s.find({}, {"%s":1, "%s":1})`, table, lookupField, groupField)},
			queryHelper{label: "Find one projected document", template: fmt.Sprintf(`db.%s.findOne({"%s":"value"}, {"%s":1, "%s":1})`, table, lookupField, lookupField, groupField)},
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
		{label: "Project top schema fields", template: fmt.Sprintf(`db.%s.find({}, {"%s":1, "%s":1})`, table, filterField, groupField)},
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
