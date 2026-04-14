package main

import (
	"os"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"dbkit/internal/config"
	"dbkit/internal/db"
)

type fakeDB struct {
	dbType string
}

func (f *fakeDB) Connect() error                                 { return nil }
func (f *fakeDB) Close()                                         {}
func (f *fakeDB) Ping() error                                    { return nil }
func (f *fakeDB) GetTables() ([]string, error)                   { return []string{"users", "orders"}, nil }
func (f *fakeDB) GetTableSchema(string) (*db.TableSchema, error) { return &db.TableSchema{}, nil }
func (f *fakeDB) RunQuery(string) (*db.QueryResult, error)       { return &db.QueryResult{}, nil }
func (f *fakeDB) Type() string                                   { return f.dbType }
func (f *fakeDB) DSN() string                                    { return "" }

func TestStaleConnectResponseIgnored(t *testing.T) {
	m := newModel(&config.Config{
		Connections: []config.Connection{
			{Name: "first", Type: "sqlite", DSN: "a.db"},
			{Name: "second", Type: "sqlite", DSN: "b.db"},
		},
	})
	m.connectReqID = 2
	m.connCursor = 1

	next, _ := m.Update(connectedMsg{
		reqID:   1,
		connIdx: 0,
		conn:    m.cfg.Connections[0],
		db:      &fakeDB{dbType: "sqlite"},
	})
	got := next.(Model)

	if got.activeDB != nil {
		t.Fatalf("stale connect should not replace active db")
	}
	if got.activeConnName != "" {
		t.Fatalf("stale connect should not change active connection name")
	}
}

func TestEditorEscReturnsFocusToResultsPane(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	got := next.(Model)

	if got.queryFocus {
		t.Fatalf("expected editor to blur on esc")
	}
	if got.focus != panelRight {
		t.Fatalf("expected focus to stay on right pane, got %v", got.focus)
	}
}

func TestSchemaRightPaneCIgnored(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabSchema
	m.focus = panelRight
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true, Nullable: false},
		},
	}
	m.syncSchemaTable()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
	got := next.(Model)

	if got.statusMsg != "" {
		t.Fatalf("expected no status change, got %q", got.statusMsg)
	}
}

func TestTableViewportWidthUsesSinglePaneWidth(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 60

	if !m.isSinglePane() {
		t.Fatalf("expected width 60 to use single pane mode")
	}
	if got, want := m.tableViewportWidth(), 56; got != want {
		t.Fatalf("table viewport width = %d, want %d", got, want)
	}
}

func TestRenderSchemaDetailUsesBubbleTable(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.height = 40
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.tableSchema = &db.TableSchema{
		Name:     "users",
		RowCount: 42,
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true, Nullable: false},
			{Name: "email", Type: "text", Nullable: false},
		},
	}
	m.resizeTables()

	view := stripANSIForTest(m.renderSchemaDetail(64, 16))
	if !strings.Contains(view, "Column") || !strings.Contains(view, "Type") || !strings.Contains(view, "Flags") {
		t.Fatalf("expected schema detail to include table headers, got %q", view)
	}
	tbl := m.schemaTable
	tbl.SetWidth(64)
	tbl.SetHeight(12)
	tableView := strings.TrimSpace(stripANSIForTest(tbl.View()))
	if !strings.Contains(view, tableView) {
		t.Fatalf("expected schema detail to embed schema table view")
	}
	if got := strings.Count(m.renderSchemaDetail(64, 16), "\n") + 1; got > 16 {
		t.Fatalf("schema detail rendered %d lines, want <= 16", got)
	}
}

func TestRenderResultsPanelStaysWithinHeightBudget(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.height = 40
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.queryResult = &db.QueryResult{
		Columns: []string{"id", "email", "created_at"},
		Rows: [][]string{
			{"1", "a@example.com", "2026-04-13"},
			{"2", "b@example.com", "2026-04-12"},
		},
	}
	m.resizeTables()

	if got := strings.Count(m.renderResultsPanel(64, 16), "\n") + 1; got > 16 {
		t.Fatalf("results panel rendered %d lines, want <= 16", got)
	}
}

func TestSplitPaneLayoutKeepsPanelsSameHeight(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.height = 32
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabResults
	m.focus = panelRight
	m.tables = []string{"users", "orders"}
	m.queryResult = &db.QueryResult{
		Columns: []string{"id", "email", "created_at"},
		Rows: [][]string{
			{"1", "a@example.com", "2026-04-13"},
			{"2", "b@example.com", "2026-04-12"},
		},
	}
	m.resizeTables()

	rendered := stripANSIForTest(m.renderPanels(20))
	lines := strings.Split(rendered, "\n")
	if len(lines) != 20 {
		t.Fatalf("split pane rendered %d lines, want 20", len(lines))
	}
}

func TestBrowseViewStaysWithinWindowHeight(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.height = 24
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabSchema
	m.focus = panelRight
	m.activeConnName = "main"
	m.tables = []string{"users", "orders"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name:     "users",
		RowCount: 42,
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true, Nullable: false},
			{Name: "email", Type: "text", Nullable: false},
			{Name: "created_at", Type: "timestamp"},
			{Name: "status", Type: "text"},
			{Name: "role", Type: "text"},
			{Name: "last_seen_at", Type: "timestamp"},
		},
	}
	m.resizeTables()

	view := stripANSIForTest(m.View())
	if got := strings.Count(view, "\n") + 1; got != m.height {
		t.Fatalf("browse view rendered %d lines, want %d", got, m.height)
	}
	if !strings.Contains(view, "dbkit") {
		t.Fatalf("expected global header to remain visible")
	}
}

func TestRenderPaneTitleFitsWidth(t *testing.T) {
	line := renderPaneTitle("very_long_table_name_for_users_and_orders", "123456 rows", 24)
	if width := len([]rune(stripANSIForTest(line))); width > 24 {
		t.Fatalf("pane title width = %d, want <= 24", width)
	}
	if !strings.Contains(stripANSIForTest(line), "123456 rows") {
		t.Fatalf("expected meta text to remain visible")
	}
}

func TestSyncResultTableHandlesColumnCountChanges(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.queryResult = &db.QueryResult{
		Columns: []string{"a", "b", "c", "d"},
		Rows:    [][]string{{"1", "2", "3", "4"}},
	}
	m.syncResultTable()

	m.queryResult = &db.QueryResult{
		Columns: []string{"a", "b"},
		Rows:    [][]string{{"1", "2"}},
	}
	m.syncResultTable()

	if got := len(m.resultTable.Columns()); got != 2 {
		t.Fatalf("result table columns = %d, want 2", got)
	}
}

func TestQueryHistoryRecall(t *testing.T) {
	m := newModel(&config.Config{})
	m.pushQueryHistory("select 1;")
	m.pushQueryHistory("select 2;")

	m.recallPreviousQuery()
	if got := m.queryInput.Value(); got != "select 2;" {
		t.Fatalf("first history recall = %q, want newest query", got)
	}

	m.recallPreviousQuery()
	if got := m.queryInput.Value(); got != "select 1;" {
		t.Fatalf("second history recall = %q, want older query", got)
	}

	m.recallNextQuery()
	if got := m.queryInput.Value(); got != "select 2;" {
		t.Fatalf("history forward recall = %q, want newer query", got)
	}
}

func TestSchemaAwareHelpersIncluded(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "email", Type: "text"},
			{Name: "created_at", Type: "timestamp"},
		},
	}

	helpers := m.helperItems()
	var foundLookup, foundPrompt bool
	for _, helper := range helpers {
		if helper.label == "Lookup by key field" && strings.Contains(helper.template, `"id" = 'value'`) {
			foundLookup = true
		}
		if helper.label == "Ask Ollama from schema" && helper.kind == "prompt" {
			foundPrompt = true
		}
	}

	if !foundLookup {
		t.Fatalf("expected schema-aware lookup helper")
	}
	if !foundPrompt {
		t.Fatalf("expected schema-aware prompt helper")
	}
}

func TestSchemaAwareHelpersExpandWithWriteAndGroupTemplates(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "status", Type: "text"},
			{Name: "email", Type: "text"},
			{Name: "role", Type: "text"},
			{Name: "created_at", Type: "timestamp"},
		},
	}

	helpers := m.schemaAwareHelpers("users")
	var foundInsert, foundUpdate, foundGroup bool
	for _, helper := range helpers {
		switch helper.label {
		case "Insert row with schema columns":
			foundInsert = strings.Contains(helper.template, `INSERT INTO "users" ("status", "email", "role", "created_at")`)
		case "Update row by key field":
			foundUpdate = strings.Contains(helper.template, `WHERE "id" = 'value'`)
		case "Group by categorical column":
			foundGroup = strings.Contains(helper.template, `GROUP BY "status"`)
		}
	}
	if !foundInsert {
		t.Fatalf("expected schema-aware insert helper")
	}
	if !foundUpdate {
		t.Fatalf("expected schema-aware update helper")
	}
	if !foundGroup {
		t.Fatalf("expected schema-aware group helper")
	}
}

func TestTypingDigitInRawQueryDoesNotSwitchTabs(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'1'}})
	got := next.(Model)

	if got.activeTab != tabQuery {
		t.Fatalf("active tab = %v, want query tab", got.activeTab)
	}
	if got.queryInput.Value() != "1" {
		t.Fatalf("raw query value = %q, want %q", got.queryInput.Value(), "1")
	}
}

func TestTabOpensColumnPickerFromQueryEditor(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "email", Type: "text"},
		},
	}
	m.queryInput.SetValue("SELECT ")

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if !got.showColumnPicker {
		t.Fatalf("expected column picker to open")
	}
	if len(got.columnPickerItems) != 3 {
		t.Fatalf("column picker items = %d, want 3", len(got.columnPickerItems))
	}
}

func TestTypingInQueryEditorDoesNotAutoOpenCompletionPicker(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "email", Type: "text"},
		},
	}
	m.queryInput.SetValue("SELECT")

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeySpace})
	got := next.(Model)

	if got.showColumnPicker {
		t.Fatalf("expected completion picker to stay closed until requested")
	}
}

func TestCtrlLClearsQueryEditor(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue("SELECT * FROM users;")
	m.snippetPlaceholders = []snippetPlaceholder{{name: "table", start: 14, end: 19, fresh: true}}
	m.queryHistoryIdx = 1

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyCtrlL})
	got := next.(Model)

	if got.queryInput.Value() != "" {
		t.Fatalf("query input = %q, want empty", got.queryInput.Value())
	}
	if len(got.snippetPlaceholders) != 0 {
		t.Fatalf("expected snippet placeholders to clear")
	}
	if got.queryHistoryIdx != -1 {
		t.Fatalf("queryHistoryIdx = %d, want -1", got.queryHistoryIdx)
	}
}

func TestDeleteConnectionRequiresConfirmation(t *testing.T) {
	m := newModel(&config.Config{
		Connections: []config.Connection{
			{ID: "a", Name: "main", Type: "sqlite", DSN: "main.db"},
		},
	})

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	got := next.(Model)

	if !got.showConfirm {
		t.Fatalf("expected delete confirmation modal")
	}
	if got.confirmAction != confirmDeleteConnection {
		t.Fatalf("confirmAction = %v, want delete connection", got.confirmAction)
	}
	if len(got.cfg.Connections) != 1 {
		t.Fatalf("connection deleted before confirmation")
	}
}

func TestDeleteConnectionConfirmationExecutesDeletion(t *testing.T) {
	m := newModel(&config.Config{
		Connections: []config.Connection{
			{ID: "a", Name: "main", Type: "sqlite", DSN: "main.db"},
		},
	})
	m.openDeleteConnectionConfirm(0)

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	if got.showConfirm {
		t.Fatalf("expected confirmation modal to close")
	}
	if len(got.cfg.Connections) != 0 {
		t.Fatalf("expected connection to be deleted after confirmation")
	}
}

func TestWriteQueryRequiresConfirmationBeforeRun(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue(`DELETE FROM "users";`)

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlR})
	got := next.(Model)

	if cmd != nil {
		t.Fatalf("expected query command to wait for confirmation")
	}
	if !got.showConfirm {
		t.Fatalf("expected write query confirmation modal")
	}
	if got.confirmAction != confirmRunQuery {
		t.Fatalf("confirmAction = %v, want run query", got.confirmAction)
	}
}

func TestWriteQueryRunsAfterConfirmation(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.queryInput.SetValue(`DELETE FROM "users";`)
	m.openRunQueryConfirm(`DELETE FROM "users";`)

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	if got.showConfirm {
		t.Fatalf("expected confirmation modal to close")
	}
	if !got.loading {
		t.Fatalf("expected loading state after confirming write query")
	}
	if cmd == nil {
		t.Fatalf("expected query command after confirmation")
	}
}

func TestPushQueryHistoryMovesDuplicateToFront(t *testing.T) {
	m := newModel(&config.Config{})
	m.pushQueryHistory("select 1;")
	m.pushQueryHistory("select 2;")
	m.pushQueryHistory("select 1;")

	if got := len(m.queryHistory); got != 2 {
		t.Fatalf("history length = %d, want 2", got)
	}
	if got := m.queryHistory[0]; got != "select 1;" {
		t.Fatalf("latest query = %q, want select 1;", got)
	}
	if got := m.historyCursor; got != 0 {
		t.Fatalf("history cursor = %d, want 0", got)
	}
}

func TestColumnPickerInsertsColumnsAtCursor(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "email", Type: "text"},
			{Name: "created_at", Type: "timestamp"},
		},
	}
	m.queryInput.SetValue(`SELECT  FROM "users";`)
	setTextareaCursor(&m.queryInput, 0, len("SELECT "))

	if !m.openCompletionForCursor(true) {
		t.Fatalf("expected column picker to open")
	}
	for i := range m.columnPickerItems {
		if m.columnPickerItems[i].label == "email" || m.columnPickerItems[i].label == "created_at" {
			m.columnPickerItems[i].selected = true
		}
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	want := `SELECT "email", "created_at" FROM "users";`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestTabOpensTablePickerFromFromClause(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "orders"}
	m.queryInput.SetValue(`SELECT * FROM `)

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if !got.showColumnPicker {
		t.Fatalf("expected completion picker to open")
	}
	if got.columnPickerTitle != "From Table" {
		t.Fatalf("picker title = %q, want From Table", got.columnPickerTitle)
	}
	if len(got.columnPickerItems) != 2 {
		t.Fatalf("picker items = %d, want 2", len(got.columnPickerItems))
	}
}

func TestMongoCompletionStartsWithCommandSuggestions(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "events"}

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if !got.showColumnPicker {
		t.Fatalf("expected completion picker to open")
	}
	if got.columnPickerTitle != "Mongo Commands" {
		t.Fatalf("picker title = %q, want Mongo Commands", got.columnPickerTitle)
	}
	if len(got.columnPickerItems) == 0 || got.columnPickerItems[0].label != "find" {
		t.Fatalf("expected find command suggestion, got %#v", got.columnPickerItems)
	}
}

func TestSnippetCompletionCreatesPlaceholderSession(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "email", Type: "text"},
		},
	}

	if !m.openCompletionForCursor(true) {
		t.Fatalf("expected completion picker to open")
	}
	m.columnPickerCursor = 0

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	if len(got.snippetPlaceholders) == 0 {
		t.Fatalf("expected snippet placeholders to be active")
	}
	if !strings.Contains(got.queryInput.Value(), "${columns}") {
		t.Fatalf("expected snippet text in query input, got %q", got.queryInput.Value())
	}
}

func TestTabJumpsSnippetPlaceholders(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue("SELECT ${columns} FROM ${table};")
	m.snippetPlaceholders = []snippetPlaceholder{
		{name: "columns", start: len("SELECT "), end: len("SELECT ${columns}"), fresh: true},
		{name: "table", start: len("SELECT ${columns} FROM "), end: len("SELECT ${columns} FROM ${table}"), fresh: true},
	}
	m.snippetIndex = 0
	m.focusSnippetPlaceholder(0)

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if got.snippetIndex != 1 {
		t.Fatalf("snippet index = %d, want 1", got.snippetIndex)
	}
}

func TestCurrentResultRowJSONReturnsValidJSON(t *testing.T) {
	m := newModel(&config.Config{})
	m.queryResult = &db.QueryResult{
		Columns: []string{"id", "payload"},
		Rows: [][]string{
			{"1", `{"meta":{"status":"ok"}}`},
		},
	}

	got := m.currentResultRowJSON()

	if !strings.Contains(got, `"id": "1"`) {
		t.Fatalf("expected scalar string field, got %q", got)
	}
	if !strings.Contains(got, `"payload": {`) {
		t.Fatalf("expected nested json object, got %q", got)
	}
}

func TestAllResultRowsCSVIncludesHeaders(t *testing.T) {
	m := newModel(&config.Config{})
	m.queryResult = &db.QueryResult{
		Columns: []string{"id", "email"},
		Rows: [][]string{
			{"1", "a@example.com"},
			{"2", "b@example.com"},
		},
	}

	got := m.allResultRowsCSV()
	if !strings.HasPrefix(got, "id,email\n") {
		t.Fatalf("csv header missing, got %q", got)
	}
	if !strings.Contains(got, "1,a@example.com") {
		t.Fatalf("csv row missing, got %q", got)
	}
}

func TestSaveCurrentQueryPersistsToConfig(t *testing.T) {
	home := t.TempDir()
	prevHome := os.Getenv("HOME")
	t.Setenv("HOME", home)
	defer func() {
		_ = os.Setenv("HOME", prevHome)
	}()

	cfg := &config.Config{
		Connections: []config.Connection{{ID: "abc123", Name: "main", Type: "sqlite", DSN: "test.db"}},
	}
	m := newModel(cfg)
	m.activeConnIdx = 0
	m.queryInput.SetValue("SELECT *\nFROM users;")

	next, _ := m.saveCurrentQuery()
	got := next.(Model)

	if len(got.savedQueries) != 1 {
		t.Fatalf("saved queries = %d, want 1", len(got.savedQueries))
	}
	if got.savedQueries[0].Label != "SELECT *" {
		t.Fatalf("saved query label = %q", got.savedQueries[0].Label)
	}
	if len(cfg.SavedQueries["abc123"]) != 1 {
		t.Fatalf("config saved queries = %d, want 1", len(cfg.SavedQueries["abc123"]))
	}
}

func TestBrowseEnterRunsDefaultQueryIntoResults(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabSchema
	m.tables = []string{"users"}
	m.tableCursor = 0

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	if got.activeTab != tabResults {
		t.Fatalf("active tab = %v, want results tab", got.activeTab)
	}
	if !got.loading {
		t.Fatalf("expected loading state after browse enter")
	}
	if got.queryInput.Value() != `SELECT * FROM "users" LIMIT 100;` {
		t.Fatalf("query input = %q", got.queryInput.Value())
	}
	if cmd == nil {
		t.Fatalf("expected query command to run")
	}
}

func TestResultsTabResetsToFirstRowAndColumn(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelLeft
	m.queryResult = &db.QueryResult{
		Columns: []string{"a", "b", "c"},
		Rows: [][]string{
			{"1", "2", "3"},
			{"4", "5", "6"},
		},
	}
	m.resultColOffset = 2
	m.resultVisibleColumn = 1
	m.resultTable.SetCursor(1)

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}})
	got := next.(Model)

	if got.activeTab != tabResults {
		t.Fatalf("active tab = %v, want results tab", got.activeTab)
	}
	if got.focus != panelRight {
		t.Fatalf("focus = %v, want right pane", got.focus)
	}
	if got.resultColOffset != 0 {
		t.Fatalf("resultColOffset = %d, want 0", got.resultColOffset)
	}
	if got.resultTable.Cursor() != 0 {
		t.Fatalf("result row cursor = %d, want 0", got.resultTable.Cursor())
	}
}

func TestCurrentResultRowJSONUsesStructuredInspectView(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.queryResult = &db.QueryResult{
		Columns: []string{"id", "payload"},
		Rows: [][]string{
			{"1", `{"meta":{"status":"ok"},"tags":["a","b"]}`},
		},
	}

	got := m.currentResultRowJSON()

	if !strings.Contains(got, `"payload":`) {
		t.Fatalf("expected payload field in copied result row, got %q", got)
	}
	if !strings.Contains(got, `"meta": {`) || !strings.Contains(got, `"tags": [`) {
		t.Fatalf("expected nested json to be expanded, got %q", got)
	}
}

func TestVisibleResultColumnsStayWithinBudget(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name", "email", "status", "created_at"},
		Rows: [][]string{
			{"1", "alice", "alice@example.com", "active", "2026-04-01T00:00:00Z"},
			{"2", "bob", "bob@example.com", "inactive", "2026-04-02T00:00:00Z"},
		},
	}

	for _, width := range []int{20, 40, 80, 120} {
		_, cols := visibleResultColumns(result, width, 0)
		if len(cols) == 0 {
			t.Fatalf("no columns selected at width %d", width)
		}
		total := 0
		for _, c := range cols {
			// bubbles/table renders each column at Width+2 chars.
			total += c.Width + 2
		}
		if total > width {
			t.Fatalf("rendered width %d exceeds budget %d (cols=%v)", total, width, cols)
		}
	}
}

func TestDataSourceLabelUsesCollectionsForMongo(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}

	if got := m.dataSourceLabel(); got != "collection" {
		t.Fatalf("label = %q, want collection", got)
	}
	if got := m.dataSourceLabelPlural(); got != "collections" {
		t.Fatalf("plural label = %q, want collections", got)
	}
}

func stripANSIForTest(s string) string {
	var b strings.Builder
	skip := false
	for _, r := range s {
		if r == '\x1b' {
			skip = true
			continue
		}
		if skip {
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
				skip = false
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
