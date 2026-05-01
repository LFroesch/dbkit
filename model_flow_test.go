package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"bobdb/internal/completion"
	"bobdb/internal/config"
	"bobdb/internal/db"
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
	m.activeTab = tabBrowse
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

func TestBrowseDataShiftCOpensCopyAsOverlay(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabBrowse
	m.focus = panelRight
	m.browseView = browseViewData
	m.browseData = &db.QueryResult{
		Columns: []string{"id", "email"},
		Rows: [][]string{
			{"1", "alice@example.com"},
			{"2", "bob@example.com"},
		},
	}
	m.syncBrowseDataTable()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'C'}})
	got := next.(Model)

	if !got.showQueryPicker {
		t.Fatalf("expected browse copy picker to open")
	}
	if got.queryPickerTitle != "Copy Browse Row As" {
		t.Fatalf("picker title = %q, want Copy Browse Row As", got.queryPickerTitle)
	}
	if len(got.queryPickerItems) != 4 {
		t.Fatalf("picker item count = %d, want 4", len(got.queryPickerItems))
	}
	if got.queryPickerItems[2].label != "All rows JSON" {
		t.Fatalf("third item = %q, want All rows JSON", got.queryPickerItems[2].label)
	}
	if got.queryPickerItems[3].label != "All rows CSV" {
		t.Fatalf("fourth item = %q, want All rows CSV", got.queryPickerItems[3].label)
	}
}

func TestHandleCLIArgsHelpVersionAndUnknown(t *testing.T) {
	t.Run("help", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code, handled := handleCLIArgs([]string{"--help"}, &stdout, &stderr)
		if !handled {
			t.Fatalf("expected help to be handled")
		}
		if code != 0 {
			t.Fatalf("help exit code = %d, want 0", code)
		}
		if !strings.Contains(stdout.String(), "Usage:") {
			t.Fatalf("help output missing usage: %q", stdout.String())
		}
		if stderr.Len() != 0 {
			t.Fatalf("expected no stderr for help, got %q", stderr.String())
		}
	})

	t.Run("version", func(t *testing.T) {
		oldVersion := version
		version = "1.2.3"
		defer func() { version = oldVersion }()

		var stdout, stderr bytes.Buffer
		code, handled := handleCLIArgs([]string{"version"}, &stdout, &stderr)
		if !handled {
			t.Fatalf("expected version to be handled")
		}
		if code != 0 {
			t.Fatalf("version exit code = %d, want 0", code)
		}
		if strings.TrimSpace(stdout.String()) != "bobdb 1.2.3" {
			t.Fatalf("version output = %q", stdout.String())
		}
		if stderr.Len() != 0 {
			t.Fatalf("expected no stderr for version, got %q", stderr.String())
		}
	})

	t.Run("unknown", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code, handled := handleCLIArgs([]string{"--wat"}, &stdout, &stderr)
		if !handled {
			t.Fatalf("expected unknown arg to be handled")
		}
		if code != 2 {
			t.Fatalf("unknown arg exit code = %d, want 2", code)
		}
		if stdout.Len() != 0 {
			t.Fatalf("expected no stdout for unknown arg, got %q", stdout.String())
		}
		if !strings.Contains(stderr.String(), "unknown argument") {
			t.Fatalf("stderr missing unknown-argument message: %q", stderr.String())
		}
		if !strings.Contains(stderr.String(), "Usage:") {
			t.Fatalf("stderr missing usage: %q", stderr.String())
		}
	})
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

	view := stripANSIForTest(m.renderBrowseDetail(64, 16))
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
	if got := strings.Count(m.renderBrowseDetail(64, 16), "\n") + 1; got > 16 {
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
	m.activeTab = tabBrowse
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
	if !strings.Contains(view, "bobdb") {
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

func TestFormatDisplayValueTrimsTimestampFractionZeros(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"2026-04-18 12:34:56.000000+00", "2026-04-18 12:34:56+00"},
		{"2026-04-18 12:34:56.120000+00", "2026-04-18 12:34:56.12+00"},
		{"2026-04-18T12:34:56.000000Z", "2026-04-18T12:34:56Z"},
		{"2026-04-18 12:34:56.000000 +0000 UTC", "2026-04-18 12:34:56 +0000 UTC"},
		{"2026-04-18 12:34:56", "2026-04-18 12:34:56"},
		{"not-a-timestamp", "not-a-timestamp"},
	}
	for _, tc := range cases {
		if got := formatDisplayValue(tc.in); got != tc.want {
			t.Fatalf("formatDisplayValue(%q) = %q, want %q", tc.in, got, tc.want)
		}
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

func TestCtrlPOpensRecentQueriesOverlay(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.pushQueryHistory("select 1;")
	m.pushQueryHistory("select 2;")

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyCtrlP})
	got := next.(Model)

	if !got.showQueryPicker {
		t.Fatalf("expected recent queries overlay to open")
	}
	if got.queryPickerTitle != "Recent Queries" {
		t.Fatalf("picker title = %q, want Recent Queries", got.queryPickerTitle)
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
	schema := &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "email", Type: "text"},
		},
	}
	m.schemaCache[schemaCacheKey(m.activeConnIdx, "users")] = schema
	m.queryInput.SetValue("SELECT * FROM users WHERE ")
	setTextareaCursor(&m.queryInput, 0, len("SELECT * FROM users WHERE "))

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if !got.showColumnPicker {
		t.Fatalf("expected column picker to open")
	}
	if len(got.columnPickerItems) < 2 {
		t.Fatalf("column picker items = %d, want >= 2", len(got.columnPickerItems))
	}
}

func TestTableFirstFlowScaffoldsAndCursorOnStar(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "orders"}
	m.schemaCache[schemaCacheKey(m.activeConnIdx, "users")] = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "email", Type: "text"},
		},
	}

	// Step 1: empty query + tab → table picker
	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	if !got.showColumnPicker {
		t.Fatalf("expected table picker to open")
	}
	if got.columnPickerTitle != "Select Table" {
		t.Fatalf("picker title = %q, want Select Table", got.columnPickerTitle)
	}
	if !got.columnPickerTableFirst {
		t.Fatalf("expected columnPickerTableFirst to be set")
	}

	// Step 2: select "users" → scaffold query with cursor on *
	next, _ = got.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got = next.(Model)
	if got.showColumnPicker {
		t.Fatalf("picker should be closed after table selection")
	}
	want := "SELECT * FROM \"users\"\nWHERE "
	if got.queryInput.Value() != want {
		t.Fatalf("query = %q, want %q", got.queryInput.Value(), want)
	}
	// Cursor should be at position 7 (on the *)
	cursorIdx := got.queryCursorIndex()
	if cursorIdx != 7 {
		t.Fatalf("cursor index = %d, want 7 (on *)", cursorIdx)
	}

	// Step 3: tab on * → column picker (multi-select)
	next, _ = got.Update(tea.KeyMsg{Type: tea.KeyTab})
	got = next.(Model)
	if !got.showColumnPicker {
		t.Fatalf("expected column picker to open on *")
	}
	if !got.columnPickerMulti {
		t.Fatalf("expected multi-select column picker")
	}
	if got.columnPickerTitle != "Select Columns" {
		t.Fatalf("picker title = %q, want Select Columns", got.columnPickerTitle)
	}
}

func TestSelectOnlyTabUsesTableFirstPicker(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "orders"}
	m.queryInput.SetValue("SELECT ")
	m.queryInput.CursorEnd()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if !got.showColumnPicker {
		t.Fatalf("expected table picker to open for bare SELECT")
	}
	if got.columnPickerTitle != "Select Table" {
		t.Fatalf("picker title = %q, want Select Table", got.columnPickerTitle)
	}
	if !got.columnPickerTableFirst {
		t.Fatalf("expected table-first picker for bare SELECT")
	}
}

func TestTypingInQueryEditorAutoOpensCompletionPicker(t *testing.T) {
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
	m.queryInput.CursorEnd()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeySpace})
	got := next.(Model)

	if !got.showColumnPicker {
		t.Fatalf("expected completion picker to open while typing in context")
	}
	if len(got.columnPickerItems) == 0 {
		t.Fatalf("expected completion items while typing")
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
	m.queryHistoryIdx = 1

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyCtrlL})
	got := next.(Model)

	if got.queryInput.Value() != "" {
		t.Fatalf("query input = %q, want empty", got.queryInput.Value())
	}
	if got.queryHistoryIdx != -1 {
		t.Fatalf("queryHistoryIdx = %d, want -1", got.queryHistoryIdx)
	}
}

func TestCtrlLClearsQueryEditorWhileCompletionPopoverIsOpen(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	schema := &db.TableSchema{
		Name:    "users",
		Columns: []db.ColumnInfo{{Name: "id", Type: "integer"}},
	}
	m.schemaCache[schemaCacheKey(m.activeConnIdx, "users")] = schema
	m.queryInput.SetValue("SELECT * FROM users WHERE ")
	setTextareaCursor(&m.queryInput, 0, len("SELECT * FROM users WHERE "))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyCtrlL})
	got := next.(Model)

	if got.queryInput.Value() != "" {
		t.Fatalf("query input = %q, want empty", got.queryInput.Value())
	}
	if got.showColumnPicker {
		t.Fatalf("expected completion picker to close after clear")
	}
}

func TestQueryReferencePgDownWorksWhileEditorFocused(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.height = 30
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	got := next.(Model)

	if got.queryRefScroll <= 0 {
		t.Fatalf("expected query reference to scroll, got %d", got.queryRefScroll)
	}
	if !got.queryFocus {
		t.Fatalf("expected editor focus to stay active")
	}
}

func TestQueryReferenceEndScrollReachesOperatorTips(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.height = 24
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnd})
	got := next.(Model)

	view := stripANSIForTest(got.renderQueryCheatSheet(got.queryReferenceWidth(), got.queryReferenceViewportHeight()+4))
	if !strings.Contains(view, "Operator tips") {
		t.Fatalf("expected operator tips section to be reachable, got %q", view)
	}
	if !strings.Contains(view, "Accepting > / >= / < / <=") {
		t.Fatalf("expected final operator tip line to be visible, got %q", view)
	}
}

func TestQueryReferenceWrapAwareEndScrollReachesFinalWrappedLine(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 90
	m.height = 24
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnd})
	got := next.(Model)

	view := stripANSIForTest(got.renderQueryCheatSheet(got.queryReferenceWidth(), got.queryReferenceViewportHeight()+4))
	if !strings.Contains(view, "Accepting > / >=") {
		t.Fatalf("expected wrapped operator tip line to remain visible, got %q", view)
	}
}

func TestBackspaceEditsQueryWhileValueSuggestionsAreOpen(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeConnIdx = 1
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue(`SELECT * FROM "users" WHERE "email" = 'ali'`)
	m.showColumnPicker = true
	m.columnPickerTitle = "Values"
	m.columnPickerItems = []completion.Item{
		{Label: "alice@example.com", InsertText: "alice@example.com"},
		{Label: "bob@example.com", InsertText: "bob@example.com"},
	}
	m.columnPickerValueCol = "email"
	m.columnPickerValueTable = "users"
	m.columnValueCache[columnValueKey(1, "users", "email")] = []string{
		"alice@example.com",
		"bob@example.com",
	}
	setTextareaCursor(&m.queryInput, 0, len(`SELECT * FROM "users" WHERE "email" = 'ali`))

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyBackspace})
	got := next.(Model)

	if got.queryInput.Value() != `SELECT * FROM "users" WHERE "email" = 'al'` {
		t.Fatalf("query text should edit in place, got %q", got.queryInput.Value())
	}
	if !got.showColumnPicker {
		t.Fatalf("expected suggestions to stay open")
	}
}

func TestSinglePaneQueryUsesReferenceLabel(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 60
	m.height = 20
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelLeft

	view := stripANSIForTest(m.renderSinglePane(10))
	if !strings.Contains(strings.ToLower(view), "reference") {
		t.Fatalf("expected compact pane label to mention reference, got %q", view)
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

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected column picker to open")
	}
	for i := range m.columnPickerItems {
		if m.columnPickerItems[i].Label == "email" || m.columnPickerItems[i].Label == "created_at" {
			m.columnPickerItems[i].Selected = true
		}
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
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
	if len(got.columnPickerItems) == 0 || got.columnPickerItems[0].Label != "find" {
		t.Fatalf("expected find command suggestion, got %#v", got.columnPickerItems)
	}
}

func TestMongoFindCompletionInsertsLiteralQuery(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}
	if m.columnPickerItems[0].Label != "find" {
		t.Fatalf("top suggestion = %q, want find", m.columnPickerItems[0].Label)
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if got.queryInput.Value() != "db.collection.find({})" {
		t.Fatalf("query input = %q, want shell-format find starter", got.queryInput.Value())
	}
}

func TestStarterCompletionInsertsLiteralQuery(t *testing.T) {
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

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}
	m.columnPickerCursor = 0

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if got.queryInput.Value() != "SELECT *\nFROM \"table_name\"\nLIMIT 50;" {
		t.Fatalf("expected literal starter query, got %q", got.queryInput.Value())
	}
}

func TestTabWithoutCompletionDoesNotEnterSnippetMode(t *testing.T) {
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
			{Name: "id", Type: "integer"},
			{Name: "email", Type: "text"},
		},
	}
	m.queryInput.SetValue(`SELECT * FROM "users";`)
	setTextareaCursor(&m.queryInput, 0, len(`SELECT * FROM "users";`))

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if got.showColumnPicker {
		t.Fatalf("expected no completion picker in neutral query position")
	}
}

func TestLimitContextSuggestsNumericValues(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue("SELECT * FROM users LIMIT ")
	setTextareaCursor(&m.queryInput, 0, len("SELECT * FROM users LIMIT "))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected limit completion to open")
	}
	if m.columnPickerTitle != "Limit" {
		t.Fatalf("picker title = %q, want Limit", m.columnPickerTitle)
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["50"] || !found["100"] {
		t.Fatalf("expected numeric suggestions, got %v", found)
	}
}

func TestInsertValuesContextStaysFreeForm(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue(`INSERT INTO "users" ("email") VALUES (`)
	setTextareaCursor(&m.queryInput, 0, len(`INSERT INTO "users" ("email") VALUES (`))

	if ok, _ := m.openCompletionForCursor(true); ok {
		t.Fatalf("expected values context to stay free-form, got picker %#v", m.columnPickerItems)
	}
}

func TestMongoSortContextOpensAfterLiteralFilterAndLimit(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeConnIdx = 0
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "created_at", Type: "date"},
		},
	}
	query := `find users {} 50 `
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(query))

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	if !got.showColumnPicker {
		t.Fatalf("expected sort completion picker to open")
	}
	if got.columnPickerTitle != "Sort" {
		t.Fatalf("picker title = %q, want Sort", got.columnPickerTitle)
	}
	found := map[string]bool{}
	for _, item := range got.columnPickerItems {
		found[item.Label] = true
	}
	if !found["created_at desc"] || !found["status asc"] {
		t.Fatalf("expected sort suggestions, got %v", found)
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

func TestSubmitNewConnEditsExistingConnection(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	cfg := &config.Config{
		Connections: []config.Connection{{ID: "abc123", Name: "main", Type: "sqlite", DSN: "test.db"}},
	}
	m := newModel(cfg)
	m.openEditConnForm(0)
	m.newConnInputs[fieldName].SetValue("warehouse")
	m.newConnTypeCur = indexOfString(dbTypes, "postgres")
	m.newConnInputs[fieldDSN].SetValue("postgres://user:pass@localhost:5432/warehouse")

	next, _ := m.submitNewConn()
	got := next.(Model)

	if len(got.cfg.Connections) != 1 {
		t.Fatalf("connections = %d, want 1", len(got.cfg.Connections))
	}
	if got.cfg.Connections[0].Name != "warehouse" {
		t.Fatalf("connection name = %q, want warehouse", got.cfg.Connections[0].Name)
	}
	if got.cfg.Connections[0].Type != "postgres" {
		t.Fatalf("connection type = %q, want postgres", got.cfg.Connections[0].Type)
	}
	if got.newConnEditIdx != -1 {
		t.Fatalf("newConnEditIdx = %d, want -1", got.newConnEditIdx)
	}
}

func TestBrowseEnterSwitchesToDataView(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabBrowse
	m.tables = []string{"users"}
	m.tableCursor = 0

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	if got.activeTab != tabBrowse {
		t.Fatalf("active tab = %v, want browse tab", got.activeTab)
	}
	if got.browseView != browseViewData {
		t.Fatalf("browse view = %v, want data view", got.browseView)
	}
	if got.focus != panelRight {
		t.Fatalf("focus = %v, want right panel", got.focus)
	}
	if cmd == nil {
		t.Fatalf("expected browse data load command")
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

func TestSlashOpensQueryTabAndFocusesEditor(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabBrowse
	m.focus = panelLeft

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	got := next.(Model)

	if got.activeTab != tabQuery {
		t.Fatalf("active tab = %v, want query tab", got.activeTab)
	}
	if !got.queryFocus {
		t.Fatalf("expected query editor focus")
	}
	if got.focus != panelRight {
		t.Fatalf("focus = %v, want right panel", got.focus)
	}
}

func TestResultsTabQNavigatesBackEvenIfQueryFocusIsStale(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabResults
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	got := next.(Model)

	if got.activeTab != tabQuery {
		t.Fatalf("active tab = %v, want query tab", got.activeTab)
	}
	if !got.queryFocus {
		t.Fatalf("expected query editor to be focused after navigating back")
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

func TestCompletionSuppressedInsideStringLiteral(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	// No matching column before the quote so value-completion is also skipped.
	m.queryInput.SetValue(`SELECT 'abc`)
	setTextareaCursor(&m.queryInput, 0, len(`SELECT 'abc`))

	ok, _ := m.openCompletionForCursor(true)
	if ok {
		t.Fatalf("expected completion to be suppressed inside string literal, but picker opened with items: %#v", m.columnPickerItems)
	}
}

func TestSQLKeywordCompletionIncludesTablesWhenEmpty(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "orders"}
	m.tableCursor = 0
	m.queryInput.SetValue("")

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open on empty editor")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["users"] || !found["orders"] {
		t.Fatalf("expected table names as starters, got %v", found)
	}
}

func TestValueCompletionUsesCachedSamples(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name:    "users",
		Columns: []db.ColumnInfo{{Name: "email", Type: "text"}},
	}
	key := columnValueKey(m.activeConnIdx, "users", "email")
	m.columnValueCache[key] = []string{"a@x.com", "b@x.com"}
	m.queryInput.SetValue(`SELECT * FROM users WHERE email = '`)
	setTextareaCursor(&m.queryInput, 0, len(`SELECT * FROM users WHERE email = '`))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected value completion to open")
	}
	if m.columnPickerTitle != "Values for email" {
		t.Fatalf("title = %q, want Values for email", m.columnPickerTitle)
	}
	labels := map[string]bool{}
	for _, item := range m.columnPickerItems {
		labels[item.Label] = true
	}
	if !labels["a@x.com"] || !labels["b@x.com"] {
		t.Fatalf("expected cached sample values, got %v", labels)
	}
}

func TestClosingQuotedSQLValueDoesNotReopenCompletion(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"posts"}
	m.schemaCache[schemaCacheKey(m.activeConnIdx, "posts")] = &db.TableSchema{
		Name: "posts",
		Columns: []db.ColumnInfo{
			{Name: "id", Type: "integer"},
			{Name: "createdAt", Type: "timestamp"},
		},
	}
	m.columnValueCache[columnValueKey(m.activeConnIdx, "posts", "createdAt")] = []string{"2026-04-09T15:51:30Z"}
	m.queryInput.SetValue(`SELECT "id" FROM "posts" WHERE "createdAt" >= '`)
	setTextareaCursor(&m.queryInput, 0, len(`SELECT "id" FROM "posts" WHERE "createdAt" >= '`))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected value completion to open")
	}
	if m.columnPickerTitle != "Values for createdAt" {
		t.Fatalf("picker title = %q, want Values for createdAt", m.columnPickerTitle)
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	if got.showColumnPicker {
		t.Fatalf("picker should close after inserting selected value")
	}

	next, _ = got.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'\''}})
	got = next.(Model)
	if got.showColumnPicker {
		t.Fatalf("closing quote should not reopen completion, got %#v", got.columnPickerItems)
	}
}

func TestSQLOperatorCompletionOffersComparisonAndNullChecks(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue(`SELECT * FROM users WHERE email `)
	setTextareaCursor(&m.queryInput, 0, len(`SELECT * FROM users WHERE email `))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected operator completion to open")
	}
	if m.columnPickerTitle != "Operator" {
		t.Fatalf("title = %q, want Operator", m.columnPickerTitle)
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["IS NULL"] || !found[">="] || !found["LIKE"] {
		t.Fatalf("expected operator suggestions, got %v", found)
	}
}

func TestEnterKeepsEditingWhenCompletionPopoverIsOpen(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	schema := &db.TableSchema{
		Name:    "users",
		Columns: []db.ColumnInfo{{Name: "email", Type: "text"}},
	}
	m.schemaCache[schemaCacheKey(m.activeConnIdx, "users")] = schema
	m.queryInput.SetValue(`SELECT * FROM users WHERE `)
	setTextareaCursor(&m.queryInput, 0, len(`SELECT * FROM users WHERE `))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}
	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	// Enter inserts a newline (keeps editing) rather than selecting an item
	if got.queryInput.Value() != "SELECT * FROM users WHERE \n" {
		t.Fatalf("query input = %q, want newline insertion", got.queryInput.Value())
	}
}

func TestStarterInsertionDoesNotCreateSnippetSessionHint(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.tableSchema = &db.TableSchema{
		Name:    "users",
		Columns: []db.ColumnInfo{{Name: "id", Type: "integer"}},
	}

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}
	// Find the SELECT starter and select it.
	for i, item := range m.columnPickerItems {
		if item.Label == "SELECT starter" {
			m.columnPickerCursor = i
			break
		}
	}
	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	_ = next.(Model)
}

func TestInlineCompletionPopoverKeepsEditorVisible(t *testing.T) {
	m := newModel(&config.Config{})
	m.width = 120
	m.height = 40
	m.cfg = &config.Config{}
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "orders"}
	m.tableCursor = 0
	m.queryInput.SetValue("SELECT * FROM ")
	setTextareaCursor(&m.queryInput, 0, len("SELECT * FROM "))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}
	rendered := stripANSIForTest(m.View())
	if !strings.Contains(rendered, "SELECT * FROM") {
		t.Fatalf("expected editor content visible alongside popover, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "From Table") {
		t.Fatalf("expected popover title rendered inline, got:\n%s", rendered)
	}
}

func TestMongoCommandCompletionDoesNotOfferHelp(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0
	m.queryInput.SetValue("")

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo completion picker to open")
	}
	for _, item := range m.columnPickerItems {
		if item.Label == "help" {
			t.Fatalf("unexpected help completion item: %#v", item)
		}
	}
}

func TestLimitClauseSuggestsNumericValues(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue("SELECT * FROM users LIMIT ")

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected limit completion picker to open")
	}
	if m.columnPickerTitle != "Limit" {
		t.Fatalf("picker title = %q, want Limit", m.columnPickerTitle)
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["50"] || !found["100"] {
		t.Fatalf("expected numeric limit suggestions, got %v", found)
	}
}

func TestOrderByDirectionSuggestionAfterColumn(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue(`SELECT * FROM "users" ORDER BY "created_at" `)

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected order direction completion picker to open")
	}
	if m.columnPickerTitle != "Order Direction" {
		t.Fatalf("picker title = %q, want Order Direction", m.columnPickerTitle)
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["ASC"] || !found["DESC"] {
		t.Fatalf("expected ASC/DESC suggestions, got %v", found)
	}
}

func TestTemplateSelectionAutoOpensPlaceholderCompletion(t *testing.T) {
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
			{Name: "id", Type: "integer"},
			{Name: "email", Type: "text"},
		},
	}
	m.queryPickerItems = []queryPickerItem{
		{label: "Template", value: `SELECT * FROM "users" WHERE `, kind: ""},
	}
	m.showQueryPicker = true

	next, _ := m.updateQueryPicker(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)
	if !got.showColumnPicker {
		t.Fatalf("expected completion picker to auto-open after loading template")
	}
	if got.columnPickerTitle != "Filter Column" {
		t.Fatalf("picker title = %q, want Filter Column", got.columnPickerTitle)
	}
}

func TestMongoFilterJSONSuggestsSchemaFields(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "email", Type: "string"},
		},
	}
	m.queryInput.SetValue(`find users {`)

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo filter completion to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["status"] || !found["email"] {
		t.Fatalf("expected schema field suggestions, got %v", found)
	}
}

func TestMongoFilterJSONSuggestsTopLevelOperators(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
		},
	}
	m.queryInput.SetValue(`find users {`)

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo filter completion to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["$or"] || !found["$and"] {
		t.Fatalf("expected top-level operator suggestions, got %v", found)
	}
}

func TestMongoFieldCompletionReplacesOnlyCurrentKey(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
			{Name: "status", Type: "string"},
		},
	}
	query := `find users {"em"}`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(`find users {"em`))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected mongo field completion to open")
	}
	found := false
	for i, item := range m.columnPickerItems {
		if item.Label == "email" {
			m.columnPickerCursor = i
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected email suggestion, got %#v", m.columnPickerItems)
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	want := `find users {"email":""}`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestMongoUpdateOperatorCompletionPreservesSurroundingObject(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	query := `update users {} {"$s"}`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(`update users {} {"$s`))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected mongo update operator completion to open")
	}
	found := false
	for i, item := range m.columnPickerItems {
		if item.Label == "$set" {
			m.columnPickerCursor = i
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected $set suggestion, got %#v", m.columnPickerItems)
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	want := `update users {} {"$set":{}}`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestMongoFilterCompletionUsesCollectionSchemaCacheAfterCollectionChange(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeConnIdx = 0
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
		},
	}
	m.schemaCache[schemaCacheKey(0, "comments")] = &db.TableSchema{
		Name: "comments",
		Columns: []db.ColumnInfo{
			{Name: "post_id", Type: "string"},
			{Name: "author", Type: "string"},
		},
	}
	m.queryInput.SetValue(`find comments {`)

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo filter completion to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["post_id"] || !found["author"] {
		t.Fatalf("expected cached comments fields, got %v", found)
	}
}

func TestMongoTypingCommandPrefixAutoOpensCompletion(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.queryInput.SetValue("")

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	got := next.(Model)
	if !got.showColumnPicker {
		t.Fatalf("expected command completion to open while typing")
	}
	labels := map[string]bool{}
	for _, item := range got.columnPickerItems {
		labels[item.Label] = true
	}
	if !labels["find"] {
		t.Fatalf("expected find command suggestion, got %v", labels)
	}
}

func TestExtractTableFromQueryHandlesMongoReadCommands(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{query: `find users {"email":"a@x.com"} 20`, want: "users"},
		{query: `count events {}`, want: "events"},
		{query: `aggregate audit [{"$limit":5}]`, want: "audit"},
		{query: `agg logs [{"$limit":5}]`, want: "logs"},
	}
	for _, tc := range cases {
		if got := extractTableFromQuery(tc.query); got != tc.want {
			t.Fatalf("extractTableFromQuery(%q) = %q, want %q", tc.query, got, tc.want)
		}
	}
}

func TestMongoFieldFilterPlaceholderUsesTypedLiteralForBool(t *testing.T) {
	req := completion.Request{
		Query:  "db.comments.find({",
		Cursor: len("db.comments.find({"),
		DBType: "mongo",
		Tables: []string{"comments"},
		Schema: &completion.SchemaInfo{
			Name: "comments",
			Columns: []completion.ColumnInfo{
				{Name: "isDemo", Type: "bool"},
			},
		},
		InferredTable: "comments",
	}
	result := completion.Complete(req)
	if result == nil {
		t.Fatalf("expected completion result")
	}
	found := false
	for _, item := range result.Items {
		if item.Label == "isDemo" && strings.Contains(item.InsertText, `"isDemo":false`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected bool field scaffold to default to unquoted false, got %#v", result.Items)
	}
}

func TestMongoTypedJSONLiteralUsesBoolWithoutQuotes(t *testing.T) {
	if got := completion.MongoTypedJSONLiteral("bool", "true"); got != "true" {
		t.Fatalf("bool true literal = %q, want true", got)
	}
	if got := completion.MongoTypedJSONLiteral("bool", "false"); got != "false" {
		t.Fatalf("bool false literal = %q, want false", got)
	}
	if got := completion.MongoTypedJSONLiteral("string", "true"); got != `"true"` {
		t.Fatalf("string true literal = %q, want quoted true", got)
	}
}

func TestMongoTypedJSONLiteralHandlesObjectIDDateAndObject(t *testing.T) {
	if got := completion.MongoTypedJSONLiteral("objectId", "507f1f77bcf86cd799439011"); got != `{"$oid":"507f1f77bcf86cd799439011"}` {
		t.Fatalf("objectId literal = %q", got)
	}
	if got := completion.MongoTypedJSONLiteral("date", "2026-04-14T12:00:00Z"); !strings.Contains(got, `"$date"`) {
		t.Fatalf("date literal should emit $date extjson, got %q", got)
	}
	if got := completion.MongoTypedJSONLiteral("object", `{"a":1}`); got != `{"a":1}` {
		t.Fatalf("object literal should preserve json object, got %q", got)
	}
}

func TestMongoPlaceholderForComplexTypes(t *testing.T) {
	if got := completion.MongoPlaceholderForType("objectId"); got != `{"$oid":"000000000000000000000000"}` {
		t.Fatalf("objectId placeholder = %q", got)
	}
	if got := completion.MongoPlaceholderForType("date"); got != `{"$date":"2026-01-01T00:00:00Z"}` {
		t.Fatalf("date placeholder = %q", got)
	}
	if got := completion.MongoPlaceholderForType("map"); got != "{}" {
		t.Fatalf("map placeholder = %q", got)
	}
}

func TestMongoBoolValueCompletionOffersTrueFalseBeforeSamples(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeConnIdx = 0
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "comments",
		Columns: []db.ColumnInfo{
			{Name: "isDemo", Type: "bool"},
		},
	}
	m.queryInput.SetValue(`find comments {"isDemo":tr`)
	setTextareaCursor(&m.queryInput, 0, len(`find comments {"isDemo":tr`))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected mongo bool value completion to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["true"] {
		t.Fatalf("expected bool literal suggestions, got %v", found)
	}
}

func TestMongoNestedOperatorCompletionSuggestsComparisonOperators(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "age", Type: "number"},
		},
	}
	m.queryInput.SetValue(`find users {"age":{"$g`)
	setTextareaCursor(&m.queryInput, 0, len(`find users {"age":{"$g`))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected mongo nested operator completion to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["$gt"] || !found["$gte"] {
		t.Fatalf("expected comparison operator suggestions, got %v", found)
	}
}

func TestMongoOperatorCompletionPreservesExistingValueWhenSwitchingOperators(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
		},
	}
	query := `find users {"email":{"$regex":"@gmail.com"}}`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(`find users {"email":{"$re`))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected mongo operator completion to open")
	}
	found := false
	for i, item := range m.columnPickerItems {
		if item.Label == "$in" {
			m.columnPickerCursor = i
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected $in suggestion, got %#v", m.columnPickerItems)
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)

	want := `find users {"email":{"$in":["@gmail.com"]}}`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestMongoValueCompletionReplacesOnlyValueLiteral(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeConnIdx = 0
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
		},
	}
	key := columnValueKey(0, "users", "email")
	m.columnValueCache[key] = []string{"alice@gmail.com", "bob@yahoo.com"}
	query := `find users {"email":"@gm"}`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(`find users {"email":"@gm`))

	ok, _ := m.openCompletionForCursor(true)
	if !ok {
		t.Fatalf("expected mongo value completion to open")
	}
	found := false
	for i, item := range m.columnPickerItems {
		if item.Label == "alice@gmail.com" {
			m.columnPickerCursor = i
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected cached mongo value suggestion, got %#v", m.columnPickerItems)
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	want := `find users {"email":"alice@gmail.com"}`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestExamplesPickerUsesBackendAwareExamples(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.tableCursor = 0

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyCtrlE})
	got := next.(Model)

	if !got.showQueryPicker {
		t.Fatalf("expected examples picker to open")
	}
	if got.queryPickerTitle != "Examples" {
		t.Fatalf("picker title = %q, want Examples", got.queryPickerTitle)
	}
	labels := map[string]bool{}
	for _, item := range got.queryPickerItems {
		labels[item.label] = true
	}
	if !labels["Reference: use current collection"] || !labels["Read: find top documents"] || !labels["Filter: nested operator"] {
		t.Fatalf("expected mongo examples, got %v", labels)
	}
}

func TestMongoShellFormatFilterCompletionSuggestsFields(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
			{Name: "status", Type: "string"},
		},
	}
	// Shell format with cursor inside the filter object
	m.queryInput.SetValue(`db.users.find({`)
	setTextareaCursor(&m.queryInput, 0, len(`db.users.find({`))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected shell-format filter completion to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["email"] || !found["status"] {
		t.Fatalf("expected schema field suggestions in shell format, got %v", found)
	}
}

func TestMongoShellFormatCollectionSwitchRebuildsQuery(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "comments", "events"}
	m.queryInput.SetValue(`db.users.find({})`)
	// Place cursor at start of collection (right after "db.")
	setTextareaCursor(&m.queryInput, 0, 3) // at "db.|users..."

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected collection completion to open")
	}
	if m.columnPickerTitle != "Collections" {
		t.Fatalf("expected Collections picker, got %q", m.columnPickerTitle)
	}
	// All collections should be available (no prefix filter at cursor start)
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["comments"] || !found["events"] || !found["users"] {
		t.Fatalf("expected all collections in picker, got %v", found)
	}
	// Selecting "comments" should rebuild the full shell expression
	for i, item := range m.columnPickerItems {
		if item.Label == "comments" {
			m.columnPickerCursor = i
			break
		}
	}
	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	if got.queryInput.Value() != "db.comments.find({})" {
		t.Fatalf("expected collection switch to rebuild query, got %q", got.queryInput.Value())
	}
}

func TestMongoFindOneCollectionSwitchRebuildsQuery(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "comments", "events"}
	m.queryInput.SetValue(`db.users.findOne({})`)
	setTextareaCursor(&m.queryInput, 0, 3)

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected collection completion to open")
	}
	for i, item := range m.columnPickerItems {
		if item.Label == "comments" {
			m.columnPickerCursor = i
			break
		}
	}
	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	if got.queryInput.Value() != "db.comments.findOne({})" {
		t.Fatalf("expected findOne collection switch to preserve method case, got %q", got.queryInput.Value())
	}
}

func TestMongoFindProjectionPickerOpensAtEndOfSingleArgFind(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
		},
	}
	query := `db.users.find({"status":"active"})`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(query))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo projection completion to open")
	}
	if m.columnPickerTitle != "Project Fields" {
		t.Fatalf("picker title = %q, want Project Fields", m.columnPickerTitle)
	}
	if !m.columnPickerMulti {
		t.Fatalf("expected multi-select projection picker")
	}
}

func TestMongoFindOneFilterCompletionSuggestsFields(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
			{Name: "status", Type: "string"},
		},
	}
	m.queryInput.SetValue(`db.users.findOne({`)
	setTextareaCursor(&m.queryInput, 0, len(`db.users.findOne({`))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected findOne filter completion to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["email"] || !found["status"] {
		t.Fatalf("expected schema field suggestions in findOne filter, got %v", found)
	}
}

func TestMongoFindProjectionPickerInsertsSecondArg(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
		},
	}
	query := `db.users.find({"status":"active"})`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(query))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo projection completion to open")
	}
	for i, item := range m.columnPickerItems {
		if item.Label == "email" || item.Label == "created_at" {
			m.columnPickerItems[i].Selected = true
		}
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	want := `db.users.find({"status":"active"}, {"email":1, "created_at":1})`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestMongoFindProjectionPickerReopensForExistingProjection(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
			{Name: "status", Type: "string"},
		},
	}
	query := `db.users.find({}, {"email":1,"created_at":1})`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(`db.users.find({}, {"email":1`))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo projection completion to open")
	}
	selected := map[string]bool{}
	for _, item := range m.columnPickerItems {
		selected[item.Label] = item.Selected
	}
	if !selected["email"] || !selected["created_at"] {
		t.Fatalf("expected existing projection selections to stay selected, got %#v", m.columnPickerItems)
	}
}

func TestMongoFindOneProjectionPickerInsertsSecondArg(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
		},
	}
	query := `db.users.findOne({"status":"active"})`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(query))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected mongo projection completion to open")
	}
	for i, item := range m.columnPickerItems {
		if item.Label == "email" || item.Label == "created_at" {
			m.columnPickerItems[i].Selected = true
		}
	}

	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	want := `db.users.findOne({"status":"active"}, {"email":1, "created_at":1})`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestMongoAggregateStageCompletionOpensAtPipelineStart(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
		},
	}
	query := `db.users.aggregate([`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(query))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected aggregate stage completion to open")
	}
	if m.columnPickerTitle != "Aggregate Stage" {
		t.Fatalf("picker title = %q, want Aggregate Stage", m.columnPickerTitle)
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["$match"] || !found["$project"] || !found["$group"] {
		t.Fatalf("expected aggregate stage suggestions, got %v", found)
	}
}

func TestMongoAggregateStageCompletionInsertsSingleStage(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tableSchema = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "status", Type: "string"},
			{Name: "created_at", Type: "timestamp"},
		},
	}
	query := `db.users.aggregate([`
	m.queryInput.SetValue(query)
	setTextareaCursor(&m.queryInput, 0, len(query))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected aggregate stage completion to open")
	}
	for i, item := range m.columnPickerItems {
		if item.Label == "$match" {
			m.columnPickerCursor = i
			break
		}
	}
	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyTab})
	got := next.(Model)
	want := `db.users.aggregate([{"$match":{"status":""}}`
	if got.queryInput.Value() != want {
		t.Fatalf("query input = %q, want %q", got.queryInput.Value(), want)
	}
}

func TestMongoPrefetchFiresLoadWhenCollectionChanges(t *testing.T) {
	// When the user types `db.users` with `comments` as the active tableSchema
	// and no cached `users` schema, prefetchInferredSchema should fire a load.
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeConnIdx = 0
	m.tables = []string{"users", "orders", "comments"}
	m.tableSchema = &db.TableSchema{
		Name:    "comments",
		Columns: []db.ColumnInfo{{Name: "body", Type: "string"}},
	}

	m.queryInput.SetValue("db.users.find({})")
	cmd := m.prefetchInferredSchema()
	if cmd == nil {
		t.Fatalf("expected a schema prefetch command for `users`")
	}
	// Pending flag should be set so a second call returns nil.
	if !m.schemaPending[schemaCacheKey(0, "users")] {
		t.Fatalf("expected schemaPending[users] to be true after prefetch")
	}
	if cmd := m.prefetchInferredSchema(); cmd != nil {
		t.Fatalf("expected second prefetch to be deduped while load is in flight")
	}
}

func TestMongoPrefetchSkipsUnknownHalfTypedCollection(t *testing.T) {
	// Typing `db.us` shouldn't fire a schema load for "us" — only exact table matches.
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeConnIdx = 0
	m.tables = []string{"users", "comments"}
	m.queryInput.SetValue("db.us")
	if cmd := m.prefetchInferredSchema(); cmd != nil {
		t.Fatalf("expected no prefetch for unknown partial name `us`")
	}
}

func TestMongoShellCollectionSwapUsesDifferentSchema(t *testing.T) {
	// Simulates the user's complaint: starting with db.comments.find({...}), then
	// switching to db.users.find({...}) — the completion must use users' schema,
	// not the previously-loaded comments schema.
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	m.activeConnIdx = 0
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users", "comments"}

	// Left panel is still on "comments" (not manually switched)
	m.tableSchema = &db.TableSchema{
		Name: "comments",
		Columns: []db.ColumnInfo{
			{Name: "body", Type: "string"},
			{Name: "author_id", Type: "objectId"},
		},
	}
	// Users schema is pre-cached (simulating a prior load)
	m.schemaCache[schemaCacheKey(0, "users")] = &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnInfo{
			{Name: "email", Type: "string"},
			{Name: "isDemo", Type: "bool"},
		},
	}

	// User edits query to target users, cursor inside the filter object
	m.queryInput.SetValue(`db.users.find({`)
	setTextareaCursor(&m.queryInput, 0, len(`db.users.find({`))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}
	found := map[string]bool{}
	for _, item := range m.columnPickerItems {
		found[item.Label] = true
	}
	if !found["email"] || !found["isDemo"] {
		t.Fatalf("expected users schema fields (email/isDemo), got %v", found)
	}
	if found["body"] || found["author_id"] {
		t.Fatalf("should not show comments fields after collection swap, got %v", found)
	}
}

func TestRankCompletionItemsMatchesContainsPrefixForDomains(t *testing.T) {
	items := []completion.Item{
		{Label: "alice@gmail.com", InsertText: "alice@gmail.com"},
		{Label: "bob@yahoo.com", InsertText: "bob@yahoo.com"},
	}
	ranked := completion.RankItems("@gmail", items)
	if len(ranked) == 0 {
		t.Fatalf("expected contains match for @gmail")
	}
	if ranked[0].Label != "alice@gmail.com" {
		t.Fatalf("top suggestion = %q, want gmail address", ranked[0].Label)
	}
}

func TestLeftRightMovesQueryCursorWhilePickerOpen(t *testing.T) {
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "sqlite"}
	m.activeTab = tabQuery
	m.focus = panelRight
	m.queryFocus = true
	m.queryInput.Focus()
	m.tables = []string{"users"}
	m.queryInput.SetValue(`SELECT * FROM users`)
	setTextareaCursor(&m.queryInput, 0, len(`SELECT * FROM users`))

	if ok, _ := m.openCompletionForCursor(true); !ok {
		t.Fatalf("expected completion picker to open")
	}
	before := m.queryCursorIndex()
	next, _ := m.updateColumnPicker(tea.KeyMsg{Type: tea.KeyLeft})
	got := next.(Model)
	after := got.queryCursorIndex()
	if after >= before {
		t.Fatalf("expected cursor to move left while picker open (before=%d after=%d)", before, after)
	}
}

// Mongo shell-syntax write commands must require confirmation — bare
// method-name matching was regressing because the original check split on
// whitespace and the shell form has no spaces until inside the args.
func TestMongoShellWriteRequiresConfirmation(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"db.users.insertOne({})", true},
		{"db.users.insertMany([{}])", true},
		{"db.users.updateOne({}, {\"$set\": {}})", true},
		{"db.users.updateMany({}, {})", true},
		{"db.users.replaceOne({}, {})", true},
		{"db.users.deleteOne({})", true},
		{"db.users.deleteMany({})", true},
		{"db.users.findOneAndUpdate({}, {})", true},
		{"db.users.findOneAndDelete({})", true},
		{"db.users.bulkWrite([])", true},
		{"db.users.drop()", true},
		{"db.users.find({})", false},
		{"db.users.findOne({})", false},
		{"db.users.aggregate([])", false},
		{"db.users.countDocuments({})", false},
		{"insert users {}", true},
		{"find users {}", false},
	}
	m := newModel(&config.Config{})
	m.activeDB = &fakeDB{dbType: "mongo"}
	for _, tc := range cases {
		if got := m.queryNeedsConfirmation(tc.query); got != tc.want {
			t.Errorf("queryNeedsConfirmation(%q) = %v, want %v", tc.query, got, tc.want)
		}
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
