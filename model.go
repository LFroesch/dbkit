package main

import (
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"dbkit/internal/completion"
	"dbkit/internal/config"
	"dbkit/internal/db"
)

type tab int

const (
	tabConnections tab = iota
	tabBrowse
	tabQuery
	tabResults
	tabHistory
	tabHelpers
	tabCount
)

var tabNames = [tabCount]string{"Connections", "Browse", "Query", "Results", "History", "Helpers"}
var primaryTabs = []tab{tabConnections, tabBrowse, tabQuery, tabResults}

type queryHelper struct {
	label    string
	template string
	kind     string
}

type queryPickerItem struct {
	label      string
	detail     string
	value      string
	kind       string
	sectionRow bool
}

type confirmAction int

const (
	confirmNone confirmAction = iota
	confirmDeleteConnection
	confirmRunQuery
)

type panel int

const (
	panelLeft panel = iota
	panelRight
)

type browseView int

const (
	browseViewSchema browseView = iota
	browseViewData
)

// Async messages
type connectedMsg struct {
	reqID   int
	connIdx int
	conn    config.Connection
	db      db.DB
	err     error
}
type tablesLoadedMsg struct {
	reqID  int
	tables []string
	err    error
}
type schemaLoadedMsg struct {
	reqID  int
	table  string
	schema *db.TableSchema
	err    error
}
type queryDoneMsg struct {
	reqID       int
	query       string
	result      *db.QueryResult
	err         error
	autoRefresh bool
}
type columnValuesMsg struct {
	reqID   int
	connIdx int
	table   string
	column  string
	values  []string
	err     error
}
type browseDataLoadedMsg struct {
	reqID  int
	table  string
	result *db.QueryResult
	err    error
}
type ollamaQueryDoneMsg struct {
	query string
	err   error
}

// New connection form field indices
const (
	fieldName = iota
	fieldDSN
	fieldCount
)

// New connection form focus positions.
const (
	newConnFocusName = iota
	newConnFocusType
	newConnFocusDSN
	newConnFocusSave
	newConnFocusCount
)

var dbTypes = []string{"sqlite", "postgres", "mongo"}

type Model struct {
	width, height int

	activeTab tab
	focus     panel

	// Config / saved connections
	cfg        *config.Config
	connCursor int

	// Active DB
	activeDB       db.DB
	activeConnIdx  int
	activeConnName string

	// New connection form
	newConnInputs  [fieldCount]textinput.Model
	newConnTypeCur int // index into dbTypes
	newConnFocus   int // one of newConnFocus*
	newConnEditIdx int

	// Browse tab
	tables              []string
	tableCursor         int
	tableSchema         *db.TableSchema
	schemaTable         table.Model
	browseView          browseView
	browseData          *db.QueryResult
	browseDataTable     table.Model
	browseDataTableName string
	browseDataReqID     int
	browseColOffset     int
	browseVisibleColumn int

	// Query tab
	queryInput          textarea.Model
	queryResult         *db.QueryResult
	queryErr            string
	queryFocus          bool // true = textarea focused
	resultTable         table.Model
	resultColOffset     int
	resultVisibleColumn int
	queryHistory        []string
	savedQueries        []config.SavedQuery
	queryHistoryIdx     int
	lastRunQuery        string
	querySourceTable    string

	// History tab
	historyCursor int

	// Helpers tab
	helperCursor int

	// Status bar
	statusMsg    string
	statusExpiry time.Time

	// Loading states
	loading bool

	connectReqID int
	tablesReqID  int
	schemaReqID  int
	queryReqID   int
	valuesReqID  int

	// Column sample value cache for value completions.
	// Keyed by "connIdx|table|column".
	columnValueCache   map[string][]string
	columnValuePending map[string]bool
	// Schema cache keyed by "connIdx|table" so query completions can resolve
	// fields for non-focused tables/collections.
	schemaCache map[string]*db.TableSchema
	// Tracks in-flight cache-only schema loads to avoid duplicate requests.
	schemaPending map[string]bool

	// Modal overlay: new connection form visible
	showNewConn bool
	// Modal overlay: help
	showHelp bool
	// Modal overlay: query picker
	showQueryPicker   bool
	queryPickerTitle  string
	queryPickerItems  []queryPickerItem
	queryPickerCursor int
	// Modal overlay: column picker
	showColumnPicker     bool
	columnPickerTitle    string
	columnPickerItems    []completion.Item
	columnPickerCursor   int
	columnPickerMulti    bool
	columnPickerStart    int
	columnPickerEnd      int
	columnPickerFallback string
	// Value-completion mode: typing filters the list without inserting into the query.
	columnPickerValueMode   bool
	columnPickerValuePrefix string
	columnPickerValueCursor int
	columnPickerValueCol    string
	columnPickerValueTable  string
	// Modal overlay: inspect selected row/value details
	showInspect   bool
	inspectTitle  string
	inspectLines  []string
	inspectCopy   string
	inspectScroll int
	// Modal overlay: destructive action confirmation
	showConfirm    bool
	confirmTitle   string
	confirmBody    []string
	confirmAccept  string
	confirmAction  confirmAction
	confirmConnIdx int
	confirmQuery   string

	// Last copied text for fallback/status purposes
	lastCopied string

	// Ollama query generator modal
	showOllamaGen    bool
	ollamaInput      textinput.Model
	ollamaResult     string
	ollamaGenerating bool
	ollamaErr        string
}

func newModel(cfg *config.Config) Model {
	// New connection inputs
	var inputs [fieldCount]textinput.Model
	inputs[fieldName] = textinput.New()
	inputs[fieldName].Placeholder = "My Database"
	inputs[fieldName].CharLimit = 64

	inputs[fieldDSN] = textinput.New()
	inputs[fieldDSN].Placeholder = "/path/to/db.sqlite or postgres://user:pass@host/db"
	inputs[fieldDSN].CharLimit = 2048

	// Query textarea
	ta := textarea.New()
	ta.Placeholder = "Write SQL — ctrl+r runs · tab completes"
	ta.ShowLineNumbers = false
	ta.SetWidth(60)
	ta.SetHeight(6)

	schemaTable := table.New()
	schemaTable.SetStyles(newTableStyles())
	schemaTable.Blur()

	resultTable := table.New()
	resultTable.SetStyles(newTableStyles())
	resultTable.Blur()

	browseDataTable := table.New()
	browseDataTable.SetStyles(newTableStyles())
	browseDataTable.Blur()

	ollamaIn := textinput.New()
	ollamaIn.Placeholder = "describe the query in plain English..."
	ollamaIn.CharLimit = 512

	return Model{
		cfg:                cfg,
		activeConnIdx:      -1,
		focus:              panelLeft,
		queryInput:         ta,
		newConnInputs:      inputs,
		newConnEditIdx:     -1,
		schemaTable:        schemaTable,
		browseDataTable:    browseDataTable,
		resultTable:        resultTable,
		queryHistoryIdx:    -1,
		columnValueCache:   make(map[string][]string),
		columnValuePending: make(map[string]bool),
		schemaCache:        make(map[string]*db.TableSchema),
		schemaPending:      make(map[string]bool),
		ollamaInput:        ollamaIn,
	}
}

func (m Model) Init() tea.Cmd {
	return tea.SetWindowTitle("dbkit")
}
