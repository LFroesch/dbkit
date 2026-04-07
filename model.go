package main

import (
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"dbkit/internal/config"
	"dbkit/internal/db"
)

type tab int

const (
	tabConnections tab = iota
	tabSchema
	tabQuery
	tabHelpers
	tabCount
)

var tabNames = [tabCount]string{"Connections", "Schema", "Query", "Helpers"}

type panel int

const (
	panelLeft panel = iota
	panelRight
)

// Async messages
type connectedMsg struct {
	db  db.DB
	err error
}
type tablesLoadedMsg struct {
	tables []string
	err    error
}
type schemaLoadedMsg struct {
	schema *db.TableSchema
	err    error
}
type queryDoneMsg struct {
	result *db.QueryResult
	err    error
}

// New connection form field indices
const (
	fieldName = 0
	fieldDSN  = 1
	fieldCount = 2
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
	newConnInputs   [fieldCount]textinput.Model
	newConnTypeCur  int // index into dbTypes
	newConnFocus    int // 0=name, 1=type, 2=dsn

	// Schema tab
	tables      []string
	tableCursor int
	tableSchema *db.TableSchema

	// Query tab
	queryInput   textarea.Model
	queryResult  *db.QueryResult
	queryErr     string
	resultScroll int
	queryFocus   bool // true = textarea focused

	// Helpers tab
	helperCursor int

	// Status bar
	statusMsg    string
	statusExpiry time.Time

	// Loading states
	loading bool

	// Modal overlay: new connection form visible
	showNewConn bool
	// Modal overlay: help
	showHelp bool
}

func newModel(cfg *config.Config) Model {
	// New connection inputs
	var inputs [fieldCount]textinput.Model
	inputs[fieldName] = textinput.New()
	inputs[fieldName].Placeholder = "My Database"
	inputs[fieldName].CharLimit = 64

	inputs[fieldDSN] = textinput.New()
	inputs[fieldDSN].Placeholder = "/path/to/db.sqlite or postgres://user:pass@host/db"
	inputs[fieldDSN].CharLimit = 256

	// Query textarea
	ta := textarea.New()
	ta.Placeholder = "Enter SQL query... (ctrl+r to run)"
	ta.ShowLineNumbers = false
	ta.SetWidth(60)
	ta.SetHeight(6)

	return Model{
		cfg:          cfg,
		activeConnIdx: -1,
		queryInput:   ta,
		newConnInputs: inputs,
	}
}

func (m Model) Init() tea.Cmd {
	return nil
}
