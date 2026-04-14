package main

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"

	"dbkit/internal/db"
)

const (
	minWidth  = 56
	minHeight = 16
)

func (m Model) View() string {
	if m.width == 0 || m.height == 0 {
		return "loading..."
	}
	if m.width < minWidth || m.height < minHeight {
		return errorStyle.Render(fmt.Sprintf("terminal too small (%dx%d) — minimum %dx%d", m.width, m.height, minWidth, minHeight))
	}

	if m.showHelp {
		return m.renderDialogPage(m.renderHelpModal())
	}
	if m.showNewConn {
		return m.renderDialogPage(m.renderNewConnModal())
	}
	if m.showColumnPicker {
		return m.renderDialogPage(m.renderColumnPickerModal())
	}
	if m.showQueryPicker {
		return m.renderDialogPage(m.renderQueryPickerModal())
	}
	if m.showInspect {
		return m.renderDialogPage(m.renderInspectModal())
	}

	header := fitBlockHeight(m.renderHeader(), 1)
	sep := dimStyle.Render(strings.Repeat("─", m.width))

	// Chrome rows consumed outside the content block: header, sep, status, sep, footer.
	chromeLines := 5
	contentH := m.height - chromeLines
	if contentH < 1 {
		contentH = 1
	}
	content := fitBlockHeight(m.renderPanels(contentH), contentH)
	footer := fitBlockHeight(m.renderFooter(), 1)
	status := fitBlockHeight(m.renderStatusLine(), 1)

	var parts []string
	parts = append(parts, header, sep, content)
	parts = append(parts, status, sep, footer)

	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

func (m Model) renderDialogPage(dialog string) string {
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
}

func (m Model) renderHeader() string {
	title := titleStyle.Render("dbkit")

	var tabs []string
	for i, current := range primaryTabs {
		if i > 0 {
			tabs = append(tabs, dimStyle.Render(" │ "))
		}
		name := tabNames[current]
		disabled := m.activeDB == nil && (current == tabSchema || current == tabQuery || current == tabResults)
		if current == m.activeTab {
			tabs = append(tabs, activeTabStyle.Render(name))
		} else if disabled {
			tabs = append(tabs, dimStyle.Render(name))
		} else {
			tabs = append(tabs, inactiveTabStyle.Render(name))
		}
	}

	left := title + "  " + strings.Join(tabs, "")
	if m.isCompact() {
		left = title + "  " + activeTabStyle.Render(tabNames[m.activeTab])
	}

	var stats []string
	stats = append(stats, dimStyle.Render(fmt.Sprintf("%d conns", len(m.cfg.Connections))))
	if m.activeDB != nil {
		stats = append(stats, primaryStyle.Render("● "+truncate(m.activeConnName, max(8, m.width/4))))
		stats = append(stats, dimStyle.Render(fmt.Sprintf("%d %s", len(m.tables), m.dataSourceLabelPlural())))
	}

	right := strings.Join(stats, dimStyle.Render(" · "))
	maxRight := max(0, m.width/3)
	if lipgloss.Width(right) > maxRight && maxRight > 0 {
		right = dimStyle.Render(truncate(m.headerStatusText(), maxRight))
	}
	gap := m.width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 2 {
		right = ""
		if lipgloss.Width(left) > m.width-1 {
			left = title
		}
		gap = max(1, m.width-lipgloss.Width(left))
	}

	return left + strings.Repeat(" ", gap) + right
}

func (m Model) renderFooter() string {
	type hint struct{ key, action string }
	var hints []hint
	add := func(key, action string) {
		hints = append(hints, hint{key: key, action: action})
	}

	switch m.activeTab {
	case tabConnections:
		if m.focus == panelLeft {
			add("enter", "connect")
			add("n", "new")
			add("d", "delete")
			add("c", "copy dsn")
		}
	case tabSchema:
		if m.focus == panelRight {
			add("↑/↓", "schema rows")
			add("v", "inspect")
			add("c", "copy field")
			add("enter", "view data")
			add("e", "edit query")
		} else {
			add("↑/↓", m.dataSourceLabelPlural())
			add("c", "copy "+m.dataSourceLabel())
			add("enter", "view data")
			add("e", "edit query")
			add("tab", "schema")
			add("r", "refresh")
		}
	case tabQuery:
		if m.queryFocus {
			add("ctrl+r", "run")
			add("ctrl+o", "history")
			add("ctrl+y", "last")
			add("ctrl+t", "templates")
			add("tab", "columns")
			add("esc", "blur")
		} else {
			add("↑/↓", m.dataSourceLabelPlural())
			add("e", "editor")
			add("f/y", "templates/history")
			add("c", "copy")
			add("enter", "view data")
			add("4", "results")
		}
	case tabResults:
		if m.focus == panelRight {
			add("↑/↓", "result rows")
			add("←/→", "columns")
			add("v", "inspect")
			add("c", "copy row")
			add("e", "query")
			add("tab", m.dataSourceLabelPlural())
		} else {
			add("↑/↓", m.dataSourceLabelPlural())
			add("e", "query")
			add("tab", "results")
			add("enter", "view data")
		}
	case tabHistory:
		add("↑/↓", "history")
		add("enter", "load")
		add("r", "rerun")
		add("c", "copy")
	case tabHelpers:
		add("↑/↓", "templates")
		add("enter", "use template")
		add("c", "copy")
	}

	add("1-4", "tabs")
	add("tab", "panel")
	add("?", "help")
	add("q", "quit")

	line := " "
	for i, item := range hints {
		seg := keyStyle.Render(item.key) + " " + actionStyle.Render(item.action)
		next := line + seg
		if i > 0 {
			next = line + dimStyle.Render(" · ") + seg
		}
		if lipgloss.Width(next) > m.width {
			break
		}
		line = next
	}
	return line
}

func (m Model) headerStatusText() string {
	parts := []string{fmt.Sprintf("%d conns", len(m.cfg.Connections))}
	if m.activeDB != nil {
		parts = append(parts, "● "+m.activeConnName, fmt.Sprintf("%d %s", len(m.tables), m.dataSourceLabelPlural()))
	}
	if m.loading {
		parts = append(parts, "loading...")
	}
	return strings.Join(parts, " · ")
}

func (m Model) renderPanels(contentH int) string {
	if m.isSinglePane() {
		return m.renderSinglePane(contentH)
	}

	leftW := m.leftPanelWidth()
	rightW := m.width - leftW - 1
	if m.isCompact() {
		leftW = max(20, m.width*28/100)
		rightW = m.width - leftW - 1
	}
	if rightW < 28 {
		rightW = 28
		leftW = max(20, m.width-rightW-1)
	}

	innerH := contentH - 2
	if innerH < 1 {
		innerH = 1
	}

	left := fitBlockHeight(m.renderLeftPanel(leftW-4, innerH), innerH)
	right := fitBlockHeight(m.renderRightPanel(rightW-4, innerH), innerH)

	leftStyle := panelStyle.Width(leftW - 2).Height(innerH)
	rightStyle := panelStyle.Width(rightW - 2).Height(innerH)

	if m.queryFocus || m.focus == panelRight {
		rightStyle = panelActiveStyle.Width(rightW - 2).Height(innerH)
	} else {
		leftStyle = panelActiveStyle.Width(leftW - 2).Height(innerH)
	}

	leftBox := leftStyle.Render(left)
	rightBox := rightStyle.Render(right)
	return lipgloss.JoinHorizontal(lipgloss.Top, leftBox, " ", rightBox)
}

func (m Model) renderSinglePane(contentH int) string {
	innerH := max(1, contentH-2)
	w := max(10, m.width-4)
	bodyH := max(1, innerH-1)

	var body string
	var style lipgloss.Style
	if m.focus == panelRight || m.queryFocus {
		body = fitBlockHeight(m.renderRightPanel(w, bodyH), bodyH)
		style = panelActiveStyle
	} else {
		body = fitBlockHeight(m.renderLeftPanel(w, bodyH), bodyH)
		style = panelActiveStyle
	}

	header := "left pane"
	if m.focus == panelRight || m.queryFocus {
		header = "right pane"
	}
	chip := dimStyle.Render(" " + header + " ")
	return style.Width(m.width - 2).Height(innerH).Render(chip + "\n" + body)
}

func (m Model) renderLeftPanel(w, h int) string {
	switch m.activeTab {
	case tabConnections:
		return m.renderConnectionList(w, h)
	case tabSchema, tabQuery, tabResults:
		return m.renderTableList(w, h)
	case tabHistory:
		return m.renderHistoryList(w, h)
	case tabHelpers:
		return m.renderHelperList(w, h)
	default:
		return ""
	}
}

func (m Model) renderConnectionList(w, h int) string {
	lines := []string{panelHeaderStyle.Render("Connections"), ""}
	if len(m.cfg.Connections) == 0 {
		lines = append(lines, dimStyle.Render("no saved connections"), dimStyle.Render("press n to create one"))
		return padLines(lines, h)
	}

	visibleH := max(1, h-2)
	start := listStartForCursor(len(m.cfg.Connections), visibleH, m.connCursor)
	end := min(len(m.cfg.Connections), start+visibleH)
	for i := start; i < end; i++ {
		conn := m.cfg.Connections[i]
		label := fmt.Sprintf("%s %s", dbIcon(conn.Type), truncate(conn.Name, w-4))
		switch {
		case i == m.connCursor:
			lines = append(lines, selectedItemStyle.Render(" "+padRight(label, w-2)+" "))
		case i == m.activeConnIdx:
			lines = append(lines, connectedStyle.Render(" "+label))
		default:
			lines = append(lines, textStyle.Render(" "+label))
		}
	}

	return padLines(lines, h)
}

func (m Model) renderTableList(w, h int) string {
	lines := []string{panelHeaderStyle.Render(m.dataSourceLabelTitlePlural()), ""}

	if m.activeDB == nil {
		return padLines(append(lines, dimStyle.Render("not connected")), h)
	}
	if len(m.tables) == 0 {
		return padLines(append(lines, dimStyle.Render("no "+m.dataSourceLabelPlural())), h)
	}

	visibleH := max(1, h-2)
	start := listStartForCursor(len(m.tables), visibleH, m.tableCursor)
	end := min(len(m.tables), start+visibleH)
	for i := start; i < end; i++ {
		table := m.tables[i]
		name := truncate(table, w-3)
		if i == m.tableCursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(name, w-2)+" "))
		} else {
			lines = append(lines, " "+name)
		}
	}

	return padLines(lines, h)
}

func (m Model) renderHelperList(w, h int) string {
	lines := []string{panelHeaderStyle.Render("Templates"), ""}
	helpers := m.helperItems()
	if len(helpers) == 0 {
		lines = append(lines, dimStyle.Render("connect and pick a "+m.dataSourceLabel()))
		return padLines(lines, h)
	}

	visibleH := max(1, h-2)
	start := listStartForCursor(len(helpers), visibleH, m.helperCursor)
	end := min(len(helpers), start+visibleH)
	for i := start; i < end; i++ {
		helper := helpers[i]
		name := truncate(helper.label, w-3)
		if i == m.helperCursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(name, w-2)+" "))
		} else {
			lines = append(lines, " "+name)
		}
	}

	return padLines(lines, h)
}

func (m Model) renderHistoryList(w, h int) string {
	lines := []string{panelHeaderStyle.Render("History"), ""}
	if len(m.queryHistory) == 0 {
		lines = append(lines, dimStyle.Render("no queries yet"), dimStyle.Render("run a query to save it here"))
		return padLines(lines, h)
	}
	visibleH := max(1, h-2)
	start := listStartForCursor(len(m.queryHistory), visibleH, m.historyCursor)
	end := min(len(m.queryHistory), start+visibleH)
	for i := start; i < end; i++ {
		name := truncate(compactInline(m.queryHistory[i]), w-3)
		if i == m.historyCursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(name, w-2)+" "))
		} else {
			lines = append(lines, " "+name)
		}
	}
	return padLines(lines, h)
}

func (m Model) renderRightPanel(w, h int) string {
	switch m.activeTab {
	case tabConnections:
		return m.renderConnectionDetail(w, h)
	case tabSchema:
		return m.renderSchemaDetail(w, h)
	case tabQuery:
		return m.renderQueryPanel(w, h)
	case tabResults:
		return m.renderResultsPanel(w, h)
	case tabHistory:
		return m.renderHistoryPanel(w, h)
	case tabHelpers:
		return m.renderHelperPreview(w, h)
	default:
		return ""
	}
}

func (m Model) renderConnectionDetail(w, h int) string {
	if len(m.cfg.Connections) == 0 {
		return padLines([]string{dimStyle.Render("press n to add a connection")}, h)
	}

	conn := m.cfg.Connections[m.connCursor]
	lines := []string{
		renderPaneTitle(conn.Name, conn.Type, w),
		"",
		accentStyle.Render("id   ") + dimStyle.Render(conn.ID),
	}

	dsn := conn.DSN
	if utf8.RuneCountInString(dsn) > w-6 {
		dsn = truncate(dsn, w-6)
	}
	lines = append(lines, accentStyle.Render("dsn  ")+textStyle.Render(dsn), "")

	if m.activeConnIdx == m.connCursor {
		lines = append(lines, connectedStyle.Render("● connected"))
	} else {
		lines = append(lines, dimStyle.Render("enter: connect   d: delete"))
	}

	return padLines(lines, h)
}

func (m Model) renderSchemaDetail(w, h int) string {
	if m.activeDB == nil {
		return padLines([]string{dimStyle.Render("not connected")}, h)
	}
	if m.tableSchema == nil {
		if m.loading {
			return padLines([]string{warnStyle.Render("loading schema...")}, h)
		}
		return padLines([]string{dimStyle.Render("select a " + m.dataSourceLabel())}, h)
	}

	s := m.tableSchema
	lines := []string{
		renderPaneTitle(s.Name, fmt.Sprintf("%d rows", s.RowCount), w),
		dimStyle.Render(strings.Repeat("─", max(1, w))),
	}
	tbl := m.schemaTable
	tbl.SetWidth(max(10, w))
	// bubbles/table height does not account for its own header chrome, so leave room
	// for that to keep the pane border aligned with the left side.
	tbl.SetHeight(max(1, h-len(lines)-2))
	lines = append(lines, strings.Split(strings.TrimRight(tbl.View(), "\n"), "\n")...)
	return padLines(lines, h)
}

func (m Model) renderQueryPanel(w, h int) string {
	if m.activeDB == nil {
		return padLines([]string{dimStyle.Render("not connected")}, h)
	}

	inputH := 5
	if h < 12 {
		inputH = 3
	}
	m.queryInput.SetWidth(max(10, w-1))
	m.queryInput.SetHeight(inputH)

	label := renderPaneTitle("Query", m.queryContextLabel(), w)
	divider := dimStyle.Render(strings.Repeat("─", max(1, w)))

	var lines []string
	lines = append(lines, label)
	lines = append(lines, strings.Split(m.queryInput.View(), "\n")...)
	lines = append(lines, divider)
	switch {
	case m.loading:
		lines = append(lines, warnStyle.Render("running query..."))
	case m.queryErr != "" && m.queryResult == nil:
		for _, line := range wrapText(m.queryErr, w) {
			lines = append(lines, errorStyle.Render(line))
		}
	case m.queryResult != nil:
		lines = append(lines, primaryStyle.Render("latest result ready in Results tab"))
		lines = append(lines, dimStyle.Render(fmt.Sprintf("%d row(s) · press 4 to inspect", len(m.queryResult.Rows))))
	default:
		lines = append(lines, dimStyle.Render("write a query, run it, then inspect rows in Results"))
	}
	meta := m.renderQueryMeta()
	if meta != "" {
		lines = append(lines, "", meta)
	}

	return padLines(lines, h)
}

func (m Model) renderResultsPanel(w, h int) string {
	if m.activeDB == nil {
		return padLines([]string{dimStyle.Render("not connected")}, h)
	}

	label := renderPaneTitle("Results", m.queryContextLabel(), w)
	divider := dimStyle.Render(strings.Repeat("─", max(1, w)))
	lines := []string{label}

	if m.loading {
		lines = append(lines, "", warnStyle.Render("running query..."))
		return padLines(lines, h)
	}
	if m.queryErr != "" {
		lines = append(lines, "")
		for _, line := range wrapText(m.queryErr, w) {
			lines = append(lines, errorStyle.Render(line))
		}
		return padLines(lines, h)
	}
	if m.queryResult == nil {
		lines = append(lines, "", dimStyle.Render("no results yet"), dimStyle.Render("run a query from the Query tab"))
		return padLines(lines, h)
	}

	tbl := m.resultTable
	tbl.SetWidth(max(10, w))
	tableH := max(1, h-len(lines)-1)
	if meta := m.renderResultMeta(); meta != "" {
		tableH = max(1, h-len(lines)-3)
	}
	tbl.SetHeight(tableH)
	lines = append(lines, divider)
	lines = append(lines, strings.Split(strings.TrimRight(tbl.View(), "\n"), "\n")...)
	meta := m.renderResultMeta()
	if meta != "" {
		lines = append(lines, "", meta)
	}
	return padLines(lines, h)
}

func (m Model) renderHistoryPanel(w, h int) string {
	lines := []string{renderPaneTitle("History", m.activeConnName, w), ""}
	query := m.currentHistoryQuery()
	if query == "" {
		lines = append(lines, dimStyle.Render("no history for this connection"))
		return padLines(lines, h)
	}
	lines = append(lines, panelHeaderStyle.Render("Selected Query"))
	for _, line := range wrapTextPreservingRuns(query, max(12, w-1)) {
		lines = append(lines, accentStyle.Render(line))
	}
	return padLines(lines, h)
}

func (m Model) renderHelperPreview(w, h int) string {
	helpers := m.helperItems()
	if len(helpers) == 0 {
		return padLines([]string{dimStyle.Render("select a helper")}, h)
	}
	if m.helperCursor >= len(helpers) {
		return padLines(nil, h)
	}

	helper := helpers[m.helperCursor]
	meta := "template"
	if m.activeDB != nil {
		meta = m.activeDB.Type() + " template"
	}
	if helper.kind == "prompt" {
		meta = "copyable prompt"
	}
	lines := []string{renderPaneTitle(helper.label, meta, w), ""}
	for _, line := range strings.Split(helper.template, "\n") {
		lines = append(lines, accentStyle.Render(truncate(line, w-1)))
	}
	return padLines(lines, h)
}

func (m Model) renderInspectModal() string {
	viewH := m.inspectViewportHeight()
	start := clampInt(m.inspectScroll, 0, max(0, len(m.inspectLines)-viewH))
	end := min(len(m.inspectLines), start+viewH)
	lines := []string{panelHeaderStyle.Render(m.inspectTitle), ""}
	for _, line := range m.inspectLines[start:end] {
		if line == "" {
			lines = append(lines, "")
			continue
		}
		if strings.Trim(line, "─") == "" {
			lines = append(lines, dimStyle.Render(strings.Repeat("─", max(8, min(40, m.width-12)))))
			continue
		}
		lines = append(lines, line) // pre-styled at construction time
	}
	lines = append(lines, "", dimStyle.Render("↑/↓ scroll · pgup/pgdn jump · c copy · esc close"))
	return dialogStyle.Width(min(104, m.width-6)).Render(strings.Join(lines, "\n"))
}

func (m Model) renderNewConnModal() string {
	var lines []string
	lines = append(lines, panelHeaderStyle.Render("New Connection"), "")

	namePrefix := "  "
	if m.newConnFocus == newConnFocusName {
		namePrefix = keyStyle.Render("▸ ")
	}
	lines = append(lines, namePrefix+accentStyle.Render("name ")+m.newConnInputs[fieldName].View())

	typePrefix := "  "
	if m.newConnFocus == newConnFocusType {
		typePrefix = keyStyle.Render("▸ ")
	}
	var typeOptions []string
	for i, t := range dbTypes {
		if i == m.newConnTypeCur {
			typeOptions = append(typeOptions, activeTabStyle.Render(t))
		} else {
			typeOptions = append(typeOptions, inactiveTabStyle.Render(t))
		}
	}
	lines = append(lines, typePrefix+accentStyle.Render("type ")+strings.Join(typeOptions, dimStyle.Render(" | ")))

	dsnPrefix := "  "
	if m.newConnFocus == newConnFocusDSN {
		dsnPrefix = keyStyle.Render("▸ ")
	}
	lines = append(lines, dsnPrefix+accentStyle.Render("dsn  ")+m.newConnInputs[fieldDSN].View())
	lines = append(lines, "")

	saveLabel := inactiveTabStyle.Render("save")
	if m.newConnFocus == newConnFocusSave {
		saveLabel = activeTabStyle.Render("save")
	}
	lines = append(lines, "  [ "+saveLabel+" ]")
	lines = append(lines, "", dimStyle.Render("tab/shift+tab navigate · left/right type · esc cancel"))

	return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
}

func (m Model) renderHelpModal() string {
	lines := []string{
		panelHeaderStyle.Render("dbkit keybinds"),
		"",
		keyStyle.Render("1-4") + " " + actionStyle.Render("switch tabs"),
		keyStyle.Render("tab") + " " + actionStyle.Render("toggle left/right pane"),
		keyStyle.Render("↑/↓") + " " + actionStyle.Render("move selection"),
		keyStyle.Render("enter") + " " + actionStyle.Render("connect / select / use"),
		keyStyle.Render("n") + " " + actionStyle.Render("new connection"),
		keyStyle.Render("d") + " " + actionStyle.Render("delete connection"),
		keyStyle.Render("r") + " " + actionStyle.Render("refresh browse list"),
		keyStyle.Render("enter") + " " + actionStyle.Render("view selected data"),
		keyStyle.Render("e") + " " + actionStyle.Render("focus query editor"),
		keyStyle.Render("ctrl+r") + " " + actionStyle.Render("run query"),
		keyStyle.Render("ctrl+o") + " " + actionStyle.Render("query history picker"),
		keyStyle.Render("ctrl+y") + " " + actionStyle.Render("last run"),
		keyStyle.Render("tab") + " " + actionStyle.Render("schema column completion in editor"),
		keyStyle.Render("f / y") + " " + actionStyle.Render("templates / history from Query"),
		keyStyle.Render("←/→") + " " + actionStyle.Render("page result columns"),
		keyStyle.Render("c") + " " + actionStyle.Render("copy query/history/result"),
		keyStyle.Render("v") + " " + actionStyle.Render("inspect selected schema/result row"),
		keyStyle.Render("esc") + " " + actionStyle.Render("close modal / blur editor"),
		keyStyle.Render("q") + " " + actionStyle.Render("quit"),
		"",
		dimStyle.Render("press any key to close"),
	}

	return dialogStyle.Width(min(64, m.width-6)).Render(strings.Join(lines, "\n"))
}

func dbIcon(dbType string) string {
	switch dbType {
	case "sqlite":
		return "◆"
	case "postgres":
		return "●"
	case "mongo":
		return "▲"
	default:
		return "○"
	}
}

func (m Model) dataSourceLabel() string {
	if m.activeDB != nil && m.activeDB.Type() == "mongo" {
		return "collection"
	}
	return "table"
}

func (m Model) dataSourceLabelPlural() string {
	if m.activeDB != nil && m.activeDB.Type() == "mongo" {
		return "collections"
	}
	return "tables"
}

func (m Model) dataSourceLabelTitlePlural() string {
	if m.activeDB != nil && m.activeDB.Type() == "mongo" {
		return "Collections"
	}
	return "Tables"
}

func (m Model) queryContextLabel() string {
	parts := []string{}
	if m.activeDB != nil {
		parts = append(parts, m.activeDB.Type())
	}
	if table := m.currentTableName(); table != "" {
		parts = append(parts, table)
	}
	return strings.Join(parts, " · ")
}

func renderPaneTitle(title, meta string, width int) string {
	title = strings.TrimSpace(title)
	meta = strings.TrimSpace(meta)
	if width <= 0 {
		return ""
	}
	if meta == "" {
		return panelHeaderStyle.Render(truncate(title, width))
	}
	metaWidth := lipgloss.Width(meta) + 2
	if metaWidth >= width-4 {
		return panelHeaderStyle.Render(truncate(title, width))
	}
	titleWidth := max(8, width-metaWidth)
	return panelHeaderStyle.Render(truncate(title, titleWidth)) + "  " + dimStyle.Render(meta)
}

func (m Model) renderSchemaColumnDetail(w, h int) string {
	if m.tableSchema == nil || len(m.tableSchema.Columns) == 0 {
		return padLines([]string{dimStyle.Render("select a field")}, h)
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
		flags = append(flags, "primary key")
	}
	if !col.Nullable {
		flags = append(flags, "not null")
	}
	if len(flags) == 0 {
		flags = append(flags, "optional")
	}

	lines := []string{
		renderPaneTitle(col.Name, fmt.Sprintf("field %d/%d", cursor+1, len(m.tableSchema.Columns)), w),
		accentStyle.Render("type  ") + textStyle.Render(col.Type),
		accentStyle.Render("flags ") + textStyle.Render(strings.Join(flags, ", ")),
		"",
		dimStyle.Render("enter views data · e opens a starter query · v shows full detail"),
	}
	return padLines(lines, h)
}

func (m Model) renderQueryMeta() string {
	parts := []string{}
	if table := m.currentTableName(); table != "" {
		parts = append(parts, dimStyle.Render("target "+table))
	}
	if m.tableSchema != nil {
		parts = append(parts, dimStyle.Render(fmt.Sprintf("%d fields", len(m.tableSchema.Columns))))
	}
	if count := len(m.helperItems()); count > 0 {
		parts = append(parts, dimStyle.Render(fmt.Sprintf("%d templates", count)))
	}
	return strings.Join(parts, dimStyle.Render(" · "))
}

func (m Model) renderResultMeta() string {
	if m.queryErr != "" {
		return dimStyle.Render("query error")
	}
	if m.queryResult == nil {
		return dimStyle.Render("results pane")
	}
	if m.queryResult.Message != "" && len(m.queryResult.Rows) == 0 {
		return primaryStyle.Render(m.queryResult.Message)
	}
	totalCols := len(m.queryResult.Columns)
	if totalCols == 0 {
		return dimStyle.Render("(no columns)")
	}
	start := m.resultColOffset + 1
	end := m.resultColOffset + m.resultVisibleColumn
	if end < start {
		end = start
	}
	parts := []string{
		dimStyle.Render(fmt.Sprintf("%d row(s)", len(m.queryResult.Rows))),
		dimStyle.Render(fmt.Sprintf("cols %d-%d/%d", start, end, totalCols)),
	}
	if totalCols > m.resultVisibleColumn {
		parts = append(parts, accentStyle.Render("←/→ columns"))
	}
	if m.isSinglePane() {
		parts = append(parts, dimStyle.Render("tab swaps panes"))
	}
	return strings.Join(parts, dimStyle.Render(" · "))
}

func (m Model) renderStatusLine() string {
	if m.statusMsg != "" && time.Now().Before(m.statusExpiry) {
		return statusStyle.Render(" " + truncate(m.statusMsg, max(1, m.width-2)))
	}
	return ""
}

func (m Model) renderQueryPickerModal() string {
	lines := []string{panelHeaderStyle.Render(m.queryPickerTitle), ""}
	rowW := max(24, min(72, m.width-14))
	if len(m.queryPickerItems) == 0 {
		lines = append(lines, dimStyle.Render("nothing available"))
		lines = append(lines, "", dimStyle.Render("esc closes"))
		return dialogStyle.Width(min(90, m.width-6)).Render(strings.Join(lines, "\n"))
	}
	visibleH := max(4, min(12, m.height-12))
	start := listStartForCursor(len(m.queryPickerItems), visibleH, m.queryPickerCursor)
	end := min(len(m.queryPickerItems), start+visibleH)
	for i := start; i < end; i++ {
		item := m.queryPickerItems[i]
		row := truncate(item.label, max(12, rowW-20))
		if item.detail != "" {
			row += "  " + truncate(item.detail, 18)
		}
		if i == m.queryPickerCursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(row, rowW)+" "))
		} else {
			lines = append(lines, " "+row)
		}
	}
	lines = append(lines, "", dimStyle.Render("enter inserts · c copies · esc closes"))
	return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
}

func (m Model) renderColumnPickerModal() string {
	lines := []string{panelHeaderStyle.Render(m.columnPickerTitle), ""}
	rowW := max(28, min(72, m.width-14))
	if len(m.columnPickerItems) == 0 {
		lines = append(lines, dimStyle.Render("no schema columns available"))
		lines = append(lines, "", dimStyle.Render("esc closes"))
		return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
	}
	visibleH := max(5, min(14, m.height-12))
	start := listStartForCursor(len(m.columnPickerItems), visibleH, m.columnPickerCursor)
	end := min(len(m.columnPickerItems), start+visibleH)
	for i := start; i < end; i++ {
		item := m.columnPickerItems[i]
		marker := "[ ]"
		if item.selected {
			marker = "[x]"
		}
		if !m.columnPickerMulti {
			marker = " • "
			if i == m.columnPickerCursor {
				marker = " ▸ "
			}
		}
		row := marker + " " + truncate(item.name, max(12, rowW-20))
		if item.dataType != "" {
			row += "  " + truncate(item.dataType, 18)
		}
		if i == m.columnPickerCursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(row, rowW)+" "))
		} else {
			lines = append(lines, " "+row)
		}
	}
	hint := "enter inserts · esc closes"
	if m.columnPickerMulti {
		hint = "space toggles · enter inserts · esc closes"
	}
	lines = append(lines, "", dimStyle.Render(hint))
	return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
}

func (m Model) renderSchemaRows(w, h int) string {
	if m.tableSchema == nil {
		return padLines(nil, h)
	}
	cursor := m.schemaTable.Cursor()
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= len(m.tableSchema.Columns) {
		cursor = len(m.tableSchema.Columns) - 1
	}

	nameW := clampInt(w*34/100, 12, max(12, w-24))
	typeW := clampInt(w*26/100, 10, max(10, w-18))
	flagW := max(8, w-nameW-typeW-4)
	visibleH := max(1, h-2)
	start := listStartForCursor(len(m.tableSchema.Columns), visibleH, cursor)
	end := min(len(m.tableSchema.Columns), start+visibleH)

	lines := []string{
		tableHeaderStyle.Render(padRight("Column", nameW) + "  " + padRight("Type", typeW) + "  " + padRight("Flags", flagW)),
		dimStyle.Render(strings.Repeat("─", max(12, min(w, nameW+typeW+flagW+4)))),
	}
	for i := start; i < end; i++ {
		col := m.tableSchema.Columns[i]
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
		row := fitTableCell(col.Name, nameW)
		row += "  " + fitTableCell(col.Type, typeW)
		row += "  " + fitTableCell(strings.Join(flags, ", "), flagW)
		row = padRight(row, max(1, w))
		if i == cursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(row, max(1, w-2))+" "))
			continue
		}
		lines = append(lines, " "+row)
	}
	return padLines(lines, h)
}

func (m Model) isCompact() bool {
	return m.width < 84
}

func (m Model) isSinglePane() bool {
	return m.width < 68
}

func compactInline(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ↵ ")
	s = strings.ReplaceAll(s, "\n", " ↵ ")
	s = strings.ReplaceAll(s, "\r", " ↵ ")
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func fitTableCell(s string, width int) string {
	return truncate(compactInline(s), max(1, width-1))
}

func (m Model) inspectViewportHeight() int {
	return max(6, min(20, m.height-10))
}

func truncate(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return string(runes[:maxLen])
	}
	return string(runes[:maxLen-3]) + "..."
}

func padRight(s string, w int) string {
	padding := w - utf8.RuneCountInString(s)
	if padding <= 0 {
		return s
	}
	return s + strings.Repeat(" ", padding)
}

func padLines(lines []string, h int) string {
	if lines == nil {
		lines = []string{}
	}
	if h < 0 {
		h = 0
	}
	if len(lines) > h {
		lines = lines[:h]
	}
	result := strings.Join(lines, "\n")
	if len(lines) < h {
		result += strings.Repeat("\n", h-len(lines))
	}
	return result
}

func fitBlockHeight(s string, h int) string {
	if h <= 0 {
		return ""
	}
	lines := strings.Split(s, "\n")
	if len(lines) > h {
		lines = lines[:h]
	}
	return padLines(lines, h)
}

func listStartForCursor(total, visible, cursor int) int {
	if total <= 0 || visible <= 0 {
		return 0
	}
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= total {
		cursor = total - 1
	}
	start := cursor - visible + 1
	if start < 0 {
		start = 0
	}
	maxStart := total - visible
	if maxStart < 0 {
		maxStart = 0
	}
	if start > maxStart {
		start = maxStart
	}
	return start
}

func wrapText(s string, w int) []string {
	if w <= 0 {
		return []string{s}
	}
	words := strings.Fields(s)
	if len(words) == 0 {
		return []string{""}
	}

	var lines []string
	line := words[0]
	for _, word := range words[1:] {
		if utf8.RuneCountInString(line)+1+utf8.RuneCountInString(word) <= w {
			line += " " + word
		} else {
			lines = append(lines, line)
			line = word
		}
	}
	lines = append(lines, line)
	return lines
}

func wrapTextPreservingRuns(s string, w int) []string {
	if w <= 0 {
		return []string{s}
	}
	runes := []rune(s)
	if len(runes) == 0 {
		return []string{""}
	}
	var lines []string
	for len(runes) > 0 {
		if len(runes) <= w {
			lines = append(lines, string(runes))
			break
		}
		lines = append(lines, string(runes[:w]))
		runes = runes[w:]
	}
	return lines
}

func visibleResultColumns(result *db.QueryResult, width, offset int) (int, []table.Column) {
	if result == nil || len(result.Columns) == 0 {
		return 0, nil
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(result.Columns) {
		offset = len(result.Columns) - 1
	}

	// bubbles/table renders each column with Padding(0,1) on cell+header,
	// so the on-screen width of a column is col.Width + 2. Budget accordingly.
	const cellChrome = 2
	remaining := max(12, width)
	cols := make([]table.Column, 0, len(result.Columns)-offset)
	start := offset
	for i := offset; i < len(result.Columns); i++ {
		colWidth := resultColumnWidth(result, i, width)
		render := colWidth + cellChrome
		if len(cols) > 0 && render > remaining {
			break
		}
		if render > remaining {
			colWidth = max(4, remaining-cellChrome)
			render = colWidth + cellChrome
		}
		cols = append(cols, table.Column{Title: fitTableCell(result.Columns[i], colWidth), Width: colWidth})
		remaining -= render
		if remaining <= cellChrome {
			break
		}
	}
	if len(cols) == 0 {
		cols = append(cols, table.Column{Title: result.Columns[start], Width: max(6, width-cellChrome)})
	}
	return start, cols
}

func resultColumnWidth(result *db.QueryResult, idx, totalWidth int) int {
	width := utf8.RuneCountInString(result.Columns[idx]) + 2
	sample := min(25, len(result.Rows))
	for i := 0; i < sample; i++ {
		if idx >= len(result.Rows[i]) {
			continue
		}
		cellWidth := utf8.RuneCountInString(result.Rows[i][idx]) + 2
		if cellWidth > width {
			width = cellWidth
		}
	}
	maxWidth := clampInt(totalWidth/3, 12, 24)
	return clampInt(width, 8, maxWidth)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func clampInt(v, low, high int) int {
	if high < low {
		high = low
	}
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}
