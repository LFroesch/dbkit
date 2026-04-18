package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"

	"bobdb/internal/db"
)

var fractionalTimestampPattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})\.(\d+)(.*)$`)

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

	if m.showOllamaGen {
		return m.renderDialogPage(m.renderOllamaGenModal())
	}
	if m.showHelp {
		return m.renderDialogPage(m.renderHelpModal())
	}
	if m.showNewConn {
		return m.renderDialogPage(m.renderNewConnModal())
	}
	if m.showQueryPicker {
		return m.renderDialogPage(m.renderQueryPickerModal())
	}
	if m.showInspect {
		return m.renderDialogPage(m.renderInspectModal())
	}
	if m.showConfirm {
		return m.renderDialogPage(m.renderConfirmModal())
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
	title := titleStyle.Render("bobdb")

	var tabs []string
	for i, current := range primaryTabs {
		if i > 0 {
			tabs = append(tabs, dimStyle.Render(" │ "))
		}
		name := tabNames[current]
		disabled := m.activeDB == nil && (current == tabBrowse || current == tabQuery || current == tabResults)
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
			add("e", "edit")
			add("d", "delete...")
			add("c", "copy dsn")
		}
	case tabBrowse:
		if m.focus == panelRight && m.browseView == browseViewData {
			add("↑/↓", "rows")
			add("←/→", "columns")
			add("enter", "schema")
			add("e", "edit cell")
			add("v", "inspect")
			add("c", "copy row")
			add("C", "copy as")
		} else if m.focus == panelRight {
			add("↑/↓", "fields")
			add("enter", "data view")
			add("e", "edit column")
			add("v", "inspect")
		} else {
			add("↑/↓", m.dataSourceLabelPlural())
			add("enter", "data view")
			add("e", "edit")
			add("r", "refresh")
			add("c", "copy name")
		}
	case tabQuery:
		if m.queryFocus {
			if m.showColumnPicker {
				add("↑/↓", "move")
				add("tab", "insert")
				add("enter", "newline")
				if m.columnPickerMulti {
					add("space", "toggle")
				}
				add("esc", "close")
			} else {
				add("/", "query")
				add("ctrl+r", "run")
				add("tab", "complete")
				add("pgup/dn", "reference")
				add("f/x/y/u", "pickers")
				add("g/s", "ai/save")
				add("ctrl+l", "clear")
				add("esc", "blur")
			}
		} else {
			add("/", "editor")
			add("enter", "editor")
			add("pgup/dn", "reference")
			add("g", "ai generate")
			add("f", "templates")
			add("x", "examples")
			add("y", "recent")
			add("u", "saved")
			add("ctrl+r", "run")
		}
	case tabResults:
		if m.focus == panelRight {
			add("↑/↓", "result rows")
			add("←/→", "columns")
			add("c", "copy row")
			add("e", "edit cell")
			add("v", "inspect")
			add("C", "copy as")
		} else {
			add("↑/↓", m.dataSourceLabelPlural())
			add("e", "edit")
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

	add("?", "help")
	add("q", "back")

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

	header := m.compactPaneLabel()
	chip := dimStyle.Render(" " + header + " ")
	return style.Width(m.width - 2).Height(innerH).Render(chip + "\n" + body)
}

func (m Model) renderLeftPanel(w, h int) string {
	switch m.activeTab {
	case tabConnections:
		return m.renderConnectionList(w, h)
	case tabBrowse:
		return m.renderTableList(w, h)
	case tabQuery:
		return m.renderQueryCheatSheet(w, h)
	case tabResults:
		return m.renderLastRunQuery(w, h)
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

func (m Model) renderQueryCheatSheet(w, h int) string {
	lines := []string{panelHeaderStyle.Render("Query Reference"), ""}
	body := m.queryReferenceLinesForWidth(w)
	viewH := max(1, h-4)
	start := clampInt(m.queryRefScroll, 0, max(0, len(body)-viewH))
	end := min(len(body), start+viewH)
	lines = append(lines, body[start:end]...)
	if start > 0 || end < len(body) {
		meta := []string{}
		if start > 0 {
			meta = append(meta, "pgup/home ↑")
		}
		if end < len(body) {
			meta = append(meta, "pgdn/end ↓")
		}
		lines = append(lines, "", dimStyle.Render(strings.Join(meta, " · ")))
	}
	return padLines(lines, h)
}

func (m Model) renderLastRunQuery(w, h int) string {
	lines := []string{panelHeaderStyle.Render("Last Query"), ""}
	q := strings.TrimSpace(m.lastRunQuery)
	if q == "" {
		lines = append(lines, dimStyle.Render("no query run yet"))
		return padLines(lines, h)
	}
	for _, line := range wrapTextPreservingRuns(q, max(12, w-1)) {
		lines = append(lines, accentStyle.Render(line))
	}
	return padLines(lines, h)
}

type queryReferenceEntry struct {
	kind string
	text string
}

func (m Model) queryReferenceEntries() []queryReferenceEntry {
	dbType := ""
	if m.activeDB != nil {
		dbType = m.activeDB.Type()
	}
	section := func(title string) queryReferenceEntry {
		return queryReferenceEntry{kind: "section", text: title}
	}
	ex := func(s string) queryReferenceEntry {
		return queryReferenceEntry{kind: "example", text: "  " + s}
	}

	lines := []queryReferenceEntry{}

	switch dbType {
	case "mongo":
		lines = append(lines,
			section("Read"),
			ex(`db.posts.find({})`),
			ex(`db.posts.find({}, {"title":1, "author_id":1})`),
			ex(`db.posts.findOne({"status":"published"}, {"title":1, "author_id":1})`),
			ex(`db.posts.find({"status":"published"})`),
			ex(`db.posts.find({"created_at":{"$gte":{"$date":"2026-04-09T00:00:00Z"}}})`),
			ex(`db.posts.find({}).sort({"created_at":-1}).limit(50)`),
			queryReferenceEntry{},
			section("Write"),
			ex(`db.posts.insertOne({"title":"hello"})`),
			ex(`db.posts.updateOne({"_id":{"$oid":"..."}} , {"$set":{"title":"new"}})`),
			ex(`db.posts.deleteMany({"deleted_at":{"$exists":true}})`),
			queryReferenceEntry{},
			section("Aggregate"),
			ex(`db.posts.aggregate([{"$match":{"status":"published"}},{"$group":{"_id":"$author_id","count":{"$sum":1}}}])`),
			queryReferenceEntry{},
			section("Operators"),
			ex(`$eq $ne $gt $gte $lt $lte`),
			ex(`$in $nin $exists $regex`),
			ex(`$and $or $not $set $unset $inc`),
		)
	default:
		lines = append(lines,
			section("Read"),
			ex(`SELECT "id", "title", "url"`),
			ex(`FROM "posts"`),
			ex(`WHERE "status" = 'published'`),
			ex(`ORDER BY "created_at" DESC`),
			ex(`LIMIT 50;`),
			queryReferenceEntry{},
			section("Dates / timestamps"),
			ex(`WHERE "created_at" >= '2026-04-09 15:51:30+00';`),
			ex(`WHERE "created_at" >= '2026-04-09T15:51:30Z';`),
			ex(`Prefer quoted timestamp literals, not bare values.`),
			queryReferenceEntry{},
			section("Write"),
			ex(`INSERT INTO "posts" ("title") VALUES ('hello');`),
			ex(`UPDATE "posts" SET "title" = 'new' WHERE "id" = 1;`),
			ex(`DELETE FROM "posts" WHERE "deleted_at" IS NOT NULL;`),
			queryReferenceEntry{},
			section("Aggregate / join"),
			ex(`SELECT "author_id", COUNT(*) FROM "posts" GROUP BY "author_id";`),
			ex(`SELECT p.*, a."name" FROM "posts" p JOIN "authors" a ON p."author_id" = a."id";`),
			queryReferenceEntry{},
			section("Operator tips"),
			ex(`= != > >= < <= LIKE IN IS NULL`),
			ex(`Accepting > / >= / < / <= inserts only the operator.`),
		)
	}
	return lines
}

func (m Model) queryReferenceLinesForWidth(w int) []string {
	entries := m.queryReferenceEntries()
	lines := make([]string, 0, len(entries))
	wrapW := max(12, w-1)
	for _, entry := range entries {
		if entry.text == "" {
			lines = append(lines, "")
			continue
		}
		for _, line := range wrapTextPreservingRuns(entry.text, wrapW) {
			switch entry.kind {
			case "section":
				lines = append(lines, accentStyle.Render(line))
			default:
				lines = append(lines, dimStyle.Render(line))
			}
		}
	}
	return lines
}

func (m Model) queryReferenceWidth() int {
	if m.isSinglePane() {
		return max(10, m.width-4)
	}
	leftW := m.leftPanelWidth()
	if m.isCompact() {
		leftW = max(20, m.width*28/100)
	}
	rightW := m.width - leftW - 1
	if rightW < 28 {
		leftW = max(20, m.width-28-1)
	}
	return max(12, leftW-4)
}

func (m Model) queryReferenceViewportHeight() int {
	contentH := max(1, m.height-5)
	if m.isSinglePane() {
		innerH := max(1, contentH-2)
		bodyH := max(1, innerH-1)
		return max(1, bodyH-4)
	}
	innerH := max(1, contentH-2)
	return max(1, innerH-4)
}

func (m Model) queryReferencePageStep() int {
	return max(1, m.queryReferenceViewportHeight()-2)
}

func (m Model) compactPaneLabel() string {
	right := m.focus == panelRight || m.queryFocus
	switch m.activeTab {
	case tabConnections:
		if right {
			return "details"
		}
		return "connections"
	case tabBrowse:
		if right {
			return "browse"
		}
		return "tables"
	case tabQuery:
		if right {
			return "editor"
		}
		return "reference"
	case tabResults:
		if right {
			return "results"
		}
		return "query"
	case tabHistory:
		if right {
			return "selected query"
		}
		return "history"
	case tabHelpers:
		if right {
			return "preview"
		}
		return "templates"
	default:
		if right {
			return "details"
		}
		return "list"
	}
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
	case tabBrowse:
		return m.renderBrowseDetail(w, h)
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

	dsn := maskDSNPassword(conn.DSN)
	if utf8.RuneCountInString(dsn) > w-6 {
		dsn = truncate(dsn, w-6)
	}
	lines = append(lines, accentStyle.Render("dsn  ")+textStyle.Render(dsn), "")

	if m.activeConnIdx == m.connCursor {
		lines = append(lines, connectedStyle.Render("● connected"))
	} else {
		lines = append(lines, dimStyle.Render("enter: connect   e: edit   d: delete..."))
	}

	return padLines(lines, h)
}

func (m Model) renderBrowseDetail(w, h int) string {
	if m.activeDB == nil {
		return padLines([]string{dimStyle.Render("not connected")}, h)
	}
	if m.browseView == browseViewData {
		return m.renderBrowseDataPanel(w, h)
	}
	return m.renderBrowseSchemaPanel(w, h)
}

func (m Model) renderBrowseSchemaPanel(w, h int) string {
	if m.tableSchema == nil {
		return padLines([]string{dimStyle.Render("select a " + m.dataSourceLabel())}, h)
	}

	s := m.tableSchema
	lines := []string{
		renderPaneTitle(s.Name, fmt.Sprintf("schema · %d fields · %d rows", len(s.Columns), s.RowCount), w),
		dimStyle.Render(strings.Repeat("─", max(1, w))),
	}
	tbl := m.schemaTable
	tbl.SetWidth(max(10, w))
	tbl.SetHeight(max(1, h-len(lines)-2))
	lines = append(lines, strings.Split(strings.TrimRight(tbl.View(), "\n"), "\n")...)
	return padLines(lines, h)
}

func (m Model) renderBrowseDataPanel(w, h int) string {
	tableName := m.currentTableName()
	if tableName == "" {
		return padLines([]string{dimStyle.Render("select a " + m.dataSourceLabel())}, h)
	}

	meta := m.browseDataMeta()
	lines := []string{
		renderPaneTitle(tableName, "data", w),
		dimStyle.Render(strings.Repeat("─", max(1, w))),
	}

	tbl := m.browseDataTable
	tbl.SetWidth(max(10, w))
	tableH := max(1, h-len(lines)-1)
	if meta != "" {
		tableH = max(1, h-len(lines)-3)
	}
	tbl.SetHeight(tableH)
	lines = append(lines, strings.Split(strings.TrimRight(tbl.View(), "\n"), "\n")...)
	if meta != "" {
		lines = append(lines, "", meta)
	}
	return padLines(lines, h)
}

func (m Model) browseDataMeta() string {
	if m.browseData == nil {
		return ""
	}
	totalCols := len(m.browseData.Columns)
	if totalCols == 0 {
		return ""
	}
	start := m.browseColOffset + 1
	end := m.browseColOffset + m.browseVisibleColumn
	if end < start {
		end = start
	}
	parts := []string{
		dimStyle.Render(fmt.Sprintf("%d row(s)", len(m.browseData.Rows))),
		dimStyle.Render(fmt.Sprintf("cols %d-%d/%d", start, end, totalCols)),
	}
	if totalCols > m.browseVisibleColumn {
		parts = append(parts, accentStyle.Render("←/→ columns"))
	}
	parts = append(parts, dimStyle.Render("enter: schema"))
	return strings.Join(parts, dimStyle.Render(" · "))
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
	if m.showColumnPicker {
		lines = append(lines, m.renderCompletionPopover(w)...)
	}
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
		lines = append(lines, dimStyle.Render("Write a query, use tab to complete, ctrl+r to run, pgup/dn for reference."))
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
		meta = ""
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

func (m Model) renderConfirmModal() string {
	lines := []string{warnStyle.Render(m.confirmTitle), ""}
	lines = append(lines, m.confirmBody...)
	lines = append(lines, "", dimStyle.Render("enter/y confirms · esc/n cancels"))
	return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
}

func (m Model) renderNewConnModal() string {
	var lines []string
	title := "New Connection"
	saveText := "save"
	if m.newConnEditIdx >= 0 {
		title = "Edit Connection"
		saveText = "save changes"
	}
	lines = append(lines, panelHeaderStyle.Render(title), "")

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

	saveLabel := inactiveTabStyle.Render(saveText)
	if m.newConnFocus == newConnFocusSave {
		saveLabel = activeTabStyle.Render(saveText)
	}
	lines = append(lines, "  [ "+saveLabel+" ]")
	lines = append(lines, "", dimStyle.Render("tab moves focus · left/right type · paste normally · esc cancel"))

	return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
}

func (m Model) renderHelpModal() string {
	lines := []string{
		panelHeaderStyle.Render("bobdb keybinds"),
		"",
		keyStyle.Render("1-4") + " " + actionStyle.Render("switch tabs"),
		keyStyle.Render("tab") + " " + actionStyle.Render("toggle left/right pane"),
		keyStyle.Render("↑/↓") + " " + actionStyle.Render("move selection"),
		keyStyle.Render("enter") + " " + actionStyle.Render("connect / select / use"),
		keyStyle.Render("n") + " " + actionStyle.Render("new connection"),
		keyStyle.Render("e") + " " + actionStyle.Render("edit selected connection"),
		keyStyle.Render("d") + " " + actionStyle.Render("delete connection (confirm)"),
		keyStyle.Render("r") + " " + actionStyle.Render("refresh browse list"),
		keyStyle.Render("enter") + " " + actionStyle.Render("toggle schema / data view"),
		keyStyle.Render("e") + " " + actionStyle.Render("edit focused cell — pre-fills current value"),
		keyStyle.Render("E") + " " + actionStyle.Render("edit focused cell — empty value (for replacing)"),
		keyStyle.Render("ctrl+r") + " " + actionStyle.Render("run query"),
		keyStyle.Render("y / enter") + " " + actionStyle.Render("confirm destructive actions"),
		keyStyle.Render("n / esc") + " " + actionStyle.Render("cancel destructive actions"),
		keyStyle.Render("ctrl+l") + " " + actionStyle.Render("clear query editor"),
		keyStyle.Render("ctrl+g") + " " + actionStyle.Render("AI generate query (ollama)"),
		keyStyle.Render("ctrl+t") + " " + actionStyle.Render("templates picker"),
		keyStyle.Render("ctrl+e") + " " + actionStyle.Render("examples picker"),
		keyStyle.Render("ctrl+o") + " " + actionStyle.Render("query history picker"),
		keyStyle.Render("ctrl+u") + " " + actionStyle.Render("saved query picker"),
		keyStyle.Render("ctrl+s") + " " + actionStyle.Render("save current query"),
		keyStyle.Render("ctrl+y") + " " + actionStyle.Render("last run"),
		keyStyle.Render("/") + " " + actionStyle.Render("jump to Query editor"),
		keyStyle.Render("pgup / pgdn") + " " + actionStyle.Render("scroll Query Reference"),
		keyStyle.Render("home / end") + " " + actionStyle.Render("jump Query Reference"),
		keyStyle.Render("tab") + " " + actionStyle.Render("accept completion"),
		keyStyle.Render("f / x / y") + " " + actionStyle.Render("templates / examples / history from Query"),
		keyStyle.Render("g / u / s") + " " + actionStyle.Render("ai generate / saved / save from Query"),
		keyStyle.Render("←/→") + " " + actionStyle.Render("page result columns"),
		keyStyle.Render("c / C") + " " + actionStyle.Render("copy direct / copy as (Browse/Results/Query)"),
		keyStyle.Render("v") + " " + actionStyle.Render("inspect selected schema/result row"),
		keyStyle.Render("esc") + " " + actionStyle.Render("close modal / blur editor"),
		keyStyle.Render("q") + " " + actionStyle.Render("back (quit from connections)"),
		"",
		dimStyle.Render("press any key to close"),
	}

	return dialogStyle.Width(min(64, m.width-6)).Render(strings.Join(lines, "\n"))
}

func dbIcon(dbType string) string {
	switch dbType {
	case "sqlite":
		return "￭"
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
	if table := m.queryInferredTable(); table != "" {
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
	if count := len(m.savedQueries); count > 0 {
		parts = append(parts, dimStyle.Render(fmt.Sprintf("%d saved", count)))
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
		if item.sectionRow {
			lines = append(lines, "", accentStyle.Render(" "+strings.ToUpper(item.label)))
			continue
		}
		labelW := rowW
		if item.detail != "" {
			labelW = max(12, rowW-20)
		}
		row := truncate(item.label, labelW)
		if item.detail != "" {
			row += "  " + truncate(item.detail, 18)
		}
		if i == m.queryPickerCursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(row, rowW)+" "))
		} else {
			lines = append(lines, " "+row)
		}
	}
	// Preview selected item value
	if item := m.currentQueryPickerItem(); item.value != "" {
		previewLines := strings.SplitN(strings.TrimSpace(item.value), "\n", 5)
		lines = append(lines, "")
		for i, pl := range previewLines {
			if i == 4 {
				lines = append(lines, dimStyle.Render("  …"))
				break
			}
			lines = append(lines, dimStyle.Render("  "+truncate(pl, max(20, rowW-2))))
		}
	}
	lines = append(lines, "", dimStyle.Render("enter loads · c copies · esc closes"))
	return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
}

func (m Model) renderCompletionPopover(w int) []string {
	popW := max(28, min(60, w-2))
	rowW := popW - 2
	title := panelHeaderStyle.Render(m.columnPickerTitle)
	lines := []string{title}
	if len(m.columnPickerItems) == 0 {
		lines = append(lines, dimStyle.Render("  no completion items available"))
		lines = append(lines, dimStyle.Render("  esc closes"))
		return lines
	}
	visibleH := max(4, min(8, m.height/3))
	start := listStartForCursor(len(m.columnPickerItems), visibleH, m.columnPickerCursor)
	end := min(len(m.columnPickerItems), start+visibleH)
	for i := start; i < end; i++ {
		item := m.columnPickerItems[i]
		marker := "[ ]"
		if item.Selected {
			marker = "[x]"
		}
		if !m.columnPickerMulti {
			marker = " • "
			if i == m.columnPickerCursor {
				marker = " ▸ "
			}
		}
		label := truncate(item.Label, max(10, rowW-20))
		row := marker + " " + label
		if item.Detail != "" {
			row += "  " + dimStyle.Render(truncate(item.Detail, 18))
		}
		if i == m.columnPickerCursor {
			lines = append(lines, selectedItemStyle.Render(" "+padRight(row, rowW)+" "))
		} else {
			lines = append(lines, " "+row)
		}
	}
	if end < len(m.columnPickerItems) {
		lines = append(lines, dimStyle.Render(fmt.Sprintf("  +%d more…", len(m.columnPickerItems)-end)))
	}
	if m.columnPickerValueMode {
		filter := m.columnPickerValuePrefix
		if filter != "" {
			runes := []rune(filter)
			cursor := clampInt(m.columnPickerValueCursor, 0, len(runes))
			filter = string(runes[:cursor]) + "|" + string(runes[cursor:])
		} else {
			filter = "|"
		}
		lines = append(lines, accentStyle.Render("  filter: "+filter))
	}
	hint := "↑/↓ move · tab insert · enter newline · esc close"
	if m.columnPickerValueMode {
		hint = "↑/↓ move · type to filter · ←/→ edit · tab insert · enter newline · esc close"
	} else if m.columnPickerMulti {
		hint = "↑/↓ · space toggle · tab insert · enter newline · esc close"
	}
	lines = append(lines, dimStyle.Render("  "+hint))
	return lines
}

func fallbackTableName(name string) string {
	if name == "" {
		return "table_name"
	}
	return name
}

func fallbackColumnName(name string) string {
	if name == "" {
		return "column_name"
	}
	return name
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
	return truncate(compactInline(formatDisplayValue(s)), max(1, width-1))
}

func formatDisplayValue(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	match := fractionalTimestampPattern.FindStringSubmatch(s)
	if len(match) != 4 {
		return s
	}
	suffix := match[3]
	if suffix != "" && !strings.HasPrefix(suffix, "Z") && !strings.HasPrefix(suffix, "+") && !strings.HasPrefix(suffix, "-") && !strings.HasPrefix(suffix, " ") {
		return s
	}
	frac := strings.TrimRight(match[2], "0")
	if frac == "" {
		return match[1] + suffix
	}
	return match[1] + "." + frac + suffix
}

func (m Model) inspectViewportHeight() int {
	return max(6, min(20, m.height-10))
}

// colorizeJSONLine applies minimal syntax coloring to a single indented JSON line.
func colorizeJSONLine(line string) string {
	trimmed := strings.TrimLeft(line, " ")
	if trimmed == "" {
		return line
	}
	indent := line[:len(line)-len(trimmed)]
	// Structural-only lines: {, }, [, ], with optional trailing comma
	clean := strings.TrimRight(trimmed, ",")
	if clean == "{" || clean == "}" || clean == "[" || clean == "]" {
		return indent + dimStyle.Render(trimmed)
	}
	// "key": value — color key in accent, value in text
	if strings.HasPrefix(trimmed, `"`) {
		if i := strings.Index(trimmed, `":`); i > 0 {
			key := trimmed[:i+2]
			rest := trimmed[i+2:]
			return indent + accentStyle.Render(key) + textStyle.Render(rest)
		}
	}
	return textStyle.Render(line)
}

// maskDSNPassword replaces the password segment of a user:pass@host DSN
// with asterisks so credentials don't appear on screen / in screenshots.
// SQLite file paths and DSNs without credentials pass through unchanged.
func maskDSNPassword(dsn string) string {
	scheme := strings.Index(dsn, "://")
	if scheme < 0 {
		return dsn
	}
	rest := dsn[scheme+3:]
	at := strings.Index(rest, "@")
	if at < 0 {
		return dsn
	}
	creds := rest[:at]
	colon := strings.Index(creds, ":")
	if colon < 0 {
		return dsn
	}
	masked := creds[:colon+1] + strings.Repeat("*", len(creds)-colon-1)
	return dsn[:scheme+3] + masked + rest[at:]
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

func (m Model) renderOllamaGenModal() string {
	lines := []string{panelHeaderStyle.Render("Generate Query with Ollama"), ""}
	rowW := max(32, min(72, m.width-14))

	switch {
	case m.ollamaGenerating:
		lines = append(lines, accentStyle.Render("  generating..."))
		lines = append(lines, "", dimStyle.Render("esc cancels"))

	case m.ollamaErr != "":
		lines = append(lines, errorStyle.Render("error: "+m.ollamaErr))
		lines = append(lines, "", dimStyle.Render("r to retry · esc closes"))

	case m.ollamaResult != "":
		resultLines := strings.Split(strings.TrimSpace(m.ollamaResult), "\n")
		for _, rl := range resultLines {
			lines = append(lines, dimStyle.Render("  "+truncate(rl, max(20, rowW-2))))
		}
		lines = append(lines, "", dimStyle.Render("enter accepts · r retry · esc cancel"))

	default:
		lines = append(lines, m.ollamaInput.View())
		lines = append(lines, "", dimStyle.Render("enter to generate · esc closes"))
	}

	return dialogStyle.Width(min(92, m.width-6)).Render(strings.Join(lines, "\n"))
}
