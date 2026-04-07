package main

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/lipgloss"

	"dbkit/internal/db"
)

const (
	uiOverhead   = 4 // header(1) + tabbar(1) + statusbar(1) + gap(1)
	minWidth     = 60
	minHeight    = 20
	borderWidth  = 2 // rounded border takes 2 chars (left+right)
	borderHeight = 2 // top+bottom border
)

func (m Model) View() string {
	if m.width == 0 {
		return "loading..."
	}
	if m.width < minWidth || m.height < minHeight {
		return lipgloss.NewStyle().Foreground(colorOrange).Bold(true).Padding(1).
			Render(fmt.Sprintf("terminal too small (%dx%d)\nminimum %dx%d", m.width, m.height, minWidth, minHeight))
	}

	header := m.renderHeader()
	tabBar := m.renderTabBar()
	panels := m.renderPanels()
	statusBar := m.renderStatusBar()

	content := lipgloss.JoinVertical(lipgloss.Left, header, tabBar, panels, statusBar)

	// Overlays
	if m.showNewConn {
		content = placeOverlay(content, m.renderNewConnModal(), m.width, m.height)
	}
	if m.showHelp {
		content = placeOverlay(content, m.renderHelpModal(), m.width, m.height)
	}

	return content
}

// --- Header ---

func (m Model) renderHeader() string {
	left := headerStyle.Render(" dbkit ")
	var connInfo string
	if m.activeDB != nil {
		connInfo = "  " + connectedStyle.Render("●") + " " +
			purpleStyle.Render(m.activeConnName) +
			grayStyle.Render(" ("+m.activeDB.Type()+")")
	} else {
		connInfo = "  " + dimItemStyle.Render("not connected")
	}
	loading := ""
	if m.loading {
		loading = "  " + orangeStyle.Render("loading...")
	}
	right := grayStyle.Render("? help")
	leftPart := lipgloss.JoinHorizontal(lipgloss.Top, left, connInfo, loading)
	gap := strings.Repeat(" ", max(1, m.width-lipgloss.Width(leftPart)-lipgloss.Width(right)))
	return leftPart + gap + right
}

// --- Tab bar ---

func (m Model) renderTabBar() string {
	var tabs []string
	for i := tab(0); i < tabCount; i++ {
		num := purpleStyle.Render(fmt.Sprintf("%d", i+1))
		name := tabNames[i]
		var t string
		if i == m.activeTab {
			t = tabActiveStyle.Render(fmt.Sprintf("%s %s", num, name))
		} else {
			disabled := m.activeDB == nil && (i == tabSchema || i == tabQuery)
			if disabled {
				t = tabInactiveStyle.Foreground(colorDark).Render(fmt.Sprintf("%s %s", num, name))
			} else {
				t = tabInactiveStyle.Render(fmt.Sprintf("%s %s", num, name))
			}
		}
		tabs = append(tabs, t)
	}
	tabRow := lipgloss.JoinHorizontal(lipgloss.Top, tabs...)
	rest := strings.Repeat(" ", max(0, m.width-lipgloss.Width(tabRow)))
	return tabBarStyle.Render(tabRow + rest)
}

// --- Panels ---

func (m Model) renderPanels() string {
	panelHeight := m.height - uiOverhead
	if panelHeight < 3 {
		panelHeight = 3
	}
	lw := m.leftPanelWidth() - borderWidth
	rw := m.rightPanelWidth() - borderWidth

	left := m.renderLeftPanel(lw, panelHeight-borderHeight)
	right := m.renderRightPanel(rw, panelHeight-borderHeight)

	lStyle := panelStyle.Width(lw).Height(panelHeight - borderHeight)
	rStyle := panelStyle.Width(rw).Height(panelHeight - borderHeight)
	if m.queryFocus || m.focus == panelRight {
		rStyle = activePanelStyle.Width(rw).Height(panelHeight - borderHeight)
	} else {
		lStyle = activePanelStyle.Width(lw).Height(panelHeight - borderHeight)
	}

	leftBox := lStyle.Render(left)
	rightBox := rStyle.Render(right)
	return lipgloss.JoinHorizontal(lipgloss.Top, leftBox, rightBox)
}

// --- Left panel ---

func (m Model) renderLeftPanel(w, h int) string {
	switch m.activeTab {
	case tabConnections:
		return m.renderConnectionList(w, h)
	case tabSchema, tabQuery:
		return m.renderTableList(w, h)
	case tabHelpers:
		return m.renderHelperList(w, h)
	}
	return ""
}

func (m Model) renderConnectionList(w, h int) string {
	var lines []string
	title := labelStyle.Render("saved connections")
	lines = append(lines, title, "")

	if len(m.cfg.Connections) == 0 {
		lines = append(lines, dimItemStyle.Render("no connections — press n to add"))
	}

	for i, conn := range m.cfg.Connections {
		icon := dbIcon(conn.Type)
		name := truncate(conn.Name, w-8)
		var line string
		if i == m.connCursor {
			line = selectedItemStyle.Render(fmt.Sprintf(" %s %-*s ", icon, w-7, name))
		} else if i == m.activeConnIdx {
			line = connectedStyle.Render(fmt.Sprintf(" %s %s", icon, name))
		} else {
			line = fmt.Sprintf(" %s %s", icon, name)
		}
		lines = append(lines, line)
	}

	return padLines(lines, h)
}

func (m Model) renderTableList(w, h int) string {
	var lines []string
	title := labelStyle.Render("tables")
	lines = append(lines, title, "")

	if m.activeDB == nil {
		lines = append(lines, dimItemStyle.Render("not connected"))
		return padLines(lines, h)
	}
	if len(m.tables) == 0 {
		lines = append(lines, dimItemStyle.Render("no tables found"))
		return padLines(lines, h)
	}

	for i, t := range m.tables {
		name := truncate(t, w-3)
		if i == m.tableCursor {
			lines = append(lines, selectedItemStyle.Render(fmt.Sprintf(" %-*s ", w-2, name)))
		} else {
			lines = append(lines, fmt.Sprintf(" %s", name))
		}
	}

	return padLines(lines, h)
}

func (m Model) renderHelperList(w, h int) string {
	var lines []string
	title := labelStyle.Render("query templates")
	lines = append(lines, title, "")

	for i, helper := range queryHelpers {
		name := truncate(helper.label, w-3)
		if i == m.helperCursor {
			lines = append(lines, selectedItemStyle.Render(fmt.Sprintf(" %-*s ", w-2, name)))
		} else {
			lines = append(lines, fmt.Sprintf(" %s", name))
		}
	}

	return padLines(lines, h)
}

// --- Right panel ---

func (m Model) renderRightPanel(w, h int) string {
	switch m.activeTab {
	case tabConnections:
		return m.renderConnectionDetail(w, h)
	case tabSchema:
		return m.renderSchemaDetail(w, h)
	case tabQuery:
		return m.renderQueryPanel(w, h)
	case tabHelpers:
		return m.renderHelperPreview(w, h)
	}
	return ""
}

func (m Model) renderConnectionDetail(w, h int) string {
	var lines []string

	if len(m.cfg.Connections) == 0 {
		lines = append(lines, dimItemStyle.Render("press n to add a connection"))
		return padLines(lines, h)
	}

	conn := m.cfg.Connections[m.connCursor]
	lines = append(lines, labelStyle.Render(conn.Name), "")
	lines = append(lines, purpleStyle.Render("type  ")+whiteStyle.Render(conn.Type))
	lines = append(lines, purpleStyle.Render("id    ")+grayStyle.Render(conn.ID))

	dsnDisplay := conn.DSN
	if len(dsnDisplay) > w-8 {
		dsnDisplay = dsnDisplay[:w-11] + "..."
	}
	lines = append(lines, purpleStyle.Render("dsn   ")+whiteStyle.Render(dsnDisplay))
	lines = append(lines, "")

	if m.activeConnIdx == m.connCursor {
		lines = append(lines, connectedStyle.Render("● connected"))
	} else {
		lines = append(lines, grayStyle.Render("enter")+whiteStyle.Render(": connect  ")+
			grayStyle.Render("d")+whiteStyle.Render(": delete"))
	}

	return padLines(lines, h)
}

func (m Model) renderSchemaDetail(w, h int) string {
	if m.activeDB == nil {
		return padLines([]string{dimItemStyle.Render("not connected")}, h)
	}
	if m.tableSchema == nil {
		return padLines([]string{dimItemStyle.Render("select a table")}, h)
	}

	s := m.tableSchema
	var lines []string
	header := labelStyle.Render(s.Name) + "  " + grayStyle.Render(fmt.Sprintf("%d rows", s.RowCount))
	lines = append(lines, header, "")

	// Column header
	colW := 20
	typeW := 14
	colHeader := tableHeaderStyle.Render(fmt.Sprintf("  %-*s %-*s %s", colW, "column", typeW, "type", "flags"))
	lines = append(lines, colHeader)

	for _, col := range s.Columns {
		name := truncate(col.Name, colW)
		typ := truncate(col.Type, typeW)
		var flags []string
		if col.PrimaryKey {
			flags = append(flags, pkStyle.Render("PK"))
		}
		if !col.Nullable {
			flags = append(flags, yellowStyle.Render("NOT NULL"))
		}
		flagStr := strings.Join(flags, " ")
		line := fmt.Sprintf("  %-*s %-*s %s", colW, name, typeW, typ, flagStr)
		lines = append(lines, line)
	}

	lines = append(lines, "", grayStyle.Render("enter/q: open in Query"))
	return padLines(lines, h)
}

func (m Model) renderQueryPanel(w, h int) string {
	if m.activeDB == nil {
		return padLines([]string{dimItemStyle.Render("not connected — go to Connections tab")}, h)
	}

	// Textarea for query input (top ~30% of panel)
	inputH := 7
	resultH := h - inputH - 3
	if resultH < 3 {
		resultH = 3
	}

	m.queryInput.SetWidth(w - 2)
	m.queryInput.SetHeight(inputH)

	inputLabel := labelStyle.Render("query") + "  " +
		grayStyle.Render("ctrl+r: run") + "  " +
		grayStyle.Render("esc: unfocus")
	divider := grayStyle.Render(strings.Repeat("─", w))

	queryBox := inputLabel + "\n" + m.queryInput.View() + "\n" + divider

	// Results
	var resultLines []string
	if m.loading {
		resultLines = append(resultLines, orangeStyle.Render("running..."))
	} else if m.queryErr != "" {
		for _, line := range wrapText(m.queryErr, w-2) {
			resultLines = append(resultLines, errorStyle.Render(line))
		}
	} else if m.queryResult != nil {
		resultLines = m.renderResultTable(m.queryResult, w-2)
	} else {
		resultLines = []string{dimItemStyle.Render("results appear here")}
	}

	// Scroll
	start := m.resultScroll
	if start >= len(resultLines) {
		start = max(0, len(resultLines)-1)
	}
	end := start + resultH
	if end > len(resultLines) {
		end = len(resultLines)
	}
	visible := resultLines[start:end]

	scrollInfo := ""
	if len(resultLines) > resultH {
		scrollInfo = " " + grayStyle.Render(fmt.Sprintf("[%d/%d] ctrl+d/u scroll", start+1, len(resultLines)))
	}

	resultContent := strings.Join(visible, "\n")

	return queryBox + "\n" + resultContent + scrollInfo
}

func (m Model) renderResultTable(result *db.QueryResult, w int) []string {
	if result.Message != "" && len(result.Rows) == 0 {
		return []string{greenStyle.Render(result.Message)}
	}

	if len(result.Columns) == 0 {
		return []string{dimItemStyle.Render("(no columns)")}
	}

	// Calculate column widths
	colWidths := make([]int, len(result.Columns))
	for i, c := range result.Columns {
		colWidths[i] = utf8.RuneCountInString(c)
	}
	for _, row := range result.Rows {
		for i, cell := range row {
			if i < len(colWidths) {
				l := utf8.RuneCountInString(cell)
				if l > colWidths[i] {
					colWidths[i] = l
				}
			}
		}
	}
	// Cap each column width
	maxColW := w / max(1, len(result.Columns))
	if maxColW < 8 {
		maxColW = 8
	}
	for i := range colWidths {
		if colWidths[i] > maxColW {
			colWidths[i] = maxColW
		}
	}

	var lines []string

	// Header
	var hdr strings.Builder
	for i, col := range result.Columns {
		hdr.WriteString(tableHeaderStyle.Render(fmt.Sprintf(" %-*s ", colWidths[i], truncate(col, colWidths[i]))))
	}
	lines = append(lines, hdr.String())

	// Rows
	for _, row := range result.Rows {
		var rowStr strings.Builder
		for i, cell := range row {
			if i >= len(colWidths) {
				break
			}
			c := truncate(cell, colWidths[i])
			if cell == "NULL" {
				rowStr.WriteString(nullableStyle.Render(fmt.Sprintf(" %-*s ", colWidths[i], c)))
			} else {
				rowStr.WriteString(fmt.Sprintf(" %-*s ", colWidths[i], c))
			}
		}
		lines = append(lines, rowStr.String())
	}

	summary := grayStyle.Render(fmt.Sprintf("%d row(s)", len(result.Rows)))
	lines = append(lines, "", summary)
	return lines
}

func (m Model) renderHelperPreview(w, h int) string {
	var lines []string
	if m.helperCursor >= len(queryHelpers) {
		return padLines(lines, h)
	}
	h2 := queryHelpers[m.helperCursor]
	lines = append(lines, labelStyle.Render(h2.label), "")
	for _, line := range strings.Split(h2.template, "\n") {
		lines = append(lines, cyanStyle.Render(truncate(line, w-2)))
	}
	lines = append(lines, "", grayStyle.Render("enter: use this template"))
	return padLines(lines, h)
}

// --- Status bar ---

func (m Model) renderStatusBar() string {
	left := ""
	if time.Now().Before(m.statusExpiry) {
		left = " " + m.statusMsg
	}

	keybinds := grayStyle.Render("1-4") + whiteStyle.Render(":tab  ") +
		grayStyle.Render("tab") + whiteStyle.Render(":panel  ") +
		grayStyle.Render("n") + whiteStyle.Render(":new  ") +
		grayStyle.Render("q") + whiteStyle.Render(":quit")

	leftW := m.width - lipgloss.Width(keybinds) - 2
	if leftW < 0 {
		leftW = 0
	}
	leftPart := lipgloss.NewStyle().Background(colorBg).Foreground(colorWhite).
		Width(leftW).Render(left)
	return lipgloss.JoinHorizontal(lipgloss.Top, leftPart, keybinds)
}

// --- Modals ---

func (m Model) renderNewConnModal() string {
	w := min(m.width-8, 60)

	var lines []string
	lines = append(lines, labelStyle.Render("new connection"), "")

	// Name
	nameLabel := purpleStyle.Render("name     ")
	nameFocused := m.newConnFocus == 0
	nameLine := nameLabel + renderInputField(m.newConnInputs[fieldName].View(), nameFocused, w-12)
	lines = append(lines, nameLine)

	// Type selector
	typeLabel := purpleStyle.Render("type     ")
	var typeOptions []string
	for i, t := range dbTypes {
		if i == m.newConnTypeCur {
			typeOptions = append(typeOptions, tabActiveStyle.Render(" "+t+" "))
		} else {
			typeOptions = append(typeOptions, tabInactiveStyle.Render(" "+t+" "))
		}
	}
	typeFocused := m.newConnFocus == 2
	typeRow := typeLabel + lipgloss.JoinHorizontal(lipgloss.Top, typeOptions...)
	if typeFocused {
		typeRow = orangeStyle.Render("▶ ") + typeRow
	} else {
		typeRow = "  " + typeRow
	}
	lines = append(lines, typeRow)

	// DSN
	dsnLabel := purpleStyle.Render("dsn/path ")
	dsnFocused := m.newConnFocus == 3
	dsnLine := dsnLabel + renderInputField(m.newConnInputs[fieldDSN].View(), dsnFocused, w-12)
	lines = append(lines, dsnLine)

	lines = append(lines, "")

	// Submit button
	submitStyle := tabInactiveStyle
	if m.newConnFocus == fieldCount+1 {
		submitStyle = tabActiveStyle
	}
	lines = append(lines, "  "+submitStyle.Render(" save "))
	lines = append(lines, "", grayStyle.Render("tab: next field  esc: cancel"))

	content := strings.Join(lines, "\n")

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(colorPurple).
		Padding(1, 2).
		Width(w).
		Render(content)
}

func renderInputField(view string, focused bool, _ int) string {
	if focused {
		return orangeStyle.Render("▶ ") + view
	}
	return "  " + view
}

func (m Model) renderHelpModal() string {
	lines := []string{
		labelStyle.Render("keybindings"), "",
		purpleStyle.Render("1-4") + whiteStyle.Render("         switch tabs"),
		purpleStyle.Render("tab") + whiteStyle.Render("         toggle left/right panel"),
		purpleStyle.Render("j/k  ↑/↓") + whiteStyle.Render("    navigate lists"),
		purpleStyle.Render("enter") + whiteStyle.Render("       connect / select / use"),
		purpleStyle.Render("n") + whiteStyle.Render("           new connection"),
		purpleStyle.Render("d") + whiteStyle.Render("           delete connection"),
		purpleStyle.Render("r") + whiteStyle.Render("           refresh tables"),
		purpleStyle.Render("e / tab") + whiteStyle.Render("     focus query editor"),
		purpleStyle.Render("ctrl+r") + whiteStyle.Render("      run query"),
		purpleStyle.Render("esc") + whiteStyle.Render("         blur query editor"),
		purpleStyle.Render("ctrl+d/u") + whiteStyle.Render("    scroll results"),
		purpleStyle.Render("q") + whiteStyle.Render("           quit (outside editor)"),
		purpleStyle.Render("?") + whiteStyle.Render("           this help"),
		"",
		grayStyle.Render("any key to close"),
	}

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(colorPurple).
		Padding(1, 2).
		Render(strings.Join(lines, "\n"))
}

// --- Utilities ---

func dbIcon(dbType string) string {
	switch dbType {
	case "sqlite":
		return "◆"
	case "postgres":
		return "●"
	case "mongo":
		return "▲"
	}
	return "○"
}

func truncate(s string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	if max <= 3 {
		return string(runes[:max])
	}
	return string(runes[:max-3]) + "..."
}

func padLines(lines []string, h int) string {
	result := strings.Join(lines, "\n")
	current := len(lines)
	if current < h {
		result += strings.Repeat("\n", h-current)
	}
	return result
}

func wrapText(s string, w int) []string {
	if w <= 0 {
		return []string{s}
	}
	var lines []string
	words := strings.Fields(s)
	line := ""
	for _, word := range words {
		if len(line)+len(word)+1 > w {
			if line != "" {
				lines = append(lines, line)
			}
			line = word
		} else {
			if line != "" {
				line += " "
			}
			line += word
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

// placeOverlay centers an overlay string over a background.
func placeOverlay(bg, overlay string, bgW, bgH int) string {
	oLines := strings.Split(overlay, "\n")
	oH := len(oLines)
	oW := 0
	for _, l := range oLines {
		if w := lipgloss.Width(l); w > oW {
			oW = w
		}
	}

	startY := (bgH - oH) / 2
	startX := (bgW - oW) / 2
	if startY < 0 {
		startY = 0
	}
	if startX < 0 {
		startX = 0
	}

	bgLines := strings.Split(bg, "\n")
	for i, ol := range oLines {
		row := startY + i
		if row >= len(bgLines) {
			break
		}
		bgLine := bgLines[row]
		// Pad background line to bgW
		bgRunes := []rune(stripANSI(bgLine))
		for len(bgRunes) < bgW {
			bgRunes = append(bgRunes, ' ')
		}
		// Replace characters at startX
		result := make([]rune, bgW)
		copy(result, bgRunes)
		olPlain := []rune(stripANSI(ol))
		for j, r := range olPlain {
			pos := startX + j
			if pos < bgW {
				result[pos] = r
			}
		}
		// Reconstruct: use overlay's styled version in the center
		pad := strings.Repeat(" ", startX)
		bgLines[row] = pad + ol
		_ = result
	}
	return strings.Join(bgLines, "\n")
}

func stripANSI(s string) string {
	// Simple ANSI stripper for width calculations
	var b strings.Builder
	inEscape := false
	for _, r := range s {
		if r == '\x1b' {
			inEscape = true
			continue
		}
		if inEscape {
			if r == 'm' {
				inEscape = false
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
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
