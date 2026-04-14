package main

import (
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
)

var (
	// Shared tui-suite palette (sb/runx family)
	colorPrimary = lipgloss.Color("#5AF78E")
	colorAccent  = lipgloss.Color("#57C7FF")
	colorWarn    = lipgloss.Color("#FF6AC1")
	colorError   = lipgloss.Color("#FF5C57")
	colorDim     = lipgloss.Color("#606060")
	colorText    = lipgloss.Color("#EEEEEE")
	colorBg      = lipgloss.Color("#1A1A2E")
	colorSurface = lipgloss.Color("#2A2A40")
	colorYellow  = lipgloss.Color("#F3F99D")

	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorPrimary)

	activeTabStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorAccent)

	inactiveTabStyle = lipgloss.NewStyle().
				Foreground(colorDim)

	keyStyle = lipgloss.NewStyle().
			Foreground(colorAccent).
			Bold(true)

	actionStyle = lipgloss.NewStyle().
			Foreground(colorPrimary)

	dimStyle = lipgloss.NewStyle().
			Foreground(colorDim)

	textStyle = lipgloss.NewStyle().
			Foreground(colorText)

	primaryStyle = lipgloss.NewStyle().
			Foreground(colorPrimary)

	accentStyle = lipgloss.NewStyle().
			Foreground(colorAccent)

	warnStyle = lipgloss.NewStyle().
			Foreground(colorWarn)

	errorStyle = lipgloss.NewStyle().
			Foreground(colorError).
			Bold(true)

	statusStyle = lipgloss.NewStyle().
			Foreground(colorDim)

	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorDim).
			Padding(0, 1)

	panelActiveStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorAccent).
				Padding(0, 1)

	panelHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(colorAccent)

	selectedItemStyle = lipgloss.NewStyle().
				Foreground(colorPrimary).
				Background(colorSurface).
				Bold(true)

	tableHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(colorAccent)

	connectedStyle = lipgloss.NewStyle().
			Foreground(colorPrimary).
			Bold(true)

	dialogStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorAccent).
			Padding(1, 2)
)

func newTableStyles() table.Styles {
	s := table.DefaultStyles()
	s.Header = s.Header.
		Foreground(colorAccent).
		BorderStyle(lipgloss.NormalBorder()).
		BorderBottom(true).
		BorderForeground(colorDim).
		Bold(true)
	s.Cell = s.Cell.
		Foreground(colorText)
	s.Selected = s.Selected.
		Foreground(colorPrimary).
		Background(colorSurface).
		Bold(true)
	return s
}
