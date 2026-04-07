package main

import "github.com/charmbracelet/lipgloss"

const (
	colorBg     = lipgloss.Color("235")
	colorPurple = lipgloss.Color("99")
	colorOrange = lipgloss.Color("214")
	colorWhite  = lipgloss.Color("255")
	colorGray   = lipgloss.Color("242")
	colorDark   = lipgloss.Color("238")
	colorGreen  = lipgloss.Color("120")
	colorRed    = lipgloss.Color("196")
	colorCyan   = lipgloss.Color("87")
	colorYellow = lipgloss.Color("226")
)

var (
	purpleStyle = lipgloss.NewStyle().Background(colorBg).Foreground(colorPurple)
	orangeStyle = lipgloss.NewStyle().Background(colorBg).Foreground(colorOrange)
	grayStyle   = lipgloss.NewStyle().Background(colorBg).Foreground(colorGray)
	whiteStyle  = lipgloss.NewStyle().Background(colorBg).Foreground(colorWhite)
	greenStyle  = lipgloss.NewStyle().Foreground(colorGreen)
	redStyle    = lipgloss.NewStyle().Foreground(colorRed)
	cyanStyle   = lipgloss.NewStyle().Foreground(colorCyan)
	yellowStyle = lipgloss.NewStyle().Foreground(colorYellow)

	headerStyle = lipgloss.NewStyle().
			Background(colorBg).
			Foreground(colorPurple).
			Bold(true).
			Padding(0, 1)

	statusStyle = lipgloss.NewStyle().
			Background(colorBg).
			Foreground(colorWhite).
			Width(0) // set dynamically

	tabActiveStyle = lipgloss.NewStyle().
			Background(colorPurple).
			Foreground(colorBg).
			Bold(true).
			Padding(0, 2)

	tabInactiveStyle = lipgloss.NewStyle().
				Background(colorDark).
				Foreground(colorGray).
				Padding(0, 2)

	tabBarStyle = lipgloss.NewStyle().
			Background(colorBg).
			Padding(0, 0)

	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorGray)

	activePanelStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorPurple)

	selectedItemStyle = lipgloss.NewStyle().
				Background(colorPurple).
				Foreground(colorBg).
				Bold(true)

	dimItemStyle = lipgloss.NewStyle().
			Foreground(colorGray)

	labelStyle = lipgloss.NewStyle().
			Foreground(colorPurple).
			Bold(true)

	connectedStyle = lipgloss.NewStyle().
			Foreground(colorGreen).
			Bold(true)

	errorStyle = lipgloss.NewStyle().
			Foreground(colorRed)

	tableHeaderStyle = lipgloss.NewStyle().
				Background(colorDark).
				Foreground(colorCyan).
				Bold(true)

	pkStyle = lipgloss.NewStyle().
		Foreground(colorOrange)

	nullableStyle = lipgloss.NewStyle().
			Foreground(colorGray)
)
