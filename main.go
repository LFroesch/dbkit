package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"bobdb/internal/config"
)

var version = "dev"

const cliHelpText = `bobdb

Keyboard-first database TUI for SQLite, Postgres, and MongoDB.

Usage:
  bobdb             Launch the TUI
  bobdb --help      Show this help
  bobdb help        Show this help
  bobdb --version   Show version
  bobdb version     Show version

Aliases:
  bob
  bdb
`

func main() {
	if code, handled := handleCLIArgs(os.Args[1:], os.Stdout, os.Stderr); handled {
		if code != 0 {
			os.Exit(code)
		}
		return
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not load config: %v\n", err)
		cfg = &config.Config{}
	}

	m := newModel(cfg)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func handleCLIArgs(args []string, stdout, stderr io.Writer) (int, bool) {
	if len(args) == 0 {
		return 0, false
	}

	switch args[0] {
	case "--help", "-h", "help":
		fmt.Fprint(stdout, cliHelpText)
		return 0, true
	case "--version", "-v", "version":
		fmt.Fprintf(stdout, "bobdb %s\n", version)
		return 0, true
	default:
		fmt.Fprintf(stderr, "bobdb: unknown argument %q\n\n%s", args[0], strings.TrimRight(cliHelpText, "\n"))
		if !strings.HasSuffix(cliHelpText, "\n") {
			fmt.Fprintln(stderr)
		}
		return 2, true
	}
}
