# dbkit

Database TUI — a dense, keyboard-first database cockpit for SQLite, Postgres, and MongoDB.

## Quick Install

Supported platforms: Linux and macOS. On Windows, use WSL.

Recommended (installs to `~/.local/bin`):

```bash
curl -fsSL https://raw.githubusercontent.com/LFroesch/dbkit/main/install.sh | bash
```

Or download a binary from [GitHub Releases](https://github.com/LFroesch/dbkit/releases).

Or build from source:

```bash
make install
```

Command:

```bash
dbkit
```

## Tabs

| # | Tab | Description |
|---|-----|-------------|
| 1 | Connections | Add/delete/connect to saved databases |
| 2 | Browse | Browse tables/collections and inspect schema fields |
| 3 | Query | Write and run raw SQL/Mongo commands with schema-aware templates and column completion |
| 4 | Results | Inspect the latest result set with structured row viewing and copy |

## Keybindings

| Key | Action |
|-----|--------|
| `1-4` | Switch tabs |
| `tab` | Toggle left/right pane focus |
| `↑` / `↓` | Navigate the focused list/table |
| `enter` | Connect / select / use |
| `n` | New connection |
| `d` | Delete connection |
| `r` | Refresh tables |
| `e` | Focus query editor |
| `ctrl+r` | Run query |
| `ctrl+o` | Open recent query history from the editor |
| `ctrl+y` | Recall the last-run query |
| `tab` | In Query editor, open schema column completion at the cursor |
| `ctrl+t` | Open templates while typing in Query editor |
| `f` / `y` | Open templates / recent queries from the Query tab |
| `esc` | Blur query editor |
| `←` / `→` | Page visible query-result columns |
| `c` | Copy the current value/detail/query/row |
| `v` | Open the structured detail viewer |
| `q` | Quit |
| `?` | Help |

On narrower terminals, `dbkit` collapses to a single active pane and `tab` swaps between the navigator and the detail pane.
Browse schema fields and query results render inside full-width tables in the detail pane so column alignment stays intact across resize changes.

The footer now carries the active navigation hints; browse/query/result panes stay focused on content.

## Supported Databases

- **SQLite** — path to `.sqlite` / `.db` file
- **Postgres** — `postgres://user:pass@host:5432/dbname`
- **MongoDB** — `mongodb://user:pass@host:27017/dbname` (query with Mongo commands like `find`, `aggregate`, `insert`, `update`, `delete`; run `help` in Query tab for syntax)

Config saved to `~/.config/dbkit/config.json`.

## License

[AGPL-3.0](LICENSE)
