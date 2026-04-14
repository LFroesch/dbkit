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
| 2 | Schema | Inspect tables/collections, fields, and types |
| 3 | Query | Write and run raw SQL/Mongo commands with backend-aware typeahead, snippets, and schema-aware completion |
| 4 | Results | Inspect the latest result set with structured row viewing and copy |

## Keybindings

| Key | Action |
|-----|--------|
| `1-4` | Switch tabs |
| `tab` | Toggle left/right pane focus |
| `↑` / `↓` | Navigate the focused list/table |
| `enter` | Connect / select / use |
| `n` | New connection |
| `d` | Delete connection, with confirmation |
| `r` | Refresh tables |
| `e` | Focus query editor |
| `ctrl+r` | Run query |
| `ctrl+l` | Clear the current query |
| `ctrl+o` | Open recent query history from the editor |
| `ctrl+y` | Recall the last-run query |
| `tab` | In Query editor, open completion or jump to the next snippet placeholder |
| `shift+tab` | Jump to the previous snippet placeholder in an active query snippet |
| `ctrl+t` | Open templates while typing in Query editor |
| `f` / `y` | Open templates / recent queries from the Query tab |
| `esc` | Blur query editor |
| `←` / `→` | Page visible query-result columns |
| `c` | Copy the current table/value/detail/query/row where available |
| `v` | Open the structured detail viewer |
| `q` | Quit |
| `?` | Help |

On narrower terminals, `dbkit` collapses to a single active pane and `tab` swaps between the navigator and the detail pane.
Schema fields and query results render inside full-width tables in the detail pane so column alignment stays intact across resize changes.

Saved-connection deletion and obvious write queries now require confirmation before they run.

The Query tab now uses a single assistance flow across backends:

- Completions are manual-only: press `tab` when you want suggestions instead of getting interrupted while typing.
- SQL completions cover starter snippets plus `SELECT` / `FROM` / `JOIN` / `WHERE` / `GROUP BY` / `ORDER BY` / `LIMIT` / `INSERT` / `UPDATE` / `DELETE` contexts.
- Mongo completions suggest command verbs first, then collection names, then command-specific JSON scaffolds.
- Snippet completions insert named placeholders like `${table}` and `${value}` so `tab` / `shift+tab` can jump through the generated query quickly.

The footer now stays focused on the highest-value actions for the current view, and the Query pane keeps a compact quick-start hint instead of a bulky assist block.

## Supported Databases

- **SQLite** — path to `.sqlite` / `.db` file
- **Postgres** — `postgres://user:pass@host:5432/dbname`
- **MongoDB** — `mongodb://user:pass@host:27017/dbname` (query with Mongo commands like `find`, `aggregate`, `insert`, `update`, `delete`; run `help` in Query tab for syntax)

Config saved to `~/.config/dbkit/config.json`.

## License

[AGPL-3.0](LICENSE)
