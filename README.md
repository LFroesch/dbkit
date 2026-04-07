# dbkit

Database TUI — scout-style panels + tabs for SQLite, Postgres, and MongoDB.

## Install

```bash
make build        # → bin/dbkit
make install      # → $GOPATH/bin/dbkit
```

Requires gcc for sqlite3 (cgo). On Ubuntu: `sudo apt install gcc`.

## Usage

```bash
dbkit
```

## Tabs

| # | Tab | Description |
|---|-----|-------------|
| 1 | Connections | Add/delete/connect to saved databases |
| 2 | Schema | Browse tables, column types, row counts |
| 3 | Query | Write and run SQL, scroll results |
| 4 | Helpers | Query templates → paste into editor |

## Keybindings

| Key | Action |
|-----|--------|
| `1-4` | Switch tabs |
| `tab` | Toggle left/right panel focus |
| `j/k` | Navigate lists |
| `enter` | Connect / select / use |
| `n` | New connection |
| `d` | Delete connection |
| `r` | Refresh tables |
| `e` / `tab` | Focus query editor |
| `ctrl+r` | Run query |
| `esc` | Blur query editor |
| `ctrl+d/u` | Scroll results |
| `q` | Quit |
| `?` | Help |

## Supported Databases

- **SQLite** — path to `.sqlite` / `.db` file
- **Postgres** — `postgres://user:pass@host:5432/dbname`
- **MongoDB** — stub, coming soon

Config saved to `~/.dbkit/config.json`.
