# dbkit

Database TUI — scout-style panels + tabs for SQLite, Postgres, and MongoDB.

## Quick Install

Supported platforms: Linux and macOS. On Windows, use WSL.

Note: building from source requires gcc for sqlite3 (cgo). On Ubuntu: `sudo apt install gcc`.

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
