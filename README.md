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
| 2 | Browse | Toggle between schema (fields/types) and data preview; press `enter` to swap views, `e` to build a contextual edit query |
| 3 | Query | Write and run raw SQL/Mongo commands with backend-aware typeahead, examples, templates, and schema-aware completion |
| 4 | Results | Inspect the latest result set with structured row viewing and copy |

## Keybindings

| Key | Action |
|-----|--------|
| `1-4` | Switch tabs |
| `tab` | Toggle left/right pane focus |
| `↑` / `↓` | Navigate the focused list/table |
| `enter` | Connect / select / use |
| `n` | New connection |
| `e` | Edit the selected saved connection |
| `d` | Delete connection, with confirmation |
| `r` | Refresh tables |
| `e` | Contextual edit — builds UPDATE targeting focused cell (Browse/Results) or opens editor |
| `ctrl+r` | Run query |
| `ctrl+l` | Clear the current query |
| `ctrl+o` | Open recent query history from the editor |
| `ctrl+y` | Recall the last-run query |
| `tab` | In Query editor, open or accept autocomplete |
| `ctrl+g` | Generate query with Ollama (natural language) |
| `ctrl+t` | Open templates while typing in Query editor |
| `ctrl+e` | Open backend-aware examples while typing in Query editor |
| `ctrl+u` | Open saved queries |
| `f` / `x` / `y` | Open templates / examples / recent queries from the Query tab |
| `esc` | Blur query editor |
| `←` / `→` | Page visible query-result columns |
| `c` | Copy the current table/value/detail/query/row where available |
| `v` | Open the structured detail viewer |
| `q` | Quit |
| `?` | Help |

On narrower terminals, `dbkit` collapses to a single active pane and `tab` swaps between the navigator and the detail pane.
Schema fields and query results render inside full-width tables in the detail pane so column alignment stays intact across resize changes.

Saved connections can be edited in place from the Connections tab with `e`.
Saved-connection deletion and obvious write queries now require confirmation before they run.

The Query tab uses a single assistance flow across backends:

- Completions open contextually while typing (command/table/field/value/limit/sort contexts), and still render as an inline popover under the editor so the query stays visible.
- Autocomplete stays optional: `tab` accepts the current suggestion, while `enter` keeps editing the query instead of silently taking a completion.
- Query/editor commands like `ctrl+l`, `ctrl+r`, `ctrl+t`, `ctrl+e`, and history actions still work while the completion popover is visible.
- Query text input is now strictly scoped to the Query tab, so Results navigation/back behavior is not affected by stale editor focus state.
- SQL completions cover starter queries, aggregate functions (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`DISTINCT`), and `SELECT` / `FROM` / `JOIN` / `WHERE` / `GROUP BY` / `ORDER BY` / `LIMIT` / `INSERT` / `UPDATE` / `DELETE` contexts.
- SQL filter building now suggests operators after a selected column, including comparisons, `LIKE`, `IN`, `IS NULL`, and `IS NOT NULL`.
- SQL value completions fetch distinct sample values after comparison operators (for example `col = '`, `col LIKE '`, `col IN ('`), with substring-aware matching so inputs like `@gmail` rank relevant email values.
- Mongo completions now guide command -> collection -> arguments, including filter/sort JSON field hints, top-level operators like `$or` / `$and`, nested comparison operators like `$gt`, and on-demand sampled value suggestions for field values.
- When replacing a nested Mongo operator inside an existing field object (for example `$regex` -> `$in`), autocomplete now preserves the current value text and reshapes it when needed instead of rebuilding the whole object.
- If you change collections inside the same query (for example `db.users.find(...)` to `db.comments.find(...)`), filter/value completions follow the new collection context.
- Mongo autocomplete now uses typed JSON literals for common scalar types (`bool`, numeric, `null`) so values like `true` are inserted without forced string quotes.
- In Mongo bool-like value positions, autocomplete now proposes `true` / `false` / `null` immediately (before sample-value queries finish).
- Mongo typed completion also provides starter literals for complex types (`objectId`, `date/time`, arrays, objects/maps/documents) using Extended JSON where applicable.
- Accepting built-in command completions inserts minimal literal starters like `db.users.find({})` or `SELECT * FROM ... LIMIT 50;`, and templates load into the editor as plain text.
- While the completion picker is open, `←` / `→` / `home` / `end` still edit cursor position; in value-filter mode they edit the filter input cursor, and otherwise they move the query-editor cursor.
- SQL `INSERT ... VALUES (` positions stay free-form instead of opening generic scaffold/keyword completions.
- Plain Mongo value completion now targets just the current value literal, so accepting a suggestion updates `"@gm"` to `"alice@gmail.com"` without replacing the surrounding `{"email": ...}` object.
- `ctrl+e` opens backend-aware examples for the current engine so you can quickly recall real query shapes without leaving the editor.
- `Templates` stay focused on quick actions and now load minimal editable starters; `Examples` stay separate and act as backend-specific reference commands (`read`, `filter`, `sort`, `aggregate`, `write`) so they teach query shape instead of behaving like generated actions.

The footer reflects the current mode — picker-open mode shows `↑/↓ · tab · enter · esc`, edit mode shows the full set of query keybinds including `ctrl+p/n` for inline history cycling.

## Supported Databases

- **SQLite** — path to `.sqlite` / `.db` file
- **Postgres** — `postgres://user:pass@host:5432/dbname`
- **MongoDB** — `mongodb://user:pass@host:27017/dbname` — uses standard shell syntax: `db.collection.find({})`, `db.collection.aggregate([...])`, `db.collection.updateOne({},{$set:{}})`, etc. Use `tab` completions, `ctrl+t` templates, `ctrl+e` examples, or `ctrl+g` to generate from natural language via Ollama.

Config saved to `~/.config/dbkit/config.json`. `dbkit` now creates that directory with owner-only access and writes the config file with `0600` permissions because DSNs often contain credentials.

## License

[AGPL-3.0](LICENSE)
