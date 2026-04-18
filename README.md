# bobdb

Database TUI — a dense, keyboard-first database cockpit for SQLite, Postgres, and MongoDB.

## Quick Install

Supported platforms: Linux and macOS. On Windows, use WSL.

Recommended (installs to `~/.local/bin`):

```bash
curl -fsSL https://raw.githubusercontent.com/LFroesch/bobdb/main/install.sh | bash
```

Or download a binary from [GitHub Releases](https://github.com/LFroesch/bobdb/releases).

Or build from source:

```bash
make install
```

Commands:

```bash
bobdb
# aliases:
bob
bdb
```

## Tabs

| # | Tab | Description |
|---|-----|-------------|
| 1 | Connections | Add/delete/connect to saved databases |
| 2 | Browse | Toggle between schema (fields/types) and data preview; press `enter` to swap views, `e` to build a contextual edit query, `C` to copy/export preview rows as JSON/CSV |
| 3 | Query | Write and run raw SQL/Mongo commands with backend-aware typeahead, examples, templates, and schema-aware completion |
| 4 | Results | Inspect the latest result set with structured row viewing and copy |

## Keybindings

| Key | Action |
|-----|--------|
| `1-4` | Switch tabs |
| `/` | Jump straight to the Query editor |
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
| `pgup` / `pgdn` | Scroll the Query Reference pane on the Query tab |
| `home` / `end` | Jump to top/bottom of the Query Reference pane |
| `ctrl+g` | Generate query with Ollama (natural language) |
| `ctrl+t` | Open templates while typing in Query editor |
| `ctrl+e` | Open backend-aware examples while typing in Query editor |
| `ctrl+u` | Open saved queries |
| `f` / `x` / `y` | Open templates / examples / recent queries from the Query tab |
| `esc` | Blur query editor |
| `←` / `→` | Page visible query-result columns |
| `c` | Copy the current table/value/detail/query/row where available |
| `C` | Open `copy as` for query strings or Browse/Results rows (`JSON`, `CSV`, language string literals) |
| `v` | Open the structured detail viewer |
| `q` | Quit |
| `?` | Help |

On narrower terminals, `bobdb` collapses to a single active pane and `tab` swaps between the navigator and the detail pane.
Schema fields and query results render inside full-width tables in the detail pane so column alignment stays intact across resize changes.

Saved connections can be edited in place from the Connections tab with `e`.
Saved-connection deletion and obvious write queries now require confirmation before they run.

The Query tab uses a single assistance flow across backends:

- Completions open contextually while typing (command/table/field/value/limit/sort contexts), and still render as an inline popover under the editor so the query stays visible.
- Autocomplete stays optional: `tab` accepts the current suggestion, while `enter` keeps editing the query instead of silently taking a completion.
- Query/editor commands like `ctrl+l`, `ctrl+r`, `ctrl+t`, `ctrl+e`, and history actions still work while the completion popover is visible.
- `/` is a global "go write a query" key when you are not already typing into an input, so you do not have to navigate by tab number to get back to Query.
- Query text input is now strictly scoped to the Query tab, so Results navigation/back behavior is not affected by stale editor focus state.
- The Query Reference pane is scrollable with `pgup` / `pgdn` / `home` / `end`, even while the editor is focused, so longer examples stay usable without changing focus.
- Query Reference scrolling is wrap-aware, so long reference examples no longer make the bottom of the left pane look clipped or half-hidden on narrower layouts.
- In SQL, `tab` on an empty editor or on bare `SELECT` opens the same table-first picker flow, then scaffolds `SELECT * FROM ... WHERE ` with the cursor on `*` so you can replace it with columns immediately.
- SQL completions cover starter queries, aggregate functions (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`DISTINCT`), and `SELECT` / `FROM` / `JOIN` / `WHERE` / `GROUP BY` / `ORDER BY` / `LIMIT` / `INSERT` / `UPDATE` / `DELETE` contexts.
- SQL completion is token-first once a query already has content: `tab` prefers the next small unit (`WHERE`, a column, an operator, `ASC`/`DESC`, a limit number) instead of dropping in a larger clause body.
- SQL filter building now suggests operators after a selected column, including comparisons, `LIKE`, `IN`, `IS NULL`, and `IS NOT NULL`; bare comparison operators no longer inject placeholder `0` literals that can corrupt timestamp/date filters.
- SQL value completions fetch distinct sample values after comparison operators (for example `col = '`, `col LIKE '`, `col IN ('`), with substring-aware matching so inputs like `@gmail` rank relevant email values; editing stays in the query itself while the suggestions refresh around it.
- Closing a quoted SQL value literal does not immediately reopen generic completion, so finishing a sampled timestamp/string stays quiet until you keep typing the next clause.
- Displayed SQL timestamps now trim useless trailing fractional zero padding for readability, but raw copied/exported values stay unchanged.
- Mongo completions now guide command -> collection -> arguments, including filter/sort JSON field hints, top-level operators like `$or` / `$and`, nested comparison operators like `$gt`, and on-demand sampled value suggestions for field values.
- Mongo `find(...)` and `findOne(...)` now have a projection multi-select flow parallel to SQL column picking: pressing `tab` at the end of `db.collection.find(filter)` or `db.collection.findOne(filter)` opens a field picker, `space` toggles fields, and `tab` inserts the projection argument while keeping Mongo's default `_id` behavior.
- Mongo `findOne(...)` now gets the same schema-aware filter-field completion as `find(...)`, so the two read commands do not diverge on the first argument.
- Mongo `aggregate([...])` now opens stage-level completion at pipeline positions such as `aggregate([` or after a stage comma, offering `$match`, `$project`, `$group`, `$sort`, `$limit`, and related stages instead of only a couple of whole-pipeline snippets.
- When replacing a nested Mongo operator inside an existing field object (for example `$regex` -> `$in`), autocomplete now preserves the current value text and reshapes it when needed instead of rebuilding the whole object.
- If you change collections inside the same query (for example `db.users.find(...)` to `db.comments.find(...)`), filter/value completions follow the new collection context.
- Mongo autocomplete now uses typed JSON literals for common scalar types (`bool`, numeric, `null`) so values like `true` are inserted without forced string quotes.
- In Mongo bool-like value positions, autocomplete now proposes `true` / `false` / `null` immediately (before sample-value queries finish).
- Mongo typed completion also provides starter literals for complex types (`objectId`, `date/time`, arrays, objects/maps/documents) using Extended JSON where applicable.
- Accepting built-in command completions inserts minimal literal starters like `db.users.find({})` or `SELECT * FROM ... LIMIT 50;`, and templates load into the editor as plain text.
- SQL is currently optimized for single-table autocomplete flows first; joins, aliases, and more advanced nested-query cases are intentionally more conservative than Mongo's JSON-aware completion path.
- While the completion picker is open, `←` / `→` / `home` / `end` still edit cursor position in the query editor instead of switching to a separate filter input.
- SQL `INSERT ... VALUES (` positions stay free-form instead of opening generic scaffold/keyword completions.
- Plain Mongo value completion now targets just the current value literal, so accepting a suggestion updates `"@gm"` to `"alice@gmail.com"` without replacing the surrounding `{"email": ...}` object.
- `ctrl+e` opens backend-aware examples for the current engine so you can quickly recall real query shapes without leaving the editor.
- `Templates` stay focused on quick actions and now load minimal editable starters; `Examples` stay separate and act as backend-specific reference commands (`read`, `filter`, `sort`, `aggregate`, `write`) so they teach query shape instead of behaving like generated actions.

History/templates/examples/saved queries all stay overlay-based. Use `ctrl+o`, `ctrl+t`, `ctrl+e`, and `ctrl+u` from the editor, or the single-key shortcuts from the unfocused Query tab.

Copy/export is intentionally clipboard-first for v1:

- In Query, press `C` to copy the current query as raw text or as Go/JavaScript/Python/JSON string literals for dropping into another app.
- In Browse data preview or Results, press `C` to copy the current row as structured text/JSON or copy the whole visible result set as JSON/CSV.
- Plain `c` stays the fast path for the most obvious thing under the cursor; `C` is the “export / alternate format” path.

Outside the TUI, `bobdb --help`, `bobdb help`, `bobdb --version`, and `bobdb version` are supported. Any other CLI args fail fast with a usage message instead of silently being ignored.

The footer reflects the current mode — picker-open mode shows `↑/↓ · tab · enter · esc`, while edit mode stays trimmed to the main flow instead of listing every `ctrl+...` chord at once.

## Supported Databases

- **SQLite** — path to `.sqlite` / `.db` file
- **Postgres** — `postgres://user:pass@host:5432/dbname`
- **MongoDB** — `mongodb://user:pass@host:27017/dbname` — uses standard shell syntax: `db.collection.find({})`, `db.collection.aggregate([...])`, `db.collection.updateOne({},{$set:{}})`, etc. Use `tab` completions, `ctrl+t` templates, `ctrl+e` examples, or `ctrl+g` to generate from natural language via Ollama.

Config saved to `~/.config/bobdb/config.json`. `bobdb` now creates that directory with owner-only access and writes the config file with `0600` permissions because DSNs often contain credentials. Passwords embedded in DSNs are masked on the Connections detail pane so they don't appear in screenshots or over-the-shoulder views; editing or copying the DSN still yields the full original string.

## Ollama integration (`ctrl+g`)

`ctrl+g` sends a natural-language prompt plus the current schema (table / column names only — never row data) to an Ollama server and inserts the generated query. Host and model are configurable via `BOBDB_OLLAMA_HOST` / `BOBDB_OLLAMA_MODEL` (or `ollama_host` / `ollama_model` in the config file); the default is `http://localhost:11434` with `qwen2.5:7b`.

If you point `BOBDB_OLLAMA_HOST` at a remote (non-localhost) endpoint, be aware that your schema metadata leaves the machine with every generation request. Schema names can themselves be sensitive (e.g. column names like `ssn`, `internal_pricing`), so treat the remote host as a third-party recipient of that information. Keep the default (local Ollama) if that's a concern.

## License

[AGPL-3.0](LICENSE)
