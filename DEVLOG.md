# Devlog

## 2026-05-04 — focused-cell edit actually focuses + Mongo shell demo parity

- **Contextual edit now uses a real horizontal selection**: Browse data and Results each track a focused column separately from the viewport offset. `←` / `→` move that focused column, the table viewport follows it when needed, the pane metadata now shows the focused field name, and `e` / `E` build edits against that actual focused cell instead of the leftmost visible column.
- **Mongo demo-path shell syntax widened**: the shell parser/relaxed-JSON layer now handles chained `find(...).sort(...).limit(...).skip(...)`, regex literals like `/foo/i`, and `NumberLong(...)`. `runFind` also understands tagged `sort:` / `limit:` / `skip:` / `projection:` options emitted by the shell parser so those chains execute instead of being silently rejected.
- **Regression coverage**: added UI tests proving contextual edit follows the focused Browse/Results column, plus parser tests for chained `find` cursor methods, regex literals, and `NumberLong(...)`.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `internal/db/mongo.go`, `internal/db/mongo_relax_test.go`, `WORK.md`, `README.md`, `DEVLOG.md`.

## 2026-05-04 — Mongo `insertMany` execution fix + v1 blocker audit

- **Silent data-loss path fixed**: shell-form `db.collection.insertMany([...])` already required confirmation, but execution still funneled through `runInsert`, which only decoded a single `bson.M`. In practice that meant `insertMany` silently degraded to one insert instead of many. `runInsert` now accepts both object and array payloads: object payloads still use `InsertOne`, while array payloads fan into `InsertMany` and report the inserted document count.
- **Regression coverage**: added a parser-level check that `parseShellQuery` preserves the array payload for `insertMany`, which is the contract `runInsert` now relies on.
- **WORK cleanup**: rewrote `WORK.md` away from "v1 done" to a narrower closeout sprint focused on the remaining user-visible issues uncovered in the audit: contextual edit/focused-cell mismatch and the remaining Mongo shell-dialect gaps.
- Touched: `internal/db/mongo.go`, `internal/db/mongo_relax_test.go`, `WORK.md`, `README.md`, `DEVLOG.md`.

## 2026-04-24 — Postgres value-bounds parity + SQL multi-table context

- **Value-replacement parity gap**: `sqlValueCompletion` previously set `End: req.Cursor`, so completing inside an existing literal (`name = 'old|stuff'`) only replaced the prefix and left the trailing `stuff'` dangling — Mongo's `MongoJSONValueBounds` extends past the literal body. Fixed: `End` now walks forward through the literal contents (handling `''` SQL escape and backslash escapes) and stops at the closing quote, so the inserted value cleanly replaces the full body while the existing closing quote stays in place.
- **JOIN ON now activates predicate context**: extended `predicateStart` to recognize ` on ` alongside ` where ` / ` and ` / ` or ` / ` having `. Before this, the column picker silently fell through to the keyword fallback inside JOIN ON clauses.
- **Multi-table column-suggestion guard**: when the query has `JOIN`, comma-separated FROM tables, CTEs (`WITH … AS (`), or derived tables (`FROM (SELECT …) sub`), un-qualified column completion is ambiguous. `sqlColumnCompletion` now returns `nil` in that case unless the user typed an alias prefix (`u.`). When the alias resolves (via the new `ParseFromBindings` / `ResolveAliasTable` helpers in `sql_context.go`) to the inferred table's schema we surface its columns; when it resolves to a different table we set `NeedSchema=<aliased_table>` and show a `loading fields…` hint so the caller can fetch the right schema. Detection runs against the full query (not just before-cursor) because in SELECT-list positions the JOIN body sits after the cursor.
- **Tests**: added `internal/completion/sql_multitable_test.go` covering (a) un-aliased column suppression in multi-table queries, (b) alias-matches-inferred-table column surfacing, (c) alias-mismatch `NeedSchema` request, (d) `JOIN ON u.|` triggers `Filter Column` instead of the clause-keyword fallback, (e) value-bounds extension to the closing quote, (f) `ParseFromBindings` for bare/`AS`-aliased/joined/comma-separated forms.
- Touched: `internal/completion/sql.go`, `internal/completion/sql_context.go`, `internal/completion/sql_multitable_test.go`, `WORK.md`, `DEVLOG.md`, `CLAUDE.md`.

## 2026-04-24 — Mongo typed-literal insert paths

- **Problem 1 (silent picker)**: `db.users.insertOne(`, `updateOne(`, `deleteOne(`, `countDocuments(` etc. produced no completion items. `mongoArgumentItems` only switched on the internal command keys (`insert`, `update`, `delete`, `count`) — shell method names like `insertone` lowercased never matched, so the cursor at the empty arg position fell to the default branch. Added `normalizeMongoCommand` to alias the full shell-method family (`insertOne`/`insertMany` → `insert`, `updateOne`/`updateMany`/`replaceOne`/`findOneAndUpdate`/`findOneAndReplace` → `update`, `deleteOne`/`deleteMany`/`remove`/`findOneAndDelete` → `delete`, `countDocuments`/`estimatedDocumentCount` → `count`).
- **Problem 2 (silent on `{` alone)**: `db.x.insertOne({` with cursor right after the brace returned no schema-driven field items. The JSON-object items path was gated behind `len(trimmedToken) > 1`, and the prefix-reset block at the end of `mongoComplete` had the same guard — leaving `prefix = "{"`, which then filtered out every non-`{`-containing item via `RankItems`. Lifted both gates so a bare `{` triggers the field/operator branch and resets the prefix to the empty key.
- **Problem 3 (weak default literals)**: `MongoPlaceholderForType` returned `"null"` for `bool`/`number` columns, so the field-key scaffold inserted `"active":null` / `"age":null`. Switched to typed defaults (`bool`/`boolean` → `false`, `int`/`uint`/`float`/`decimal`/`number` → `0`); ObjectId / date / array / object scaffolds are unchanged. The pre-existing `MongoTypedJSONLiteral` already emitted unquoted JSON for typed literals, and `MongoLiteralCandidates` already offered unquoted `true`/`false`/`0`/`1`/etc. — confirmed both with new tests.
- **Tests**: added `internal/completion/mongo_insert_types_test.go` covering (a) `MongoTypedJSONLiteral` round-trip JSON validity for bool/number/null/ObjectId/date/string fallback, (b) `MongoLiteralCandidates` typed-JSON shape, (c) `MongoPlaceholderForType` valid-JSON guarantee for every supported type kind, (d) end-to-end `Complete(db.users.insertOne({)` field-key insertions with mixed types, (e) shell-method empty-arg scaffold reachability for `insertOne`/`updateOne`/`deleteOne`/`countDocuments`. Updated the existing `TestMongoFieldFilterPlaceholderUsesTypedLiteralForBool` to assert the new `false` default instead of `null`.
- Touched: `internal/completion/mongo.go`, `internal/completion/mongo_insert_types_test.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`.

## 2026-04-24 — SQL row cap (v1 blocker cleared)

- **Root cause**: `internal/db/sqlite.go` and `internal/db/postgres.go` scanned the full result set into `result.Rows` with no ceiling. A bare `SELECT *` on a large table loaded every row into memory and could OOM `bobdb`. Mongo had been capping at `mongoMaxLimit=1000` for ages; SQL backends had drifted.
- **Fix**: added `SQLMaxRows=1000` constant in `internal/db/db.go` (mirrors `mongoMaxLimit`). SQLite and Postgres `RunQuery` break out of the scan loop once `len(result.Rows) >= SQLMaxRows` and stamp `result.Message` with a "capped — result set larger; add LIMIT/WHERE to narrow" warning.
- **Surface**: `renderResultMeta` in `view.go` now renders `result.Message` as a warn-styled segment alongside the row/column counts whenever rows are present — so the truncation indicator persists past the 6s status-bar expiry. Empty-result path (existing behavior) still renders `Message` as the primary meta line.
- **Why cap-in-scanner over auto-LIMIT injection**: query mutation is fragile (has to respect existing LIMITs, CTE bodies, subqueries, dialect quirks); scanner-cap works universally and keeps the query text user-authored.
- Touched: `internal/db/db.go`, `internal/db/sqlite.go`, `internal/db/postgres.go`, `view.go`, `CLAUDE.md`, `WORK.md`, `DEVLOG.md`.

## 2026-04-23 — Mongo shell-relaxed JSON + projection forwarding

- **Root cause**: `db.users.findOne({ username : "sofroesch" }, ...)` failed with "invalid JSON input" because the backend fed the raw argument to `bson.UnmarshalExtJSON`, which rejects unquoted keys. Shell syntax (bare keys, single quotes, trailing commas, `ObjectId(...)`, `ISODate(...)`) is what users actually paste from `mongosh` or type by hand, so rejecting it was a UX dead end.
- **Relaxer**: added `relaxJSON` in `internal/db/mongo.go` — a single-pass normalizer that quotes bare identifier keys, converts single-quoted strings, drops trailing commas before `}` / `]`, and rewrites `ObjectId("...")` / `ISODate("...")` literals into their `{"$oid":"..."}` / `{"$date":"..."}` extJSON equivalents. All Mongo JSON argument sites now route through `unmarshalMongoJSON`, which also flips `canonical` to `false` so relaxed `$date` forms parse.
- **Projection forwarding**: `find(filter, projection)` and `findOne(filter, projection)` now forward the second argument. `parseShellQuery` emits a `projection:<json>` trailing token and `runFind` wires it into `findOpts.SetProjection`. Previously the projection arg was silently dropped.
- **Quote handling**: `extractJSONArg`, `extractShellArgs`, and `splitShellArgs` now recognize single quotes as string delimiters, so brace/paren counting doesn't get confused by embedded `'` in relaxed input.
- **Tests**: added `internal/db/mongo_relax_test.go` covering `relaxJSON` (bare keys, single quotes, trailing commas, ObjectId, ISODate, `$`-prefixed operators) and `parseShellQuery` projection forwarding for both `find` and `findOne`.
- Touched: `internal/db/mongo.go`, `internal/db/mongo_relax_test.go`, `CLAUDE.md`, `WORK.md`, `DEVLOG.md`.

## 2026-04-18 — v1 CLI surface and Browse export parity

- **CLI wrap-up**: finished the non-TUI command-line surface so `bobdb` now responds cleanly to `--help` / `-h` / `help` and `--version` / `-v` / `version`. Unknown args now fail fast with a usage message instead of being ignored.
- **Browse copy/export parity**: Browse data preview now gets the same `C` “copy as” flow that Results already had, so copying rows or exporting preview data as JSON/CSV no longer requires running the query first just to reach the Results tab.
- **Tests/docs**: added regression coverage for the CLI argument handler and the Browse `C` overlay, and updated README/WORK to reflect the v1 copy/export and CLI scope.
- Touched: `main.go`, `update.go`, `view.go`, `model_flow_test.go`, `README.md`, `WORK.md`, `DEVLOG.md`.

## 2026-04-18 — rename to `bobdb` with `bob` / `bdb` aliases

- **Product rename**: renamed the app’s canonical product/binary/UI identity from `dbkit` to `bobdb`. Version output, window title, header text, keybind modal copy, module path, build output, and release artifact naming now use `bobdb`.
- **CLI aliases**: source installs and release installs now create `bob` and `bdb` aliases alongside the canonical `bobdb` binary so users can launch whichever short form they prefer without fragmenting the product name in docs.
- **Config/env move**: config now lives at `~/.config/bobdb/config.json` and Ollama env vars now use `BOBDB_OLLAMA_HOST` / `BOBDB_OLLAMA_MODEL`.
- **Docs**: README now documents `bobdb` as the canonical command and calls out `bob` / `bdb` as convenience aliases. Added a short pre-v1 checklist to `WORK.md` for the remaining launch polish.
- Touched: `go.mod`, `main.go`, `model.go`, `view.go`, `update.go`, `internal/config/config.go`, `internal/ollama/ollama.go`, `Makefile`, `install.sh`, `.github/workflows/release.yml`, `README.md`, `WORK.md`, `DEVLOG.md`.

## 2026-04-18 — Mongo autocomplete cleanup: `findOne` parity and aggregate stages

- **`findOne` parity fixes**: `findOne(...)` now gets the same schema-aware first-argument filter completion as `find(...)`, and collection-switch completion preserves the proper shell method casing (`findOne`, not invalid `findone`) when rebuilding `db.collection.method(...)`.
- **Aggregate stage completion**: `aggregate([...])` no longer falls back only to two whole-pipeline starter snippets. At stage positions like `db.users.aggregate([` or after a top-level pipeline comma, completion now offers stage-level inserts (`$match`, `$project`, `$group`, `$sort`, `$limit`, etc.) with schema-aware defaults for the common stages.
- **Tests**: added regression coverage for `findOne` filter completion parity, `findOne` collection-switch rebuilding, and aggregate stage completion/insertion at pipeline-start positions.
- Touched: `internal/completion/mongo.go`, `model_flow_test.go`, `WORK.md`, `README.md`, `DEVLOG.md`.

## 2026-04-18 — Mongo projection picker for `find` / `findOne`

- **Mongo projection parity**: added a Mongo-native field-selection flow that mirrors the SQL `SELECT *` column picker without inventing SQL-shaped syntax. At the end of a one-argument shell query like `db.users.find({"status":"active"})` or `db.users.findOne({"status":"active"})`, pressing `tab` now opens a multi-select `Project Fields` picker. `space` toggles fields and `tab` inserts the projection argument as an inclusion object such as `{"email":1, "created_at":1}`.
- **Existing projection re-entry**: if the cursor is already inside `db.users.find({}, {"email":1})`, the same picker reopens against the existing projection object and preserves already-selected fields so edits are reversible instead of destructive. The same completion path now also applies to `findOne(...)`.
- **Teaching surfaces updated**: Mongo helpers, examples, schema-aware helpers, monitor helpers, and the Query Reference pane now show `find(..., projection)` examples so “select columns” in Mongo is discoverable in the same places SQL exposes column selection.
- **Tests**: added engine and model-flow coverage for single-arg projection opening, projection insertion, and reopening on existing projection objects.
- Touched: `internal/completion/mongo.go`, `internal/completion/engine.go`, `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `internal/completion/engine_test.go`, `WORK.md`, `README.md`, `DEVLOG.md`.

## 2026-04-18 — display polish: wrap-aware Query Reference and timestamp cleanup

- **Wrap-aware Query Reference scrolling**: the Query Reference was still tracking scroll position in terms of source lines, but the left pane can visually wrap long examples into multiple on-screen rows. That mismatch is what made the bottom look clipped or oddly cut off. `view.go` now expands the reference into wrapped display rows for the current pane width and `update.go` scrolls against that rendered row count instead of the raw line count.
- **Display-only timestamp cleanup**: SQL results were showing the raw driver string form of timestamps, including long all-zero fractional tails like `.000000`. `view.go` now trims only useless trailing fractional-second zero padding for timestamp-shaped display strings. This is render-time only: stored query results, copies, JSON export, and CSV export still use the raw original values.
- **Tests**: added regression coverage in `model_flow_test.go` for timestamp display trimming and Query Reference end-scroll when the pane width forces wrapped rows.
- Touched: `view.go`, `update.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

## 2026-04-17 — query entry polish: global `/` shortcut and leaner footer

- **Natural Query entry key**: `3` still works, but there was no obvious non-numeric way to jump straight into the Query editor from elsewhere in the app. `update.go` now maps `/` to "open Query and focus the editor" whenever the app is not already routing typing into a text input, so you can jump from Browse/Results into query writing with a single natural key.
- **Leaner Query footer**: the focused Query footer had turned into a dense list of `ctrl+...` hints. `view.go` now keeps the high-frequency actions visible (`/`, run, complete, reference paging, clear, blur) and compresses the auxiliary actions into the existing single-key Query affordances (`f/x/y/u` pickers, `g/s` AI/save) instead of rendering a long row of control-chord hints.
- **Tests**: added regression coverage in `model_flow_test.go` to ensure `/` opens the Query tab and focuses the editor.
- Touched: `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `README.md`, `DEVLOG.md`.

## 2026-04-17 — query UX follow-up: bare SELECT picker, reference end-scroll, quote-close suppression

- **Bare `SELECT` table-first flow**: pressing `tab` on an empty SQL editor already opened the table-first picker, but `SELECT<tab>` and `SELECT <tab>` were falling back to generic completion instead of treating that as the same starting intent. `update.go` now routes both empty SQL and bare-`SELECT` states through the same table picker flow so the next action is consistently "pick a table, scaffold the query, then replace `*`".
- **Query Reference bottom reachability**: the Query Reference scroll bounds were computed from a viewport height that did not match the actual rendered left-pane body, especially in split-pane mode. That made the bottom section (`Operator tips`) partially unreachable and looked like the left panel was cut off. `view.go` now derives the reference viewport from the real panel/body height path used by rendering, so `end` and repeated paging can reach the final lines.
- **Closing quoted SQL values no longer reopens generic completion**: after accepting a sampled SQL value and typing the closing quote, completion auto-trigger logic treated that quote like any other character and reopened a generic picker immediately. `update.go` now suppresses auto-trigger when a typed quote is specifically closing an existing SQL string literal, so finishing `'2026-04-09T15:51:30Z'` does not immediately flash unrelated suggestions.
- **Tests**: added regression coverage in `model_flow_test.go` for bare-`SELECT` table picking, Query Reference end-scroll showing the final operator-tip line, and closing a quoted SQL value without reopening completion.
- Touched: `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `README.md`, `DEVLOG.md`.

## 2026-04-17 — query UX stabilization: operator fix, value suggestions, reference paging

- **SQL operator completion fix**: comparison operators (`>`, `>=`, `<`, `<=`) were inserting placeholder numeric literals like `>= 0`. If the user then accepted a sampled timestamp/string value, the query turned into malformed SQL such as `>= 0 '2026-04-09...'`. Operator items in `internal/completion/sql_helpers.go` now insert only the operator plus spacing, while quoted scaffolds remain for operators that actually need them (`=`, `!=`, `LIKE`, `IN`, etc.).
- **Value-suggestion flow fix**: the sample-value picker no longer takes over editing with a separate local filter buffer. Query edits stay in the textarea, backspace/delete continue changing the query itself, and sampled values refresh around the current literal so autocomplete feels like a helper instead of a mode switch.
- **Scrollable Query Reference**: the left Query Reference pane now has its own viewport state and responds to `pgup` / `pgdn` / `home` / `end` even while the editor is focused. Reworked the reference content to emphasize valid query shapes, especially quoted timestamp examples for SQL, and added overflow hints so it is clear when more content exists.
- **Query-page polish**: history/templates/examples/saved queries stay overlay-based, `ctrl+p` / `ctrl+n` now open the recent-query overlay instead of cycling inline, compact single-pane labels use contextual names like `reference` / `editor`, Query footer/help copy now advertises the actual ctrl-shortcuts and reference paging, and the new-connection modal no longer incorrectly claims that `tab` inserts text.
- **Tests**: added regression coverage for the operator insertion fix, recent-query overlay opening, Query Reference paging while editor-focused, compact Query labels, and backspace editing while value suggestions are open.
- Touched: `model.go`, `update.go`, `view.go`, `internal/completion/item.go`, `internal/completion/sql_helpers.go`, `model_flow_test.go`, `internal/completion/tokens_test.go`, `README.md`, `WORK.md`, `DEVLOG.md`.

## 2026-04-17 — v1 wrap-up: confirmation bug, multi-line clauses, DSN masking

- **Mongo write confirmation fix**: `queryNeedsConfirmation` was splitting the query on whitespace, but shell syntax (`db.users.insertOne({...})`) has no space until inside the args — so the first "word" was the whole expression and never matched `insert` / `update` / `delete`. Destructive Mongo commands were silently bypassing the confirm dialog. Replaced with a shell-aware `mongoQueryIsWrite` that extracts the method name between the second `.` and the first `(` and checks against the full write-method set (`insertOne`, `insertMany`, `updateOne`, `updateMany`, `replaceOne`, `deleteOne`, `deleteMany`, `findOneAndUpdate`, `findOneAndDelete`, `findOneAndReplace`, `bulkWrite`, `drop`, `renameCollection`). The legacy internal format (`insert users {}`) still confirms via the whitespace path.
- **Multi-line SQL clause detection**: context predicates in `internal/completion/sql_context.go` looked for `" where "`, `" and "`, `" or "`, `" having "`, `" order by "`, `" limit "` — keywords with a leading *space*. When the clause keyword sat at the start of a new line (`\nwhere`, `\nand`), no match. Added `normalizeWhitespace` that collapses any run of whitespace (`\n`, `\t`, multiple spaces) to a single space before the keyword searches run. `ResolveSQLContext`, `InWhereClause`, `InHavingClause`, `sqlColumnCompletion.beforeToken`, and `sqlKeywordItems.before` all consume the normalized form now.
- **Tests**: `TestMultiLineClauseDetection` in `internal/completion/tokens_test.go` covers newline-prefixed WHERE / AND / ORDER BY / LIMIT / HAVING and the ORDER BY direction edge case. `TestMongoShellWriteRequiresConfirmation` in `model_flow_test.go` covers every shell-syntax write method.
- **DSN masking**: `renderConnectionDetail` now pipes `conn.DSN` through `maskDSNPassword`, which replaces the password segment (`user:***@host`) on display only. Edit form and `c` (copy DSN) still work against the real string so the user retains full control.
- **Cleanup**: removed the local `max` in `sql_context.go` (Go 1.25 module; builtin `max` is available).
- **README**: added a dedicated Ollama section. Schema names (not row data) are transmitted per request, and pointing `BOBDB_OLLAMA_HOST` at a remote endpoint means that metadata leaves the machine — call that out explicitly so users stay on localhost unless they opt in.
- Touched: `update.go`, `view.go`, `internal/completion/sql.go`, `internal/completion/sql_context.go`, `internal/completion/tokens_test.go`, `model_flow_test.go`, `README.md`, `WORK.md`, `DEVLOG.md`.

## 2026-04-16 — SQL token-first completion cleanup

- Refactored SQL completion context handling around an explicit `ResolveSQLContext` pass in `internal/completion/sql_context.go` instead of spreading overlapping clause heuristics across `sql.go`.
- Tightened single-table SQL flows so mid-query `tab` favors the next local unit only: filter columns, operators, values, `ORDER BY` direction, `LIMIT` values, and bare clause keywords like `WHERE` / `ORDER BY` / `LIMIT`.
- Removed aggressive mid-query clause scaffolds from SQL keyword completion. Empty-editor starters still insert full starter queries, but once the query has content the picker no longer injects larger bodies like `WHERE col = ''` or `ORDER BY col DESC`.
- Fixed a context bug exposed by the refactor where `FROM users ` could still reopen table completion and where a blank `WHERE ` position could be misread as an operator context.
- Updated regression coverage for SQL context resolution and the new token-first clause behavior. Also updated the query-template flow test to expect `WHERE ` to open filter-column completion instead of operator completion.
- Touched: `internal/completion/sql.go`, `internal/completion/sql_context.go`, `internal/completion/engine_test.go`, `internal/completion/tokens_test.go`, `model_flow_test.go`, `WORK.md`, `README.md`, `DEVLOG.md`.

## 2026-04-16 — table-first SQL flow + completion fixes

- **Table-first SQL completion**: empty query + tab now opens a table picker (SQLite/Postgres only). Selecting a table scaffolds `SELECT * FROM <table>\nWHERE ` with cursor positioned on `*`. Tabbing on `*` opens multi-select column picker — selected columns replace `*`. Gives a natural table→columns flow without inventing non-standard SQL.
- **Mongo field ranking**: operators (`$and`, `$or`, etc.) now appear at the top of the field completion list, not the bottom.
- **Mongo prefix bug**: `MongoJSONKeyBounds` was computing the correct replacement range but not updating the prefix used for `RankItems`, causing most fields to be filtered out when cursor was inside `{}`. Fixed by syncing prefix from the key bounds fallback.
- **Multi-line SQL completion**: removed `\n` as a blocker from all SQL context detectors (`InWhereClause`, `InFromTable`, `InUpdateSetList`, etc.). Previously, any query spanning multiple lines would break clause detection — e.g. `SELECT *\nFROM users\nWHERE ` wouldn't trigger column completion on the WHERE line.
- **Mongo schema sampling**: switched `GetTableSchema` from `Find().SetLimit(100)` (first 100 docs in insertion order) to `$sample` aggregation (random 100 docs across collection). Discovers fields that only exist in newer or less common documents.

## 2026-04-16 — browse panel decoupling + query cheat sheet

- Removed browse-panel fallback from `queryInferredTable()` and `effectiveTable()` — completion schema/columns now come exclusively from what's parsed in the query text, never from the left-panel cursor. Fixes the long-standing issue where selecting a table in Browse would silently override typed query context.
- Removed `BrowseTable` field from `completion.Request` (dead code after fallback removal).
- `resolveSchemaForCompletion()` returns nil when no table is inferred instead of falling back to `m.tableSchema`.
- Replaced the table list on the Query tab's left panel with a context-aware cheat sheet showing keybindings, SQL/Mongo syntax examples, and operators (switches content based on DB type).
- Query tab now defaults focus to the editor (right panel) when switching via `3` key; left panel nav keys (`j/k/enter`) jump straight to the editor since the panel is static reference.

## 2026-04-16 — completion engine rewrite

- Rewrote autocomplete as a stateless `completion.Engine` in `internal/completion/`
- New files: `engine.go` (Request/Result/SchemaInfo types, Complete() entry), `sql.go` (all SQL completion), extended `mongo.go` (mongo completion dispatcher + argument/value logic)
- update.go: 4576→3698 lines (-878); removed `columnPickerItem` type, `queryColumnContext`, `rankCompletionItems`, all SQL/Mongo completion methods, `mongoSchemaFields`, `mongoFieldType`, etc.
- Model is now a thin adapter: `buildCompletionRequest()` → `completion.Complete()` → apply result to picker state
- **Fixed stale collection bug** (two layers): (1) `resolveSchemaForCompletion()` only passes schema matching the inferred table. (2) Engine validates `req.Schema.Name` matches the token-parsed collection — if mismatched, ignores the schema and signals `NeedSchema`. Both SQL and Mongo paths have this guard. Switching `db.users.find({})` → `db.comments.find({})` now correctly loads comments schema instead of showing users fields.
- Eliminated `columnPickerItem` — picker uses `completion.Item` directly (exported fields: Label, Detail, InsertText, Selected)
- Eliminated duplicate `rankCompletionItems` in main — uses `completion.RankItems` everywhere
- All existing tests pass

## 2026-04-15 — completion extraction phase 1

- Created `internal/completion/` package (tokens.go + sql_context.go, 333 lines)
- Moved all SQL clause predicates, token helpers, FuzzyMatch, LastKeyword out of update.go
- update.go: 5644→5331 lines (-313); all callers now use `completion.X()` prefix
- Tests: TokenBounds, TokenValue, InWhereClause, InSelectList, FuzzyMatch
- `rankCompletionItems` stays in main for now (depends on `columnPickerItem` type)

## 2026-04-15 — inferred-schema header + results focus fix

- Query/Results pane headers now read from `queryInferredTable` instead of the left-panel cursor, so the title shows the collection/table the editor is actually targeting (e.g. `mongo · users` while typing `db.users.find({})` even if the Browse cursor is still on `comments`). Makes the "which schema am I using" question obvious at a glance.
- Fixed a longstanding annoyance where the Results tab required an escape keypress before accepting input: when `ctrl+r` ran with the completion popover open, `showColumnPicker` stayed true through the tab transition and the first Results keystroke got routed to the closed-picker handler. `openResultsTab` now clears `showColumnPicker`/`showQueryPicker` on entry.
- Extended inferred-schema use to the actual completion items, not just the header: `prefetchInferredSchema` now also parses SELECT queries (via `extractSelectTable`), so typing `SELECT * FROM users …` kicks off the `users` schema load even when Browse is pinned elsewhere. SQL column completion also swapped from `loadSchema` (which would clobber `m.tableSchema`) to `loadSchemaForCache`, matching the Mongo path. Added a "loading fields…" hint so the SQL picker stays open while the swap is in flight rather than collapsing to empty.

## 2026-04-15 — mongo shell-format autocomplete

- Fixed the core mongo autocomplete bug: `mongoTokens` was splitting on whitespace, so a full shell-format query like `db.users.find({` collapsed into a single token and the completion system thought you were always on token 0 (commands). Added `mongoShellTokens` that parses `db.collection.method(args)` into virtual tokens (method, collection, args) with positions mapping back to the original query. All the existing JSON key/value/operator completion logic now works inside `db.c.find(...)` the same way it did for internal `find c ...` format.
- Fixed cursor-to-token lookup in `mongoCompletionContext` — shell tokens aren't position-ordered (method at pos 9, collection at pos 3), so the short-circuit "first token past cursor" logic picked the wrong one. Now it checks all tokens for a direct hit before falling back to next-by-position.
- Added `loadSchemaForCache` — when autocomplete for `db.users.find({` triggers a schema load, the response no longer overwrites the left-panel `tableSchema` (which the user may have pinned to a different collection). Uses a stale reqID so the schema handler only writes to `schemaCache` and refreshes the picker.
- Added `prefetchInferredSchema` to fire on every Query-editor keystroke: as soon as `extractTableFromQuery` can resolve the text to a known collection (e.g. `db.users` once "users" matches a listed collection), the schema load kicks off in the background so the fields are ready before the user reaches `{`. Deduped via `schemaPending` to avoid repeat requests.
- Added "loading fields…" hint row when fields are pending but operators are already available, so the user sees that async work is in flight after switching collections.
- Fixed `extractTableFromQuery` to also parse internal mongo commands (`find users {}`), not only shell-format and SQL — query history and auto-refresh flows hit this path.
- Collection suggestions in shell-format rebuild the full `db.X.method(args)` expression when selected, so `db.users.find({"x":1})` → pick `comments` → becomes `db.comments.find({"x":1})`.

## 2026-04-15 — ollama query generator

- Added `internal/ollama` package with `Client.GenerateQuery` — sends NLP prompt + schema context to ollama and returns a raw SQL/Mongo query.
- New modal on Query tab (`ctrl+g` focused / `g` unfocused): type a plain-English description, press enter, ollama generates the query. Enter accepts into the editor, `r` retries, esc cancels.
- Moved Saved Queries picker from `ctrl+g` → `ctrl+u` (focused) and `g` → `u` (unfocused) to free up `ctrl+g` for AI generate.
- Schema context (table + column names from `schemaCache`) is passed to ollama automatically.
- Model/host configurable via `BOBDB_OLLAMA_MODEL` / `BOBDB_OLLAMA_HOST` env vars (defaults: `qwen2.5:7b`, `localhost:11434`).

## 2026-04-15

- Reworked Query-tab autocomplete so it behaves like optional inline assistance instead of a picker-first mode: suggestions auto-open only in recognized contexts, `tab` accepts them, and `enter` keeps editing the query.
- Removed the remaining built-in scaffold-heavy command insertion from Query autocomplete and templates: starters now insert minimal literal queries (`find users {} 50`, `SELECT * ... LIMIT 50`, etc.) instead of `${...}` placeholder sessions.
- Fixed Query key routing so editor shortcuts like `ctrl+l` still work while the completion popover is visible instead of being swallowed by the picker.
- Fixed a Results-tab navigation regression: stale `queryFocus` no longer causes Results to capture text-input routing or block `q`/back navigation.
- Fixed Mongo nested-operator editing so autocomplete replaces just the operator key when you switch operators inside an existing field object, preserving the current value text instead of resetting the whole scaffold.
- Removed snippet-style Query navigation from the normal editor flow: `tab` no longer advances `${...}` fields, picker/template insertion no longer spawns snippet sessions, and built-ins/templates now stay plain-text/editor-first.
- Suppressed autocomplete inside SQL `INSERT ... VALUES (` positions so value entry stays free-form instead of falling back to bogus loading/keyword pickers.
- Tightened structural Mongo replacement ranges: nested operator swaps now replace the operator/value pair when needed so value shape can change without wiping surrounding text, and direct value completion now replaces only the current value literal instead of the whole `{"field":...}` object.
- Removed the last live placeholder-style Query inserts from Mongo/SQL helpers and operator completions, replacing them with concrete JSON/SQL literals (`"$regex":""`, `"$in":[""]`, `= ''`, `{"$set":{}}`, etc.).
- Removed the last snippet-state control-flow hooks from completion insertion/clear paths so the Query editor no longer reopens or branches based on placeholder session state.
- Deleted the remaining snippet placeholder model fields/tests that were only carrying dead state; Query now has a single editor-first completion path in both behavior and code structure.
- Finished the Mongo object-context cleanup: bare filter `{` suggestions still expose field starters plus `$or`/`$and` operators, while partial key/operator edits inside existing objects now replace only the current key/value pair and preserve the surrounding JSON structure.
- Added SQL operator completions after filter columns, including comparison operators, `LIKE`, `IN`, `IS NULL`, and `IS NOT NULL`.
- Extended Mongo JSON autocomplete with top-level operators (`$or`, `$and`, etc.), update operators (`$set`, `$inc`, etc.), and nested comparison-operator suggestions like `$gt` / `$gte` inside field objects.
- Added a backend-aware Query `Examples` picker (`ctrl+e` in-editor, `x` from the Query tab) that surfaces concrete commands for the current database engine.
- Curated `Examples` as a separate reference surface instead of recycling `Templates`: example labels and content now emphasize backend-native query shapes (`read`, `filter`, `sort`, `aggregate`, `write`) while templates stay quick-action oriented.
- Updated regression coverage for literal starter insertion, tab-vs-enter completion behavior, SQL operator suggestions, Mongo operator suggestions, value-only replacement, the examples picker, and `ctrl+l` with the popover open.
- Touched: `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

## 2026-04-14i

- Tightened query-completion routing so fresh snippet placeholders are resolved by intent instead of falling through to the generic SQL keyword picker.
- `${table}` / `${columns}` / `${limit}` now open the matching picker, Mongo `${sort}` now opens schema-aware sort suggestions, and free-form placeholders like `${filter}` / `${values}` no longer hijack `tab`.
- Updated query-tab `tab` behavior to skip placeholders that should remain hand-written and immediately try the next placeholder's completion.
- Added regression coverage for these placeholder-routing flows in `model_flow_test.go`.
- Touched: `update.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

## 2026-04-14h

Contextual autocomplete overhaul for Query + templates:

- Completion picker now auto-opens while typing in recognized contexts (SQL/Mongo command, table/collection, field, value, limit, sort) instead of waiting for `tab` only.
- `tab` remains as a manual fallback/refresh key and still falls back to snippet field navigation when no completion applies.
- SQL clause-specific suggestions now include `LIMIT` numeric candidates and `ORDER BY` direction candidates (`ASC`/`DESC`) after a selected order column.
- Mongo argument completion now uses schema-aware filter/sort field suggestions and on-demand value samples for JSON value positions.
- Mongo value sampling now runs via an aggregate distinct-value query so value completions work like SQL's existing sample-value flow.
- Template/snippet loading now auto-opens relevant completion at the active `${...}` placeholder, including picker-loaded templates.
- Value-filter mode in the completion popover now supports in-place cursor editing (`←`/`→`/`home`/`end`) and completion ranking now includes substring matches (useful for domain fragments like `@gmail`).
- Non-value picker mode now keeps query cursor navigation active (`←`/`→`/`home`/`end`) while suggestions remain open, so edits don’t require closing the picker.
- Fixed a regression where pressing space in value-filter mode edited the query text instead of the filter input; value filtering now stays isolated from query edits.
- Fixed Mongo collection-switch completion context: after changing `find users ...` to `find comments ...`, filter field suggestions now resolve against the target collection via per-collection schema cache instead of sticking to the originally focused table.
- Updated Mongo autocomplete scaffolds/value insertion to respect schema scalar types: bool/number/null placeholders are unquoted and sampled bool values now insert as JSON booleans instead of string literals.
- Mongo bool value completion now offers immediate scalar literals (`true`/`false`/`null`) even before sample-value lookups complete, including unquoted in-progress JSON values (e.g. `{"isDemo":tr`).
- Extended typed Mongo value handling for complex types: objectId/date emit Extended JSON literals (`$oid`/`$date`), and array/object/map/document types now get structural literal starters (`[]`, `{}`, etc.) instead of forced quoted strings.
- Added/updated regression tests for auto-open behavior, SQL limit/order suggestions, template placeholder auto-open, and Mongo command/filter typing flow.
- Touched: `update.go`, `model_flow_test.go`, `README.md`, `WORK.md`, `DEVLOG.md`.

## 2026-04-14g

Root-cause fixes for query completion flow:

- **Template fix**: Column placeholders in SQL templates were wrapped in double-quotes (`"${col}"`) which made `cursorInsideString` return true, blocking all completions at that position. Stripped the surrounding quotes from all column/table placeholders — `columnInsertionValue` already adds proper quoting when a name is picked.
- **Value completion guard**: Added `${...}` check in `queryValueCompletionContext` — when the "column" is still an unfilled placeholder, skip value lookup (was generating useless "(no samples)" for column `${col}`).
- **Loading placeholder**: When schema is fetching async and the picker has no items yet, shows "loading columns…" (non-snippet mode only) so the picker opens and refreshes when schema arrives.
- Touched: `update.go`.

## 2026-04-14f

- Results tab left panel now shows the last-run query instead of the table list.
- Tab completions in WHERE/SELECT/etc now infer the table from the `FROM` clause of the current query, not the left-panel cursor. Schema is loaded async if needed and the picker refreshes when it arrives.
- Column completion fallback: if schema isn't loaded yet, uses result columns from the last query run.
- Value completions (`WHERE col = '...'`) also use the query-inferred table for sample value lookups. Removed `schemaHasColumn` gate — tries to load values whenever a column name is detected before `=`.
- WHERE column completion: after picking a column, auto-appends ` = '${value}'` snippet (skipped when an operator already follows, e.g. in templates).
- `schemaLoadedMsg` guard relaxed to reqID-only; refreshes open completion picker when schema arrives.
- Tab order flipped: completions are tried first; snippet navigation is fallback when no completions exist.
- Template flow: picking a completion at a `${placeholder}` replaces the placeholder text and correctly shifts remaining placeholders — subsequent `${table}` and `${value}` snippets remain navigable.
- `*` only offered in SELECT completion when actual schema columns are known (avoids blocking template navigation).
- `currentFreshSnippet()` helper detects when cursor is at an unedited placeholder, enabling completion to override its span.
- Touched: `update.go`, `view.go`.

## 2026-04-14e

- `e` contextual edit pre-fills current cell value; long/multiline values format as `UPDATE\nSET\nWHERE` for readability in the textarea. `E` opens the same UPDATE with empty value for clean replace.
- Inspect modal (`v`) colorizes JSON lines: keys in accent, structural braces dimmed, values in text.
- Template/history picker preview: shows up to 4 lines of selected item's content below the list; label width no longer penalized when no detail column is present.
- Removed Quick Start panel from Query tab — footer already covers the key hints.
- Touched: `update.go`, `view.go`, `model_flow_test.go`, `DEVLOG.md`, `WORK.md`.

## 2026-04-14d

- `e` edit query keeps `''` but cursor now lands between the quotes via `focusCursorAtIndex`, ready to type.
- Auto-refresh SELECT after UPDATE/DELETE now targets the edited row via the original WHERE clause (`SELECT * FROM "t" WHERE pk = 'id' LIMIT 1`) instead of full table scan.
- Fixed over-scroll right in Browse and Results: `shiftBrowseColumns`/`shiftResultColumns` now stop when the last column is already visible.
- Templates loaded from picker now activate snippet navigation for `${...}` placeholders.
- Generic SQL templates converted to use `${col}`, `${value}`, etc. for tab-navigable snippet holes.
- Touched: `update.go`, `WORK.md`, `DEVLOG.md`.

## 2026-04-14c

- Fixed write-query auto-refresh not firing for Mongo: `runUpdate`/`runDelete`/`runInsert` now set `Affected` on `QueryResult`.
- Extended `extractTableFromQuery` to parse Mongo command syntax (`insert/delete/remove <collection>`), not just SQL.
- Auto-refresh follow-up SELECT no longer pollutes query history or overwrites `lastRunQuery`; status shows original write message + "showing updated data".
- Touched: `internal/db/mongo.go`, `update.go`, `model.go`, `WORK.md`, `DEVLOG.md`.

## 2026-04-14b

- Fixed Postgres UUID columns rendering as byte arrays instead of proper strings (was breaking `e` edit commands on UUID-keyed tables).
- Changed `q` from quit to back-navigation: Results→Query→Browse→Connections→quit. `ctrl+c` still quits from anywhere.
- Browse tab now defaults to data view instead of schema; remembers view choice when switching tables.
- `e` edit template now uses empty quotes instead of `${value}` placeholder for cleaner editing.
- After write queries (UPDATE/INSERT/DELETE), results auto-refresh with a SELECT to show the updated data; browse data cache is also invalidated.
- Touched: `internal/db/postgres.go`, `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`.

## 2026-04-14

- Simplified the `bobdb` Makefile back to the suite's basic shape (`build` + `install` only, no version ldflags, trimpath, or extra test/vet/clean targets) to keep local iteration lightweight.
- Synced internal build conventions doc to match the simplified Makefile.
- Touched: `Makefile`, `CLAUDE.md`, `DEVLOG.md`.

- Removed the ad-hoc Mongo `help` command and all Query-tab copy that pointed users toward it; the editor now stays centered on completions, templates, and concrete command verbs instead of a special in-band help query.
- Added regression coverage to keep `help` out of Mongo command completions and quick-start guidance.
- Touched: `internal/db/mongo.go`, `update.go`, `view.go`, `model_flow_test.go`, `README.md`, `DEVLOG.md`.

- Added in-place editing for saved connections from the Connections tab, widened the DSN field for long pasted URIs, and updated the modal copy to make paste-friendly behavior explicit.
- Hardened config persistence so `bobdb` now writes `~/.config/bobdb/config.json` with owner-only permissions and prefers a non-empty legacy config if an empty XDG config was created during migration.
- Added regression coverage for connection editing and config-permission / legacy-fallback behavior.
- Touched: `internal/config/config.go`, `internal/config/config_test.go`, `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

- Rebuilt the Query-tab completion UX around an inline popover that renders under the editor instead of a centered full-screen dialog, so the SQL you're writing stays visible while browsing completions.
- Broadened context coverage: completions are suppressed inside string literals unless preceded by a comparison (`WHERE col = '…|'`), in which case a lazy sample-value cache populates distinct values from the column. SELECT-list completions now include aggregate functions (COUNT/SUM/AVG/MIN/MAX, DISTINCT). Empty-editor `tab` offers starter snippets plus per-table `SELECT * FROM …` shortcuts.
- Made the snippet placeholder session visible: the status line shows `field N/M: ${name}` while active, and `esc` now ends the session without leaving the editor.
- Audited the Query-tab footer: picker-open mode shows `↑/↓ · enter · esc`, edit mode folds `tab` into a single `complete / next field` hint and documents `ctrl+p/n` history cycling. Help modal gained `ctrl+t` and `ctrl+p/n`.
- Added tests for string-literal suppression, empty-editor table shortcuts, cached value completion, snippet-session hint, and inline popover rendering.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`, `CLAUDE.md`.


- Reworked the Schema tab into a Browse tab with a dual right-pane view: `enter` toggles between schema columns and a scrollable data preview (arrows + ←/→ for columns, like Results).
- Made `e` contextual: from Browse data/schema or the Results tab, it builds an UPDATE (or Mongo `update`) targeting the focused cell with `${value}` snippet placeholder and cursor-focuses it in the Query editor. Falls back to opening the default query when there's no cell context.
- Added browse data preview infrastructure: async loader with request-id guard, separate browseDataTable, column offset/shift, row inspect, and row copy. Data is lazy-loaded on first `enter` per table and cached until the table cursor changes or `r` refreshes.
- Tracked `querySourceTable` on the Results tab so `e` from Results inherits the source table for contextual edits when the result came from a browse query.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `CLAUDE.md`, `README.md`, `WORK.md`, `DEVLOG.md`.

- Removed the low-value Schema right-pane field copy action so the Schema view stays focused on inspect, browse, and query-entry actions.
- Added a regression test to keep Schema-pane `c` from quietly coming back as a noisy shortcut.
- Touched: `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

- Added destructive-action confirmations for saved-connection deletion and write queries so bobdb no longer executes obvious deletes/updates/inserts immediately.
- Reused the modal overlay flow for confirmations and added regression coverage for confirm-before-delete and confirm-before-run behavior.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

- Tuned the Query UX to be less aggressive: completions now stay manual-only on `tab`, the editor gained `ctrl+l` to clear the current query, and the placeholder/help copy was rewritten to be shorter and more direct.
- Trimmed footer hints and replaced the bulky in-pane assist copy with a compact quick-start block so the query flow stays discoverable without crowding the screen.
- Added regression coverage for manual-only completion behavior and the new clear-query keybind.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

- Reworked the Query tab around a single backend-aware completion flow shared by SQLite, Postgres, and MongoDB.
- Added SQL starters/clauses, Mongo command/collection/JSON scaffold suggestions, prefix-first ranking with fuzzy fallback, and live completion refresh while typing.
- Added snippet insertion with named placeholders and `tab` / `shift+tab` navigation so generated queries can be filled in without leaving the editor flow.
- Updated footer/help messaging to make completion and snippet behavior discoverable in the Query tab.
- Added regression coverage for Mongo command completion, snippet placeholder sessions, and placeholder tab-jump behavior.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

## 2026-04-13

- Fixed the Schema-only right-pane overflow by reserving Bubble table cell padding in schema column width calculations, preventing wrapped schema rows from stretching the panel.
- Clamped split-pane body rendering to the panel height so the Schema right pane border cannot extend below the left pane; verified with targeted layout tests.
- Clamped the shared page render budget so the Schema tab cannot push the top header off-screen when the schema pane overflows; added a regression test for Schema height stability.
- Touched: `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`.
- Fixed the schema/results pane layout so Bubble tables render at the full detail-pane width instead of falling back to a cramped left-aligned block.
- Switched Schema rendering back onto the live `bubbles/table` model and corrected pane-width budgeting used by schema/result tables.
- Fixed right-pane table height budgeting so pane headers stay visible and left/right panel boxes keep the same height after rendering schema/results tables.
- Fixed an additional Results-pane off-by-one in the split layout so the right panel no longer grows one line taller when result metadata is present.
- Added regression coverage for the corrected single-pane viewport width and schema table rendering path.
- Touched: `view.go`, `update.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.
- Removed the guided query builder and consolidated the Query tab around the raw editor only.
- Added schema-aware column completion in the Query editor, including cursor-aware insertion and a multi-select column picker modal.
- Expanded schema-aware helper generation with real table columns for lookup, insert, update, delete, grouping, recency, and null-audit templates.
- Simplified Query panel hints so query actions live in the footer/status flow instead of an in-panel quick-actions block.
- Reworked Schema/Results navigation so the surfaced UX uses arrow keys, Results opens on row 1 / column 1, and schema rows render with a full-row cursor.
- Tightened Results rendering by truncating header labels to column width, moving action hints out of pane bodies, and making row copy/detail use the structured viewer output for nested JSON-like values.
- Refreshed query helpers and editor flow with a history picker (`ctrl+o`), more practical starter templates, and schema reloads when switching tables from Query/Results.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `README.md`, `WORK.md`, `DEVLOG.md`.
- Reclaimed the trailing blank line under the panel box — chromeLines miscounted the status row so `contentH` was short by one.
- Fixed rightmost result column bleeding past the panel edge by budgeting each column's `+2` cell padding when picking visible columns; added a regression test.
- Reworked Mongo `find` to accept an optional trailing sort JSON (`find users {} 20 {"created_at":-1}`), updated `help` output, and rebuilt the Mongo template list with realistic filters, sort, ObjectID lookup, and aggregation examples.
