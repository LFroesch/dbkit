# Devlog

## 2026-04-14

- Removed the low-value Schema right-pane field copy action so the Schema view stays focused on inspect, browse, and query-entry actions.
- Added a regression test to keep Schema-pane `c` from quietly coming back as a noisy shortcut.
- Touched: `update.go`, `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.

- Added destructive-action confirmations for saved-connection deletion and write queries so dbkit no longer executes obvious deletes/updates/inserts immediately.
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
