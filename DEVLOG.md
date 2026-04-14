# Devlog

## 2026-04-13

- Fixed the Browse-only right-pane overflow by reserving Bubble table cell padding in schema column width calculations, preventing wrapped schema rows from stretching the panel.
- Clamped split-pane body rendering to the panel height so the Browse right pane border cannot extend below the left pane; verified with targeted layout tests.
- Clamped the shared page render budget so the Browse tab cannot push the top header off-screen when the schema pane overflows; added a regression test for Browse height stability.
- Touched: `view.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`.
- Fixed the schema/results pane layout so Bubble tables render at the full detail-pane width instead of falling back to a cramped left-aligned block.
- Switched Browse schema rendering back onto the live `bubbles/table` model and corrected pane-width budgeting used by schema/result tables.
- Fixed right-pane table height budgeting so pane headers stay visible and left/right panel boxes keep the same height after rendering schema/results tables.
- Fixed an additional Results-pane off-by-one in the split layout so the right panel no longer grows one line taller when result metadata is present.
- Added regression coverage for the corrected single-pane viewport width and schema table rendering path.
- Touched: `view.go`, `update.go`, `model_flow_test.go`, `WORK.md`, `DEVLOG.md`, `README.md`.
- Removed the guided query builder and consolidated the Query tab around the raw editor only.
- Added schema-aware column completion in the Query editor, including cursor-aware insertion and a multi-select column picker modal.
- Expanded schema-aware helper generation with real table columns for lookup, insert, update, delete, grouping, recency, and null-audit templates.
- Simplified Query panel hints so query actions live in the footer/status flow instead of an in-panel quick-actions block.
- Reworked Browse/Results navigation so the surfaced UX uses arrow keys, Results opens on row 1 / column 1, and browse rows render with a full-row cursor.
- Tightened Results rendering by truncating header labels to column width, moving action hints out of pane bodies, and making row copy/detail use the structured viewer output for nested JSON-like values.
- Refreshed query helpers and editor flow with a history picker (`ctrl+o`), more practical starter templates, and schema reloads when switching tables from Query/Results.
- Touched: `model.go`, `update.go`, `view.go`, `model_flow_test.go`, `README.md`, `WORK.md`, `DEVLOG.md`.
- Reclaimed the trailing blank line under the panel box — chromeLines miscounted the status row so `contentH` was short by one.
- Fixed rightmost result column bleeding past the panel edge by budgeting each column's `+2` cell padding when picking visible columns; added a regression test.
- Reworked Mongo `find` to accept an optional trailing sort JSON (`find users {} 20 {"created_at":-1}`), updated `help` output, and rebuilt the Mongo template list with realistic filters, sort, ObjectID lookup, and aggregation examples.
