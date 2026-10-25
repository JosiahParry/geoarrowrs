---
name: milestones
description: Show the R Consortium ISC grant milestones for geoarrowrs as a table of objectives, progress, and whether the user has verified each one. Use when asked about milestone or grant progress, what is left for a milestone, or what is verified; when the user reports reviewing or verifying an objective; and after finishing work on any milestone objective.
---

# Milestone tracker

`tracker.md`, next to this file, holds one row per objective in the
submitted R Consortium ISC proposal, with a progress status and a verified
status for each.

## Show the tracker

1. Read `tracker.md`.
2. Refresh the Progress and Evidence columns against the repo, as below.
   Leave Verified alone.
3. Print every table in `tracker.md`, then one line per milestone: done out
   of total, verified out of total, and the due date.
4. Below the tables, list any row the refresh changed.

## Refresh progress

For each row, check the repo:

- `NAMESPACE` for the `ga_` export.
- `R/sf-compat.R` for the sf method or masked function, where sf has one.
- `tests/testthat/` for a test that compares against sf row by row.
- `git log` for the commit that added it.

Progress takes one of three values:

- `Done`: the `ga_` function is exported, the sf wrapper exists where sf
  has the function, and a test compares against sf. Where sf has no
  counterpart, a test of the behaviour stands in for the sf comparison.
- `In progress`: some of those pieces exist.
- `Not started`: none of them exist.

Evidence names the functions, test files, and short commit hashes a
reviewer needs, plus any known gap, in one cell.

## Verified

Only the user sets Verified.

- Set a row to `Yes (YYYY-MM-DD)` only when the user reports reviewing that
  row, using today's date.
- Passing tests, a clean lint, or your own review never count as
  verification.
- If the code behind a verified row changes after its verification date,
  set the row back to `No (changed YYYY-MM-DD)` and say so.

## Rules

- The objectives come from `~/github/isc-proposal-2026/proposal/04-timeline.qmd`
  and `05-success.qmd`. The proposal is submitted: read it, never edit it,
  and never suggest changes to it.
- Keep the rows fixed. Add or split a row only when the user asks, for
  example to name a polygon constructor under 1.10.
- Commit `tracker.md` changes only when the user asks.
