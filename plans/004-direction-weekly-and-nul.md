# Plan 004 (direction, NOT auto-executed): weekly unit + NUL-delimited I/O

> Needs a maintainer decision before execution — both change public surface.

## Status
- **Priority**: P3 · **Effort**: M · **Risk**: MED · **Depends on**: 001 · **Category**: direction
- **Planned at**: commit `ae876e0`, 2026-11-08

## A. `weekly` unit
Borg/restic/sanoid all offer weekly retention. snappr's closest, `daily:7`, buckets on an arbitrary day-0 epoch (weeks start Friday-ish), not calendar weeks.
Decision needed: (1) new `Unit` constant placement — inserting `Weekly` between `Daily` and `Monthly` (natural order, affects `Period.Compare`, `Each` order, `MarshalText` canonical order) renumbers `Monthly`/`Yearly`, breaking anyone persisting the int values; appending after `Yearly` keeps ints but gives odd sort order; (2) week start: ISO Monday vs configurable.
Implementation sketch: bucket = `floorDiv(dayNumber(t) + offset, 7)` with offset chosen so Monday starts a bucket; parse `weekly` in `ParsePolicy`, `Period.String` → `N week`, help text unit list.

## B. `-z/--null` NUL-delimited input/output
Snapshot names with newlines can't be represented safely. `-z` would split stdin on `\0` and terminate output records with `\0` (pairs with `xargs -0`). Implement with `bufio.Scanner` custom split func; route output through the plan-001 reconcile/dedupe path.

## Not recommended
- Warning on equal timestamps across different records without `-n`: legit for recursive snapshots; noisy. `--group-by-name` already covers it.
