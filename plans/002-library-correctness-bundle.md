# Plan 002: Library correctness bundle (monthly:N alignment, `need` marshal, sub-second durations, small cleanups)

> **Executor instructions**: Do NOT run `go test` or CI (operator instruction). Only `go build ./... && go vet ./...` is allowed. STOP conditions → stop and report. Commit per logical unit.
>
> **Drift check**: `git diff --stat ae876e0..HEAD -- snappr.go snappr_regression_test.go`; on mismatch with excerpts, STOP.

## Status
- **Priority**: P1 (item A), P3 (rest) · **Effort**: S–M · **Risk**: MED (A changes which snapshots are kept) · **Depends on**: none · **Category**: bug + tech-debt
- **Planned at**: commit `ae876e0`, 2026-11-08

## Why this matters
Library `github.com/pgaskin/snappr` (`snappr.go`) decides which snapshots survive. Several small defects either keep the wrong snapshots or silently misrepresent a policy.

## Scope
**In scope**: `snappr.go`, `snappr_regression_test.go` (append), `snappr_test.go` (only the hash constant, see A).
**Out of scope**: `cmd/snappr/*` (another executor may be editing it concurrently — never stage it), README, CI.

## Items

### A. `monthly:N` buckets are off by one month (bug, P1)
`snappr.go:389-391`:
```go
case Monthly:
    year, month, _ := t.Date()
    current = (int64(year)*12 + int64(month))
```
`month` is 1–12, then `floorDiv(current, Interval)`. So `monthly:3` groups Dec–Feb (not calendar quarters), `monthly:12` groups Dec–Nov and disagrees with `yearly`. Fix: `int64(year)*12 + int64(month) - 1`. Compare: Yearly uses `year` directly, Daily uses a day number — both aligned to year 0.

Consequence: `TestPrune` in `snappr_test.go:~399` asserts a SHA-256 hash over a policy that uses `Monthly, 6` and `Monthly, 2`; it will change. You cannot run tests to recompute it. Add a comment on that line: `// FIXME(monthly-alignment): hash must be regenerated after monthly:N alignment fix` and leave the old value. Do not guess a new hash.

New test (append to `snappr_regression_test.go`, pattern: `TestPruneSelectsTheExpectedRecordsForEveryUnit`): monthly snapshots on the 1st of each month Jan 2023–Mar 2024 UTC, policy `monthly:3` → retained months are exactly Jan, Apr, Jul, Oct 2023, Jan 2024. Also: `monthly:12` retains the same records as `yearly` for that input.

### B. `need` from `Prune` contains zero counts that `MarshalText` encodes as infinite (bug)
`Prune` ends each period with `need.count[period] = count` (`snappr.go:419`), so a satisfied period has count 0. `Set` never stores 0 (it deletes), and `MarshalText` (`snappr.go:287-309`) only writes `N@` when `count > 0` — so a 0 count marshals as e.g. `last`, which parses back as infinite (-1).
Fix in `MarshalText`: skip periods with `count == 0` entirely (`if count == 0 { return }` at top of the `Each` callback; be careful the `b != nil` separator logic still works — set separator only when appending). Do NOT change `Prune`'s zero entries — the CLI `--summarize` relies on iterating them. Add one sentence to the `Prune` doc comment: need may contain periods with a zero count (satisfied); MarshalText omits them.
Test: `1@last` with 3 snapshots → `need.MarshalText()` yields `""`; `need.Get(Period{Last,1}) == 0`.

### C. Sub-second `secondly` durations silently truncated (bug)
`snappr.go:245-253`:
```go
vx, err := strconv.ParseInt(x, 10, strconv.IntSize)
if vu == Secondly && err != nil {
    var tmp time.Duration
    tmp, err = time.ParseDuration(x)
    vx = int64(tmp / time.Second)
    ...
```
`secondly:1.5s` → 1s; `secondly:90.9s` → 90s. Fix: after successful ParseDuration, if `tmp%time.Second != 0` return error `rule %q: interval %q must be a whole number of seconds`. Test: `ParsePolicy("secondly:1.5s")` and `("secondly:1999ms")` error; `("secondly:2000ms")` ok with Interval 2.

### D. Small cleanups (no behavior change)
- `snappr.go:19` comment `// wallclock seconds` → `// unix seconds (timezone-independent)`; that's what the code does (`t.Unix()`).
- `snappr.go:355` comment says `// sort the snapshots descending`; sort is ascending → fix comment.
- `snappr.go:406-407` reverse loop reassigns the range variable: replace with `for i := len(match) - 1; i >= 0; i-- {` and drop the reassignment.
- Perf (cheap): `snapshots[sorted[i]].In(loc).Truncate(-1)` is recomputed per period, also for `Last`. Precompute once before `policy.Each`: `zoned := make([]time.Time, len(snapshots))` in sorted order. Keep logic identical otherwise.
- `Policy` doc comment: add "Copying a Policy value shares its underlying state; use Clone for an independent copy." (copy `c := a; c.Set(...)` mutates `a`).

## Verify
After each item: `go build ./... && go vet ./...` → exit 0.

## Git workflow
Branch `improve` (already checked out). Stage only `snappr.go snappr_test.go snappr_regression_test.go` explicitly. Suggested commits: `snappr: align monthly:N buckets to calendar year`, `snappr: omit satisfied periods from MarshalText, reject sub-second intervals`, `snappr: minor cleanups`. End each message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Don't push.

## STOP conditions
- Excerpts don't match.
- Any change would require editing `cmd/snappr/*`.

## Maintenance note
A changes retention results for existing `monthly:N>1` users; the operator must mention it in release notes and regenerate the `TestPrune` hash.
