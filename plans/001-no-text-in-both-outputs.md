# Plan 001: CLI never prints a prune candidate whose text equals a retained record

> **Executor instructions**: Follow step by step. Do NOT run `go test` or any CI (operator instruction). The only allowed verification is `go build ./... && go vet ./...` (vet compiles tests too). If a STOP condition occurs, stop and report. Commit when done (see Git workflow).
>
> **Drift check (run first)**: `git diff --stat ae876e0..HEAD -- cmd/snappr/main.go cmd/snappr/regression_test.go`. If `main.go` changed, compare excerpts below against live code; mismatch = STOP.

## Status
- **Priority**: P1 · **Effort**: M · **Risk**: MED · **Depends on**: none · **Category**: bug (data loss)
- **Planned at**: commit `ae876e0`, 2026-11-08

## Why this matters
`snappr` output is typically piped into a delete command (`xargs btrfs subvolume delete`, `zfs destroy`). Every non-empty input line is a separate record, so two records with identical *output text* can land on opposite sides: one kept, one pruned. The delete command then destroys the name the policy wanted to keep.
- Plain duplicates: `printf '100\n100\n200\n' | snappr daily` → prints `100`, though one `100` record is retained.
- `--only` makes this common: `tank/a@1700000000` and `tank/b@1700000000` with `-E -o -e '@([0-9]+)$' daily` → prints `@1700000000` on the prune side AND the keep side.

Invariant to establish: **no string ever appears in both the default (prune) output and the `--invert` (keep) output for the same input/args.** Also: the prune output prints each distinct text at most once (deleting the same name twice makes `xargs` fail).

## Current state
`cmd/snappr/main.go` (single-file CLI, `Main(args, stdin, stdout, stderr) int`):
- `lines []string` holds one entry per non-empty input record; with `--only` the entry is replaced by the regexp match `m[0]` (`main.go:274-277`).
- `processSnapshots(...)` (`main.go:49-104`) calls `snappr.Prune`, sets `discard[recordIndexes[at]] = len(reason) == 0`, and *in the same function* writes `--why` lines and `--summarize` lines (including `pruning %d/%d snapshots`, computed from `keep`) into a diagnostics `strings.Builder`.
- Output loop (`main.go:340-359`):
```go
discard := make([]bool, len(lines))
var diagnostics strings.Builder
for _, group := range groups {
    processSnapshots(*GroupByName, group.Name, group.Times, group.RecordIndexes, discard, policy, *In, *Why, *Summarize, &diagnostics)
}
for i, prunable := range discard {
    if prunable == *Invert {
        continue
    }
    if _, err := fmt.Fprintln(stdout, lines[i]); err != nil { ... return 1 }
}
if diagnostics.Len() != 0 { io.WriteString(stderr, diagnostics.String()) ... }
```
- Records that are unmatched/invalid keep `discard[i] == false` (they pass through on `--invert`).
- Warnings use the form `snappr: warning: ...\n` and are suppressed by `--quiet` (`*Quiet`). Match that.

## Scope
**In scope**: `cmd/snappr/main.go`, `cmd/snappr/regression_test.go` (append tests).
**Out of scope**: `snappr.go` (library), golden files under `cmd/snappr/test/` (should not need changes — if one needs changing, STOP and report which and why), README.

## Steps

### Step 1: Split classification from diagnostics
Change the flow so that **all groups are pruned first**, then reconciliation (step 2) runs, then `--why`/`--summarize` diagnostics are produced. Suggested shape: `processSnapshots` → keep calling `snappr.Prune` per group and storing `keep`/`need` per group (e.g. add fields to the local `NamedSnapshots` type: `Keep [][]snappr.Period; Need snappr.Policy`), and move the diagnostic printing into a separate function called after reconciliation. The summary line `pruning %d/%d snapshots` must count records with `discard[recordIndexes[at]] == true` **after** reconciliation, not `len(reason)==0`. Keep every existing diagnostic format string byte-identical otherwise (golden files in `cmd/snappr/test/` assert them).

**Verify**: `go build ./... && go vet ./...` → exit 0.

### Step 2: Reconcile by output text
After all groups are classified and before writing stdout:
```go
retained := make(map[string]bool)
for i, prunable := range discard {
    if !prunable {
        retained[lines[i]] = true
    }
}
for i, prunable := range discard {
    if prunable && retained[lines[i]] {
        discard[i] = false
        // warning unless *Quiet:
        // snappr: warning: not pruning %q: identical to a retained record
    }
}
```
Warnings go through the same stderr-write-error handling style as existing warnings (on failure: `snappr: fatal: failed to write stderr: %v`, return 1, nothing written to stdout yet). Emit these warnings before any stdout output.

For `--why`: a record flipped here has no `snappr.Period` reasons; print it as kept with reason text `identical to a retained record` instead of the period list, using the same `snappr: why: keep [...]` prefix format as other lines (grouped variant includes `(%q)` name). Ponytail: if this complicates the code noticeably, skip the `--why` line for flipped records — the warning already explains it — and leave a `// ponytail:` comment.

**Verify**: `go build ./... && go vet ./...` → exit 0.

### Step 3: Deduplicate prune output
In the stdout loop, when `!*Invert`, skip a prunable record whose text was already printed (track a `map[string]bool`). Do NOT dedupe `--invert` output (keeps pass-through records verbatim).

**Verify**: `go build ./... && go vet ./...` → exit 0.

### Step 4: Tests (write, do not run)
Append to `cmd/snappr/regression_test.go`, using the existing `runMain(args, stdin)` helper (pattern: `TestUnmatchedRecordsPassThroughOnlyOnKeepOutput`):
1. `TestIdenticalTextNeverOnBothSides`: input `"100\n100\n200\n"`, args `snappr daily` → prune stdout `""`; with `--invert` → `"100\n100\n200\n"`.
2. `--only` collision: input `"tank/a@1700000000\ntank/b@1700000000\n"`, args `snappr -E -o -e '@([0-9]+)$' daily` → prune `""`; invert contains `@1700000000`.
3. Dedupe: input `"x 100\nx 100\nx 200\n"` with `-E -o -e '^x ([0-9]+)$' 1@last`? — careful: that collides with retained `x 200`? No (`x 200` differs). Expected prune: `"x 100\n"` — but both `x 100` records are prunable only if neither is retained; with `1@last` only `x 200` is retained → prune `"x 100\n"` exactly once.
4. Property test (no fuzz engine needed): for a handful of inputs, assert the set of prune lines ∩ set of invert lines is empty.
Note `FuzzMainPartitionsValidGroupedRecords` uses unique lines, so it still holds.

**Verify**: `go vet ./cmd/snappr` → exit 0.

## Git workflow
Branch is already `improve`. Stage only in-scope files explicitly (`git add cmd/snappr/main.go cmd/snappr/regression_test.go`) — another executor may be editing other files concurrently. Commit message style (from `git log`): short imperative, e.g. `cmd/snappr: never prune text identical to a retained record`. End message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Don't push.

## STOP conditions
- Excerpts don't match live code.
- A golden file under `cmd/snappr/test/` would need its expected stdout/stderr changed.

## Maintenance note
Any future output mode (e.g. NUL-delimited) must route through the same reconcile + dedupe step. Reviewers: check the invariant "prune ∩ keep = ∅ by text".
