# Plan 003: CLI robustness bundle (unix lower bound, buffered output, ignored-flag warnings, messages)

> **Executor instructions**: Do NOT run `go test` or CI. Only `go build ./... && go vet ./...`. STOP conditions → stop and report.
>
> **Drift check**: this plan runs AFTER plan 001 modified `cmd/snappr/main.go`. Line numbers below are from `ae876e0` and will have shifted — locate code by the quoted snippets. If a snippet can't be found, STOP.

## Status
- **Priority**: P2 · **Effort**: S · **Risk**: LOW · **Depends on**: plans/001-no-text-in-both-outputs.md · **Category**: bug + dx
- **Planned at**: commit `ae876e0`, 2026-11-08

## Scope
**In scope**: `cmd/snappr/main.go`, `cmd/snappr/regression_test.go`.
**Out of scope**: `snappr.go`, golden files in `cmd/snappr/test/` except `help.txt` (item E only), README.

## Items

### A. No lower bound on unix timestamps (bug)
```go
// Go time.Time stores seconds relative to year 1 in an int64. Unix seconds
// above this value overflow that internal representation in time.Unix.
const maxUnixSecond int64 = (1<<63 - 1) - 62135596800
...
n, err := strconv.ParseInt(ts, 10, 64)
if err == nil && n > maxUnixSecond {
    err = fmt.Errorf("value is outside the representable time range")
}
```
For very negative `n` (e.g. `-9223372036854775808`) Go's calendar math wraps: `Year()` returns ~292277026596, so a bogus record can share a `yearly` bucket with a real one and push a real snapshot into the prune output. Add `const minUnixSecond = -maxUnixSecond` (symmetric; keeps Go's internal absolute-time arithmetic from wrapping) and reject `n < minUnixSecond` with the same error. Update the const comment to explain both bounds.
Existing test `TestRepresentableUnixSecondBoundariesAreClassified` asserts `-9223372036854775808` is classified: change its input's first line to the new minimum (`-9223371974719179007`) so it stays a valid-boundary test, and add a case to `TestUnrepresentableUnixSecondsStayOnTheSafeSide` style asserting `-9223372036854775808` stays out of prune output and passes through with `--invert`.

### B. Unbuffered stdout (perf)
Each `fmt.Fprintln(stdout, ...)` is a syscall. Wrap: `w := bufio.NewWriter(stdout)`, write lines to `w`, then `if err := w.Flush(); err != nil { fatal "failed to write stdout" ; return 1 }`. Keep existing per-write error check (bufio returns sticky errors). Don't buffer stderr warnings (they're interleaved semantics-wise and few).

### C. Flags silently ignored (dx)
After flag parsing, warn (unless `--quiet`): `-o/--only` or `-E/--extended-regexp` without `-e/--extract`; `-Z/--parse-timezone` without `-p/--parse`. Format: `snappr: warning: --only has no effect without --extract`. Use `opt.Changed("only")` etc. Warnings only — not fatal (don't break scripts). Golden files: check `cmd/snappr/test/*.txt` `-- args --` sections with `rg -n -A1 -- '-- args --' cmd/snappr/test`; if any golden case uses these flags without their dependency AND checks stderr, STOP and report.

### D. Misleading error message
Scanner closure returns `fmt.Errorf("write stderr: %w", err)` for warning-write failures, and the caller prints `snappr: fatal: failed to read stdin: %v`. Make the caller distinguish: e.g. return a sentinel-wrapped error or print `snappr: fatal: %v` with the closure producing `read stdin: ...` / `write stderr: ...` prefixes. Exit code stays 1.

### E. Help text fixes (help.txt golden must be updated in the same commit)
- Note references `--parse-in`, which doesn't exist → `--parse-timezone`. Also the parenthesis opened in "(if the offset isn't included..." is never closed — close it.
- Add a policy note line: `  - use -- before policies with a negative N (e.g. -- -1@daily), or omit N@ instead`.
- Update `cmd/snappr/test/help.txt` `-- stdout --` section to match the new help output exactly (help is assembled in `Main` under `if *Help {`; flag usage lines come from pflag `FlagUsages()` — don't change those).

### F. Nits
- `timezoneFlag.Set`: `switch string(s)` → `switch s`.

## Verify
`go build ./... && go vet ./...` → exit 0 after each item.

## Git workflow
Branch `improve`. Stage only in-scope files explicitly. Commit(s) like `cmd/snappr: reject unrepresentable negative timestamps`, `cmd/snappr: buffer stdout, warn on ignored flags, fix help`. End with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Don't push.

## STOP conditions
- Snippets not found. Golden stderr conflict (item C).
