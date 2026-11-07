# Plans (advisor run 2026-11-08, base `ae876e0`, branch `improve`)

Operator constraints: no test/CI runs; tests written but not executed; docs/CI out of focus.

| # | Plan | Pri | Effort | Depends | Status |
|---|------|-----|--------|---------|--------|
| 001 | [CLI: no text on both prune/keep sides, dedupe](001-no-text-in-both-outputs.md) | P1 | M | — | TODO |
| 002 | [Library correctness bundle (monthly:N, need marshal, sub-second, cleanups)](002-library-correctness-bundle.md) | P1/P3 | S–M | — | TODO |
| 003 | [CLI robustness bundle (unix lower bound, buffering, flag warnings, help)](003-cli-robustness-bundle.md) | P2 | S | 001 | TODO |
| 004 | [Direction: weekly unit, NUL I/O](004-direction-weekly-and-nul.md) | P3 | M | 001 | NEEDS DECISION |

Order: 001 ∥ 002 (disjoint files) → 003.

## Considered and rejected
- Warn on equal timestamps from different records without `-n` — by design; recursive snapshots share timestamps; `-n` exists.
- Big refactor of `Main`/`processSnapshots` — only the split needed by 001; no standalone refactor.
- `Policy` copy-on-write to avoid map aliasing — overkill; documented instead (002-D).
- `daily:N` epoch alignment — arbitrary but consistent; superseded by a real `weekly` unit (004).
- Negative counts need `--` — pflag limitation; help note added (003-E).
- 64KB line limit — fails closed before output; intended and tested.
