<h1 align="center">snappr</h1>

<a href="https://pkg.go.dev/github.com/pgaskin/snappr"><img align="right" src="https://pkg.go.dev/badge/github.com/pgaskin/snappr.svg" alt="Go Reference"></a>

**CLI and library for pruning time-based snapshots with a flexible retention policy.**

- Standalone: works with any tool or script that outputs a list with dates in it.
- Extracts dates in arbitrary formats from arbitrary parts of a line, preserving the whole line.
- Multiple intervals per unit (e.g., one per month for 6 months, plus one per two months for 12).
- Zone-aware, with `--why` and `--summarize` to explain what was kept and why.

```bash
go install github.com/pgaskin/snappr/cmd/snappr@latest
```

#### Quick start

```bash
# rsync+btrfs snapshots: prune everything the policy doesn't keep
$ btrfs subvol list -r /mnt/bkp/ |
  snappr -sw \
    -e 'path snap\.([0-9-]{15})$' -Eqo \
    -p '20060102-150405' \
    1@last 12@secondly:1h 7@daily 6@monthly yearly:10 |
  cut -d ' ' -f2- |
  xargs --no-run-if-empty btrfs subvolume delete
```

- `-e ... -E -p ...`: extract the timestamp from capture group 1 and parse it with a Go time layout.
- `-o -q`: print only the matched part of each line, and silence warnings.
- `-s -w`: summarize the policy and explain each kept snapshot (on stderr).

#### Policy rules

A rule is `N@unit:X`: keep the last N buckets of X units each. The oldest snapshot in each bucket is kept, so you usually want `1@last` too.

| Unit       | Bucket                  | Alignment                                  | Example                          |
| ---------- | ----------------------- | ------------------------------------------ | -------------------------------- |
| `last`     | the newest snapshots    | X must be 1                                | `3@last`                         |
| `secondly` | X real seconds          | UTC-based, ignores timezone; `1h30m` ok    | `12@secondly:1h`                 |
| `daily`    | X calendar days         | counted from a fixed epoch (not weeks)     | `7@daily`, `4@daily:7`           |
| `monthly`  | X calendar months       | starts at January                          | `monthly:3` = calendar quarters  |
| `yearly`   | X calendar years        | aligned to year 0                          | `yearly:10` = 2020-2029          |

- Omit `N@`, or use a negative N, to keep infinitely many. On the CLI, put `--` before a negative N (e.g., `snappr -- -1@daily`).
- `:X` defaults to 1. There may be only one N per `unit:X`.
- `monthly:12` is the same as `yearly`.
- `monthly:N` (N>1) buckets were previously offset by one month, so upgrading changes which snapshots those rules keep.

#### Input and output

- Input is one record per line on stdin: a unix timestamp, or any line with `-e` (and `-p` for non-unix formats).
- Output is the prune candidates; `-v`/`--invert` prints the snapshots to keep instead.
- Unmatched or invalid lines are never prune candidates; they are passed through with `--invert` (with a warning unless `-q`).
- `-o` prints only the regexp match of each line.
- `-n` applies the policy independently per name (`-e` needs two groups: name, then timestamp).
- `-z` sets the timezone for splitting days/months/years; `-Z` the one assumed for parsed timestamps lacking one.

#### Safety

- Snappr only classifies records; it never deletes anything or treats records as filenames.
- A name (compared after trimming whitespace) retained by any record is never a prune candidate; snappr warns "not pruning ... identical to a retained record" instead. Common with `--only` on recursive snapshots (`tank/a@T`, `tank/b@T`) or duplicate lines. So prune and `--invert` output never share a name.
- Prune output prints each name once.
- Unix timestamps outside Go's representable range are invalid (kept side).
- All stdout is written in one go after classification; `-q` silences all warnings.
- Stage the output and check the exit status before running a destructive command; a live `snappr | delete` pipe is not transactional.
- The whole input is buffered; bound its size if the producer is untrusted.

#### CLI reference

<details>
<summary>snappr --help</summary>

```
usage: snappr [options] policy...

options:
  -E, --extended-regexp     use full regexp syntax rather than POSIX (see pkg.go.dev/regexp/syntax)
  -e, --extract string      extract the timestamp using a regexp with one capture group, or two with --group-by-name
  -n, --group-by-name       apply the policy independently by name (capture group 1 is the name and capture group 2 is the timestamp)
  -h, --help                show this help text
  -v, --invert              output the snapshots to keep instead of the ones to prune
  -o, --only                only print the part of the line matching the regexp
  -p, --parse string        parse the timestamp using the specified Go time format (see pkg.go.dev/time#pkg-constants and the examples below) rather than a unix timestamp
  -Z, --parse-timezone tz   use a specific timezone rather than whatever is set for --timezone if no timezone is parsed from the timestamp itself
  -q, --quiet               do not show warnings
  -s, --summarize           summarize retention policy results to stderr
  -z, --timezone tz         convert all timestamps to this timezone while pruning snapshots (use "local" for the default system timezone) (default UTC)
  -w, --why                 explain why each snapshot is being kept to stderr

time format examples:
  - Mon Jan 02 15:04:05 2006
  - 02 Jan 06 15:04 MST
  - 2006-01-02T15:04:05Z07:00
  - 2006-01-02T15:04:05

policy: N@unit:X
  - keep the last N snapshots every X units
  - omit the N@ to keep an infinite number of snapshots
  - a negative N also keeps an infinite number (pass -- before such policies, e.g. -- -1@daily)
  - if :X is omitted, it defaults to :1
  - there may only be one N specified for each unit:X pair

unit:
  last       snapshot count (X must be 1)
  secondly   clock seconds (can also use the format #h#m#s, omitting any zeroed units)
  daily      calendar days
  monthly    calendar months
  yearly     calendar years

notes:
  - output lines consist of filtered input lines
  - input is read from stdin, and should consist of unix timestamps (or more if --extract and/or --parse are set)
  - invalid/unmatched input lines are ignored, or passed through if --invert is set (and a warning is printed unless --quiet is set)
  - everything will still work correctly even if timezones are different
  - snapshots are always ordered by their real (i.e., UTC) time
  - if using --parse-timezone, beware of duplicate timestamps at DST transitions (if the offset isn't included whatever you use as the
    snapshot name, and your timezone has DST, you may end up with two snapshots for different times with the same name)
  - timezones will only affect the exact point at which calendar days/months/years are split
```

</details>

#### Library

```go
var times []time.Time
// ...

var policy snappr.Policy
policy.MustSet(snappr.Yearly, 5, -1)
policy.MustSet(snappr.Yearly, 2, 10)
policy.MustSet(snappr.Monthly, 6, 4)
policy.MustSet(snappr.Daily, 1, 7)
policy.MustSet(snappr.Last, 1, 3)

keep, need := snappr.Prune(times, policy, time.UTC)
for at, reason := range keep {
    if len(reason) == 0 {
        fmt.Println("delete", times[at])
    }
}
fmt.Println("unfulfilled:", need) // may contain satisfied (zero-count) periods, which MarshalText omits
```

See [pkg.go.dev](https://pkg.go.dev/github.com/pgaskin/snappr) for the full API.
