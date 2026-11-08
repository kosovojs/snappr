<h1 align="center">snappr</h1>

<a href="https://pkg.go.dev/github.com/pgaskin/snappr"><img align="right" src="https://pkg.go.dev/badge/github.com/pgaskin/snappr.svg" alt="Go Reference"></a>

**CLI and library for pruning time-based snapshots with a flexible retention policy.**

#### Features

- **Standalone.** \
  Works with any tool or script which can output a list with dates somewhere in it.

- **Approximate snapshot selection.** \
  Snapshots periods are not fixed to specific dates. The first matching snapshot for each period is kept (note that this means you'll usually want to keep at least the last snapshot in addition to whatever other rules you have).

- **Robust retention policies.** \
  Multiple intervals are supported for each period (last, secondly, daily, monthly, yearly). You can have one snapshot every month for 6 months, while also having one every two for 12.

- **Flexible command line interface.** \
  Can extract dates in arbitrary formats from arbitrary parts of a line, preserving the entire line, and ignoring or passing-through unmatched or invalid lines.

- **Zone-aware timestamp handling.** \
  Will work correctly with different timezones, sorting and determining secondly snapshots by the real time, but using the calendar day/month/year from the zoned time.

- **Verbose debugging information.** \
  You can view which intervals caused a specific snapshot to be retained, and whether a retention policy wants more snapshots than it found.

#### CLI Example

```bash
# install latest version
$ go install github.com/pgaskin/snappr/cmd/snappr@latest
```

```bash
# install from source
$ go install ./cmd/snappr
```

```bash
# testing a range of dates
$ seq 946684800 $((13+55*60*6)) 1735689600 |
  snappr -sw 1@last 7@daily 6@monthly 4@monthly:6 6@yearly 4@yearly:12 >/dev/null
```

```bash
# simple rsync+btrfs snapshots
$ rsync ... /mnt/bkp/cur/
$ btrfs subvol snap -r /mnt/bkp/cur/ /mnt/bkp/snap.$(date --utc +%Y%m%d-%H%M%S)
$ btrfs subvol list -r /mnt/bkp/ |
  snappr -sw \
    -e 'path snap\.([0-9-]{15})$' -Eqo \
    -p '20060102-150405' \
    1@last 12@secondly:1h 7@daily 4@daily:7 6@monthly 5@yearly yearly:10 |
  cut -d ' ' -f2- |
  xargs --no-run-if-empty btrfs subvolume delete
```

#### CLI Usage

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

#### Policy semantics

Intervals are calendar-aligned buckets, and the first snapshot in each bucket is kept:

- `monthly:N` buckets start at January (e.g., `monthly:3` is calendar quarters, and `monthly:12` is the same as `yearly`).
- `yearly:N` buckets are aligned to year 0 (e.g., `yearly:10` is 2020-2029).
- `daily:N` counts days from a fixed epoch, so it is not week-aligned.
- `secondly` buckets use real (UTC-based) time, regardless of timezone.
- A negative N means infinite; on the CLI, put `--` before it (e.g., `snappr -- -1@daily`).

Note: `monthly:N` (N>1) buckets were previously offset by one month, so upgrading changes which snapshots those rules keep.

#### Library Example

```go
var times []time.Time
// ...

var policy snappr.Policy
policy.MustSet(snappr.Yearly, 5, -1)
policy.MustSet(snappr.Yearly, 2, 10)
policy.MustSet(snappr.Yearly, 1, 3)
policy.MustSet(snappr.Monthly, 6, 4)
policy.MustSet(snappr.Monthly, 2, 6)
policy.MustSet(snappr.Daily, 1, 7)
policy.MustSet(snappr.Secondly, int(time.Hour/time.Second), 6)
policy.MustSet(snappr.Last, 1, 3)

keep, need := snappr.Prune(times, policy, time.UTC)
for at, reason := range keep {
    if len(reason) == 0 {
        fmt.Println("delete", times[at])
    }
}
fmt.Println("unfulfilled:", need) // may contain satisfied (zero-count) periods, which MarshalText omits
```

#### Production safety

Snappr only classifies records; it never deletes snapshots or interprets records as filenames. Default output is the prune-candidate set, while `--invert` outputs the retain set. Invalid or unmatched non-empty records are excluded from prune output and passed through in retain output. With `--only`, every regexp-matched record is projected to the complete match even if its timestamp is invalid; unmatched records remain unchanged. Unix timestamps outside Go's representable range are treated as invalid (kept side).

A name (compared after trimming surrounding whitespace) that is retained by any record is never printed as a prune candidate; Snappr warns "not pruning ... identical to a retained record" instead. This is common with `--only` on recursive snapshots (e.g., `tank/a@T` and `tank/b@T`) or duplicate input lines. As a result, prune and `--invert` output never share a name. Prune output prints each name once.

All stdout is written in one go after classification. `-q` silences all warnings.

When a downstream command is destructive, stage Snappr's complete output first and check its exit status before invoking that command. A live `snappr | delete-command` pipeline is not transactional: a consumer can act on early records before a later read or write failure is known.

Snappr buffers the complete input before producing classification output. Bound the input size externally when the producer is untrusted or potentially unbounded.
