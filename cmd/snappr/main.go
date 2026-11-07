// Command snappr prunes time-based snapshots from stdin.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pgaskin/snappr"
	"github.com/spf13/pflag"
)

// Go time.Time stores seconds relative to year 1 in an int64. Unix seconds
// above this value overflow that internal representation in time.Unix.
const maxUnixSecond int64 = (1<<63 - 1) - 62135596800

func main() {
	if status := Main(os.Args, os.Stdin, os.Stdout, os.Stderr); status != 0 {
		os.Exit(status)
	}
}

type timezoneFlag struct {
	loc *time.Location
}

func pflag_TimezoneP(opt *pflag.FlagSet, name, shorthand string, value *time.Location, usage string) **time.Location {
	f := &timezoneFlag{value}
	opt.VarP(f, name, shorthand, usage)
	return &f.loc
}

func (t *timezoneFlag) Type() string {
	return "tz"
}

func (t *timezoneFlag) String() string {
	if t.loc == nil {
		return ""
	}
	return t.loc.String()
}

func explainSnapshots(grouped bool, name string, times []time.Time, recordIndexes []int, keep [][]snappr.Period, need snappr.Policy, discard []bool, policy snappr.Policy, why, summarize bool, stderr io.Writer) {
	var pruned int
	ndig := digits(len(keep))
	for at, reason := range keep {
		if !discard[recordIndexes[at]] {
			if why {
				ps := make([]string, len(reason))
				for i, period := range reason {
					ps[i] = period.String()
				}
				explanation := strings.Join(ps, ", ")
				if len(reason) == 0 {
					explanation = "identical to a retained record"
				}
				if !grouped {
					fmt.Fprintf(stderr, "snappr: why: keep [%*d/%*d] %s :: %s\n",
						ndig, at+1, ndig, len(keep),
						times[at].Format("Mon 2006 Jan _2 15:04:05"), explanation)
				} else {
					fmt.Fprintf(stderr, "snappr: why: keep [%*d/%*d] %s (%q) :: %s\n",
						ndig, at+1, ndig, len(keep),
						times[at].Format("Mon 2006 Jan _2 15:04:05"), name, explanation)
				}
			}
		} else {
			pruned++
		}
	}

	if summarize {
		var cmax int
		policy.Each(func(_ snappr.Period, count int) {
			cmax = max(cmax, count)
		})
		cdig := digits(cmax)
		need.Each(func(period snappr.Period, count int) {
			prefix := "snappr: summary:"
			if grouped {
				prefix = fmt.Sprintf("snappr: summary (%q):", name)
			}
			if count < 0 {
				fmt.Fprintf(stderr, "%s (%s) %s\n", prefix, strings.Repeat("*", cdig), period)
			} else if count == 0 {
				fmt.Fprintf(stderr, "%s (%*d) %s\n", prefix, cdig, policy.Get(period), period)
			} else {
				fmt.Fprintf(stderr, "%s (%*d) %s (missing %d)\n", prefix, cdig, policy.Get(period), period, count)
			}
		})
		if !grouped {
			fmt.Fprintf(stderr, "snappr: summary: pruning %d/%d snapshots\n", pruned, len(keep))
		} else {
			fmt.Fprintf(stderr, "snappr: summary (%q): pruning %d/%d snapshots\n", name, pruned, len(keep))
		}
	}
}

func (t *timezoneFlag) Set(s string) error {
	switch string(s) {
	case "":
		return fmt.Errorf("timezone must not be empty")
	case "UTC", "utc":
		t.loc = time.UTC
	case "Local", "local":
		t.loc = time.Local
	default:
		loc, err := time.LoadLocation(s)
		if err != nil {
			return err
		}
		t.loc = loc
	}
	return nil
}

func Main(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		args = []string{"snappr"}
	}
	opt := pflag.NewFlagSet(args[0], pflag.ContinueOnError)
	var (
		Quiet       = opt.BoolP("quiet", "q", false, "do not show warnings about invalid or unmatched input lines")
		Extract     = opt.StringP("extract", "e", "", "extract the timestamp using a regexp with one capture group, or two with --group-by-name")
		Extended    = opt.BoolP("extended-regexp", "E", false, "use full regexp syntax rather than POSIX (see pkg.go.dev/regexp/syntax)")
		Only        = opt.BoolP("only", "o", false, "only print the part of the line matching the regexp")
		Parse       = opt.StringP("parse", "p", "", "parse the timestamp using the specified Go time format (see pkg.go.dev/time#pkg-constants and the examples below) rather than a unix timestamp")
		ParseIn     = pflag_TimezoneP(opt, "parse-timezone", "Z", nil, "use a specific timezone rather than whatever is set for --timezone if no timezone is parsed from the timestamp itself")
		In          = pflag_TimezoneP(opt, "timezone", "z", time.UTC, "convert all timestamps to this timezone while pruning snapshots (use \"local\" for the default system timezone)")
		Invert      = opt.BoolP("invert", "v", false, "output the snapshots to keep instead of the ones to prune")
		Why         = opt.BoolP("why", "w", false, "explain why each snapshot is being kept to stderr")
		Summarize   = opt.BoolP("summarize", "s", false, "summarize retention policy results to stderr")
		GroupByName = opt.BoolP("group-by-name", "n", false, "apply the policy independently by name (capture group 1 is the name and capture group 2 is the timestamp)")
		Help        = opt.BoolP("help", "h", false, "show this help text")
	)
	if err := opt.Parse(args[1:]); err != nil {
		fmt.Fprintf(stderr, "snappr: fatal: %v\n", err)
		return 2
	}

	if *Help {
		var help strings.Builder
		fmt.Fprintf(&help, "usage: %s [options] policy...\n", args[0])
		fmt.Fprintf(&help, "\noptions:\n%s", opt.FlagUsages())
		fmt.Fprint(&help, "\ntime format examples:\n")
		fmt.Fprint(&help, "  - Mon Jan 02 15:04:05 2006\n")
		fmt.Fprint(&help, "  - 02 Jan 06 15:04 MST\n")
		fmt.Fprint(&help, "  - 2006-01-02T15:04:05Z07:00\n")
		fmt.Fprint(&help, "  - 2006-01-02T15:04:05\n")
		fmt.Fprint(&help, "\npolicy: N@unit:X\n")
		fmt.Fprint(&help, "  - keep the last N snapshots every X units\n")
		fmt.Fprint(&help, "  - omit the N@ to keep an infinite number of snapshots\n")
		fmt.Fprint(&help, "  - if :X is omitted, it defaults to :1\n")
		fmt.Fprint(&help, "  - there may only be one N specified for each unit:X pair\n")
		fmt.Fprint(&help, "\nunit:\n")
		fmt.Fprint(&help, "  last       snapshot count (X must be 1)\n")
		fmt.Fprint(&help, "  secondly   clock seconds (can also use the format #h#m#s, omitting any zeroed units)\n")
		fmt.Fprint(&help, "  daily      calendar days\n")
		fmt.Fprint(&help, "  monthly    calendar months\n")
		fmt.Fprint(&help, "  yearly     calendar years\n")
		fmt.Fprint(&help, "\nnotes:\n")
		fmt.Fprint(&help, "  - output lines consist of filtered input lines\n")
		fmt.Fprint(&help, "  - input is read from stdin, and should consist of unix timestamps (or more if --extract and/or --parse are set)\n")
		fmt.Fprint(&help, "  - invalid/unmatched input lines are ignored, or passed through if --invert is set (and a warning is printed unless --quiet is set)\n")
		fmt.Fprint(&help, "  - everything will still work correctly even if timezones are different\n")
		fmt.Fprint(&help, "  - snapshots are always ordered by their real (i.e., UTC) time\n")
		fmt.Fprint(&help, "  - if using --parse-in, beware of duplicate timestamps at DST transitions (if the offset isn't included whatever you use as the\n")
		fmt.Fprint(&help, "    snapshot name, and your timezone has DST, you may end up with two snapshots for different times with the same name.\n")
		fmt.Fprint(&help, "  - timezones will only affect the exact point at which calendar days/months/years are split\n")
		if _, err := io.WriteString(stdout, help.String()); err != nil {
			fmt.Fprintf(stderr, "snappr: fatal: failed to write stdout: %v\n", err)
			return 1
		}
		return 0
	}

	if opt.NArg() < 1 {
		fmt.Fprintf(stderr, "snappr: fatal: at least one policy must be specified (see --help)\n")
		return 2
	}

	if *ParseIn == nil {
		*ParseIn = *In
	}

	policy, err := snappr.ParsePolicy(opt.Args()...)
	if err != nil {
		fmt.Fprintf(stderr, "snappr: fatal: invalid policy: %v\n", err)
		return 2
	}

	var extract *regexp.Regexp
	if *GroupByName && *Extract == "" {
		fmt.Fprintf(stderr, "snappr: fatal: --group-by-name requires --extract\n")
		return 2
	}
	if *Extract != "" {
		var err error
		if *Extended {
			extract, err = regexp.Compile(*Extract)
		} else {
			extract, err = regexp.CompilePOSIX(*Extract)
		}
		if err == nil {
			want := 1
			if *GroupByName {
				want = 2
			}
			if extract.NumSubexp() != want {
				err = fmt.Errorf("must contain exactly %d capture group(s)", want)
			}
		}
		if err != nil {
			fmt.Fprintf(stderr, "snappr: fatal: --extract regexp is invalid: %v\n", err)
			return 2
		}
	}

	type NamedSnapshots struct {
		Name          string
		Times         []time.Time
		RecordIndexes []int
		Keep          [][]snappr.Period
		Need          snappr.Policy
	}

	var (
		lines      []string
		groups     []*NamedSnapshots
		grouped    map[string]*NamedSnapshots
		defaultSet = &NamedSnapshots{}
	)

	err = func() error {
		sc := bufio.NewScanner(stdin)
		for sc.Scan() {
			line := sc.Text()
			if len(line) == 0 {
				continue
			}
			recordIndex := len(lines)
			lines = append(lines, line)

			var (
				bad  bool
				ts   string
				name string
			)

			if extract == nil {
				ts = strings.TrimSpace(line)
				name = "default"
			} else {
				m := extract.FindStringSubmatch(line)
				if m == nil {
					if !*Quiet {
						if _, err := fmt.Fprintf(stderr, "snappr: warning: failed extract timestamp from %q using regexp %q\n", line, extract.String()); err != nil {
							return fmt.Errorf("write stderr: %w", err)
						}
					}
					continue
				}
				if *GroupByName {
					name = m[1]
					ts = m[2]
				} else {
					ts = m[1]
				}
				if *Only {
					line = m[0]
					lines[recordIndex] = line
				}
			}

			var t time.Time
			if *Parse == "" {
				n, err := strconv.ParseInt(ts, 10, 64)
				if err == nil && n > maxUnixSecond {
					err = fmt.Errorf("value is outside the representable time range")
				}
				if err != nil {
					if !*Quiet {
						if _, err := fmt.Fprintf(stderr, "snappr: warning: failed to parse unix timestamp %q: %v\n", ts, err); err != nil {
							return fmt.Errorf("write stderr: %w", err)
						}
					}
					bad = true
				} else {
					t = time.Unix(n, 0)
				}
			} else {
				v, err := time.ParseInLocation(*Parse, ts, *ParseIn)
				if err != nil {
					if !*Quiet {
						if _, err := fmt.Fprintf(stderr, "snappr: warning: failed to parse timestamp %q using layout %q: %v\n", ts, *Parse, err); err != nil {
							return fmt.Errorf("write stderr: %w", err)
						}
					}
					bad = true
				} else {
					t = v
				}
			}
			if bad {
				continue
			}
			t = t.In(*In)

			if *GroupByName {
				if grouped == nil {
					grouped = make(map[string]*NamedSnapshots)
				}
				if _, ok := grouped[name]; !ok {
					grouped[name] = &NamedSnapshots{Name: name}
					groups = append(groups, grouped[name])
				}
				grouped[name].Times = append(grouped[name].Times, t)
				grouped[name].RecordIndexes = append(grouped[name].RecordIndexes, recordIndex)
			} else {
				defaultSet.Times = append(defaultSet.Times, t)
				defaultSet.RecordIndexes = append(defaultSet.RecordIndexes, recordIndex)
			}
		}
		return sc.Err()

	}()
	if err != nil {
		fmt.Fprintf(stderr, "snappr: fatal: failed to read stdin: %v\n", err)
		return 1
	}

	if !*GroupByName {
		groups = append(groups, defaultSet)
	}
	discard := make([]bool, len(lines))
	for _, group := range groups {
		group.Keep, group.Need = snappr.Prune(group.Times, policy, *In)
		for at, reason := range group.Keep {
			discard[group.RecordIndexes[at]] = len(reason) == 0
		}
	}

	// never prune a record whose output text is identical to a retained one,
	// since the output is usually fed to a delete command by name
	// (keyed on the trimmed text, since xargs-style consumers split on whitespace)
	retained := make(map[string]bool)
	for i, prunable := range discard {
		if !prunable {
			retained[strings.TrimSpace(lines[i])] = true
		}
	}
	warned := make(map[string]bool)
	for i, prunable := range discard {
		key := strings.TrimSpace(lines[i])
		if prunable && retained[key] {
			discard[i] = false
			if !*Quiet && !warned[key] {
				warned[key] = true
				if _, err := fmt.Fprintf(stderr, "snappr: warning: not pruning %q: identical to a retained record\n", lines[i]); err != nil {
					fmt.Fprintf(stderr, "snappr: fatal: failed to write stderr: %v\n", err)
					return 1
				}
			}
		}
	}

	var diagnostics strings.Builder
	for _, group := range groups {
		explainSnapshots(*GroupByName, group.Name, group.Times, group.RecordIndexes, group.Keep, group.Need, discard, policy, *Why, *Summarize, &diagnostics)
	}

	printed := make(map[string]bool)
	for i, prunable := range discard {
		if prunable == *Invert {
			continue
		}
		if prunable {
			key := strings.TrimSpace(lines[i])
			if printed[key] {
				continue
			}
			printed[key] = true
		}
		if _, err := fmt.Fprintln(stdout, lines[i]); err != nil {
			fmt.Fprintf(stderr, "snappr: fatal: failed to write stdout: %v\n", err)
			return 1
		}
	}
	if diagnostics.Len() != 0 {
		if _, err := io.WriteString(stderr, diagnostics.String()); err != nil {
			return 1
		}
	}

	return 0
}

func digits(n int) int {
	if n == 0 {
		return 1
	}
	count := 0
	for n != 0 {
		n /= 10
		count++
	}
	return count
}
