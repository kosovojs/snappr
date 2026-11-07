package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // Europe/Riga etc. must resolve on hosts without zoneinfo
)

// scExpect runs the CLI and compares status, stdout and stderr exactly.
func scExpect(t *testing.T, args []string, stdin string, wantStatus int, wantOut, wantErr string) {
	t.Helper()
	status, out, errOut := runMain(append([]string{"snappr"}, args...), stdin)
	if status != wantStatus || out != wantOut || errOut != wantErr {
		t.Fatalf("args=%q stdin=%q\nstatus: want %d got %d\nstdout: want %q got %q\nstderr: want %q got %q",
			args, stdin, wantStatus, status, wantOut, out, wantErr, errOut)
	}
}

// 1. CRLF, missing trailing newline, blank and whitespace-only lines.

func TestScenarioCRLFInput(t *testing.T) {
	// scanner strips \r, so "100\r" is parsed as 100 and printed without \r
	scExpect(t, []string{"1@last"}, "100\r\n200\r\n300\r\n", 0, "100\n200\n", "")
	scExpect(t, []string{"-v", "1@last"}, "100\r\n200\r\n300\r\n", 0, "300\n", "")
	// no trailing newline on the last record
	scExpect(t, []string{"1@last"}, "100\r\n200", 0, "100\n", "")
	// a "$"-anchored regexp only matches because \r was stripped; blank CRLF line skipped
	scExpect(t, []string{"-E", "-o", "-e", `^([0-9]+)$`, "1@last"}, "100\r\n\r\n200\r\n", 0, "100\n", "")
}

func TestScenarioBlankAndWhitespaceOnlyLines(t *testing.T) {
	// "" lines are skipped entirely. "   " and "\t" are records whose trimmed
	// timestamp "" is unparsable: one warning each, never pruned, and printed
	// (as-is) only with --invert, in input order.
	const input = "100\n\n   \n\t\n200\n\n"
	const warn = `snappr: warning: failed to parse unix timestamp "": strconv.ParseInt: parsing "": invalid syntax` + "\n"

	scExpect(t, []string{"1@last"}, input, 0, "100\n", warn+warn)
	scExpect(t, []string{"-v", "1@last"}, input, 0, "   \n\t\n200\n", warn+warn)
	scExpect(t, []string{"-q", "-v", "1@last"}, input, 0, "   \n\t\n200\n", "")
}

// 2. POSIX leftmost-longest vs Perl-style leftmost-first.

func TestScenarioPOSIXvsExtendedCapture(t *testing.T) {
	// Pattern (1|12):
	//   POSIX (leftmost-longest): "12" -> m[1]="12"; "1" -> "1"
	//   -E    (leftmost-first)  : "12" -> m[1]="1";  "1" -> "1"
	// Input "12\n1\n", 1@last:
	//   POSIX: times 12, 1 -> newest is line 0, prune "1".
	//   -E   : times 1, 1 -> tie, later input is newer, keep line 1, prune "12".
	const input = "12\n1\n"
	scExpect(t, []string{"-e", "(1|12)", "1@last"}, input, 0, "1\n", "")
	scExpect(t, []string{"-E", "-e", "(1|12)", "1@last"}, input, 0, "12\n", "")

	// --only prints m[0]: the longest vs first alternative on "123".
	scExpect(t, []string{"-v", "-o", "-e", "(1|12)", "1@last"}, "123\n", 0, "12\n", "")
	scExpect(t, []string{"-v", "-E", "-o", "-e", "(1|12)", "1@last"}, "123\n", 0, "1\n", "")
}

func TestScenarioPOSIXRejectsPerlSyntax(t *testing.T) {
	// \d is not POSIX ERE: fatal without -E, fine with -E.
	status, out, _ := runMain([]string{"snappr", "-e", `(\d+)`, "1@last"}, "a5\nb6\n")
	if status != 2 || out != "" {
		t.Fatalf("want status 2 and no output, got status=%d stdout=%q", status, out)
	}
	scExpect(t, []string{"-E", "-e", `(\d+)`, "1@last"}, "a5\nb6\n", 0, "a5\n", "")
}

// 3. --only with --group-by-name.

func TestScenarioOnlyGroupByName(t *testing.T) {
	args := []string{"-E", "-n", "-o", "-e", `^([a-z]+)@([0-9]+)`, "1@last"}

	// Distinct projections behave per group: a@1 and b@1 are the older ones.
	scExpect(t, args, "a@1\nb@1\na@2\nb@2\n", 0, "a@1\nb@1\n", "")

	// Trailing text is dropped by --only. Projected: a@1 a@1 a@2 b@1 b@1.
	//   group a (times 1,1,2): keep idx2; prune idx0, idx1 (both "a@1", printed once)
	//   group b (times 1,1)  : tie -> later input is newer -> keep idx4;
	//     idx3 is prunable but its text "b@1" is retained -> not pruned, warned once.
	// The name is part of m[0], so identical projections can't span groups.
	const input = "a@1 x\na@1 y\na@2 z\nb@1 q\nb@1 r\n"
	const warn = `snappr: warning: not pruning "b@1": identical to a retained record` + "\n"
	scExpect(t, args, input, 0, "a@1\n", warn)
	scExpect(t, append([]string{"-v"}, args...), input, 0, "a@2\nb@1\nb@1\n", warn)
	scExpect(t, append([]string{"-q"}, args...), input, 0, "a@1\n", "")
}

// 4. Exact --why / --summarize output.

func TestScenarioWhySummarizeUngrouped(t *testing.T) {
	// 0=Thu Jan 1 00:00:00, 1800=00:30:00, 3600=01:00:00, 90000=Fri Jan 2 01:00:00 (1970 UTC)
	// policy: 2@last 3@daily monthly(inf)   [Each order: last, daily, monthly]
	//   last   : keeps idx3, idx2                      -> need 0
	//   daily  : first of day1 = idx0, day2 = idx3     -> need 3-2 = 1
	//   monthly: first of Jan 1970 = idx0 (inf)        -> need stays -1
	//   keep: idx0=[1 day, 1 month] idx1=[] idx2=[last] idx3=[last, 1 day]
	// cmax=3 -> one digit; ndig=digits(4)=1; idx1 (1800) is pruned.
	const input = "0\n1800\n3600\n90000\n"
	const wantErr = "" +
		"snappr: why: keep [1/4] Thu 1970 Jan  1 00:00:00 :: 1 day, 1 month\n" +
		"snappr: why: keep [3/4] Thu 1970 Jan  1 01:00:00 :: last\n" +
		"snappr: why: keep [4/4] Fri 1970 Jan  2 01:00:00 :: last, 1 day\n" +
		"snappr: summary: (2) last\n" +
		"snappr: summary: (3) 1 day (missing 1)\n" +
		"snappr: summary: (*) 1 month\n" +
		"snappr: summary: pruning 1/4 snapshots\n"
	scExpect(t, []string{"-w", "-s", "2@last", "3@daily", "monthly"}, input, 0, "1800\n", wantErr)
}

func TestScenarioSummaryPadding(t *testing.T) {
	// 10@daily makes cdig=2: "( 2) last", "(10) 1 day (missing 8)", "(**) 1 month".
	// Three snapshots on one day: last keeps idx2,idx1; daily/monthly match only idx0,
	// so daily need = 10-1 = 9. Everything is retained (pruning 0/3).
	const input = "0\n1\n2\n"
	const wantErr = "" +
		"snappr: summary: ( 2) last\n" +
		"snappr: summary: (10) 1 day (missing 9)\n" +
		"snappr: summary: (**) 1 month\n" +
		"snappr: summary: pruning 0/3 snapshots\n"
	scExpect(t, []string{"-s", "2@last", "10@daily", "monthly"}, input, 0, "", wantErr)
}

func TestScenarioWhyIndexPadding(t *testing.T) {
	// 10 snapshots at unix seconds 1..10, 3@last keeps 8, 9, 10 (indexes 8..10)
	// ndig=digits(10)=2 -> "[ 8/10]", "[ 9/10]", "[10/10]".
	var in strings.Builder
	for i := 1; i <= 10; i++ {
		in.WriteString(strconv.Itoa(i) + "\n")
	}
	const wantErr = "" +
		"snappr: why: keep [ 8/10] Thu 1970 Jan  1 00:00:08 :: last\n" +
		"snappr: why: keep [ 9/10] Thu 1970 Jan  1 00:00:09 :: last\n" +
		"snappr: why: keep [10/10] Thu 1970 Jan  1 00:00:10 :: last\n" +
		"snappr: summary: (3) last\n" +
		"snappr: summary: pruning 7/10 snapshots\n"
	scExpect(t, []string{"-w", "-s", "3@last"}, in.String(), 0, "1\n2\n3\n4\n5\n6\n7\n", wantErr)
}

func TestScenarioWhySummarizeGrouped(t *testing.T) {
	// records: 0 "a 0", 1 "b 0", 2 "a 3600", 3 "a 90000"; policy 1@last 2@daily; cdig=1.
	// group "a" (times 0, 3600, 90000):
	//   last : newest -> a[2]; daily: day1 first a[0], day2 first a[2] -> need 0
	//   keep a[0]=[1 day] a[1]=[] a[2]=[last, 1 day]  -> "a 3600" pruned
	// group "b" (time 0): keep [last, 1 day]; daily need 2-1 = 1.
	// Groups are explained in first-appearance order, why before summary per group.
	const input = "a 0\nb 0\na 3600\na 90000\n"
	const wantErr = "" +
		`snappr: why: keep [1/3] Thu 1970 Jan  1 00:00:00 ("a") :: 1 day` + "\n" +
		`snappr: why: keep [3/3] Fri 1970 Jan  2 01:00:00 ("a") :: last, 1 day` + "\n" +
		`snappr: summary ("a"): (1) last` + "\n" +
		`snappr: summary ("a"): (2) 1 day` + "\n" +
		`snappr: summary ("a"): pruning 1/3 snapshots` + "\n" +
		`snappr: why: keep [1/1] Thu 1970 Jan  1 00:00:00 ("b") :: last, 1 day` + "\n" +
		`snappr: summary ("b"): (1) last` + "\n" +
		`snappr: summary ("b"): (2) 1 day (missing 1)` + "\n" +
		`snappr: summary ("b"): pruning 0/1 snapshots` + "\n"
	args := []string{"-E", "-n", "-e", `^([a-z]+) ([0-9]+)$`, "-w", "-s", "1@last", "2@daily"}
	scExpect(t, args, input, 0, "a 3600\n", wantErr)
}

// 5. --parse layouts and timezones.

func TestScenarioParseRFC3339Offsets(t *testing.T) {
	// A=23:30Z Mar10, B=01:30+03:00 (=22:30Z Mar10), C=00:30Z Mar11. Real order: B, A, C.
	const input = "2024-03-10T23:30:00Z\n2024-03-11T01:30:00+03:00\n2024-03-11T00:30:00Z\n"
	const a, b, c = "2024-03-10T23:30:00Z\n", "2024-03-11T01:30:00+03:00\n", "2024-03-11T00:30:00Z\n"
	base := []string{"-p", time.RFC3339}

	// UTC days: Mar10={B,A} first=B ; Mar11={C} first=C. 2@daily keeps B, C; A pruned.
	scExpect(t, append(base[:2:2], "2@daily"), input, 0, a, "")
	scExpect(t, append(base[:2:2], "-v", "2@daily"), input, 0, b+c, "")

	// Riga is +02:00 in March: B=Mar11 00:30, A=Mar11 01:30, C=Mar11 02:30 -> one day,
	// first=B kept, A and C pruned (input order).
	scExpect(t, append(base[:2:2], "-z", "Europe/Riga", "2@daily"), input, 0, a+c, "")
}

func TestScenarioParseZoneOnlyAppliesWithoutZoneInLayout(t *testing.T) {
	const layout = "2006-01-02 15:04:05"
	const input = "2024-03-10 23:30:00\n2024-03-11 01:30:00\n"

	// no -Z: wall times are UTC -> different UTC days; newest day kept, first pruned.
	scExpect(t, []string{"-p", layout, "1@daily"}, input, 0, "2024-03-10 23:30:00\n", "")
	// -Z Riga (+02:00): 21:30Z and 23:30Z, both Mar 10 UTC -> first kept, second pruned.
	scExpect(t, []string{"-p", layout, "-Z", "Europe/Riga", "1@daily"}, input, 0, "2024-03-11 01:30:00\n", "")

	// layout with a zone: the string's own zone wins, -Z is ignored (and not warned about).
	// 23:30Z Mar10 and 00:30Z Mar11 are different UTC days -> first pruned.
	// (If -Z Tokyo were applied to the wall time, both would be Mar 10 UTC.)
	scExpect(t, []string{"-p", time.RFC3339, "-Z", "Asia/Tokyo", "1@daily"},
		"2024-03-10T23:30:00Z\n2024-03-11T00:30:00Z\n", 0, "2024-03-10T23:30:00Z\n", "")
}

func TestScenarioParseFailureStaysOnKeepSide(t *testing.T) {
	const input = "2024-01-01\nnot-a-date\n2024-01-02\n"
	args := []string{"-p", "2006-01-02", "1@last"}
	const warnPrefix = `snappr: warning: failed to parse timestamp "not-a-date" using layout "2006-01-02": `

	status, out, errOut := runMain(append([]string{"snappr"}, args...), input)
	if status != 0 || out != "2024-01-01\n" {
		t.Fatalf("prune: status=%d stdout=%q", status, out)
	}
	if !strings.HasPrefix(errOut, warnPrefix) || strings.Count(errOut, "\n") != 1 {
		t.Fatalf("want exactly one parse warning, got %q", errOut)
	}

	status, out, errOut = runMain(append([]string{"snappr", "-v"}, args...), input)
	if status != 0 || out != "not-a-date\n2024-01-02\n" || !strings.HasPrefix(errOut, warnPrefix) {
		t.Fatalf("keep: status=%d stdout=%q stderr=%q", status, out, errOut)
	}

	scExpect(t, append([]string{"-q", "-v"}, args...), input, 0, "not-a-date\n2024-01-02\n", "")
}

// 6. Exit statuses.

func TestScenarioConfigurationErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string // exact stderr, or prefix when wantPfx
		wantPfx bool
	}{
		{"unknown unit", []string{"bogus"}, `snappr: fatal: invalid policy: rule "bogus": unknown unit "bogus"` + "\n", false},
		{"zero count", []string{"0@daily"}, "snappr: fatal: invalid policy: ", true},
		{"unknown flag", []string{"--nope", "last"}, "snappr: fatal: unknown flag: --nope", true},
		{"no policy", []string{"-q"}, "snappr: fatal: at least one policy must be specified (see --help)\n", false},
		{"group without extract", []string{"-n", "last"}, "snappr: fatal: --group-by-name requires --extract\n", false},
		{"one capture with -n", []string{"-E", "-n", "-e", "(x)", "last"},
			"snappr: fatal: --extract regexp is invalid: must contain exactly 2 capture group(s)\n", false},
		{"two captures without -n", []string{"-E", "-e", "(x)(y)", "last"},
			"snappr: fatal: --extract regexp is invalid: must contain exactly 1 capture group(s)\n", false},
		{"bad regexp", []string{"-E", "-e", "(", "last"}, "snappr: fatal: --extract regexp is invalid: ", true},
		{"bad timezone", []string{"-z", "Nowhere/Land", "last"}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, out, errOut := runMain(append([]string{"snappr"}, tt.args...), "100\n200\n")
			if status != 2 || out != "" {
				t.Fatalf("want status 2 and empty stdout, got status=%d stdout=%q", status, out)
			}
			if tt.wantPfx && !strings.Contains(errOut, tt.wantErr) || !tt.wantPfx && errOut != tt.wantErr {
				t.Fatalf("stderr: want %q got %q", tt.wantErr, errOut)
			}
		})
	}
}

// 7. Unix timestamp parsing.

func TestScenarioUnixTimestampLeniency(t *testing.T) {
	// "+100", "007", "  200  " all parse (sign, leading zeros, surrounding space trimmed);
	// 1@last keeps 300, the rest are pruned and printed unchanged (with spaces).
	scExpect(t, []string{"1@last"}, "+100\n007\n  200  \n300\n", 0, "+100\n007\n  200  \n", "")

	// numeric, not lexical, ordering: 0010 (=10) is newer than 9.
	scExpect(t, []string{"1@last"}, "0010\n9\n", 0, "9\n", "")
}

func TestScenarioUnixTimestampRejects(t *testing.T) {
	const input = "1e9\n0x10\n1.5\n5\n"
	const wantErr = "" +
		`snappr: warning: failed to parse unix timestamp "1e9": strconv.ParseInt: parsing "1e9": invalid syntax` + "\n" +
		`snappr: warning: failed to parse unix timestamp "0x10": strconv.ParseInt: parsing "0x10": invalid syntax` + "\n" +
		`snappr: warning: failed to parse unix timestamp "1.5": strconv.ParseInt: parsing "1.5": invalid syntax` + "\n"
	// invalid records are never pruned; only "5" is valid and 1@last keeps it
	scExpect(t, []string{"1@last"}, input, 0, "", wantErr)
	scExpect(t, []string{"-v", "1@last"}, input, 0, input, wantErr)
}

// 8. Large input.

func TestScenarioLargeInput(t *testing.T) {
	// 100000 snapshots at t = 600*i seconds from the epoch (every 10 min, 144/day,
	// every day starts at a multiple of 144 lines). Day 0 = 1970-01-01.
	// Last line i=99999 -> day 694 (partial day: lines 99936..99999) = 1971-11-26 10:30.
	// (1971-01-01 is day 365; 694-365=329 -> 0-based day-of-year 329, November starts at 304.)
	//
	// 1@last   : i=99999
	// 7@daily  : first line of days 688..694 -> i = 144*688 .. 144*694
	//            = 99072, 99216, 99360, 99504, 99648, 99792, 99936
	// 4@monthly: first line of Aug..Nov 1971 (months fully/partly in data) ->
	//            day starts 365+212, 365+243, 365+273, 365+304 = 577, 608, 638, 669
	//            -> i = 83088, 87552, 91872, 96336 (disjoint from the daily days and from i=99999)
	// Total kept: 12, all with distinct text.
	const n = 100000
	kept := []int{83088, 87552, 91872, 96336, 99072, 99216, 99360, 99504, 99648, 99792, 99936, 99999}
	isKept := make(map[int]bool, len(kept))
	for _, i := range kept {
		isKept[i] = true
	}

	var in, wantPrune, wantKeep strings.Builder
	for i := 0; i < n; i++ {
		line := strconv.Itoa(600*i) + "\n"
		in.WriteString(line)
		if isKept[i] {
			wantKeep.WriteString(line)
		} else {
			wantPrune.WriteString(line)
		}
	}
	policy := []string{"1@last", "7@daily", "4@monthly"}

	status, prune, errOut := runMain(append([]string{"snappr"}, policy...), in.String())
	if status != 0 || errOut != "" {
		t.Fatalf("prune: status=%d stderr=%q", status, errOut)
	}
	if got := strings.Count(prune, "\n"); got != n-len(kept) {
		t.Fatalf("prune count: want %d got %d", n-len(kept), got)
	}
	if prune != wantPrune.String() { // also asserts input order is preserved
		t.Fatalf("prune output differs (len want %d got %d)", wantPrune.Len(), len(prune))
	}

	status, keep, errOut := runMain(append([]string{"snappr", "-v"}, policy...), in.String())
	if status != 0 || errOut != "" {
		t.Fatalf("keep: status=%d stderr=%q", status, errOut)
	}
	if keep != wantKeep.String() {
		t.Fatalf("keep output differs: want %q got %q", wantKeep.String(), keep)
	}
}

// 9. Ignored-flag warnings.

func TestScenarioIgnoredFlagWarnings(t *testing.T) {
	const (
		wOnly = "snappr: warning: --only has no effect without --extract\n"
		wExt  = "snappr: warning: --extended-regexp has no effect without --extract\n"
		wZone = "snappr: warning: --parse-timezone has no effect without --parse\n"
	)
	// -e '' is the same as no -e, so -o is ignored too
	scExpect(t, []string{"-o", "-e", "", "last"}, "0\n", 0, "", wOnly)
	scExpect(t, []string{"-E", "last"}, "0\n", 0, "", wExt)
	scExpect(t, []string{"-Z", "UTC", "last"}, "0\n", 0, "", wZone)
	// fixed order: only, extended-regexp, parse-timezone
	scExpect(t, []string{"-Z", "UTC", "-E", "-o", "last"}, "0\n", 0, "", wOnly+wExt+wZone)
	// -q silences them
	scExpect(t, []string{"-q", "-Z", "UTC", "-E", "-o", "last"}, "0\n", 0, "", "")
	// all flags used correctly: no warnings
	scExpect(t, []string{"-Z", "UTC", "-p", "2006-01-02", "-E", "-o", "-e", `^(.*)$`, "last"}, "2024-01-01\n", 0, "", "")
}

// 10. --invert with --only on unmatched lines.

func TestScenarioInvertOnlyUnmatchedLineUnchanged(t *testing.T) {
	// "plain line  " doesn't match: warned, kept, and printed in full (trailing spaces
	// and all). Matched kept record is projected to m[0] ("id=2"); "id=1" is pruned.
	const input = "id=1 aaa\nplain line  \nid=2 bbb\n"
	const wantErr = `snappr: warning: failed extract timestamp from "plain line  " using regexp "^id=([0-9]+)"` + "\n"
	args := []string{"-E", "-o", "-e", `^id=([0-9]+)`, "1@last"}

	scExpect(t, append([]string{"-v"}, args...), input, 0, "plain line  \nid=2\n", wantErr)
	scExpect(t, args, input, 0, "id=1\n", wantErr)
}
