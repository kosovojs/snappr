package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"testing"
)

func runMain(args []string, stdin string) (status int, stdout, stderr string) {
	var out, err bytes.Buffer
	status = Main(args, strings.NewReader(stdin), &out, &err)
	return status, out.String(), err.String()
}

func TestUnmatchedRecordsPassThroughOnlyOnKeepOutput(t *testing.T) {
	args := []string{"snappr", "-E", "-e", `^record ([0-9]+)$`, "1@last"}
	input := "record 100\nunmatched opaque record\nrecord 200\n"

	status, prune, _ := runMain(args, input)
	if status != 0 || prune != "record 100\n" {
		t.Fatalf("prune result: status=%d stdout=%q", status, prune)
	}

	args = append(args[:1], append([]string{"--invert"}, args[1:]...)...)
	status, keep, _ := runMain(args, input)
	if status != 0 || keep != "unmatched opaque record\nrecord 200\n" {
		t.Fatalf("keep result: status=%d stdout=%q", status, keep)
	}
}

func TestGroupByNameClassifiesIndependentlyInInputOrder(t *testing.T) {
	args := []string{"snappr", "-E", "--group-by-name", "-e", `^([^ ]+) ([0-9]+)$`, "1@last"}
	input := "alpha 100\nbeta 100\nalpha 200\nbeta 200\n"
	want := "alpha 100\nbeta 100\n"

	for i := 0; i < 20; i++ {
		status, got, _ := runMain(args, input)
		if status != 0 || got != want {
			t.Fatalf("run %d: status=%d, want %q, got %q", i, status, want, got)
		}
	}
}

func TestEmptyGroupNameIsIdentifiedInDiagnostics(t *testing.T) {
	args := []string{"snappr", "-E", "--group-by-name", "-e", "^(.*):([0-9]+)$", "--why", "--summarize", "1@last"}
	status, _, stderr := runMain(args, ":100\n:200\n")
	if status != 0 {
		t.Fatalf("unexpected status %d", status)
	}
	if got, want := strings.Count(stderr, `("")`), 3; got != want {
		t.Fatalf("empty group identity missing from diagnostics: want %d labels, got %d\n%s", want, got, stderr)
	}
}

func TestGroupedInvalidAndUnmatchedRecordsStayOnKeepSide(t *testing.T) {
	args := []string{"snappr", "-E", "--group-by-name", "-e", `^([^ ]+) ([^ ]+)$`, "1@last"}
	input := "alpha 100\nunmatched opaque record\nalpha bad\nbeta 100\nalpha 200\nbeta 200\n"

	status, prune, _ := runMain(args, input)
	if status != 0 || prune != "alpha 100\nbeta 100\n" {
		t.Fatalf("prune result: status=%d stdout=%q", status, prune)
	}

	keepArgs := append(args[:1:1], append([]string{"--invert"}, args[1:]...)...)
	status, keep, _ := runMain(keepArgs, input)
	want := "unmatched opaque record\nalpha bad\nalpha 200\nbeta 200\n"
	if status != 0 || keep != want {
		t.Fatalf("keep result: status=%d want=%q got=%q", status, want, keep)
	}
}

func TestExtractCaptureCountMatchesMode(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no capture", []string{"snappr", "-E", "-e", `[0-9]+`, "last"}},
		{"two captures without grouping", []string{"snappr", "-E", "-e", `(.+) ([0-9]+)`, "last"}},
		{"one capture with grouping", []string{"snappr", "-E", "-n", "-e", `([0-9]+)`, "last"}},
		{"grouping without extraction", []string{"snappr", "-n", "last"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, stdout, _ := runMain(tt.args, "record 100\n")
			if status != 2 || stdout != "" {
				t.Fatalf("want configuration failure with no output, got status=%d stdout=%q", status, stdout)
			}
		})
	}
}

func TestEmptyTimezoneIsAConfigurationError(t *testing.T) {
	for _, flag := range []string{"-z", "-Z"} {
		t.Run(flag, func(t *testing.T) {
			status, stdout, _ := runMain([]string{"snappr", flag, "", "last"}, "0\n")
			if status != 2 || stdout != "" {
				t.Fatalf("want configuration failure with no output, got status=%d stdout=%q", status, stdout)
			}
		})
	}
}

type failingWriter struct {
	writes int
}

func (w *failingWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, io.ErrShortWrite
}

func TestStdoutFailureReturnsNonzero(t *testing.T) {
	out := &failingWriter{}
	status := Main([]string{"snappr", "--invert", "last"}, strings.NewReader("1\n"), out, io.Discard)
	if status == 0 || out.writes == 0 {
		t.Fatalf("want attempted write and nonzero status, got status=%d writes=%d", status, out.writes)
	}
}

func TestHelpStdoutFailureReturnsNonzero(t *testing.T) {
	out := &failingWriter{}
	status := Main([]string{"snappr", "--help"}, strings.NewReader(""), out, io.Discard)
	if status == 0 || out.writes == 0 {
		t.Fatalf("want attempted help write and nonzero status, got status=%d writes=%d", status, out.writes)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("injected read failure")
}

func TestReadFailureProducesNoClassificationOutput(t *testing.T) {
	var stdout bytes.Buffer
	stdin := io.MultiReader(strings.NewReader("1\n"), errorReader{})
	status := Main([]string{"snappr", "last"}, stdin, &stdout, io.Discard)
	if status == 0 || stdout.Len() != 0 {
		t.Fatalf("want fail-closed read error, got status=%d stdout=%q", status, stdout.String())
	}
}

func TestValidZeroTimeIsNotTreatedAsParseFailure(t *testing.T) {
	args := []string{"snappr", "-p", "2006-01-02", "1@last"}
	status, stdout, _ := runMain(args, "0001-01-01\n0002-01-01\n")
	if status != 0 || stdout != "0001-01-01\n" {
		t.Fatalf("want the older valid record pruned, got status=%d stdout=%q", status, stdout)
	}
}

func TestUnrepresentableUnixSecondsStayOnTheSafeSide(t *testing.T) {
	const input = "0\n9223371974719179008\n"

	status, prune, _ := runMain([]string{"snappr", "1@last"}, input)
	if status != 0 || prune != "" {
		t.Fatalf("unrepresentable time entered prune output: status=%d stdout=%q", status, prune)
	}

	status, keep, _ := runMain([]string{"snappr", "--invert", "1@last"}, input)
	if status != 0 || keep != input {
		t.Fatalf("safe-side records changed: status=%d want=%q got=%q", status, input, keep)
	}
}

func TestRepresentableUnixSecondBoundariesAreClassified(t *testing.T) {
	const input = "-9223371974719179007\n0\n9223371974719179007\n"
	status, prune, _ := runMain([]string{"snappr", "1@last"}, input)
	if status != 0 || prune != "-9223371974719179007\n0\n" {
		t.Fatalf("representable boundary classification failed: status=%d stdout=%q", status, prune)
	}
}

func TestVeryNegativeUnixSecondsStayOnTheSafeSide(t *testing.T) {
	const input = "-9223372036854775808\n0\n"

	status, prune, _ := runMain([]string{"snappr", "1@last"}, input)
	if status != 0 || prune != "" {
		t.Fatalf("unrepresentable time entered prune output: status=%d stdout=%q", status, prune)
	}

	status, keep, _ := runMain([]string{"snappr", "--invert", "1@last"}, input)
	if status != 0 || keep != input {
		t.Fatalf("safe-side records changed: status=%d want=%q got=%q", status, input, keep)
	}
}

func TestOnlyProjectsMatchedInvalidRecords(t *testing.T) {
	const input = "record bad suffix\nunmatched opaque\nrecord 100 suffix\n"
	args := []string{"snappr", "-E", "--invert", "--only", "-e", `^record ([^ ]+)`, "1@last"}
	status, keep, _ := runMain(args, input)
	const want = "record bad\nunmatched opaque\nrecord 100\n"
	if status != 0 || keep != want {
		t.Fatalf("--only projection changed: status=%d want=%q got=%q", status, want, keep)
	}
}

func TestMainAcceptsAnEmptyArgumentSlice(t *testing.T) {
	status, stdout, _ := runMain(nil, "")
	if status != 2 || stdout != "" {
		t.Fatalf("want usage failure with no output, got status=%d stdout=%q", status, stdout)
	}
}

func TestOpaqueRecordsArePreserved(t *testing.T) {
	input := "id=\"α β\" ts=100\nid=*?[x] ts=200"
	args := []string{"snappr", "-E", "-e", `ts=([0-9]+)$`, "1@last"}
	status, stdout, _ := runMain(args, input)
	if status != 0 || stdout != "id=\"α β\" ts=100\n" {
		t.Fatalf("opaque record changed: status=%d stdout=%q", status, stdout)
	}

	args = append(args[:1], append([]string{"--only"}, args[1:]...)...)
	status, stdout, _ = runMain(args, input)
	if status != 0 || stdout != "ts=100\n" {
		t.Fatalf("--only did not emit the exact match: status=%d stdout=%q", status, stdout)
	}
}

func TestDiagnosticFlagsDoNotChangeClassification(t *testing.T) {
	input := "bad\n1\n2\n3\n"
	base := []string{"snappr", "2@last"}
	status, want, _ := runMain(base, input)
	if status != 0 {
		t.Fatalf("baseline status=%d", status)
	}
	for _, flags := range [][]string{{"-q"}, {"-w"}, {"-s"}, {"-q", "-w", "-s"}} {
		args := append([]string{"snappr"}, flags...)
		args = append(args, "2@last")
		status, got, _ := runMain(args, input)
		if status != 0 || got != want {
			t.Fatalf("flags %v changed classification: status=%d want=%q got=%q", flags, status, want, got)
		}
	}
}

func TestOverlongInputFailsBeforeProducingOutput(t *testing.T) {
	input := "1\n2\n" + strings.Repeat("x", bufio.MaxScanTokenSize+1) + "\n"
	status, stdout, _ := runMain([]string{"snappr", "1@last"}, input)
	if status == 0 || stdout != "" {
		t.Fatalf("want fail-closed oversized record, got status=%d stdout=%q", status, stdout)
	}
}

func FuzzMainPartitionsValidGroupedRecords(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{255, 0, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 50 {
			t.Skip()
		}
		var input strings.Builder
		want := make(map[string]int, len(data))
		for i, value := range data {
			line := fmt.Sprintf("group-%d opaque-%d %d", value%4, i, int8(value))
			fmt.Fprintln(&input, line)
			want[line]++
		}
		args := []string{"snappr", "-qEn", "-e", `^(group-[0-9]+) .* (-?[0-9]+)$`, "3@last", "2@daily"}
		status, prune, _ := runMain(args, input.String())
		if status != 0 {
			t.Fatalf("prune status=%d", status)
		}
		keepArgs := append(args[:1:1], append([]string{"--invert"}, args[1:]...)...)
		status, keep, _ := runMain(keepArgs, input.String())
		if status != 0 {
			t.Fatalf("keep status=%d", status)
		}
		got := make(map[string]int, len(data))
		for _, output := range []string{prune, keep} {
			for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
				if line != "" {
					got[line]++
				}
			}
		}
		if !maps.Equal(got, want) {
			t.Fatalf("outputs did not partition inputs:\nwant=%v\n got=%v\nprune=%q\nkeep=%q", want, got, prune, keep)
		}
		againStatus, againPrune, _ := runMain(args, input.String())
		if againStatus != status || againPrune != prune {
			t.Fatal("grouped output is not deterministic")
		}
	})
}

func TestIdenticalTextNeverOnBothSides(t *testing.T) {
	args := []string{"snappr", "daily"}
	input := "100\n100\n200\n"

	status, prune, stderr := runMain(args, input)
	if status != 0 || prune != "200\n" {
		t.Fatalf("prune result: status=%d stdout=%q", status, prune)
	}
	if !strings.Contains(stderr, `snappr: warning: not pruning "100": identical to a retained record`) {
		t.Fatalf("missing warning: stderr=%q", stderr)
	}

	status, keep, _ := runMain([]string{"snappr", "--invert", "daily"}, input)
	if status != 0 || keep != "100\n100\n" {
		t.Fatalf("keep result: status=%d stdout=%q", status, keep)
	}
}

func TestOnlyMatchCollisionIsNotPruned(t *testing.T) {
	args := []string{"snappr", "-E", "-o", "-e", `@([0-9]+)$`, "daily"}
	input := "tank/a@1700000000\ntank/b@1700000000\n"

	status, prune, _ := runMain(args, input)
	if status != 0 || prune != "" {
		t.Fatalf("prune result: status=%d stdout=%q", status, prune)
	}

	args = append(args[:1:1], append([]string{"--invert"}, args[1:]...)...)
	status, keep, _ := runMain(args, input)
	if status != 0 || keep != "@1700000000\n@1700000000\n" {
		t.Fatalf("keep result: status=%d stdout=%q", status, keep)
	}
}

func TestIdenticalTextWarningIsEmittedOncePerText(t *testing.T) {
	const input = "100\n100\n100\n200\n"

	_, _, stderr := runMain([]string{"snappr", "-w", "-s", "daily"}, input)
	for _, want := range []string{":: identical to a retained record", "pruning 1/4 snapshots"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("missing %q: stderr=%q", want, stderr)
		}
	}
	if got := strings.Count(stderr, "not pruning"); got != 1 {
		t.Fatalf("want 1 warning, got %d: stderr=%q", got, stderr)
	}

	_, _, stderr = runMain([]string{"snappr", "-q", "-w", "-s", "daily"}, input)
	if strings.Contains(stderr, "not pruning") {
		t.Fatalf("--quiet did not suppress warning: stderr=%q", stderr)
	}
}

func TestWhitespaceVariantsNeverOnOppositeSides(t *testing.T) {
	const input = "100\n100 \n200\n"
	_, prune, _ := runMain([]string{"snappr", "daily"}, input)
	_, keep, _ := runMain([]string{"snappr", "--invert", "daily"}, input)

	kept := make(map[string]bool)
	for _, line := range strings.Split(keep, "\n") {
		kept[strings.TrimSpace(line)] = true
	}
	pruned := make(map[string]bool)
	for _, line := range strings.Split(prune, "\n") {
		key := strings.TrimSpace(line)
		if line != "" && (kept[key] || pruned[key]) {
			t.Fatalf("%q is in both outputs or duplicated:\nprune=%q\nkeep=%q", line, prune, keep)
		}
		pruned[key] = true
	}
}

func TestPruneOutputIsDeduplicated(t *testing.T) {
	args := []string{"snappr", "-E", "-o", "-e", `^x ([0-9]+)$`, "1@last"}
	status, prune, _ := runMain(args, "x 100\nx 100\nx 200\n")
	if status != 0 || prune != "x 100\n" {
		t.Fatalf("prune result: status=%d stdout=%q", status, prune)
	}
}

func TestPruneAndKeepOutputsNeverShareText(t *testing.T) {
	for _, tc := range []struct {
		args  []string
		input string
		texts []string // distinct output texts expected (after --only projection)
	}{
		{[]string{"snappr", "1@last"}, "100\n100\n100\n", []string{"100"}},
		{[]string{"snappr", "daily"}, "100\n200\n100\n86400\n100\n", []string{"100", "200", "86400"}},
		{[]string{"snappr", "2@secondly"}, "1\n2\n3\n1\n3\nbad\n", []string{"1", "2", "3", "bad"}},
		{[]string{"snappr", "-E", "-o", "-e", `@([0-9]+)$`, "1@last"}, "a@5\nb@5\nc@6\nd@4\n", []string{"@5", "@6", "@4"}},
		{[]string{"snappr", "-E", "-n", "-o", "-e", `^([a-z]+)@([0-9]+)$`, "1@last"}, "a@1\nb@1\na@2\nb@1\n", []string{"a@1", "b@1", "a@2"}},
		{[]string{"snappr", "-E", "-n", "-e", `^([a-z]+) .*@([0-9]+)$`, "1@last"}, "a x@1\nb x@1\na x@2\nb x@1\n", []string{"a x@1", "b x@1", "a x@2"}},
	} {
		status, prune, _ := runMain(tc.args, tc.input)
		if status != 0 {
			t.Fatalf("%v: prune status=%d", tc.args, status)
		}
		keepArgs := append(tc.args[:1:1], append([]string{"--invert"}, tc.args[1:]...)...)
		status, keep, _ := runMain(keepArgs, tc.input)
		if status != 0 {
			t.Fatalf("%v: keep status=%d", tc.args, status)
		}
		kept := make(map[string]bool)
		for _, line := range strings.Split(keep, "\n") {
			kept[line] = line != ""
		}
		union := maps.Clone(kept)
		pruned := make(map[string]bool)
		for _, line := range strings.Split(prune, "\n") {
			if kept[line] {
				t.Fatalf("%v: %q is in both outputs:\nprune=%q\nkeep=%q", tc.args, line, prune, keep)
			}
			if line != "" {
				if pruned[line] {
					t.Fatalf("%v: %q is duplicated in prune output: %q", tc.args, line, prune)
				}
				pruned[line] = true
				union[line] = true
			}
		}
		delete(union, "")
		want := make(map[string]bool)
		for _, text := range tc.texts {
			want[text] = true
		}
		if !maps.Equal(union, want) {
			t.Fatalf("%v: distinct output texts differ:\nwant=%v\n got=%v", tc.args, want, union)
		}
	}
}

func TestIgnoredFlagWarning(t *testing.T) {
	const want = "--only has no effect"
	_, _, stderr := runMain([]string{"snappr", "-o", "last"}, "0\n")
	if !strings.Contains(stderr, want) {
		t.Fatalf("missing %q: stderr=%q", want, stderr)
	}
	_, _, stderr = runMain([]string{"snappr", "-q", "-o", "last"}, "0\n")
	if strings.Contains(stderr, want) {
		t.Fatalf("--quiet did not suppress warning: stderr=%q", stderr)
	}
	_, _, stderr = runMain([]string{"snappr", "-e", "(.*)", "-o", "last"}, "0\n")
	if strings.Contains(stderr, want) {
		t.Fatalf("unexpected warning: stderr=%q", stderr)
	}
}
