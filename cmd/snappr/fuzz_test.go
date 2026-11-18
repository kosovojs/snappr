package main

import (
	"bufio"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"
)

// fzModes are flag combinations for FuzzMainNeverPrunesRetainedText. Each one
// is valid, so Main must succeed on any (size-bounded) stdin.
var fzModes = []struct {
	only bool // --only: output lines are substrings of input lines
	args []string
}{
	{false, []string{"2@last", "daily"}},
	{true, []string{"-E", "-o", "-e", `@(-?[0-9]+)`, "1@last", "3@secondly:1h"}},
	{false, []string{"-E", "-n", "-e", `^(\S*)\s+(-?[0-9]+)`, "2@last", "monthly"}},
	{false, []string{"-p", "2006-01-02 15:04", "3@daily", "yearly"}},
	{false, []string{"-z", "Europe/Riga", "daily:2", "1@last"}},
	{true, []string{"-E", "-n", "-o", "-e", `([a-z]*)@([0-9]*)`, "1@last", "2@daily"}},
	{false, []string{"-w", "-s", "-e", `ts=([0-9]+)`, "secondly:90m"}},
	{true, []string{"-p", time.RFC3339, "-Z", "Asia/Kolkata", "-z", "Pacific/Chatham", "-E", "-o", "-e", `\[(.+?)\]`, "2@monthly:3", "yearly"}},
}

// fzLines splits Main output into lines (every line is '\n'-terminated, and a
// line may be empty, e.g. an empty --only match).
func fzLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// fzInputLines returns the non-empty records Main reads from stdin.
func fzInputLines(stdin string) []string {
	var lines []string
	sc := bufio.NewScanner(strings.NewReader(stdin))
	for sc.Scan() {
		if sc.Text() != "" {
			lines = append(lines, sc.Text())
		}
	}
	return lines
}

func FuzzMainNeverPrunesRetainedText(f *testing.F) {
	for _, seed := range []struct {
		mode  byte
		stdin string
	}{
		{0, "100\n100 \n 100\n200\nbad\n\n86400\n-86400\n100\n"},
		{0, "9223372036854775807\n-9223372036854775808\n0\n0\r\n1\n"},
		{1, "tank/a@100\ntank/b@100\ntank/a@200\nopaque\n@-5\n@\nx@1@2\n"},
		{2, "alpha 100\nbeta 100\nalpha 200\n 300\nalpha bad\nbeta 100\n"},
		{3, "2024-01-01 00:00\n2024-01-01 12:00\n2024-01-02 00:00\n2024-01-02 00:00 \nnope\n"},
		{4, "1711846799\n1711846800\n1729990800\n1729994400\n1729994400\n"},
		{5, "@\n@\na@\nb@1\n@1\nzz@100 zz@200\n"},
		{6, "id=1 ts=100\nid=2 ts=100\nts=200\nts=\nts=200 \n"},
		{7, "[2024-03-31T00:00:00Z] a\n[2024-03-31T00:00:00+05:30] b\n[2024-04-30T23:59:59-09:00]\n[] c\n"},
	} {
		f.Add(seed.mode, seed.stdin)
	}
	f.Fuzz(func(t *testing.T, sel byte, stdin string) {
		if len(stdin) > 4096 {
			t.Skip()
		}
		mode := fzModes[int(sel)%len(fzModes)]
		args := append([]string{"snappr"}, mode.args...)
		keepArgs := append([]string{"snappr", "--invert"}, mode.args...)

		status, prune, stderr := runMain(args, stdin)
		if status != 0 {
			t.Fatalf("%q: status %d\nstderr: %s", args, status, stderr)
		}
		keepStatus, keep, _ := runMain(keepArgs, stdin)
		if keepStatus != 0 {
			t.Fatalf("%q: status %d", keepArgs, keepStatus)
		}

		if s, p, e := runMain(args, stdin); s != status || p != prune || e != stderr {
			t.Fatalf("%q: not deterministic", args)
		}

		kept := map[string]bool{}
		for _, line := range fzLines(keep) {
			kept[strings.TrimSpace(line)] = true
		}
		pruned := map[string]bool{}
		for _, line := range fzLines(prune) {
			key := strings.TrimSpace(line)
			if kept[key] {
				t.Fatalf("%q: %q is pruned but also kept\nprune=%q\nkeep=%q", args, line, prune, keep)
			}
			if pruned[key] {
				t.Fatalf("%q: %q pruned twice\nprune=%q", args, line, prune)
			}
			pruned[key] = true
		}

		input := fzInputLines(stdin)
		for _, out := range []string{prune, keep} {
			for _, line := range fzLines(out) {
				ok := slices.Contains(input, line)
				if mode.only && !ok {
					ok = slices.ContainsFunc(input, func(in string) bool { return strings.Contains(in, line) })
				}
				if !ok {
					t.Fatalf("%q: output line %q is not from the input\nstdin=%q", args, line, stdin)
				}
			}
		}

		// without --only, every input record's text ends up on one side (prune
		// prints each text once, so compare by key)
		if !mode.only {
			for _, line := range input {
				if key := strings.TrimSpace(line); !kept[key] && !pruned[key] {
					t.Fatalf("%q: input %q missing from both outputs\nprune=%q\nkeep=%q", args, line, prune, keep)
				}
			}
		}
	})
}

func FuzzMainArgs(f *testing.F) {
	for _, seed := range []string{
		"",
		"last",
		"--help",
		"-qh",
		"-- -1@daily",
		"-1@daily",
		"--invert 2@last daily",
		"-v -w -s 1@last",
		"-E -o -e @([0-9]+) 1@last",
		"-E -n -e ^(.*)@([0-9]+)$ 1@last",
		"-n last",
		"-e ( last",
		"-E -e (a)(b) last",
		"-p 2006-01-02 -Z Asia/Kolkata last",
		"-z Europe/Riga -z local daily",
		"-z  last",
		"--timezone= last",
		"-z ../../etc/passwd last",
		"-z",
		"--nope last",
		"-o -E -Z UTC last",
		"daily:0",
		"secondly:1h30m 9223372036854775807@yearly:9223372036854775807",
	} {
		f.Add(seed)
	}
	const stdin = "0\n100\n100 \n86400\nbad\nx@5\nx@6\n2024-01-02\n"
	inputs := fzInputLines(stdin)
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 512 {
			t.Skip()
		}
		args := []string{"snappr"}
		if s != "" {
			args = append(args, strings.Split(s, " ")...)
		}
		if len(args) > 17 {
			t.Skip()
		}

		status, stdout, stderr := runMain(args, stdin)
		// status 1 is reserved for I/O failures, which can't happen here
		if status != 0 && status != 2 {
			t.Fatalf("%q: status %d\nstderr: %s", args, status, stderr)
		}
		if status == 2 && stdout != "" {
			t.Fatalf("%q: configuration error but wrote stdout %q", args, stdout)
		}
		if status == 0 && !strings.HasPrefix(stdout, "usage: snappr ") {
			for _, line := range fzLines(stdout) {
				if !slices.ContainsFunc(inputs, func(in string) bool { return strings.Contains(in, line) }) {
					t.Fatalf("%q: output line %q is not from the input", args, line)
				}
			}
		}
		if s2, o2, e2 := runMain(args, stdin); s2 != status || o2 != stdout || e2 != stderr {
			t.Fatalf("%q: not deterministic", args)
		}
	})
}

func BenchmarkMain(b *testing.B) {
	var sb strings.Builder
	start := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	for i := int64(0); i < 100000; i++ {
		sb.WriteString("tank/data@auto-")
		sb.WriteString(strconv.FormatInt(start+i*3600+i*7%3600, 10))
		sb.WriteByte('\n')
	}
	stdin := sb.String()
	args := []string{"snappr", "-q", "-E", "-o", "-e", `@auto-([0-9]+)$`, "3@last", "24@secondly:1h", "7@daily", "12@monthly", "yearly"}
	b.SetBytes(int64(len(stdin)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if status := Main(args, strings.NewReader(stdin), io.Discard, io.Discard); status != 0 {
			b.Fatalf("status %d", status)
		}
	}
}
