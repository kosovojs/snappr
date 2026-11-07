package snappr

import (
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // make zone lookups independent of the host
)

func scnPolicy(t *testing.T, rules string) Policy {
	t.Helper()
	p, err := ParsePolicy(strings.Fields(rules)...)
	if err != nil {
		t.Fatalf("ParsePolicy(%q): %v", rules, err)
	}
	return p
}

func scnLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Skipf("load location %q: %v", name, err)
	}
	return loc
}

// scnKept returns the indexes of snapshots with at least one reason.
func scnKept(keep [][]Period) []int {
	var got []int
	for i, reasons := range keep {
		if len(reasons) != 0 {
			got = append(got, i)
		}
	}
	return got
}

func TestScenarioTimezoneDecidesCalendarDay(t *testing.T) {
	// a/b straddle UTC midnight; c/d straddle New York midnight. New York is
	// on EDT (UTC-4) here: US DST began 2024-03-10 02:00 EST = 07:00 UTC.
	// Riga is on EET (UTC+2) until 2024-03-31.
	a := time.Date(2024, 3, 10, 23, 30, 0, 0, time.UTC) // Riga 03-11 01:30, NY 03-10 19:30
	b := time.Date(2024, 3, 11, 0, 30, 0, 0, time.UTC)  // Riga 03-11 02:30, NY 03-10 20:30
	c := time.Date(2024, 3, 11, 3, 30, 0, 0, time.UTC)  // Riga 03-11 05:30, NY 03-10 23:30
	d := time.Date(2024, 3, 11, 4, 30, 0, 0, time.UTC)  // Riga 03-11 06:30, NY 03-11 00:30

	for _, tt := range []struct {
		name      string
		loc       string
		snapshots []time.Time
		want      []int
	}{
		{"UTC splits a/b at UTC midnight", "UTC", []time.Time{a, b}, []int{0, 1}},
		{"Riga (+2) puts a/b both on Mar 11", "Europe/Riga", []time.Time{a, b}, []int{0}},
		{"New York (-4) puts a/b both on Mar 10", "America/New_York", []time.Time{a, b}, []int{0}},
		{"UTC puts c/d both on Mar 11", "UTC", []time.Time{c, d}, []int{0}},
		{"Riga puts c/d both on Mar 11", "Europe/Riga", []time.Time{c, d}, []int{0}},
		{"New York splits c/d at local midnight", "America/New_York", []time.Time{c, d}, []int{0, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			keep, _ := Prune(tt.snapshots, scnPolicy(t, "daily"), scnLoc(t, tt.loc))
			if got := scnKept(keep); !slices.Equal(got, tt.want) {
				t.Fatalf("want kept %v, got %v (%v)", tt.want, got, keep)
			}
		})
	}
}

func TestScenarioDailyAcrossRigaDST(t *testing.T) {
	riga := scnLoc(t, "Europe/Riga")
	utc := func(mo time.Month, d, h int) time.Time {
		return time.Date(2024, mo, d, h, 0, 0, 0, time.UTC)
	}

	// EU DST: 2024-03-31 01:00 UTC (EET +2 -> EEST +3, 23h local day) and
	// 2024-10-27 01:00 UTC (EEST +3 -> EET +2, 25h local day). 72 hourly
	// snapshots from `from`; kept = first snapshot of each local day, which is
	// the first one in range, then each local midnight converted to UTC.
	for _, tt := range []struct {
		name     string
		from     time.Time
		want     []time.Time
		dayLen   time.Duration // gap between kept[1] and kept[2] (the DST day)
		dstCount int           // snapshots falling on the DST day
	}{
		{
			name: "spring forward 2024-03-31 is 23h",
			from: utc(3, 30, 0), // Mar 30 02:00 EET
			want: []time.Time{
				utc(3, 30, 0),  // first in range (Mar 30 02:00 local)
				utc(3, 30, 22), // Mar 31 00:00 EET (+2)
				utc(3, 31, 21), // Apr 1 00:00 EEST (+3)
				utc(4, 1, 21),  // Apr 2 00:00 EEST
			},
			dayLen:   23 * time.Hour,
			dstCount: 23,
		},
		{
			name: "fall back 2024-10-27 is 25h",
			from: utc(10, 26, 0), // Oct 26 03:00 EEST
			want: []time.Time{
				utc(10, 26, 0),  // first in range (Oct 26 03:00 local)
				utc(10, 26, 21), // Oct 27 00:00 EEST (+3)
				utc(10, 27, 22), // Oct 28 00:00 EET (+2)
				utc(10, 28, 22), // Oct 29 00:00 EET
			},
			dayLen:   25 * time.Hour,
			dstCount: 25,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			snapshots := make([]time.Time, 72)
			for i := range snapshots {
				snapshots[i] = tt.from.Add(time.Duration(i) * time.Hour)
			}
			keep, need := Prune(snapshots, scnPolicy(t, "daily"), riga)

			var got []time.Time
			for _, i := range scnKept(keep) {
				got = append(got, snapshots[i])
			}
			if !slices.EqualFunc(got, tt.want, time.Time.Equal) {
				t.Fatalf("want kept %v, got %v", tt.want, got)
			}
			if gap := got[2].Sub(got[1]); gap != tt.dayLen {
				t.Errorf("DST day length: want %s, got %s", tt.dayLen, gap)
			}
			dstDay := tt.want[1].In(riga).Format(time.DateOnly)
			n := 0
			for _, s := range snapshots {
				if s.In(riga).Format(time.DateOnly) == dstDay {
					n++
				}
			}
			if n != tt.dstCount {
				t.Errorf("hourly snapshots on %s: want %d, got %d", dstDay, tt.dstCount, n)
			}
			if c := need.Get(Period{Unit: Daily, Interval: 1}); c != -1 {
				t.Errorf("need daily: want -1, got %d", c)
			}
		})
	}
}

func TestScenarioRigaFallBackRepeatedWallClock(t *testing.T) {
	riga := scnLoc(t, "Europe/Riga")
	s0 := time.Date(2024, 10, 26, 23, 30, 0, 0, time.UTC) // Oct 27 02:30 EEST
	s1 := time.Date(2024, 10, 27, 0, 30, 0, 0, time.UTC)  // Oct 27 03:30 EEST
	s2 := time.Date(2024, 10, 27, 1, 30, 0, 0, time.UTC)  // Oct 27 03:30 EET (repeated hour)

	const wall = "2006-01-02 15:04"
	if w1, w2 := s1.In(riga).Format(wall), s2.In(riga).Format(wall); w1 != "2024-10-27 03:30" || w1 != w2 {
		t.Fatalf("precondition: expected identical wall clocks, got %q and %q", w1, w2)
	}

	// Deliberately not chronological: index 0 is the newest instant.
	snapshots := []time.Time{s2, s0, s1}

	for _, tt := range []struct {
		policy   string
		want     []int
		wantNeed int
	}{
		{"1@last", []int{0}, 0},    // s2 is the newest instant despite equal wall clock
		{"2@last", []int{0, 2}, 0}, // s2, s1
		{"3@last", []int{0, 1, 2}, 0},
		{"4@last", []int{0, 1, 2}, 1},       // repeated hour counted as two snapshots
		{"daily", []int{1}, -1},             // s0 is the first on Oct 27
		{"secondly:1h", []int{0, 1, 2}, -1}, // UTC hours 23, 0, 1
	} {
		t.Run(tt.policy, func(t *testing.T) {
			policy := scnPolicy(t, tt.policy)
			keep, need := Prune(snapshots, policy, riga)
			if got := scnKept(keep); !slices.Equal(got, tt.want) {
				t.Fatalf("want kept %v, got %v", tt.want, got)
			}
			policy.Each(func(period Period, _ int) {
				if c := need.Get(period); c != tt.wantNeed {
					t.Errorf("need %s: want %d, got %d", period, tt.wantNeed, c)
				}
			})
		})
	}
}

func TestScenarioSecondlyIgnoresTimezone(t *testing.T) {
	kolkata := scnLoc(t, "Asia/Kolkata") // +05:30, no DST

	t.Run("secondly:1h splits at :30 local", func(t *testing.T) {
		at := func(h, m int) time.Time { return time.Date(2024, 6, 1, h, m, 0, 0, kolkata) }
		snapshots := []time.Time{
			at(10, 0), at(10, 29), // 04:30, 04:59 UTC
			at(10, 30), at(10, 59), at(11, 0), at(11, 29), // 05:00..05:59 UTC
			at(11, 30), // 06:00 UTC
		}
		want := []int{0, 2, 6}
		for _, loc := range []*time.Location{kolkata, time.UTC} {
			keep, _ := Prune(snapshots, scnPolicy(t, "secondly:1h"), loc)
			if got := scnKept(keep); !slices.Equal(got, want) {
				t.Errorf("loc %s: want kept %v, got %v", loc, want, got)
			}
		}
	})

	t.Run("secondly:24h differs from daily outside UTC", func(t *testing.T) {
		snapshots := []time.Time{
			time.Date(2024, 6, 1, 4, 0, 0, 0, kolkata),  // 05-31 22:30 UTC
			time.Date(2024, 6, 1, 6, 0, 0, 0, kolkata),  // 06-01 00:30 UTC
			time.Date(2024, 6, 1, 23, 0, 0, 0, kolkata), // 06-01 17:30 UTC
			time.Date(2024, 6, 2, 1, 0, 0, 0, kolkata),  // 06-01 19:30 UTC
		}
		for _, tt := range []struct {
			policy string
			loc    *time.Location
			want   []int
		}{
			{"daily", kolkata, []int{0, 3}},        // local days 06-01 x3, 06-02
			{"secondly:24h", kolkata, []int{0, 1}}, // UTC days 05-31, 06-01 x3
			{"daily", time.UTC, []int{0, 1}},       // UTC daily == unix-day buckets
			{"secondly:24h", time.UTC, []int{0, 1}},
		} {
			keep, _ := Prune(snapshots, scnPolicy(t, tt.policy), tt.loc)
			if got := scnKept(keep); !slices.Equal(got, tt.want) {
				t.Errorf("%s in %s: want kept %v, got %v", tt.policy, tt.loc, tt.want, got)
			}
		}
	})
}

func TestScenarioCalendarIntervalBoundaries(t *testing.T) {
	ymd := func(y int, mo time.Month, d int) time.Time { return time.Date(y, mo, d, 0, 0, 0, 0, time.UTC) }
	last := func(y int, mo time.Month, d int) time.Time {
		return time.Date(y, mo, d, 23, 59, 59, 0, time.UTC)
	}
	for _, tt := range []struct {
		name      string
		policy    string
		snapshots []time.Time
		want      []int
	}{
		{
			// buckets 2019/10=201, 2020..2029 -> 202, 2030 -> 203
			name:      "yearly:10 decades are 2020-2029",
			policy:    "yearly:10",
			snapshots: []time.Time{last(2019, 12, 31), ymd(2020, 1, 1), ymd(2025, 6, 1), last(2029, 12, 31), ymd(2030, 1, 1)},
			want:      []int{0, 1, 4},
		},
		{
			// month index y*12+m-1: Jan 2024=24288 (/6=4048), Jun=24293 (4048),
			// Jul=24294 (4049), Dec=24299 (4049), Jan 2025=24300 (4050)
			name:      "monthly:6 halves are Jan-Jun and Jul-Dec",
			policy:    "monthly:6",
			snapshots: []time.Time{ymd(2024, 1, 1), last(2024, 6, 30), ymd(2024, 7, 1), last(2024, 12, 31), ymd(2025, 1, 1)},
			want:      []int{0, 2, 4},
		},
		{
			// floorDiv: -2->-1, -1->-1, 0->0, 1->0, 2->1. Truncation would put
			// -1 with 0 and 1 (want {0,1,4}).
			name:      "yearly:2 floors negative years",
			policy:    "yearly:2",
			snapshots: []time.Time{ymd(-2, 1, 1), ymd(-1, 1, 1), ymd(0, 1, 1), ymd(1, 1, 1), ymd(2, 1, 1)},
			want:      []int{0, 2, 4},
		},
		{
			// month index: Dec -2 = -13 (/12 -> -2), Jan -1 = -12 (-1),
			// Dec -1 = -1 (-1), Jan 0 = 0 (0), Dec 0 = 11 (0), Jan 1 = 12 (1).
			// Truncation would give -1,-1,0,0,0,1 (want {0,2,5}).
			name:      "monthly:12 floors negative years",
			policy:    "monthly:12",
			snapshots: []time.Time{ymd(-2, 12, 1), ymd(-1, 1, 1), ymd(-1, 12, 1), ymd(0, 1, 1), ymd(0, 12, 1), ymd(1, 1, 1)},
			want:      []int{0, 1, 3, 5},
		},
		{
			name:      "yearly matches monthly:12 for negative years",
			policy:    "yearly",
			snapshots: []time.Time{ymd(-2, 12, 1), ymd(-1, 1, 1), ymd(-1, 12, 1), ymd(0, 1, 1), ymd(0, 12, 1), ymd(1, 1, 1)},
			want:      []int{0, 1, 3, 5},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			keep, _ := Prune(tt.snapshots, scnPolicy(t, tt.policy), time.UTC)
			if got := scnKept(keep); !slices.Equal(got, tt.want) {
				t.Fatalf("want kept %v, got %v", tt.want, got)
			}
		})
	}
}

func TestScenarioExactNeed(t *testing.T) {
	day := func(d, h int) time.Time { return time.Date(2024, 1, d, h, 0, 0, 0, time.UTC) }
	var (
		pLast    = Period{Unit: Last, Interval: 1}
		pDaily   = Period{Unit: Daily, Interval: 1}
		pMonthly = Period{Unit: Monthly, Interval: 1}
		pYearly  = Period{Unit: Yearly, Interval: 1}
	)

	t.Run("5@daily with 3 distinct days needs 2", func(t *testing.T) {
		_, need := Prune([]time.Time{day(1, 0), day(1, 12), day(2, 0), day(3, 0)}, scnPolicy(t, "5@daily"), time.UTC)
		if c := need.Get(pDaily); c != 2 {
			t.Fatalf("want 2, got %d", c)
		}
		if b, _ := need.MarshalText(); string(b) != "2@daily" {
			t.Fatalf("want %q, got %q", "2@daily", b)
		}
	})

	t.Run("unlimited daily stays -1", func(t *testing.T) {
		_, need := Prune([]time.Time{day(1, 0), day(2, 0)}, scnPolicy(t, "-1@daily"), time.UTC)
		if c := need.Get(pDaily); c != -1 {
			t.Fatalf("want -1, got %d", c)
		}
		if b, _ := need.MarshalText(); string(b) != "daily" {
			t.Fatalf("want %q, got %q", "daily", b)
		}
	})

	t.Run("mixed policy", func(t *testing.T) {
		// s0 Jan 1 00:00, s1 Jan 1 12:00, s2 Jan 2 00:00.
		//   1@last:    candidates s0,s1,s2 -> keep s2, need 0
		//   3@daily:   candidates s0,s2    -> keep both, need 1
		//   2@monthly: candidates s0       -> keep s0, need 1
		//   1@yearly:  candidates s0       -> keep s0, need 0
		snapshots := []time.Time{day(1, 0), day(1, 12), day(2, 0)}
		keep, need := Prune(snapshots, scnPolicy(t, "1@last 3@daily 2@monthly 1@yearly"), time.UTC)
		for period, want := range map[Period]int{pLast: 0, pDaily: 1, pMonthly: 1, pYearly: 0} {
			if c := need.Get(period); c != want {
				t.Errorf("need %s: want %d, got %d", period, want, c)
			}
		}
		if b, _ := need.MarshalText(); string(b) != "1@daily 1@monthly" {
			t.Errorf("need text: want %q, got %q", "1@daily 1@monthly", b)
		}
		if s := need.String(); s != "last (0), 1 day (1), 1 month (1), 1 year (0)" {
			t.Errorf("need string: got %q", s)
		}
		wantKeep := [][]Period{{pDaily, pMonthly, pYearly}, nil, {pLast, pDaily}}
		if !reflect.DeepEqual(keep, wantKeep) {
			t.Errorf("keep:\nwant: %v\n got: %v", wantKeep, keep)
		}
	})

	t.Run("no snapshots needs the whole policy", func(t *testing.T) {
		policy := scnPolicy(t, "1@last 3@daily monthly 2@secondly:1h")
		for _, snapshots := range [][]time.Time{nil, {}} {
			keep, need := Prune(snapshots, policy, time.UTC)
			if keep == nil || len(keep) != 0 {
				t.Errorf("keep: want empty non-nil, got %#v", keep)
			}
			if !reflect.DeepEqual(need.count, policy.count) {
				t.Errorf("need: want %v, got %v", policy.count, need.count)
			}
			want, _ := policy.MarshalText()
			if got, _ := need.MarshalText(); string(got) != string(want) {
				t.Errorf("need text: want %q, got %q", want, got)
			}
			need.Set(pLast, 7)
			if c := policy.Get(pLast); c != 1 {
				t.Errorf("need shares state with policy: policy last = %d", c)
			}
		}
	})
}

func TestScenarioOrderIndependence(t *testing.T) {
	riga := scnLoc(t, "Europe/Riga")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshots := make([]time.Time, 50)
	for i := range snapshots {
		// irregular step, no duplicate instants, spans Jan 1 .. mid-Mar
		snapshots[i] = base.Add(time.Duration(i) * (37*time.Hour + 11*time.Minute))
	}
	policy := scnPolicy(t, "3@last 4@secondly:6h 5@daily daily:3 monthly 1@yearly")

	byInstant := func(snaps []time.Time, keep [][]Period) map[int64][]Period {
		m := make(map[int64][]Period, len(snaps))
		for i, s := range snaps {
			m[s.UnixNano()] = keep[i]
		}
		return m
	}

	keep, need := Prune(snapshots, policy, riga)
	want := byInstant(snapshots, keep)
	if n := len(scnKept(keep)); n == 0 || n == len(snapshots) {
		t.Fatalf("degenerate fixture: %d of %d kept", n, len(snapshots))
	}

	reversed := slices.Clone(snapshots)
	slices.Reverse(reversed)
	shuffled := slices.Clone(snapshots)
	rand.New(rand.NewSource(1)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	if slices.Equal(shuffled, snapshots) {
		t.Fatal("shuffle did not change order")
	}

	for name, snaps := range map[string][]time.Time{"reversed": reversed, "shuffled": shuffled} {
		t.Run(name, func(t *testing.T) {
			gotKeep, gotNeed := Prune(snaps, policy, riga)
			if got := byInstant(snaps, gotKeep); !reflect.DeepEqual(got, want) {
				t.Errorf("reasons per instant differ:\nwant: %v\n got: %v", want, got)
			}
			if !reflect.DeepEqual(gotNeed.count, need.count) {
				t.Errorf("need differs: want %v, got %v", need.count, gotNeed.count)
			}
		})
	}
}

func TestScenarioPruneDoesNotMutatePolicy(t *testing.T) {
	policy := scnPolicy(t, "1@last 2@daily monthly")
	beforeText, _ := policy.MarshalText()
	beforeString := policy.String()
	beforeCount := maps.Clone(policy.count)

	// 3 days: last and daily are fully satisfied (need hits 0), monthly is -1.
	snapshots := []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
	}
	_, need := Prune(snapshots, policy, time.UTC)

	if b, _ := need.MarshalText(); string(b) != "monthly" {
		t.Fatalf("need text: want %q, got %q", "monthly", b)
	}
	if afterText, _ := policy.MarshalText(); string(afterText) != string(beforeText) {
		t.Errorf("MarshalText changed: %q -> %q", beforeText, afterText)
	}
	if s := policy.String(); s != beforeString {
		t.Errorf("String changed: %q -> %q", beforeString, s)
	}
	for period, want := range map[Period]int{
		{Unit: Last, Interval: 1}:    1,
		{Unit: Daily, Interval: 1}:   2,
		{Unit: Monthly, Interval: 1}: -1,
	} {
		if c := policy.Get(period); c != want {
			t.Errorf("policy %s: want %d, got %d", period, want, c)
		}
	}
	if !reflect.DeepEqual(policy.count, beforeCount) {
		t.Errorf("policy map changed: %v -> %v", beforeCount, policy.count)
	}
}

func TestScenarioPeriodString(t *testing.T) {
	for _, tt := range []struct {
		period Period
		want   string
	}{
		{Period{Last, 1}, "last"},
		{Period{Last, 0}, "last"}, // interval normalized to 1
		{Period{Last, 5}, "last"},
		{Period{Secondly, 1}, "1s time"},
		{Period{Secondly, 60}, "1m time"}, // "1m0s" -> "1m"
		{Period{Secondly, 90}, "1m30s time"},
		{Period{Secondly, 3600}, "1h time"}, // "1h0m0s" -> "1h0m" -> "1h"
		{Period{Secondly, 3601}, "1h0m1s time"},
		{Period{Secondly, 3660}, "1h1m time"}, // "1h1m0s" -> "1h1m"
		{Period{Secondly, 86400}, "24h time"},
		{Period{Daily, 1}, "1 day"},
		{Period{Daily, 7}, "7 day"},
		{Period{Monthly, 2}, "2 month"},
		{Period{Yearly, 1}, "1 year"},
		{Period{Unit(99), 1}, ""},
		{Period{Unit(-1), 1}, ""},
		{Period{Daily, 0}, ""},
		{Period{Daily, -1}, ""},
		{Period{Secondly, 0}, ""},
	} {
		t.Run(fmt.Sprintf("%d:%d", int(tt.period.Unit), tt.period.Interval), func(t *testing.T) {
			if got := tt.period.String(); got != tt.want {
				t.Fatalf("%#v: want %q, got %q", tt.period, tt.want, got)
			}
		})
	}
}

func TestScenarioMultipleReasonsSorted(t *testing.T) {
	// Rules given out of order; reasons must come back in Period.Compare order.
	policy := scnPolicy(t, "yearly daily:7 1@last secondly:1h monthly daily secondly:60")
	snapshots := []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC), // same hour/day/month/year, new minute
	}
	keep, _ := Prune(snapshots, policy, time.UTC)
	want := [][]Period{
		{{Secondly, 60}, {Secondly, 3600}, {Daily, 1}, {Daily, 7}, {Monthly, 1}, {Yearly, 1}},
		{{Last, 1}, {Secondly, 60}},
	}
	if !reflect.DeepEqual(keep, want) {
		t.Fatalf("\nwant: %v\n got: %v", want, keep)
	}
}

func TestScenarioMonotonicAndLocationIrrelevant(t *testing.T) {
	riga := scnLoc(t, "Europe/Riga")
	ny := scnLoc(t, "America/New_York")
	kolkata := scnLoc(t, "Asia/Kolkata")

	now := time.Now()
	if !strings.Contains(now.String(), " m=") {
		t.Skip("no monotonic clock reading on this platform")
	}

	t.Run("variants of the same instants", func(t *testing.T) {
		locs := []*time.Location{time.UTC, riga, ny, kolkata, time.Local}
		var mono, stripped, utc, mixed []time.Time
		for i := 0; i < 40; i++ {
			s := now.Add(-time.Duration(i) * 7 * time.Hour) // keeps monotonic reading
			mono = append(mono, s)
			stripped = append(stripped, s.Round(0))
			utc = append(utc, s.UTC())
			mixed = append(mixed, s.In(locs[i%len(locs)]))
		}
		policy := scnPolicy(t, "2@last secondly:6h 5@daily monthly yearly")
		for _, loc := range []*time.Location{time.UTC, riga, kolkata} {
			want, wantNeed := Prune(stripped, policy, loc)
			for name, snaps := range map[string][]time.Time{"monotonic": mono, "utc": utc, "mixed": mixed} {
				got, gotNeed := Prune(snaps, policy, loc)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("%s in %s: keep differs:\nwant: %v\n got: %v", name, loc, want, got)
				}
				if !reflect.DeepEqual(gotNeed.count, wantNeed.count) {
					t.Errorf("%s in %s: need differs: want %v, got %v", name, loc, wantNeed.count, gotNeed.count)
				}
			}
		}
	})

	t.Run("equal instants in different locations tie by input order", func(t *testing.T) {
		at := time.Date(2024, 5, 5, 12, 0, 0, 0, time.UTC)
		snaps := []time.Time{at.In(ny), at, at.In(kolkata)}
		keep, _ := Prune(snaps, scnPolicy(t, "1@last daily"), riga)
		want := [][]Period{{{Daily, 1}}, nil, {{Last, 1}}}
		if !reflect.DeepEqual(keep, want) {
			t.Fatalf("\nwant: %v\n got: %v", want, keep)
		}
	})

	t.Run("monotonic and stripped copy of one instant tie by input order", func(t *testing.T) {
		keep, _ := Prune([]time.Time{now, now.Round(0)}, scnPolicy(t, "1@last"), time.UTC)
		want := [][]Period{nil, {{Last, 1}}}
		if !reflect.DeepEqual(keep, want) {
			t.Fatalf("\nwant: %v\n got: %v", want, keep)
		}
	})
}

func TestScenarioCountVersusCandidates(t *testing.T) {
	// 4 snapshots, 3 distinct days: daily candidates are 0, 2, 3.
	snapshots := []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
	}
	for _, tt := range []struct {
		name     string
		policy   string
		period   Period
		want     []int
		wantNeed int
	}{
		{"daily count above candidates keeps all", "10@daily", Period{Daily, 1}, []int{0, 2, 3}, 7},
		{"daily count equal to candidates keeps all", "3@daily", Period{Daily, 1}, []int{0, 2, 3}, 0},
		{"daily count below candidates keeps newest", "2@daily", Period{Daily, 1}, []int{2, 3}, 0},
		{"daily unlimited keeps all", "daily", Period{Daily, 1}, []int{0, 2, 3}, -1},
		{"last count equal to snapshots", "4@last", Period{Last, 1}, []int{0, 1, 2, 3}, 0},
		{"last count above snapshots", "5@last", Period{Last, 1}, []int{0, 1, 2, 3}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			keep, need := Prune(snapshots, scnPolicy(t, tt.policy), time.UTC)
			if got := scnKept(keep); !slices.Equal(got, tt.want) {
				t.Fatalf("want kept %v, got %v", tt.want, got)
			}
			if c := need.Get(tt.period); c != tt.wantNeed {
				t.Fatalf("need: want %d, got %d", tt.wantNeed, c)
			}
		})
	}
}
