package snappr

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPruneDailyKeepsDistinctCalendarDaysAcrossYears(t *testing.T) {
	snapshots := []time.Time{
		time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	policy, err := ParsePolicy("daily")
	if err != nil {
		t.Fatal(err)
	}

	keep, _ := Prune(snapshots, policy, time.UTC)
	want := [][]Period{
		{{Unit: Daily, Interval: 1}},
		{{Unit: Daily, Interval: 1}},
	}
	if !reflect.DeepEqual(keep, want) {
		t.Fatalf("daily retention crossed calendar years:\nwant: %#v\n got: %#v", want, keep)
	}
}

func TestPruneSecondlyUsesFloorBucketsBeforeUnixEpoch(t *testing.T) {
	snapshots := []time.Time{
		time.Unix(-1, 0),
		time.Unix(0, 0),
	}
	policy, err := ParsePolicy("secondly:2")
	if err != nil {
		t.Fatal(err)
	}

	keep, _ := Prune(snapshots, policy, time.UTC)
	want := [][]Period{
		{{Unit: Secondly, Interval: 2}},
		{{Unit: Secondly, Interval: 2}},
	}
	if !reflect.DeepEqual(keep, want) {
		t.Fatalf("secondly retention merged buckets across the Unix epoch:\nwant: %#v\n got: %#v", want, keep)
	}
}

func TestPruneNilLocationDefaultsToUTC(t *testing.T) {
	policy, err := ParsePolicy("daily")
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []time.Time{time.Unix(0, 0)}

	want, _ := Prune(snapshots, policy, time.UTC)
	got, _ := Prune(snapshots, policy, nil)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("nil location did not use UTC:\nwant: %#v\n got: %#v", want, got)
	}
}

func TestParsePolicyRejectsCountsOutsidePlatformIntRange(t *testing.T) {
	tooLarge := "9223372036854775808"
	if strconv.IntSize == 32 {
		tooLarge = "4294967296"
	}
	if _, err := ParsePolicy(tooLarge + "@last"); err == nil {
		t.Fatalf("ParsePolicy accepted count %s on a %d-bit platform", tooLarge, strconv.IntSize)
	}
}

func TestLargeSecondlyIntervalSurvivesTextRoundTrip(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Skip("interval is outside the 32-bit int range")
	}

	for _, tt := range []struct {
		rule        string
		wantEncoded string
		wantDisplay string
	}{
		{"2@secondly:9223372036", "2@secondly:2562047h47m16s", "2562047h47m16s time (2)"},
		{"2@secondly:9223372037", "2@secondly:9223372037", "9223372037s time (2)"},
		{"2@secondly:9223372036854775807", "2@secondly:9223372036854775807", "9223372036854775807s time (2)"},
	} {
		t.Run(tt.rule, func(t *testing.T) {
			policy, err := ParsePolicy(tt.rule)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := policy.MarshalText()
			if err != nil {
				t.Fatal(err)
			}
			if got := string(encoded); got != tt.wantEncoded {
				t.Fatalf("large interval was corrupted: want %q, got %q", tt.wantEncoded, got)
			}

			decoded, err := ParsePolicy(string(encoded))
			if err != nil {
				t.Fatalf("cannot parse marshaled policy %q: %v", encoded, err)
			}
			if !reflect.DeepEqual(decoded.count, policy.count) {
				t.Fatalf("round trip changed policy: want %v, got %v", policy.count, decoded.count)
			}
			if got := policy.String(); got != tt.wantDisplay {
				t.Fatalf("large interval display overflowed: want %q, got %q", tt.wantDisplay, got)
			}
		})
	}
}

func TestPruneSelectsTheExpectedRecordsForEveryUnit(t *testing.T) {
	snapshots := []time.Time{
		time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2024, time.January, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2024, time.February, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		policy string
		want   []int
	}{
		{"2@last", []int{3, 4}},
		{"daily", []int{0, 2, 3, 4}},
		{"2@daily", []int{3, 4}},
		{"monthly", []int{0, 3, 4}},
		{"2@monthly", []int{3, 4}},
		{"yearly", []int{0, 4}},
		{"1@yearly", []int{4}},
		{"1@last 2@daily", []int{3, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.policy, func(t *testing.T) {
			policy, err := ParsePolicy(strings.Fields(tt.policy)...)
			if err != nil {
				t.Fatal(err)
			}
			keep, _ := Prune(snapshots, policy, time.UTC)
			var got []int
			for i, reasons := range keep {
				if len(reasons) != 0 {
					got = append(got, i)
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("wrong retained records: want %v, got %v", tt.want, got)
			}
		})
	}
}

func TestPruneDailyIntervalStaysContinuousAcrossCentury(t *testing.T) {
	start := time.Date(2099, time.December, 1, 0, 0, 0, 0, time.UTC)
	var snapshots []time.Time
	for i := 0; i < 70; i++ {
		snapshots = append(snapshots, start.AddDate(0, 0, i))
	}
	policy, err := ParsePolicy("daily:7")
	if err != nil {
		t.Fatal(err)
	}
	keep, _ := Prune(snapshots, policy, time.UTC)
	var retained []time.Time
	for i, reasons := range keep {
		if len(reasons) != 0 {
			retained = append(retained, snapshots[i])
		}
	}
	for i := 2; i < len(retained); i++ {
		if gap := retained[i].Sub(retained[i-1]); gap != 7*24*time.Hour {
			t.Fatalf("daily:7 shifted at a calendar boundary: %s to %s is %s", retained[i-1], retained[i], gap)
		}
	}
}

func TestPruneUsesInputOrderToBreakEqualTimestampTies(t *testing.T) {
	at := time.Unix(100, 0)
	policy, err := ParsePolicy("1@last")
	if err != nil {
		t.Fatal(err)
	}
	keep, _ := Prune([]time.Time{at, at, at}, policy, time.UTC)
	want := [][]Period{nil, nil, {{Unit: Last, Interval: 1}}}
	if !reflect.DeepEqual(keep, want) {
		t.Fatalf("equal timestamp tie-break changed:\nwant: %#v\n got: %#v", want, keep)
	}
}

func FuzzParsePolicyRoundTrip(f *testing.F) {
	for _, seed := range []string{"last", "3@daily 2@monthly:6", "secondly:1h", "", "0@last", "daily daily"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		var policy Policy
		if err := policy.UnmarshalText([]byte(text)); err != nil {
			return
		}
		encoded, err := policy.MarshalText()
		if err != nil {
			t.Fatal(err)
		}
		var decoded Policy
		if err := decoded.UnmarshalText(encoded); err != nil {
			t.Fatalf("cannot parse canonical policy %q: %v", encoded, err)
		}
		if !reflect.DeepEqual(policy.count, decoded.count) {
			t.Fatalf("round trip changed policy %q", encoded)
		}
	})
}

func FuzzPruneOutputInvariants(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{255, 0, 255, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			t.Skip()
		}
		base := time.Unix(-32, 0)
		snapshots := make([]time.Time, len(data))
		for i, value := range data {
			snapshots[i] = base.Add(time.Duration(int8(value)) * time.Hour)
		}
		policy, err := ParsePolicy("3@last", "4@secondly:2h", "5@daily:7", "monthly")
		if err != nil {
			t.Fatal(err)
		}
		keep, need := Prune(snapshots, policy, time.UTC)
		if len(keep) != len(snapshots) {
			t.Fatalf("keep length %d != input length %d", len(keep), len(snapshots))
		}
		for _, reasons := range keep {
			if !slices.IsSortedFunc(reasons, Period.Compare) {
				t.Fatalf("reasons are not sorted: %v", reasons)
			}
			for i := 1; i < len(reasons); i++ {
				if reasons[i] == reasons[i-1] {
					t.Fatalf("duplicate reason: %v", reasons[i])
				}
			}
		}
		againKeep, againNeed := Prune(snapshots, policy, time.UTC)
		if !reflect.DeepEqual(keep, againKeep) || !reflect.DeepEqual(need.count, againNeed.count) {
			t.Fatal("Prune is not deterministic")
		}
	})
}

func ExamplePrune_classification() {
	policy, _ := ParsePolicy("1@last", "daily")
	snapshots := []time.Time{
		time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2024, time.January, 2, 0, 0, 0, 0, time.UTC),
	}
	keep, _ := Prune(snapshots, policy, time.UTC)
	for i, reasons := range keep {
		fmt.Printf("%d: keep=%t\n", i, len(reasons) != 0)
	}
	// Output:
	// 0: keep=true
	// 1: keep=false
	// 2: keep=true
}
