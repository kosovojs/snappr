package snappr

import (
	"bytes"
	"hash/fnv"
	"maps"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"
)

// fzLocations are the zones fuzzed Prune inputs are evaluated in. None of them
// has a transition where the local calendar date goes backwards, which the
// per-bucket invariants below rely on (calendar buckets are monotonic in time).
var fzLocations = func() []*time.Location {
	var locs []*time.Location
	for _, name := range []string{
		"UTC",
		"Europe/Riga",
		"America/New_York",
		"Asia/Kolkata",
		"Australia/Lord_Howe", // 30-minute DST
		"Pacific/Chatham",     // +12:45/+13:45
	} {
		loc, err := time.LoadLocation(name)
		if err != nil {
			panic(err)
		}
		locs = append(locs, loc)
	}
	return locs
}()

// fzAnchors are unix seconds near interesting calendar/zone boundaries.
var fzAnchors = []int64{
	0,
	-1,
	-62135596800,  // 0001-01-01
	-2208988800,   // 1900-01-01
	-2019705670,   // ~1906 Asia/Kolkata offset change
	-872058600,    // 1942-05-14 Asia/Kolkata offset change
	-764145000,    // 1945-10-14 17:30Z Asia/Kolkata +0630 -> IST (war-time end)
	951782400,     // 2000-02-29
	1711846800,    // 2024-03-31 01:00Z Riga spring forward
	1729990800,    // 2024-10-27 01:00Z Riga fall back
	1730613600,    // 2024-11-03 06:00Z New York fall back
	1712415600,    // 2024-04-06 15:00Z Lord Howe fall back
	1712412000,    // 2024-04-06 14:00Z Chatham fall back
	4107542400,    // 2100-03-01
	253402300799,  // 9999-12-31 23:59:59
	-377705116800, // ~year -10000
}

var fzBigSecondly = []int{60, 3600, 86400, 86400 * 7, 86400 * 30, 31556952, math.MaxInt32}

type fzBytes struct{ b []byte }

func (r *fzBytes) u8() byte {
	if len(r.b) == 0 {
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *fzBytes) u16() uint16 { return uint16(r.u8())<<8 | uint16(r.u8()) }

func (r *fzBytes) u64() uint64 {
	return uint64(r.u16())<<48 | uint64(r.u16())<<32 | uint64(r.u16())<<16 | uint64(r.u16())
}

// fzPruneInput deterministically decodes a policy, location, and up to
// maxSnapshots snapshot times from fuzz data. Missing bytes read as zero.
func fzPruneInput(data []byte, maxSnapshots int) (snapshots []time.Time, policy Policy, loc *time.Location) {
	r := &fzBytes{data}
	loc = fzLocations[int(r.u8())%len(fzLocations)]

	for i, n := 0, 1+int(r.u8()%6); i < n; i++ {
		unit := Unit(r.u8() % byte(numUnits))
		sel, iv := r.u8(), r.u16()
		interval := 1 + int(iv)%400
		if unit == Secondly && sel&1 != 0 {
			interval = fzBigSecondly[int(sel>>1)%len(fzBigSecondly)]
		}
		count := int(r.u8()%12) - 1 // -1 (infinite), 0 (removes), 1..10
		policy.Set(Period{Unit: unit, Interval: interval}, count)
	}

	n := int(r.u8()) % (maxSnapshots + 1)
	snapshots = make([]time.Time, 0, n)
	for i := 0; i < n; i++ {
		mode := r.u8()
		zone := fzLocations[int(mode>>4)%len(fzLocations)]
		scale := []int64{1, 60, 3600, 86400}[(mode>>2)%4]
		var t time.Time
		switch {
		case mode%4 == 0 && i > 0: // exact duplicate, possibly in another zone
			t = snapshots[int(r.u8())%i].In(zone)
		case mode%4 == 1 && i > 0: // cluster near the previous one
			t = snapshots[i-1].Add(time.Duration(int64(int16(r.u16()))*scale) * time.Second)
		case mode%4 == 3: // wide spread, roughly +/- 34k years
			t = time.Unix(int64(r.u64())>>23, 0)
		default: // near an anchor
			t = time.Unix(fzAnchors[int(r.u8())%len(fzAnchors)]+int64(int16(r.u16()))*scale, 0)
		}
		if mode&0x80 != 0 {
			t = t.Add(time.Duration(r.u16()) * 15259) // < 1s
		}
		snapshots = append(snapshots, t.In(zone))
	}
	return
}

func fzPruneSeeds(f *testing.F) {
	f.Add([]byte{})
	// Lord_Howe, 3 periods: 2@daily, inf@secondly:3600, 3@last; 6 snapshots
	// clustered around the Lord Howe fall back with duplicates.
	f.Add([]byte{
		4, 2,
		2, 0, 0, 0, 3,
		1, 3, 0, 0, 0,
		0, 0, 0, 0, 4,
		6,
		2 | 2<<2, 10, 0, 0,
		1 | 1<<2, 0, 20,
		1 | 1<<2, 0, 20,
		0, 1,
		1 | 2<<2, 0xff, 0xff,
		0x80 | 1, 0, 1, 0x12, 0x34,
	})
	// Riga, 3@monthly:3 1@yearly:2 daily:7; wide spread, year 1, duplicates.
	f.Add([]byte{1, 2, 3, 0, 0, 2, 4, 4, 0, 0, 1, 2, 2, 0, 0, 6, 0, 8, 3, 1, 2, 3, 4, 5, 6, 7, 8, 3, 0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 2, 2, 0, 0})
	for s := int64(1); s <= 8; s++ {
		b := make([]byte, 64+32*s)
		rand.New(rand.NewSource(s)).Read(b)
		f.Add(b)
	}
}

// FuzzPruneRandomPolicy checks the guarantees documented by pruneCorrectness
// for random policies, times, and locations.
func FuzzPruneRandomPolicy(f *testing.F) {
	fzPruneSeeds(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2048 {
			t.Skip()
		}
		snapshots, policy, loc := fzPruneInput(data, 96)
		if len(snapshots) != 0 {
			// pruneCorrectness only checks prefixes shorter than the input, so
			// append an extra (old, duplicate) snapshot to cover the full set.
			snapshots = append(snapshots, snapshots[0])
		}
		if err := pruneCorrectness(snapshots, policy, loc); err != nil {
			t.Fatalf("policy %q in %s: %v", policy.String(), loc, err)
		}
	})
}

func fzFloorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// fzBucket computes the bucket of t for a non-Last period independently of
// Prune's day-number arithmetic.
func fzBucket(t time.Time, p Period, loc *time.Location) int64 {
	var x int64
	switch t = t.In(loc); p.Unit {
	case Secondly:
		x = t.Unix()
	case Daily:
		y, m, d := t.Date()
		// Prune numbers days from a proleptic 0000-01-00; 1970-01-01 is day
		// 719529. The offset matters for the alignment of daily:N buckets.
		x = fzFloorDiv(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix(), 86400) + 719529
	case Monthly:
		y, m, _ := t.Date()
		x = int64(y)*12 + int64(m) - 1
	case Yearly:
		x = int64(t.Year())
	default:
		panic("unexpected unit")
	}
	return fzFloorDiv(x, int64(p.Interval))
}

// fzSortedOrder returns input indexes ordered by (instant, input index), which
// is the order Prune documents.
func fzSortedOrder(snapshots []time.Time) []int {
	order := make([]int, len(snapshots))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return snapshots[a].Compare(snapshots[b]) })
	return order
}

func FuzzPruneInvariants(f *testing.F) {
	fzPruneSeeds(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			t.Skip()
		}
		snapshots, policy, loc := fzPruneInput(data, 256)
		before := policy.Clone()

		keep, need := Prune(snapshots, policy, loc)

		if !maps.Equal(policy.count, before.count) {
			t.Fatalf("Prune mutated policy: before %q after %q", before.String(), policy.String())
		}
		if len(keep) != len(snapshots) {
			t.Fatalf("keep length %d != input length %d", len(keep), len(snapshots))
		}
		for i, reasons := range keep {
			for _, p := range reasons {
				if _, ok := policy.count[p]; !ok {
					t.Fatalf("snapshot %d kept for %v which is not in policy %q", i, p, policy.String())
				}
			}
		}
		if !slices.Equal(mapKeysSorted(need.count, Period.Compare), mapKeysSorted(policy.count, Period.Compare)) {
			t.Fatalf("need periods %q != policy periods %q", need.String(), policy.String())
		}

		order := fzSortedOrder(snapshots)
		policy.Each(func(p Period, count int) {
			var kept []int // input indexes kept for p, oldest first
			for _, i := range order {
				if slices.Contains(keep[i], p) {
					kept = append(kept, i)
				}
			}

			if got := need.count[p]; count < 0 {
				if got != -1 {
					t.Fatalf("%v: infinite count but need=%d", p, got)
				}
			} else if len(kept) > count || got != count-len(kept) {
				t.Fatalf("%v: count=%d kept=%d need=%d", p, count, len(kept), got)
			}

			if p.Unit == Last {
				want := order
				if count >= 0 && count < len(order) {
					want = order[len(order)-count:]
				}
				if !slices.Equal(kept, want) {
					t.Fatalf("%v count=%d: kept %v, want newest %v", p, count, kept, want)
				}
				return
			}

			bucket := make([]int64, len(snapshots))
			for i := range snapshots {
				bucket[i] = fzBucket(snapshots[i], p, loc)
			}
			var (
				distinct []int64
				first    = map[int64]int{} // bucket -> earliest input index
			)
			for k, i := range order {
				if k > 0 && bucket[i] < bucket[order[k-1]] {
					t.Fatalf("%v: %s buckets are not monotonic in time (test precondition)", p, loc)
				}
				if _, ok := first[bucket[i]]; !ok {
					first[bucket[i]] = i
					distinct = append(distinct, bucket[i])
				}
			}
			keptBuckets := make([]int64, len(kept))
			for k, i := range kept {
				keptBuckets[k] = bucket[i]
				if k > 0 && keptBuckets[k] == keptBuckets[k-1] {
					t.Fatalf("%v: snapshots %d and %d kept in the same bucket %d", p, kept[k-1], i, bucket[i])
				}
				if first[bucket[i]] != i {
					t.Fatalf("%v: kept snapshot %d (%s) is not the earliest in its bucket (%d is)", p, i, snapshots[i], first[bucket[i]])
				}
			}
			wantN := len(distinct)
			if count >= 0 {
				wantN = min(count, wantN)
			}
			if !slices.Equal(keptBuckets, distinct[len(distinct)-wantN:]) {
				t.Fatalf("%v count=%d: kept buckets %v, want newest %d of %v", p, count, keptBuckets, wantN, distinct)
			}
		})

		// a last-only policy keeps exactly the newest min(N, len) snapshots
		{
			n := 1 + len(data)%9
			var lp Policy
			lp.Set(Period{Unit: Last}, n)
			lkeep, _ := Prune(snapshots, lp, loc)
			want := order[len(order)-min(n, len(order)):]
			var got []int
			for _, i := range order {
				if len(lkeep[i]) != 0 {
					got = append(got, i)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%d@last: kept %v, want %v", n, got, want)
			}
		}

		// permutation invariance: the reasons along the sorted order depend only
		// on the sorted instants (even with duplicates), and need is unchanged
		{
			h := fnv.New64a()
			h.Write(data)
			perm := rand.New(rand.NewSource(int64(h.Sum64()))).Perm(len(snapshots))
			shuffled := make([]time.Time, len(snapshots))
			for k, i := range perm {
				shuffled[k] = snapshots[i]
			}
			skeep, sneed := Prune(shuffled, policy, loc)
			if !maps.Equal(need.count, sneed.count) {
				t.Fatalf("need changed after shuffle: %q != %q", need.String(), sneed.String())
			}
			sorder := fzSortedOrder(shuffled)
			for k := range order {
				a, b := order[k], sorder[k]
				if !snapshots[a].Equal(shuffled[b]) {
					t.Fatalf("sorted instants differ at %d", k)
				}
				if !reflect.DeepEqual(keep[a], skeep[b]) {
					t.Fatalf("reasons for %s changed after shuffle: %v != %v", snapshots[a], keep[a], skeep[b])
				}
			}
		}
	})
}

func fzCheckPolicy(t *testing.T, p Policy) {
	t.Helper()
	var prev *Period
	p.Each(func(period Period, count int) {
		if np, ok := period.Normalize(); !ok || np != period {
			t.Fatalf("policy contains non-normalized period %#v", period)
		}
		if count == 0 || count < -1 {
			t.Fatalf("policy period %v has count %d", period, count)
		}
		if got := p.Get(period); got != count {
			t.Fatalf("Get(%v)=%d, Each gave %d", period, got, count)
		}
		if prev != nil && prev.Compare(period) >= 0 {
			t.Fatalf("Each not strictly ascending: %v then %v", *prev, period)
		}
		prev = &period
	})
	if s := p.String(); (s == "") != (len(p.count) == 0) {
		t.Fatalf("String()=%q for policy with %d periods", s, len(p.count))
	}
	m1, err := p.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	var q Policy
	if err := q.UnmarshalText(m1); err != nil {
		t.Fatalf("cannot parse canonical %q: %v", m1, err)
	}
	if !maps.Equal(p.count, q.count) {
		t.Fatalf("round trip of %q changed policy: %q != %q", m1, p.String(), q.String())
	}
	m2, err := q.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m1, m2) {
		t.Fatalf("canonical form not idempotent: %q -> %q", m1, m2)
	}
}

func FuzzParsePolicyNoPanic(f *testing.F) {
	for _, s := range []string{
		"", " ", "last", "3@last", "-1@daily", "-9@DAILY", "+3@daily:+2",
		"secondly:1h30m", "secondly:90m", "secondly:9223372036", "secondly:2562047h47m16s", "secondly:1.5s",
		"5@secondly:60,2@monthly:6", "daily, monthly", "daily,daily", "yearly:0", "last:2", "0@last",
		"1@@daily", "@daily", "daily:", "daily:1:2", "2@monthly:3 4@yearly:400 last",
		"9223372036854775807@daily", "9223372036854775808@daily", "\tdaily\nmonthly\r\n",
	} {
		f.Add(s)
	}
	sentinel, err := ParsePolicy("3@daily", "secondly:1h")
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, text string) {
		if len(text) > 1024 {
			t.Skip()
		}

		fields, _ := ParsePolicy(strings.Fields(text)...)
		p := sentinel.Clone()
		uerr := p.UnmarshalText([]byte(text))
		if uerr != nil {
			if !maps.Equal(p.count, sentinel.count) {
				t.Fatalf("failed UnmarshalText modified policy: %q", p.String())
			}
		} else {
			if !maps.Equal(p.count, fields.count) {
				t.Fatalf("UnmarshalText %q != ParsePolicy(Fields) %q", p.String(), fields.String())
			}
			fzCheckPolicy(t, p)
		}

		// comma-separated rules may contain arbitrary whitespace
		if split, err := ParsePolicy(strings.Split(text, ",")...); err == nil {
			fzCheckPolicy(t, split)
		}
	})
}

func FuzzPeriodString(f *testing.F) {
	big := int64(9223372036) // just under/over the time.Duration limit on 64-bit
	for _, s := range [][3]int{
		{0, 1, 1}, {0, 0, 3}, {0, -7, -1}, {1, 1, 1}, {1, 60, -1}, {1, 86400 * 7, 3}, {1, 5400, 2},
		{1, int(big), 2}, {1, int(big + 1), 2}, {1, math.MaxInt, 1}, {1, 0, 1}, {2, 1, 7}, {2, -5, 1},
		{3, 400, 0}, {4, math.MaxInt, -5}, {5, 1, 1}, {-1, 1, 1}, {math.MinInt, math.MinInt, math.MinInt},
	} {
		f.Add(s[0], s[1], s[2])
	}
	f.Fuzz(func(t *testing.T, unit, interval, count int) {
		u := Unit(unit)
		if (u.String() == "") == u.IsValid() {
			t.Fatalf("Unit(%d).String()=%q IsValid=%t", unit, u.String(), u.IsValid())
		}

		p := Period{Unit: u, Interval: interval}
		np, ok := p.Normalize()
		if want := u.IsValid() && (u == Last || interval > 0); ok != want {
			t.Fatalf("%#v: Normalize ok=%t, want %t", p, ok, want)
		}
		if np2, ok2 := np.Normalize(); np2 != np || ok2 != ok {
			t.Fatalf("%#v: Normalize not idempotent: %#v,%t -> %#v,%t", p, np, ok, np2, ok2)
		}
		if s := p.String(); (s != "") != ok || s != np.String() {
			t.Fatalf("%#v: String()=%q, normalized %q, ok=%t", p, s, np.String(), ok)
		}

		var pol Policy
		if pol.Set(p, count) != ok {
			t.Fatalf("%#v: Set ok mismatch", p)
		}
		if !ok {
			if len(pol.count) != 0 {
				t.Fatalf("%#v: invalid period was stored", p)
			}
			return
		}
		if u == Last && np.Interval != 1 {
			t.Fatalf("last normalized to interval %d", np.Interval)
		}
		if np.Compare(np) != 0 {
			t.Fatalf("%#v: Compare with itself != 0", np)
		}

		text, err := pol.MarshalText()
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParsePolicy(strings.Fields(string(text))...)
		if err != nil {
			t.Fatalf("%#v count=%d: cannot parse %q: %v", p, count, text, err)
		}
		want := count
		if want < 0 {
			want = -1
		}
		if want == 0 {
			if len(got.count) != 0 {
				t.Fatalf("zero count encoded as %q", text)
			}
			return
		}
		if len(got.count) != 1 || got.Get(p) != want {
			t.Fatalf("%#v count=%d: round trip via %q gave %q", p, count, text, got.String())
		}
	})
}

func BenchmarkPrune(b *testing.B) {
	policy, err := ParsePolicy("3@last", "24@secondly:1h", "7@daily", "4@daily:7", "12@monthly", "yearly")
	if err != nil {
		b.Fatal(err)
	}
	loc, err := time.LoadLocation("Europe/Riga")
	if err != nil {
		b.Fatal(err)
	}
	start := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, n := range []int{1000, 100000} {
		snapshots := make([]time.Time, n)
		for i := range snapshots {
			snapshots[i] = start.Add(time.Duration(i)*time.Hour + time.Duration(prand(3600, i, 0xABCDEF0123456789))*time.Second)
		}
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Prune(snapshots, policy, loc)
			}
		})
	}
}

func BenchmarkParsePolicy(b *testing.B) {
	rules := []string{"3@last", "24@secondly:1h", "secondly:90m", "7@daily", "4@daily:7", "12@monthly", "2@monthly:6", "yearly"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParsePolicy(rules...); err != nil {
			b.Fatal(err)
		}
	}
}
