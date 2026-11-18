package snappr

import (
	"slices"
	"testing"
)

func policyAPIPanics(fn func()) (panicked bool) {
	defer func() { panicked = recover() != nil }()
	fn()
	return
}

func policyAPIMarshal(t *testing.T, p Policy) string {
	t.Helper()
	b, err := p.MarshalText()
	if err != nil {
		t.Fatalf("marshal: unexpected error: %v", err)
	}
	return string(b)
}

func policyAPIParse(t *testing.T, rules ...string) Policy {
	t.Helper()
	p, err := ParsePolicy(rules...)
	if err != nil {
		t.Fatalf("parse %q: unexpected error: %v", rules, err)
	}
	return p
}

func TestPolicyAPI_UnitString(t *testing.T) {
	for _, tc := range []struct {
		name string
		unit Unit
		want string
	}{
		{"last", Last, "last"},
		{"secondly", Secondly, "secondly"},
		{"daily", Daily, "daily"},
		{"monthly", Monthly, "monthly"},
		{"yearly", Yearly, "yearly"},
		{"negative is invalid", Unit(-1), ""},
		{"numUnits is invalid", numUnits, ""},
		{"beyond numUnits is invalid", numUnits + 5, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.unit.String(); got != tc.want {
				t.Errorf("Unit(%d).String() = %q, want %q", int(tc.unit), got, tc.want)
			}
		})
	}
}

func TestPolicyAPI_UnitIsValid(t *testing.T) {
	for _, tc := range []struct {
		name string
		unit Unit
		want bool
	}{
		{"last", Last, true},
		{"secondly", Secondly, true},
		{"daily", Daily, true},
		{"monthly", Monthly, true},
		{"yearly", Yearly, true},
		{"negative", Unit(-1), false},
		{"numUnits", numUnits, false},
		{"beyond numUnits", numUnits + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.unit.IsValid(); got != tc.want {
				t.Errorf("Unit(%d).IsValid() = %v, want %v", int(tc.unit), got, tc.want)
			}
		})
	}
}

func TestPolicyAPI_UnitCompare(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b Unit
		want int
	}{
		{"equal", Daily, Daily, 0},
		{"less", Last, Yearly, -1},
		{"greater", Yearly, Last, 1},
		{"adjacent less", Daily, Monthly, -1},
		{"adjacent greater", Monthly, Daily, 1},
		{"invalid compares numerically", Unit(-1), Last, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Errorf("%d.Compare(%d) = %d, want %d", int(tc.a), int(tc.b), got, tc.want)
			}
		})
	}
}

func TestPolicyAPI_PeriodNormalize(t *testing.T) {
	for _, tc := range []struct {
		name   string
		in     Period
		want   Period
		wantOK bool
	}{
		{"last interval 1", Period{Last, 1}, Period{Last, 1}, true},
		{"last interval 7 normalized", Period{Last, 7}, Period{Last, 1}, true},
		{"last interval 0 normalized", Period{Last, 0}, Period{Last, 1}, true},
		{"last negative interval normalized", Period{Last, -3}, Period{Last, 1}, true},
		{"daily valid", Period{Daily, 3}, Period{Daily, 3}, true},
		{"secondly valid", Period{Secondly, 90}, Period{Secondly, 90}, true},
		{"daily zero interval", Period{Daily, 0}, Period{Daily, 0}, false},
		{"daily negative interval", Period{Daily, -5}, Period{Daily, -5}, false},
		{"yearly zero interval", Period{Yearly, 0}, Period{Yearly, 0}, false},
		{"invalid unit negative", Period{Unit(-1), 1}, Period{Unit(-1), 1}, false},
		{"invalid unit numUnits", Period{numUnits, 1}, Period{numUnits, 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.in.Normalize()
			if ok != tc.wantOK {
				t.Errorf("Normalize(%+v) ok = %v, want %v", tc.in, ok, tc.wantOK)
			}
			if got != tc.want {
				t.Errorf("Normalize(%+v) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

func TestPolicyAPI_PeriodCompare(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b Period
		want int
	}{
		{"equal", Period{Daily, 3}, Period{Daily, 3}, 0},
		{"same unit smaller interval", Period{Daily, 1}, Period{Daily, 7}, -1},
		{"same unit larger interval", Period{Daily, 7}, Period{Daily, 1}, 1},
		{"unit dominates interval", Period{Daily, 100}, Period{Monthly, 1}, -1},
		{"unit dominates interval reversed", Period{Monthly, 1}, Period{Daily, 100}, 1},
		{"secondly before daily", Period{Secondly, 99999}, Period{Daily, 1}, -1},
		{"last before everything", Period{Last, 1}, Period{Secondly, 1}, -1},
		{"strict: last intervals not normalized", Period{Last, 1}, Period{Last, 5}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Errorf("%+v.Compare(%+v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
			if got := tc.b.Compare(tc.a); got != -tc.want {
				t.Errorf("%+v.Compare(%+v) = %d, want %d (antisymmetry)", tc.b, tc.a, got, -tc.want)
			}
		})
	}
}

func TestPolicyAPI_SetGet(t *testing.T) {
	t.Run("set on zero policy allocates", func(t *testing.T) {
		var p Policy
		if !p.Set(Period{Daily, 1}, 3) {
			t.Fatal("Set returned false for valid period")
		}
		if p.count == nil {
			t.Error("expected map to be allocated")
		}
		if got := p.Get(Period{Daily, 1}); got != 3 {
			t.Errorf("Get = %d, want 3", got)
		}
	})

	t.Run("set replaces existing", func(t *testing.T) {
		var p Policy
		p.Set(Period{Daily, 1}, 3)
		p.Set(Period{Daily, 1}, 5)
		if got := p.Get(Period{Daily, 1}); got != 5 {
			t.Errorf("Get = %d, want 5", got)
		}
		if len(p.count) != 1 {
			t.Errorf("len = %d, want 1", len(p.count))
		}
	})

	t.Run("zero count deletes", func(t *testing.T) {
		var p Policy
		p.Set(Period{Daily, 1}, 3)
		p.Set(Period{Yearly, 1}, 2)
		if !p.Set(Period{Daily, 1}, 0) {
			t.Fatal("Set returned false for valid period")
		}
		if got := p.Get(Period{Daily, 1}); got != 0 {
			t.Errorf("Get = %d, want 0", got)
		}
		if len(p.count) != 1 {
			t.Errorf("len = %d, want 1 (entry should be deleted, not zeroed)", len(p.count))
		}
	})

	t.Run("zero count on absent period is ok and stores nothing", func(t *testing.T) {
		var p Policy
		if !p.Set(Period{Daily, 1}, 0) {
			t.Fatal("Set returned false for valid period")
		}
		if len(p.count) != 0 {
			t.Errorf("len = %d, want 0", len(p.count))
		}
	})

	t.Run("negative counts normalize to -1", func(t *testing.T) {
		for _, n := range []int{-1, -2, -100} {
			var p Policy
			p.Set(Period{Monthly, 2}, n)
			if got := p.Get(Period{Monthly, 2}); got != -1 {
				t.Errorf("Set(%d): Get = %d, want -1", n, got)
			}
		}
	})

	t.Run("invalid periods return false and create nothing", func(t *testing.T) {
		for _, period := range []Period{
			{Daily, 0},
			{Daily, -5},
			{Unit(-1), 1},
			{numUnits, 1},
		} {
			var p Policy
			if p.Set(period, 3) {
				t.Errorf("Set(%+v) = true, want false", period)
			}
			if p.count != nil {
				t.Errorf("Set(%+v) allocated map on zero policy", period)
			}
			q := policyAPIParse(t, "yearly")
			if q.Set(period, 3) {
				t.Errorf("Set(%+v) = true on non-empty policy, want false", period)
			}
			if len(q.count) != 1 {
				t.Errorf("Set(%+v) changed non-empty policy: %s", period, q)
			}
		}
	})

	t.Run("get on invalid period is zero", func(t *testing.T) {
		p := policyAPIParse(t, "3@daily")
		var zero Policy
		for _, period := range []Period{{Daily, 0}, {Daily, -1}, {Unit(-1), 1}, {numUnits, 1}} {
			if got := p.Get(period); got != 0 {
				t.Errorf("Get(%+v) = %d, want 0", period, got)
			}
			if got := zero.Get(period); got != 0 {
				t.Errorf("zero.Get(%+v) = %d, want 0", period, got)
			}
		}
	})

	t.Run("get on absent period is zero", func(t *testing.T) {
		p := policyAPIParse(t, "3@daily")
		if got := p.Get(Period{Daily, 2}); got != 0 {
			t.Errorf("Get = %d, want 0", got)
		}
	})

	t.Run("get normalizes last", func(t *testing.T) {
		var p Policy
		p.Set(Period{Last, 1}, 4)
		if got := p.Get(Period{Last, 5}); got != 4 {
			t.Errorf("Get(Last,5) = %d, want 4", got)
		}
		if got := p.Get(Period{Last, 0}); got != 4 {
			t.Errorf("Get(Last,0) = %d, want 4", got)
		}
	})

	t.Run("set normalizes last", func(t *testing.T) {
		var p Policy
		p.Set(Period{Last, 9}, 2)
		p.Set(Period{Last, 3}, 6)
		if len(p.count) != 1 {
			t.Errorf("len = %d, want 1", len(p.count))
		}
		if got := p.Get(Period{Last, 1}); got != 6 {
			t.Errorf("Get(Last,1) = %d, want 6", got)
		}
	})
}

func TestPolicyAPI_MustSet(t *testing.T) {
	t.Run("valid does not panic", func(t *testing.T) {
		var p Policy
		if policyAPIPanics(func() { p.MustSet(Daily, 1, 3) }) {
			t.Fatal("unexpected panic")
		}
		if got := p.Get(Period{Daily, 1}); got != 3 {
			t.Errorf("Get = %d, want 3", got)
		}
	})

	t.Run("zero count on new period does not panic", func(t *testing.T) {
		var p Policy
		if policyAPIPanics(func() { p.MustSet(Daily, 1, 0) }) {
			t.Fatal("unexpected panic")
		}
		if got := p.Get(Period{Daily, 1}); got != 0 {
			t.Errorf("Get = %d, want 0", got)
		}
	})

	t.Run("duplicate panics", func(t *testing.T) {
		var p Policy
		p.MustSet(Daily, 1, 3)
		if !policyAPIPanics(func() { p.MustSet(Daily, 1, 5) }) {
			t.Error("expected panic on duplicate")
		}
		if got := p.Get(Period{Daily, 1}); got != 3 {
			t.Errorf("Get = %d, want original 3", got)
		}
	})

	t.Run("duplicate infinite panics", func(t *testing.T) {
		var p Policy
		p.MustSet(Yearly, 1, -1)
		if !policyAPIPanics(func() { p.MustSet(Yearly, 1, -1) }) {
			t.Error("expected panic on duplicate")
		}
	})

	t.Run("duplicate after normalization panics", func(t *testing.T) {
		var p Policy
		p.MustSet(Last, 1, 2)
		if !policyAPIPanics(func() { p.MustSet(Last, 5, 2) }) {
			t.Error("expected panic on duplicate last")
		}
	})

	for _, tc := range []struct {
		name     string
		unit     Unit
		interval int
	}{
		{"zero interval", Daily, 0},
		{"negative interval", Daily, -5},
		{"negative unit", Unit(-1), 1},
		{"numUnits unit", numUnits, 1},
	} {
		t.Run("invalid panics: "+tc.name, func(t *testing.T) {
			var p Policy
			if !policyAPIPanics(func() { p.MustSet(tc.unit, tc.interval, 3) }) {
				t.Error("expected panic on invalid period")
			}
			if p.count != nil {
				t.Error("invalid MustSet must not allocate")
			}
		})
	}
}

func TestPolicyAPI_Each(t *testing.T) {
	type entry struct {
		period Period
		count  int
	}
	want := []entry{
		{Period{Last, 1}, 1},
		{Period{Secondly, 1}, 2},
		{Period{Secondly, 60}, 3},
		{Period{Daily, 1}, 4},
		{Period{Daily, 7}, 5},
		{Period{Monthly, 3}, 6},
		{Period{Yearly, 1}, -1},
	}

	collect := func(p Policy) []entry {
		var got []entry
		p.Each(func(period Period, count int) {
			got = append(got, entry{period, count})
		})
		return got
	}

	t.Run("zero policy visits nothing", func(t *testing.T) {
		if got := collect(Policy{}); len(got) != 0 {
			t.Errorf("visited %v, want nothing", got)
		}
	})

	t.Run("emptied policy visits nothing", func(t *testing.T) {
		var p Policy
		p.Set(Period{Daily, 1}, 1)
		p.Set(Period{Daily, 1}, 0)
		if got := collect(p); len(got) != 0 {
			t.Errorf("visited %v, want nothing", got)
		}
	})

	orders := map[string][]int{
		"ascending":  {0, 1, 2, 3, 4, 5, 6},
		"descending": {6, 5, 4, 3, 2, 1, 0},
		"shuffled A": {3, 6, 0, 5, 2, 4, 1},
		"shuffled B": {5, 1, 6, 3, 0, 2, 4},
	}
	for name, order := range orders {
		t.Run("order independent of insertion: "+name, func(t *testing.T) {
			var p Policy
			for _, i := range order {
				p.MustSet(want[i].period.Unit, want[i].period.Interval, want[i].count)
			}
			got := collect(p)
			if !slices.Equal(got, want) {
				t.Errorf("Each visited\n%v\nwant\n%v", got, want)
			}
		})
	}
}

func TestPolicyAPI_Clone(t *testing.T) {
	t.Run("mutating clone leaves original", func(t *testing.T) {
		orig := policyAPIParse(t, "3@daily", "yearly")
		c := orig.Clone()
		c.Set(Period{Daily, 1}, 9)
		c.Set(Period{Yearly, 1}, 0)
		c.Set(Period{Monthly, 1}, 2)

		if got := orig.Get(Period{Daily, 1}); got != 3 {
			t.Errorf("orig daily = %d, want 3", got)
		}
		if got := orig.Get(Period{Yearly, 1}); got != -1 {
			t.Errorf("orig yearly = %d, want -1", got)
		}
		if got := orig.Get(Period{Monthly, 1}); got != 0 {
			t.Errorf("orig monthly = %d, want 0", got)
		}
		if got := c.Get(Period{Daily, 1}); got != 9 {
			t.Errorf("clone daily = %d, want 9", got)
		}
	})

	t.Run("mutating original leaves clone", func(t *testing.T) {
		orig := policyAPIParse(t, "3@daily")
		c := orig.Clone()
		orig.Set(Period{Daily, 1}, 9)
		orig.Set(Period{Yearly, 1}, 1)
		if got := c.Get(Period{Daily, 1}); got != 3 {
			t.Errorf("clone daily = %d, want 3", got)
		}
		if got := c.Get(Period{Yearly, 1}); got != 0 {
			t.Errorf("clone yearly = %d, want 0", got)
		}
	})

	t.Run("clone has same contents", func(t *testing.T) {
		orig := policyAPIParse(t, "3@daily", "yearly:2", "secondly:90")
		if a, b := policyAPIMarshal(t, orig), policyAPIMarshal(t, orig.Clone()); a != b {
			t.Errorf("clone %q != original %q", b, a)
		}
	})

	t.Run("zero policy", func(t *testing.T) {
		var orig Policy
		c := orig.Clone()
		if c.count != nil {
			t.Error("clone of zero policy should be zero")
		}
		if !c.Set(Period{Daily, 1}, 1) {
			t.Fatal("Set on clone failed")
		}
		if orig.count != nil || orig.Get(Period{Daily, 1}) != 0 {
			t.Error("original zero policy was affected by clone")
		}
	})
}

func TestPolicyAPI_CopyAliasing(t *testing.T) {
	t.Run("copy of non-empty policy shares state", func(t *testing.T) {
		orig := policyAPIParse(t, "3@daily")
		cp := orig
		cp.Set(Period{Yearly, 1}, 2)
		cp.Set(Period{Daily, 1}, 7)
		if got := orig.Get(Period{Yearly, 1}); got != 2 {
			t.Errorf("orig yearly = %d, want 2 (shared)", got)
		}
		if got := orig.Get(Period{Daily, 1}); got != 7 {
			t.Errorf("orig daily = %d, want 7 (shared)", got)
		}
	})

	t.Run("copy of zero policy does not share state", func(t *testing.T) {
		var orig Policy
		cp := orig
		cp.Set(Period{Daily, 1}, 3)
		if got := orig.Get(Period{Daily, 1}); got != 0 {
			t.Errorf("orig daily = %d, want 0", got)
		}
		if orig.count != nil {
			t.Error("orig should remain zero")
		}
	})
}

func TestPolicyAPI_String(t *testing.T) {
	t.Run("zero policy", func(t *testing.T) {
		if got := (Policy{}).String(); got != "" {
			t.Errorf("String() = %q, want empty", got)
		}
	})

	t.Run("emptied policy", func(t *testing.T) {
		var p Policy
		p.Set(Period{Daily, 1}, 1)
		p.Set(Period{Daily, 1}, 0)
		if got := p.String(); got != "" {
			t.Errorf("String() = %q, want empty", got)
		}
	})

	t.Run("full policy", func(t *testing.T) {
		var p Policy
		p.MustSet(Yearly, 1, 2)
		p.MustSet(Monthly, 3, -1)
		p.MustSet(Daily, 7, 4)
		p.MustSet(Daily, 1, 7)
		p.MustSet(Secondly, 90, 2)
		p.MustSet(Secondly, 45, 1)
		p.MustSet(Last, 1, 1)
		want := "last (1), 45s time (1), 1m30s time (2), 1 day (7), 7 day (4), 3 month (inf), 1 year (2)"
		if got := p.String(); got != want {
			t.Errorf("String() =\n%q\nwant\n%q", got, want)
		}
	})

	t.Run("negative counts print inf", func(t *testing.T) {
		var p Policy
		p.Set(Period{Last, 1}, -7)
		if got, want := p.String(), "last (inf)"; got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})
}

func TestPolicyAPI_MarshalText(t *testing.T) {
	t.Run("zero policy", func(t *testing.T) {
		b, err := Policy{}.MarshalText()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(b) != 0 {
			t.Errorf("got %q, want empty", b)
		}
	})

	t.Run("ordered by unit then interval", func(t *testing.T) {
		var p Policy
		p.MustSet(Yearly, 1, -1)
		p.MustSet(Monthly, 3, 2)
		p.MustSet(Daily, 7, 4)
		p.MustSet(Daily, 1, 7)
		p.MustSet(Secondly, 3600, 5)
		p.MustSet(Secondly, 90, -1)
		p.MustSet(Secondly, 30, 1)
		p.MustSet(Last, 1, 2)
		want := "2@last 1@secondly:30 secondly:1m30s 5@secondly:1h 7@daily 4@daily:7 2@monthly:3 yearly"
		if got := policyAPIMarshal(t, p); got != want {
			t.Errorf("got\n%q\nwant\n%q", got, want)
		}
	})

	t.Run("zero-count entries omitted", func(t *testing.T) {
		p := Policy{count: map[Period]int{
			{Daily, 1}:  0,
			{Yearly, 1}: 2,
			{Last, 1}:   0,
		}}
		if got, want := policyAPIMarshal(t, p), "2@yearly"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
		p = Policy{count: map[Period]int{{Daily, 1}: 0}}
		if got := policyAPIMarshal(t, p); got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})

	t.Run("deleted entries omitted", func(t *testing.T) {
		var p Policy
		p.Set(Period{Daily, 1}, 3)
		p.Set(Period{Daily, 1}, 0)
		if got := policyAPIMarshal(t, p); got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})

	for _, tc := range []struct {
		name string
		rule string
		want string
	}{
		{"infinite omits count", "-5@daily", "daily"},
		{"explicit interval 1 omitted", "daily:1", "daily"},
		{"count with interval 1", "5@daily:1", "5@daily"},
		{"count and interval", "5@daily:3", "5@daily:3"},
		{"infinite with interval", "monthly:2", "monthly:2"},
		{"last", "last", "last"},
		{"last counted", "4@last", "4@last"},
		{"secondly 1 omitted", "secondly:1", "secondly"},
		{"secondly 59 plain", "secondly:59", "secondly:59"},
		{"secondly 60 duration", "secondly:60", "secondly:1m"},
		{"secondly 61 duration", "secondly:61", "secondly:1m1s"},
		{"secondly 90 duration", "secondly:90", "secondly:1m30s"},
		{"secondly duration input 1m30s", "secondly:1m30s", "secondly:1m30s"},
		{"secondly 1h", "secondly:3600", "secondly:1h"},
		{"secondly 1h30m", "secondly:5400", "secondly:1h30m"},
		{"secondly 3h25m45s", "secondly:12345", "secondly:3h25m45s"},
		{"secondly with count", "2@secondly:2h", "2@secondly:2h"},
		{"uppercase unit", "3@DAILY", "3@daily"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := policyAPIMarshal(t, policyAPIParse(t, tc.rule)); got != tc.want {
				t.Errorf("rule %q marshals to %q, want %q", tc.rule, got, tc.want)
			}
		})
	}

	t.Run("equivalent inputs give identical output", func(t *testing.T) {
		for _, group := range [][]string{
			{"daily", "daily:1", "DAILY", "-5@daily", "-1@daily:1", "Daily:1", "-1@DAILY"},
			{"last", "last:1", "-3@last"},
			{"secondly:90", "secondly:1m30s", "secondly:1.5m", "-2@secondly:90"},
			{"secondly:3600", "secondly:1h", "secondly:60m", "secondly:3600s"},
			{"secondly", "secondly:1", "secondly:1s", "secondly:1000ms"},
		} {
			want := policyAPIMarshal(t, policyAPIParse(t, group[0]))
			for _, rule := range group[1:] {
				if got := policyAPIMarshal(t, policyAPIParse(t, rule)); got != want {
					t.Errorf("rule %q marshals to %q, want %q (same as %q)", rule, got, want, group[0])
				}
			}
		}
	})

	t.Run("rule order does not matter", func(t *testing.T) {
		a := policyAPIMarshal(t, policyAPIParse(t, "yearly", "3@daily", "last"))
		b := policyAPIMarshal(t, policyAPIParse(t, "last", "yearly", "3@daily"))
		if a != b {
			t.Errorf("%q != %q", a, b)
		}
		if want := "last 3@daily yearly"; a != want {
			t.Errorf("got %q, want %q", a, want)
		}
	})
}

func TestPolicyAPI_UnmarshalText(t *testing.T) {
	t.Run("whitespace separation", func(t *testing.T) {
		for name, in := range map[string]string{
			"spaces":      "3@daily monthly yearly yearly:2",
			"newlines":    "3@daily\nmonthly\nyearly\nyearly:2",
			"tabs":        "3@daily\tmonthly\tyearly\tyearly:2",
			"crlf":        "3@daily\r\nmonthly\r\nyearly\r\nyearly:2",
			"mixed":       "  3@daily\n\tmonthly \t\n yearly\n\n  yearly:2\n",
			"unordered":   "yearly:2 monthly yearly 3@daily",
			"extra blank": "\n\n3@daily   monthly\t\t\tyearly yearly:2  \n",
		} {
			t.Run(name, func(t *testing.T) {
				var p Policy
				if err := p.UnmarshalText([]byte(in)); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got, want := policyAPIMarshal(t, p), "3@daily monthly yearly yearly:2"; got != want {
					t.Errorf("got %q, want %q", got, want)
				}
			})
		}
	})

	t.Run("empty or blank input yields empty policy replacing old", func(t *testing.T) {
		for _, in := range []string{"", "   ", "\n\t "} {
			p := policyAPIParse(t, "3@daily")
			if err := p.UnmarshalText([]byte(in)); err != nil {
				t.Fatalf("input %q: unexpected error: %v", in, err)
			}
			if got := policyAPIMarshal(t, p); got != "" {
				t.Errorf("input %q: got %q, want empty", in, got)
			}
		}
	})

	t.Run("success replaces existing", func(t *testing.T) {
		p := policyAPIParse(t, "3@daily", "yearly")
		if err := p.UnmarshalText([]byte("2@monthly")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := policyAPIMarshal(t, p), "2@monthly"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("error leaves receiver unchanged", func(t *testing.T) {
		for _, in := range []string{
			"bogus",
			"yearly bogus",
			"monthly 0@daily",
			"monthly daily daily",
			"2@monthly secondly:1.5s",
		} {
			p := policyAPIParse(t, "3@daily", "yearly:2")
			before := policyAPIMarshal(t, p)
			if err := p.UnmarshalText([]byte(in)); err == nil {
				t.Errorf("input %q: expected error", in)
			}
			if got := policyAPIMarshal(t, p); got != before {
				t.Errorf("input %q: receiver changed to %q, want %q", in, got, before)
			}
		}
	})

	t.Run("error leaves zero receiver zero", func(t *testing.T) {
		var p Policy
		if err := p.UnmarshalText([]byte("monthly bogus")); err == nil {
			t.Fatal("expected error")
		}
		if p.count != nil {
			t.Errorf("receiver modified: %s", p)
		}
	})
}

func TestPolicyAPI_ParsePolicyExtras(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rules []string
		want  string // canonical MarshalText on success
		err   bool
	}{
		// success
		{name: "plus sign count", rules: []string{"+3@daily"}, want: "3@daily"},
		{name: "leading zeros count", rules: []string{"003@daily"}, want: "3@daily"},
		{name: "secondly seconds integer", rules: []string{"secondly:90"}, want: "secondly:1m30s"},
		{name: "secondly fractional hours whole seconds", rules: []string{"secondly:1.5h"}, want: "secondly:1h30m"},
		{name: "last explicit interval 1", rules: []string{"last:1"}, want: "last"},
		{name: "counted last", rules: []string{"5@last"}, want: "5@last"},
		{name: "mixed case unit with interval", rules: []string{"2@Yearly:3"}, want: "2@yearly:3"},
		{name: "no rules", rules: nil, want: ""},

		// errors
		{name: "missing count before at", rules: []string{"@daily"}, err: true},
		{name: "missing unit after at", rules: []string{"3@"}, err: true},
		{name: "empty interval", rules: []string{"3@daily:"}, err: true},
		{name: "non-numeric interval", rules: []string{"daily:x"}, err: true},
		{name: "extra colon", rules: []string{"daily:1:2"}, err: true},
		{name: "extra at", rules: []string{"3@daily@x"}, err: true},
		{name: "unknown unit", rules: []string{"bogus"}, err: true},
		{name: "unknown unit with count", rules: []string{"3@weekly"}, err: true},
		{name: "zero count daily", rules: []string{"0@daily"}, err: true},
		{name: "count overflow", rules: []string{"99999999999999999999@daily"}, err: true},
		{name: "negative interval", rules: []string{"daily:-1"}, err: true},
		{name: "secondly zero duration", rules: []string{"secondly:0s"}, err: true},
		{name: "secondly negative duration", rules: []string{"secondly:-1m"}, err: true},
		{name: "secondly fractional seconds", rules: []string{"secondly:1.5s"}, err: true},
		{name: "secondly empty interval", rules: []string{"secondly:"}, err: true},
		{name: "duration on non-secondly unit", rules: []string{"daily:1h"}, err: true},
		{name: "duplicate secondly integer vs duration", rules: []string{"secondly:90", "secondly:1m30s"}, err: true},
		{name: "duplicate secondly duration vs integer", rules: []string{"secondly:1m30s", "secondly:90"}, err: true},
		{name: "duplicate implicit vs explicit", rules: []string{"daily", "3@daily:1"}, err: true},
		{name: "duplicate last forms", rules: []string{"last", "2@last:1"}, err: true},
		{name: "duplicate different case", rules: []string{"daily", "DAILY"}, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePolicy(tc.rules...)
			if tc.err {
				if err == nil {
					t.Fatalf("ParsePolicy(%q) succeeded (policy: %q), want error", tc.rules, policyAPIMarshal(t, p))
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePolicy(%q): unexpected error: %v", tc.rules, err)
			}
			if got := policyAPIMarshal(t, p); got != tc.want {
				t.Errorf("ParsePolicy(%q) marshals to %q, want %q", tc.rules, got, tc.want)
			}
		})
	}

	t.Run("secondly:1.5h is 5400 seconds", func(t *testing.T) {
		p := policyAPIParse(t, "2@secondly:1.5h")
		if got := p.Get(Period{Secondly, 5400}); got != 2 {
			t.Errorf("Get(Secondly,5400) = %d, want 2", got)
		}
	})

	t.Run("secondly:90 and 1m30s are the same period", func(t *testing.T) {
		a := policyAPIParse(t, "4@secondly:90")
		b := policyAPIParse(t, "4@secondly:1m30s")
		if a.Get(Period{Secondly, 90}) != 4 || b.Get(Period{Secondly, 90}) != 4 {
			t.Error("both forms should set Secondly:90 to 4")
		}
	})
}
