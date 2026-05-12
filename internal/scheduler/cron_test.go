package scheduler

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, expr string) *Schedule {
	t.Helper()
	s, err := Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q) failed: %v", expr, err)
	}
	return s
}

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := LoadLocation(name)
	if err != nil {
		t.Skipf("timezone %q unavailable: %v", name, err)
	}
	return loc
}

func TestParseInvalidExpressions(t *testing.T) {
	cases := []string{
		"",
		"* * * *",     // only 4 fields
		"* * * * * *", // 6 fields (we deliberately reject seconds)
		"60 * * * *",  // minute out of range
		"* 24 * * *",  // hour out of range
		"* * 0 * *",   // dom out of range
		"* * 32 * *",  // dom out of range
		"* * * 0 *",   // month out of range
		"* * * 13 *",  // month out of range
		"* * * * 8",   // dow out of range
		"*/0 * * * *", // step zero
		"5-3 * * * *", // reversed range
		"a * * * *",   // garbage
		"@nope",       // bad macro
	}
	for _, expr := range cases {
		if _, err := Parse(expr); err == nil {
			t.Errorf("Parse(%q) succeeded but expected error", expr)
		}
	}
}

func TestParseValidExpressions(t *testing.T) {
	cases := []string{
		"* * * * *",
		"0 0 * * *",
		"*/15 * * * *",
		"0 9-17 * * 1-5",
		"0 0 1,15 * *",
		"0 0 29 2 *",
		"@hourly",
		"@daily",
		"@midnight",
		"@weekly",
		"@monthly",
		"@yearly",
		"@annually",
		"0 0 * * 7", // Sunday alt notation
	}
	for _, expr := range cases {
		if _, err := Parse(expr); err != nil {
			t.Errorf("Parse(%q) failed: %v", expr, err)
		}
	}
}

func TestNextAfterEveryFifteenMinutesUTC(t *testing.T) {
	s := mustParse(t, "*/15 * * * *")
	after := time.Date(2026, 5, 12, 12, 7, 30, 0, time.UTC)
	got := s.NextAfter(after, time.UTC)
	want := time.Date(2026, 5, 12, 12, 15, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("NextAfter = %s, want %s", got, want)
	}
}

func TestNextAfterMacroDaily(t *testing.T) {
	s := mustParse(t, "@daily")
	after := time.Date(2026, 5, 12, 23, 59, 0, 0, time.UTC)
	got := s.NextAfter(after, time.UTC)
	want := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("NextAfter = %s, want %s", got, want)
	}
}

func TestNextAfterDayOfMonthOrDayOfWeek(t *testing.T) {
	// Standard cron OR-semantics: 1st of month OR Friday.
	s := mustParse(t, "0 0 1 * 5")
	// Start mid-week (Monday 2026-05-04 noon UTC). Next fire is the
	// nearest Friday or the 1st, whichever first.
	after := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	got := s.NextAfter(after, time.UTC)
	// Friday 2026-05-08 00:00 UTC
	want := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("NextAfter = %s, want %s", got, want)
	}
}

func TestNextAfterLeapDay(t *testing.T) {
	s := mustParse(t, "0 0 29 2 *")
	// 2026 is not a leap year; next 29 Feb is 2028.
	after := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	got := s.NextAfter(after, time.UTC)
	want := time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("NextAfter = %s, want %s", got, want)
	}
}

// TestNextAfterSaoPauloDST exercises the America/Sao_Paulo timezone.
// Brazil suspended DST in 2019, so modern dates do not jump — but the
// historic 2018 transitions are still encoded in zoneinfo and we use them
// to verify the evaluator never duplicates or skips a 15-minute tick
// across the boundary.
func TestNextAfterSaoPauloDST(t *testing.T) {
	loc := mustLoc(t, "America/Sao_Paulo")
	s := mustParse(t, "*/15 * * * *")

	// Brazil fall-forward 2018: 04 Nov 2018 00:00 -> 01:00 (clocks
	// jumped from 23:59 03-Nov to 01:00 04-Nov). Start a minute before.
	after := time.Date(2018, 11, 3, 23, 50, 0, 0, loc)
	next := s.NextAfter(after, loc)
	if !next.After(after) {
		t.Fatalf("next %s not after %s", next, after)
	}
	// Run 6 consecutive ticks and assert strict monotonicity at >= 15m
	// stride (jump may be larger across the DST gap, never smaller).
	prev := next
	for i := 0; i < 6; i++ {
		got := s.NextAfter(prev, loc)
		if !got.After(prev) {
			t.Fatalf("tick %d: %s not after %s", i, got, prev)
		}
		if got.Sub(prev) < 15*time.Minute {
			t.Fatalf("tick %d: stride %s < 15m", i, got.Sub(prev))
		}
		prev = got
	}
}

// TestNextAfterUSEasternDST covers the America/New_York spring-forward.
// 02:00 EST -> 03:00 EDT on the second Sunday of March; an hourly cron
// must skip 02:00 (which does not exist locally).
func TestNextAfterUSEasternDST(t *testing.T) {
	loc := mustLoc(t, "America/New_York")
	s := mustParse(t, "0 * * * *") // top of every hour

	// 2026-03-08 is the second Sunday of March: clocks jump at 02:00.
	after := time.Date(2026, 3, 8, 1, 30, 0, 0, loc)
	next := s.NextAfter(after, loc)
	// Expected: 03:00 EDT — there is no 02:00 local time.
	wantHour := 3
	if next.Hour() != wantHour {
		t.Fatalf("spring-forward: got hour=%d (%s), want %d", next.Hour(), next, wantHour)
	}

	// Run 4 hourly ticks and assert strict monotonicity at +1h stride.
	prev := next
	for i := 0; i < 4; i++ {
		got := s.NextAfter(prev, loc)
		if !got.After(prev) {
			t.Fatalf("tick %d: %s not after %s", i, got, prev)
		}
		if got.Sub(prev) != time.Hour {
			t.Fatalf("tick %d: stride %s != 1h", i, got.Sub(prev))
		}
		prev = got
	}
}

// TestNextAfterUSEasternFallBack exercises the fall-back transition where
// 01:00-02:00 EDT repeats as 01:00-02:00 EST. Our walker advances one
// minute at a time in wall-clock space, so it must not double-fire
// during the duplicated hour.
func TestNextAfterUSEasternFallBack(t *testing.T) {
	loc := mustLoc(t, "America/New_York")
	s := mustParse(t, "30 * * * *") // half-past each hour

	// 2026-11-01 fall-back happens at 02:00 EDT -> 01:00 EST.
	after := time.Date(2026, 11, 1, 0, 45, 0, 0, loc)
	prev := after
	var ticks []time.Time
	for i := 0; i < 5; i++ {
		got := s.NextAfter(prev, loc)
		if !got.After(prev) {
			t.Fatalf("tick %d: %s not after %s", i, got, prev)
		}
		ticks = append(ticks, got)
		prev = got
	}
	// Every neighbouring pair should be >= 1h apart in absolute time
	// (the walker can never go backwards in instants).
	for i := 1; i < len(ticks); i++ {
		if ticks[i].Sub(ticks[i-1]) < time.Hour {
			t.Fatalf("fall-back drift between ticks: %s -> %s", ticks[i-1], ticks[i])
		}
	}
}

func TestLoadLocationDefaultsToUTC(t *testing.T) {
	loc, err := LoadLocation("")
	if err != nil {
		t.Fatal(err)
	}
	if loc != time.UTC {
		t.Fatalf("LoadLocation(\"\") = %v, want UTC", loc)
	}
	if _, err := LoadLocation("Mars/Olympus_Mons"); err == nil {
		t.Fatal("expected error for invalid TZ")
	}
}

func TestValidateCronWrapper(t *testing.T) {
	if err := ValidateCron("*/5 * * * *"); err != nil {
		t.Fatalf("ValidateCron good expr: %v", err)
	}
	if err := ValidateCron("nope"); err == nil {
		t.Fatal("ValidateCron should reject garbage")
	}
}

func TestStepExpressionsAreRespected(t *testing.T) {
	s := mustParse(t, "*/5 * * * *")
	// At 12:00:00, next match should be 12:05:00, not 12:01.
	after := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	got := s.NextAfter(after, time.UTC)
	want := time.Date(2026, 5, 12, 12, 5, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("step expr: got %s want %s", got, want)
	}
}

func TestSundayAliasing(t *testing.T) {
	// 7 should be Sunday, same as 0.
	a := mustParse(t, "0 0 * * 0")
	b := mustParse(t, "0 0 * * 7")
	after := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC) // a Monday
	if !a.NextAfter(after, time.UTC).Equal(b.NextAfter(after, time.UTC)) {
		t.Fatal("Sunday aliasing 0==7 broken")
	}
}
