// Package scheduler hosts pure-Go schedule primitives shared between
// cs-scheduler (tick loop) and cs-control (schedule validation).
//
// E4.01 introduced cron-based triggers. We considered github.com/robfig/cron/v3
// (the de-facto Go cron parser) but it is not vendored in go.sum and the issue
// requires no new dependencies. So this package ships a minimal, well-tested
// 5-field CRON parser (POSIX subset: minute hour day-of-month month
// day-of-week) plus a NextAfter evaluator that handles IANA timezones and DST
// forward/backward jumps.
//
// Supported grammar (per field):
//
//   - wildcard, all valid values
//     N                literal integer
//     A-B              inclusive range
//     *\/S or A-B/S    step (S must be >= 1)
//     X,Y,Z            comma-separated list of any of the above
//
// Macros: @yearly, @annually, @monthly, @weekly, @daily, @midnight, @hourly.
//
// Day-of-week: 0 or 7 == Sunday. day-of-month and day-of-week behave like
// standard cron: if BOTH are restricted (non-"*"), a match on EITHER field
// fires the schedule (OR semantics); if one is "*", the other constrains
// alone.
//
// DST handling: NextAfter walks forward in the destination timezone using
// time.Date, which normalises non-existent local times (spring-forward) by
// jumping forward and resolves ambiguous local times (fall-back) by the
// default zoneinfo rules. Tests pin behaviour for America/Sao_Paulo and
// America/New_York DST transitions.
package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed 5-field cron expression that can compute the next
// fire time after an arbitrary point in time.
type Schedule struct {
	minute     fieldSet
	hour       fieldSet
	dayOfMonth fieldSet
	month      fieldSet
	dayOfWeek  fieldSet
	// domStar / dowStar record whether the user wrote "*" for these
	// fields, since standard cron OR-semantics depend on it.
	domStar bool
	dowStar bool
}

// fieldSet stores allowed values as a bitmap so membership tests are O(1).
type fieldSet uint64

func (s fieldSet) contains(v int) bool {
	if v < 0 || v >= 64 {
		return false
	}
	return s&(1<<uint(v)) != 0
}

// Parse parses a 5-field cron expression or one of the @-macros and
// returns a Schedule.
func Parse(expr string) (*Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("cron: empty expression")
	}
	if strings.HasPrefix(expr, "@") {
		return parseMacro(expr)
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d", len(fields))
	}
	sch := &Schedule{
		domStar: fields[2] == "*",
		dowStar: fields[4] == "*",
	}
	var err error
	if sch.minute, err = parseField(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("cron: minute: %w", err)
	}
	if sch.hour, err = parseField(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("cron: hour: %w", err)
	}
	if sch.dayOfMonth, err = parseField(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("cron: day-of-month: %w", err)
	}
	if sch.month, err = parseField(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("cron: month: %w", err)
	}
	if sch.dayOfWeek, err = parseDOW(fields[4]); err != nil {
		return nil, fmt.Errorf("cron: day-of-week: %w", err)
	}
	return sch, nil
}

func parseMacro(expr string) (*Schedule, error) {
	switch expr {
	case "@yearly", "@annually":
		return Parse("0 0 1 1 *")
	case "@monthly":
		return Parse("0 0 1 * *")
	case "@weekly":
		return Parse("0 0 * * 0")
	case "@daily", "@midnight":
		return Parse("0 0 * * *")
	case "@hourly":
		return Parse("0 * * * *")
	default:
		return nil, fmt.Errorf("cron: unknown macro %q", expr)
	}
}

func parseField(raw string, min, max int) (fieldSet, error) {
	var set fieldSet
	for _, term := range strings.Split(raw, ",") {
		if term == "" {
			return 0, fmt.Errorf("empty term")
		}
		bits, err := parseTerm(term, min, max)
		if err != nil {
			return 0, err
		}
		set |= bits
	}
	if set == 0 {
		return 0, fmt.Errorf("no values matched %q", raw)
	}
	return set, nil
}

func parseDOW(raw string) (fieldSet, error) {
	// Map 7 to 0 (Sunday) before parsing so the bitmap stays within 0-6.
	terms := strings.Split(raw, ",")
	mapped := make([]string, 0, len(terms))
	for _, t := range terms {
		mapped = append(mapped, remapSunday(t))
	}
	return parseField(strings.Join(mapped, ","), 0, 6)
}

func remapSunday(term string) string {
	// Replace any standalone "7" with "0". Only literal digits — avoid
	// disturbing step expressions like "*\/7" (which only legitimately
	// shows up in fields with a wider range, so this is conservative).
	if term == "7" {
		return "0"
	}
	// Handle "7-..." or "...-7" or step suffixes. Keep this narrow.
	if strings.HasPrefix(term, "7-") {
		return "0-" + term[2:]
	}
	if strings.HasSuffix(term, "-7") {
		return term[:len(term)-1] + "0"
	}
	return term
}

func parseTerm(term string, min, max int) (fieldSet, error) {
	step := 1
	if i := strings.Index(term, "/"); i >= 0 {
		s, err := strconv.Atoi(term[i+1:])
		if err != nil || s < 1 {
			return 0, fmt.Errorf("invalid step %q", term[i+1:])
		}
		step = s
		term = term[:i]
	}
	var lo, hi int
	switch {
	case term == "*":
		lo, hi = min, max
	case strings.Contains(term, "-"):
		parts := strings.SplitN(term, "-", 2)
		l, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, fmt.Errorf("invalid range start %q", parts[0])
		}
		h, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, fmt.Errorf("invalid range end %q", parts[1])
		}
		lo, hi = l, h
	default:
		v, err := strconv.Atoi(term)
		if err != nil {
			return 0, fmt.Errorf("invalid value %q", term)
		}
		lo, hi = v, v
	}
	if lo < min || hi > max || lo > hi {
		return 0, fmt.Errorf("value out of range [%d,%d]: %d-%d", min, max, lo, hi)
	}
	var set fieldSet
	for v := lo; v <= hi; v += step {
		set |= 1 << uint(v)
	}
	return set, nil
}

// LoadLocation resolves an IANA timezone name; an empty value yields UTC.
func LoadLocation(name string) (*time.Location, error) {
	if strings.TrimSpace(name) == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("cron: invalid timezone %q: %w", name, err)
	}
	return loc, nil
}

// NextAfter returns the first time strictly after `after` that matches the
// schedule, evaluated in the supplied location. The returned time is in
// the same location.
//
// The algorithm walks one minute at a time in the target zone, bounded at
// 8 years of search (covers leap-year-Feb-29 edge cases). For most cron
// expressions it converges in microseconds since fields are usually dense.
//
// DST robustness: we advance using time.Time.Add (which moves absolute
// Unix time) rather than recomposing via time.Date when the next step
// would re-enter a forward-DST gap (Go's time.Date resolves DST gaps
// by snapping to the earlier candidate, which would move the walker
// BACKWARDS in absolute time and cause an infinite loop). For jump
// shortcuts to the next hour/day/month we still recompose, then if the
// recomposed instant lands at-or-before the current cursor we fall back
// to a single-minute Add and re-localise.
func (s *Schedule) NextAfter(after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	// Start at the next whole minute (in absolute time) and discard sub-
	// minute components. Working in absolute time keeps DST safe.
	t := after.Add(time.Minute).Truncate(time.Minute).In(loc)
	// Hard cap: 8 years of minutes (~ 4.2M iterations worst case).
	// Pathological expressions like "0 0 29 2 *" need a few-year leap.
	limit := t.Add(8 * 366 * 24 * time.Hour)
	for t.Before(limit) {
		y, m, d := t.Date()
		hh, mm, _ := t.Clock()
		if !s.month.contains(int(m)) {
			t = advanceTo(t, time.Date(y, m+1, 1, 0, 0, 0, 0, loc), loc)
			continue
		}
		if !s.dayMatches(t) {
			t = advanceTo(t, time.Date(y, m, d+1, 0, 0, 0, 0, loc), loc)
			continue
		}
		if !s.hour.contains(hh) {
			t = advanceTo(t, time.Date(y, m, d, hh+1, 0, 0, 0, loc), loc)
			continue
		}
		if !s.minute.contains(mm) {
			t = advanceTo(t, time.Date(y, m, d, hh, mm+1, 0, 0, loc), loc)
			continue
		}
		return t
	}
	// Should never happen for valid expressions; return zero value to
	// signal "no fire". Callers treat zero as a permanent disable hint.
	return time.Time{}
}

// advanceTo moves the walker to `target` if that strictly advances absolute
// time; otherwise it nudges forward one minute via Add. The fallback covers
// the DST-forward-jump case where time.Date resolved an ambiguous local
// time to an instant at or before the current cursor.
func advanceTo(cur, target time.Time, loc *time.Location) time.Time {
	if target.After(cur) {
		return target.In(loc)
	}
	return cur.Add(time.Minute).In(loc)
}

// dayMatches encodes standard cron OR-semantics between day-of-month and
// day-of-week: if both are restricted, either match fires; if one is "*",
// only the restricted field gates.
func (s *Schedule) dayMatches(t time.Time) bool {
	dom := int(t.Day())
	dow := int(t.Weekday()) // time.Sunday == 0
	domOK := s.dayOfMonth.contains(dom)
	dowOK := s.dayOfWeek.contains(dow)
	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dowOK
	case s.dowStar:
		return domOK
	default:
		return domOK || dowOK
	}
}

// ValidateCron parses a cron expression and returns nil on success. It is a
// thin wrapper exposed so cs-control validation can reuse the parser
// without holding a Schedule reference.
func ValidateCron(expr string) error {
	_, err := Parse(expr)
	return err
}
