package main

import "time"

// replaySchedule paces events by their trade time instead of a fixed rate. The
// first event goes out at once; every later one is due when the wall clock has
// advanced (highest trade time so far - first trade time) / speed since then.
// Using the highest trade time means an out-of-order event is due at once and
// the schedule never runs backwards. The schedule is anchored to the first
// event, so time spent publishing does not add up: a producer that falls behind
// sends back to back until it has caught up.
type replaySchedule struct {
	speed   float64
	started bool
	start   time.Time // wall clock when the first event was due
	first   time.Time // the first event's trade time
	highest time.Time // the highest trade time so far
}

// due returns when the event with this trade time should be published; now is
// the wall clock when the event was read and anchors the first event.
func (s *replaySchedule) due(tradeTime, now time.Time) time.Time {
	if !s.started {
		s.started = true
		s.start, s.first, s.highest = now, tradeTime, tradeTime
		return now
	}
	if tradeTime.After(s.highest) {
		s.highest = tradeTime
	}
	return s.start.Add(time.Duration(float64(s.highest.Sub(s.first)) / s.speed))
}
