package main

import "time"

// maxRateLag is how far the rate schedule may fall behind before it restarts.
// Below it, late sends are made up; above it (a stalled broker, a rate above
// what the producer can send) catching up would burst past -rate.
const maxRateLag = 100 * time.Millisecond

// ratePacer spaces sends evenly on a fixed schedule: each send is due one
// interval after the previous one was due, not after it went out. A timer
// that wakes late (Go cannot sleep much less than 1 ms) or a slow send is
// made up by the next sends, so the achieved rate matches -rate.
type ratePacer struct {
	interval time.Duration
	next     time.Time
}

// due returns when the next send should start; now is the wall clock when the
// event was read.
func (p *ratePacer) due(now time.Time) time.Time {
	if p.next.IsZero() || now.Sub(p.next) > maxRateLag {
		p.next = now
	}
	due := p.next
	p.next = due.Add(p.interval)
	return due
}
