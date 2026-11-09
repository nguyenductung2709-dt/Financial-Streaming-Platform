package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestRatePacerFirstSendIsDueAtOnce(t *testing.T) {
	p := ratePacer{interval: time.Millisecond}
	now := time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC)
	if got := p.due(now); !got.Equal(now) {
		t.Fatalf("due=%v; want %v", got, now)
	}
}

func TestRatePacerSpacesSendsByInterval(t *testing.T) {
	p := ratePacer{interval: time.Millisecond}
	start := time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC)
	p.due(start)
	// Read early: the send waits for its slot.
	if got, want := p.due(start), start.Add(time.Millisecond); !got.Equal(want) {
		t.Fatalf("due=%v; want %v", got, want)
	}
}

func TestRatePacerMakesUpLateSends(t *testing.T) {
	p := ratePacer{interval: time.Millisecond}
	start := time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC)
	p.due(start)
	// Woke 5 ms late: the next slots stay on the schedule, so they go out at once.
	late := start.Add(5 * time.Millisecond)
	for i := 1; i <= 3; i++ {
		if got, want := p.due(late), start.Add(time.Duration(i)*time.Millisecond); !got.Equal(want) {
			t.Fatalf("send %d due=%v; want %v", i, got, want)
		}
	}
}

func TestRatePacerRestartsAfterStall(t *testing.T) {
	p := ratePacer{interval: time.Millisecond}
	start := time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC)
	p.due(start)
	// Exactly maxRateLag behind still catches up.
	edge := start.Add(time.Millisecond + maxRateLag)
	if got, want := p.due(edge), start.Add(time.Millisecond); !got.Equal(want) {
		t.Fatalf("due=%v; want %v", got, want)
	}
	// Further behind: the schedule restarts instead of bursting.
	stalled := start.Add(time.Second)
	if got := p.due(stalled); !got.Equal(stalled) {
		t.Fatalf("due=%v; want %v", got, stalled)
	}
	if got, want := p.due(stalled), stalled.Add(time.Millisecond); !got.Equal(want) {
		t.Fatalf("due=%v; want %v", got, want)
	}
}

func TestRunWithClientReachesRateAboveTimerResolution(t *testing.T) {
	// 2,000 events/s is a 500 us gap, shorter than Go's timer can sleep. Rescheduling
	// from each wake-up took about 1.1 ms per event; the fixed schedule keeps 500 us.
	const events = 200
	path := writeCSVFile(t, csvContents(testCSVHeader, strings.Repeat(validRunRow, events)))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPaths: []string{path}, topic: "trade-events", rate: 2000}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.produceTimes) != events {
		t.Fatalf("got %d sends; want %d", len(client.produceTimes), events)
	}
	elapsed := client.produceTimes[events-1].Sub(client.produceTimes[0])
	// 199 gaps of 500 us. Under the old pacing this took over 200 ms.
	if elapsed < 99*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Fatalf("%d sends took %v; want about 99.5 ms", events, elapsed)
	}
}
