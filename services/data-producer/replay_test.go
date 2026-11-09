package main

import (
	"bytes"
	"context"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestReplayScheduleScalesTradeTimeGaps(t *testing.T) {
	wall := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	trade := time.Date(2021, 11, 12, 9, 0, 0, 0, time.UTC)
	s := replaySchedule{speed: 100}
	steps := []struct {
		trade time.Duration // after the first trade time
		read  time.Duration // wall clock when the event is read, after the start
		want  time.Duration // due, after the start
	}{
		{0, 0, 0},
		{10 * time.Second, time.Millisecond, 100 * time.Millisecond},
		// Out of order: due at the schedule's current point, so at once.
		{5 * time.Second, 150 * time.Millisecond, 100 * time.Millisecond},
		{60 * time.Second, 151 * time.Millisecond, 600 * time.Millisecond},
		// Equal trade times share a due time.
		{60 * time.Second, 700 * time.Millisecond, 600 * time.Millisecond},
	}
	for i, step := range steps {
		got := s.due(trade.Add(step.trade), wall.Add(step.read))
		if want := wall.Add(step.want); !got.Equal(want) {
			t.Fatalf("step %d: due %v after start; want %v", i, got.Sub(wall), step.want)
		}
	}
}

func TestReplayScheduleIsAnchoredToTheFirstEvent(t *testing.T) {
	wall := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	trade := time.Date(2021, 11, 12, 9, 0, 0, 0, time.UTC)
	s := replaySchedule{speed: 2}
	s.due(trade, wall)
	// Read late (the previous send was slow): the due time does not move, so a
	// lagging producer catches up instead of drifting.
	got := s.due(trade.Add(4*time.Second), wall.Add(5*time.Second))
	if want := wall.Add(2 * time.Second); !got.Equal(want) {
		t.Fatalf("due %v after start; want 2s", got.Sub(wall))
	}
}

func TestReplayScheduleSlowMotion(t *testing.T) {
	wall := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	trade := time.Date(2021, 11, 12, 9, 0, 0, 0, time.UTC)
	s := replaySchedule{speed: 0.5}
	s.due(trade, wall)
	if got := s.due(trade.Add(time.Second), wall); !got.Equal(wall.Add(2 * time.Second)) {
		t.Fatalf("due %v after start; want 2s", got.Sub(wall))
	}
}

func TestParseConfigSpeed(t *testing.T) {
	cfg, err := parseConfig([]string{"-speed=250", "-maxEvents=10"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.speed != 250 || cfg.rate != 0 || cfg.maxEvents != 10 {
		t.Fatalf("unexpected configuration: %+v", cfg)
	}
	// -rate=0 is the default and does not conflict.
	if _, err := parseConfig([]string{"-speed=0.5", "-rate=0"}, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestParseConfigRejectsInvalidSpeed(t *testing.T) {
	for _, args := range [][]string{
		{"-speed=-1"}, {"-speed=NaN"}, {"-speed=+Inf"}, {"-speed=abc"},
		{"-speed=100", "-rate=1000"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if _, err := parseConfig(args, io.Discard); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestRunWithClientReplaysTradeTimeGaps(t *testing.T) {
	// Trade times 0 s, +1 s, +0.5 s (late) and +2 s at speed 50: due at 0, 20, 20 and 40 ms.
	path := writeCSVFile(t, csvContents(testCSVHeader,
		"RUI.FR,E,28.040000,09:00:00.000,12-11-2021\n"+
			"A0HN5C.ETR,E,,,\n"+
			"RUI.FR,E,28.050000,09:00:01.000,12-11-2021\n"+
			"RUI.FR,E,28.060000,09:00:00.500,12-11-2021\n"+
			"RUI.FR,E,28.070000,09:00:02.000,12-11-2021\n"))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPaths: []string{path}, topic: "trade-events", speed: 50}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.produceTimes) != 4 {
		t.Fatalf("got %d sends; want 4", len(client.produceTimes))
	}
	start := client.produceTimes[0]
	// The fake records each send a few instructions after it was due; allow 1 ms of slack.
	for i, want := range []time.Duration{0, 20 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond} {
		if got := client.produceTimes[i].Sub(start); got < want-time.Millisecond {
			t.Fatalf("send %d at %v; want %v or later", i, got, want)
		}
	}
	summary := output.String()
	if !strings.Contains(summary, "completed: published=4 skipped=1 rejected=0") {
		t.Fatalf("unexpected summary: %s", summary)
	}
	if !regexp.MustCompile(` replay_lag_max_ms=\d+\n$`).MatchString(summary) {
		t.Fatalf("summary lacks the replay lag: %q", summary)
	}
}

func TestRunWithClientCancellationDuringReplayWait(t *testing.T) {
	// A one-hour gap at speed 1 must not hold up cancellation.
	path := writeCSVFile(t, csvContents(testCSVHeader,
		"RUI.FR,E,28.040000,09:00:00.000,12-11-2021\n"+
			"RUI.FR,E,28.050000,10:00:00.000,12-11-2021\n"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeRunClient{afterProduce: func() {
		time.AfterFunc(20*time.Millisecond, cancel)
	}}
	var output bytes.Buffer
	started := time.Now()
	err := runWithClient(ctx, config{csvPaths: []string{path}, topic: "trade-events", speed: 1}, client, &output)
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error=%v; want context canceled", err)
	}
	if len(client.records) != 1 {
		t.Fatalf("sent %d records; want 1", len(client.records))
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("cancellation waited for the replay: %v", elapsed)
	}
}
