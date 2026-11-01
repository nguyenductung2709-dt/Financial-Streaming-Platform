package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func checkpointConfig(t *testing.T, rows string) config {
	t.Helper()
	return config{csvPath: writeCSVFile(t, csvContents(testCSVHeader, rows)), brokers: []string{"localhost:19092"},
		topic: "trade-events", dlqTopic: "trade-events-dlq", checkpointPath: filepath.Join(t.TempDir(), "progress.json"), checkpointInterval: time.Hour}
}

func checkpointRow(t *testing.T, path string) uint64 {
	t.Helper()
	state, err := loadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	return state.LastCompletedRow
}

func TestCheckpointResumeAfterLimit(t *testing.T) {
	cfg := checkpointConfig(t, strings.Repeat(validRunRow, 3))
	cfg.maxEvents = 1
	first := &fakeRunClient{}
	if err := runWithClient(context.Background(), cfg, first, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 14 {
		t.Fatalf("row=%d", got)
	}
	cfg.resume = true
	cfg.maxEvents = 0
	second := &fakeRunClient{}
	if err := runWithClient(context.Background(), cfg, second, io.Discard); err != nil {
		t.Fatal(err)
	}
	if len(first.records) != 1 || len(second.records) != 2 {
		t.Fatalf("sent %d then %d", len(first.records), len(second.records))
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 16 {
		t.Fatalf("row=%d", got)
	}
	third := &fakeRunClient{}
	if err := runWithClient(context.Background(), cfg, third, io.Discard); err != nil || len(third.records) != 0 {
		t.Fatalf("EOF resume: err=%v records=%d", err, len(third.records))
	}
}

func TestCheckpointFailedSendDoesNotAdvance(t *testing.T) {
	cfg := checkpointConfig(t, strings.Repeat(validRunRow, 3))
	client := &fakeRunClient{}
	sendErr := errors.New("broker failed")
	client.afterProduce = func() { client.produceErr = sendErr }
	if err := runWithClient(context.Background(), cfg, client, io.Discard); !errors.Is(err, sendErr) {
		t.Fatalf("err=%v", err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 14 {
		t.Fatalf("failed row saved: %d", got)
	}
	cfg.resume = true
	next := &fakeRunClient{}
	if err := runWithClient(context.Background(), cfg, next, io.Discard); err != nil || len(next.records) != 2 {
		t.Fatalf("resume err=%v records=%d", err, len(next.records))
	}
}

func TestCheckpointSavesSuccessfulAckBeforeCancellation(t *testing.T) {
	cfg := checkpointConfig(t, strings.Repeat(validRunRow, 2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeRunClient{afterProduce: cancel}
	if err := runWithClient(ctx, cfg, client, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 14 {
		t.Fatalf("row=%d", got)
	}
}

func TestCheckpointSkipsAndDLQAcknowledgments(t *testing.T) {
	cfg := checkpointConfig(t, validRunRow+"EMPTY.FR,E,,09:00:00.000,12-11-2021\n"+"BAD.FR,E,invalid,09:00:00.000,12-11-2021\n"+validRunRow)
	cfg.dlqEnabled = true
	client := &fakeRunClient{}
	if err := runWithClient(context.Background(), cfg, client, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 17 {
		t.Fatalf("row=%d", got)
	}
	if len(client.records) != 3 || client.records[1].Topic != cfg.dlqTopic {
		t.Fatal("DLQ was not acknowledged")
	}
	cfg.resume = true
	next := &fakeRunClient{}
	if err := runWithClient(context.Background(), cfg, next, io.Discard); err != nil || len(next.records) != 0 {
		t.Fatalf("resume err=%v records=%d", err, len(next.records))
	}
}

func TestCheckpointFailedDLQDoesNotAdvance(t *testing.T) {
	cfg := checkpointConfig(t, "EMPTY.FR,E,,09:00:00.000,12-11-2021\nBAD.FR,E,invalid,09:00:00.000,12-11-2021\n")
	cfg.dlqEnabled = true
	sendErr := errors.New("DLQ failed")
	if err := runWithClient(context.Background(), cfg, &fakeRunClient{produceErr: sendErr}, io.Discard); !errors.Is(err, sendErr) {
		t.Fatalf("err=%v", err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 14 {
		t.Fatalf("row=%d", got)
	}
}

func TestCheckpointLockAndExplicitResume(t *testing.T) {
	cfg := checkpointConfig(t, validRunRow)
	first, err := openCheckpoint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.close()
	cfg.resume = true
	if second, err := openCheckpoint(cfg); err == nil {
		second.close()
		t.Fatal("concurrent checkpoint opened")
	}
	first.close()
	cfg.resume = false
	if second, err := openCheckpoint(cfg); err == nil {
		second.close()
		t.Fatal("existing checkpoint overwritten")
	}
	cfg.resume = true
	second, err := openCheckpoint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	second.close()
}

func TestCheckpointRejectsSourceAndDestinationChanges(t *testing.T) {
	for _, change := range []string{"contents", "topic", "brokers", "DLQ topic", "DLQ mode"} {
		t.Run(change, func(t *testing.T) {
			cfg := checkpointConfig(t, validRunRow)
			tracker, err := openCheckpoint(cfg)
			if err != nil {
				t.Fatal(err)
			}
			tracker.close()
			cfg.resume = true
			switch change {
			case "contents":
				if err := os.WriteFile(cfg.csvPath, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "topic":
				cfg.topic = "other"
			case "brokers":
				cfg.brokers = []string{"other:9092"}
			case "DLQ topic":
				cfg.dlqTopic = "other-dlq"
			case "DLQ mode":
				cfg.dlqEnabled = true
			}
			if tracker, err := openCheckpoint(cfg); err == nil {
				tracker.close()
				t.Fatal("mismatched checkpoint accepted")
			}
		})
	}
}

func TestCheckpointRejectsCorruptAndMissingFiles(t *testing.T) {
	for _, contents := range []string{"", "{", `{"version":99}`, `{"version":1,"unexpected":1}`, `{} {}`} {
		cfg := checkpointConfig(t, validRunRow)
		if err := os.WriteFile(cfg.checkpointPath, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		cfg.resume = true
		if tracker, err := openCheckpoint(cfg); err == nil {
			tracker.close()
			t.Fatalf("accepted %q", contents)
		}
	}
	cfg := checkpointConfig(t, validRunRow)
	cfg.resume = true
	if tracker, err := openCheckpoint(cfg); err == nil {
		tracker.close()
		t.Fatal("missing checkpoint accepted")
	}
}

func TestCheckpointPeriodicAndFinalSave(t *testing.T) {
	cfg := checkpointConfig(t, validRunRow)
	tracker, err := openCheckpoint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.close()
	tracker.lastSave = time.Now().Add(-2 * time.Hour)
	if err := tracker.complete(14); err != nil {
		t.Fatal(err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 14 {
		t.Fatalf("periodic row=%d", got)
	}
	if err := tracker.complete(15); err != nil {
		t.Fatal(err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 14 {
		t.Fatal("interval ignored")
	}
	if err := tracker.save(); err != nil {
		t.Fatal(err)
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 15 {
		t.Fatalf("final row=%d", got)
	}
	tracker.state.LastCompletedRow = 16
	tracker.dirty = true
	tracker.path = filepath.Join(t.TempDir(), "absent", "checkpoint.json")
	if err := tracker.save(); err == nil {
		t.Fatal("write failure ignored")
	}
	if got := checkpointRow(t, cfg.checkpointPath); got != 15 {
		t.Fatal("previous checkpoint damaged")
	}
}

func TestCheckpointInvalidRowCannotSkipUnpublishedEvents(t *testing.T) {
	for _, row := range []uint64{13, 99} {
		cfg := checkpointConfig(t, validRunRow)
		tracker, err := openCheckpoint(cfg)
		if err != nil {
			t.Fatal(err)
		}
		tracker.state.LastCompletedRow = row
		tracker.dirty = true
		if err := tracker.save(); err != nil {
			t.Fatal(err)
		}
		tracker.close()
		cfg.resume = true
		client := &fakeRunClient{}
		if err := runWithClient(context.Background(), cfg, client, io.Discard); err == nil || len(client.records) != 0 {
			t.Fatalf("row=%d err=%v records=%d", row, err, len(client.records))
		}
	}
}

func TestCheckpointSaveFailureIsNotHiddenByCancellation(t *testing.T) {
	cfg := checkpointConfig(t, strings.Repeat(validRunRow, 2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeRunClient{afterProduce: func() {
		cancel()
		if err := os.Rename(filepath.Dir(cfg.checkpointPath), filepath.Dir(cfg.checkpointPath)+"-moved"); err != nil {
			t.Fatal(err)
		}
	}}
	err := runWithClient(ctx, cfg, client, io.Discard)
	if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "save producer checkpoint") {
		t.Fatalf("err=%v", err)
	}
}

func TestCheckpointFlags(t *testing.T) {
	for _, args := range [][]string{{"-resume"}, {"-checkpointInterval=0"}, {"-checkpointInterval=-1s"}, {"-checkpointInterval=bad"}} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	cfg, err := parseConfig([]string{"-checkpointPath= checkpoints/producer.json ", "-resume", "-checkpointInterval=2s"}, io.Discard)
	if err != nil || !cfg.resume || cfg.checkpointPath != "checkpoints/producer.json" || cfg.checkpointInterval != 2*time.Second {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}
