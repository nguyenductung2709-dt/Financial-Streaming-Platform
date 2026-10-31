package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"time"
)

type checkpointIdentity struct {
	SHA256     string   `json:"source_sha256"`
	Size       int64    `json:"source_size"`
	Brokers    []string `json:"brokers"`
	Topic      string   `json:"topic"`
	DLQTopic   string   `json:"dlq_topic"`
	DLQEnabled bool     `json:"dlq_enabled"`
}

type producerCheckpoint struct {
	Version          int                `json:"version"`
	Identity         checkpointIdentity `json:"identity"`
	LastCompletedRow uint64             `json:"last_completed_row"`
}

type checkpointTracker struct {
	path        string
	lock        *os.File
	state       producerCheckpoint
	resumeRow   uint64
	foundResume bool
	interval    time.Duration
	lastSave    time.Time
	dirty       bool
}

func checkpointSourceIdentity(cfg config) (checkpointIdentity, error) {
	f, err := os.Open(cfg.csvPath)
	if err != nil {
		return checkpointIdentity{}, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return checkpointIdentity{}, err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return checkpointIdentity{}, err
	}
	after, err := f.Stat()
	if err != nil {
		return checkpointIdentity{}, err
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return checkpointIdentity{}, fmt.Errorf("CSV changed while calculating its checkpoint identity")
	}
	brokers := append([]string(nil), cfg.brokers...)
	sort.Strings(brokers)
	return checkpointIdentity{hex.EncodeToString(hash.Sum(nil)), after.Size(), brokers, cfg.topic, cfg.dlqTopic, cfg.dlqEnabled}, nil
}

func openCheckpoint(cfg config) (*checkpointTracker, error) {
	if cfg.checkpointPath == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(cfg.checkpointPath), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(cfg.checkpointPath+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("checkpoint is already in use: %w", err)
	}
	tracker := &checkpointTracker{path: cfg.checkpointPath, lock: lock, interval: cfg.checkpointInterval}
	ok := false
	defer func() {
		if !ok {
			tracker.close()
		}
	}()
	if tracker.interval == 0 {
		tracker.interval = time.Second
	}
	if tracker.interval < 0 {
		return nil, fmt.Errorf("checkpoint interval must be positive")
	}
	identity, err := checkpointSourceIdentity(cfg)
	if err != nil {
		return nil, fmt.Errorf("identify CSV: %w", err)
	}
	if cfg.resume {
		state, err := loadCheckpoint(cfg.checkpointPath)
		if err != nil {
			return nil, err
		}
		if state.Version != 1 {
			return nil, fmt.Errorf("unsupported checkpoint version %d", state.Version)
		}
		if !reflect.DeepEqual(state.Identity, identity) {
			return nil, fmt.Errorf("checkpoint CSV contents or destination settings do not match this run")
		}
		tracker.state = state
		tracker.resumeRow = state.LastCompletedRow
	} else {
		if _, err := os.Stat(cfg.checkpointPath); err == nil {
			return nil, fmt.Errorf("checkpoint already exists; use -resume or choose a new -checkpointPath")
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		tracker.state = producerCheckpoint{Version: 1, Identity: identity}
		tracker.dirty = true
		if err := tracker.save(); err != nil {
			return nil, err
		}
	}
	tracker.foundResume = tracker.resumeRow == 0
	ok = true
	return tracker, nil
}

func loadCheckpoint(path string) (producerCheckpoint, error) {
	f, err := os.Open(path)
	if err != nil {
		return producerCheckpoint{}, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var state producerCheckpoint
	if err := decoder.Decode(&state); err != nil {
		return state, fmt.Errorf("decode checkpoint: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return state, fmt.Errorf("checkpoint contains trailing data")
	}
	return state, nil
}

func (c *checkpointTracker) skip(row uint64) (bool, error) {
	if c == nil {
		return false, nil
	}
	if row == c.resumeRow {
		c.foundResume = true
	}
	if row > c.resumeRow && !c.foundResume {
		return false, fmt.Errorf("checkpoint row %d is not a CSV record boundary", c.resumeRow)
	}
	return row <= c.resumeRow, nil
}

// Call only after a successful broker acknowledgment or an intentional skip.
func (c *checkpointTracker) complete(row uint64) error {
	if c == nil {
		return nil
	}
	c.state.LastCompletedRow = row
	c.dirty = true
	if time.Since(c.lastSave) >= c.interval {
		return c.save()
	}
	return nil
}

func (c *checkpointTracker) save() error {
	if c == nil || !c.dirty {
		return nil
	}
	dir := filepath.Dir(c.path)
	f, err := os.CreateTemp(dir, ".producer-checkpoint-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := json.NewEncoder(f).Encode(c.state); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), c.path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return err
	}
	c.lastSave = time.Now()
	c.dirty = false
	return nil
}

func (c *checkpointTracker) close() {
	if c != nil {
		c.lock.Close()
	}
}
