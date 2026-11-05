package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ResolveCSVPaths preserves argument order and expands immediate .csv files in
// each directory in filename order. Validate every path before streaming starts.
func ResolveCSVPaths(inputs []string) ([]string, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("at least one CSV path is required")
	}
	var paths []string
	for _, input := range inputs {
		if strings.TrimSpace(input) == "" {
			return nil, fmt.Errorf("CSV path must not be empty")
		}
		info, err := os.Stat(input)
		if err != nil {
			return nil, fmt.Errorf("inspect CSV path %q: %w", input, err)
		}
		if info.Mode().IsRegular() {
			paths = append(paths, input)
			continue
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("CSV path %q is not a regular file or directory", input)
		}
		entries, err := os.ReadDir(input) // os.ReadDir sorts by filename.
		if err != nil {
			return nil, fmt.Errorf("list CSV directory %q: %w", input, err)
		}
		start := len(paths)
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".csv" {
				continue
			}
			path := filepath.Join(input, entry.Name())
			fileInfo, err := os.Stat(path)
			if err != nil {
				return nil, fmt.Errorf("inspect CSV path %q: %w", path, err)
			}
			if fileInfo.Mode().IsRegular() {
				paths = append(paths, path)
			}
		}
		if len(paths) == start {
			return nil, fmt.Errorf("CSV directory %q contains no .csv files", input)
		}
	}
	return paths, nil
}

// StreamCSVs reads one continuous sequence, with independent headers and
// cumulative physical line identifiers. The handler owns run-wide limits/state.
func StreamCSVs(inputs []string, handle func(SourceRow) error) error {
	paths, err := ResolveCSVPaths(inputs)
	if err != nil {
		return err
	}
	var base uint64
	for _, path := range paths {
		if err := streamCSV(path, &base, handle); err != nil {
			return fmt.Errorf("CSV %q: %w", path, err)
		}
	}
	return nil
}

// Count physical lines without a second pass, including blank lines and the
// final line of a file without a terminating newline. Used only after file EOF.
type csvLineReader struct {
	reader   io.Reader
	newlines uint64
	readAny  bool
	lastByte byte
}

func (r *csvLineReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.newlines += uint64(bytes.Count(p[:n], []byte{'\n'}))
		r.readAny = true
		r.lastByte = p[n-1]
	}
	return n, err
}

func (r *csvLineReader) lines() uint64 {
	if r.readAny && r.lastByte != '\n' {
		return r.newlines + 1
	}
	return r.newlines
}
