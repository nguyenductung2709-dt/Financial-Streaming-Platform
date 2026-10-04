package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

type SourceRow struct {
	Line    uint64
	Values  []string
	Columns map[string]int // read-only
}

func StreamCSV(filePath string, handle func(SourceRow) error) error {
	// Open the file
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open CSV %q: %w", filePath, err)
	}
	defer f.Close()

	// Create the CSV reader
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1 // skip FieldsPerRecord check, will validate rows against header
	reader.LazyQuotes = false

	// Read the CSV file
	var columns map[string]int
	maxRequiredIndex := -1
	requiredColumns := []string{
		"ID", "SecType", "Last", "Trading time", "Trading date",
	}

	for {
		record, err := reader.Read()

		// Stop the read process when encounter EOF
		if err == io.EOF {
			if columns == nil {
				return fmt.Errorf("CSV header is missing")
			}
			return nil
		}

		if err != nil {
			return fmt.Errorf("read CSV %q: %w", filePath, err)
		}

		line, _ := reader.FieldPos(0)

		// Skip lines 1-11 and line 13 (file and header descriptions)
		if line < 12 || line == 13 {
			continue
		}

		// Parse line 12 to columns, update maxRequiredIndex for later check
		if line == 12 {
			columns = make(map[string]int)
			for index, name := range record {
				columns[strings.TrimSpace(name)] = index
			}

			for _, name := range requiredColumns {
				index, ok := columns[name]
				if !ok {
					return fmt.Errorf("CSV header is missing column %q", name)
				}
				if index > maxRequiredIndex {
					maxRequiredIndex = index
				}
			}
			continue
		}

		if columns == nil {
			return fmt.Errorf("CSV header is missing at line 12")
		}

		if len(record) <= maxRequiredIndex {
			return fmt.Errorf("CSV line %d is missing required fields", line)
		}

		if err := handle(SourceRow{
			Line:    uint64(line),
			Values:  record,
			Columns: columns,
		}); err != nil {
			return fmt.Errorf("handle CSV line %d: %w", line, err)
		}
	}
}
