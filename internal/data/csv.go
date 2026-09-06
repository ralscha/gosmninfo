package data

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
)

var utf8BOM = []byte{0xef, 0xbb, 0xbf}

// ParseCSV parses and validates a MeteoSwiss VQHA80 document.
func ParseCSV(input []byte) ([]StationData, error) {
	input = bytes.TrimPrefix(input, utf8BOM)
	reader := csv.NewReader(bytes.NewReader(input))
	reader.Comma = ';'
	headerIndexes, err := readCSVHeader(reader)
	if err != nil {
		return nil, err
	}

	var records []StationData
	seen := make(map[string]int)
	for rowNumber := 2; ; rowNumber++ {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV row %d: %w", rowNumber, err)
		}

		var record StationData
		record.Station = strings.TrimSpace(row[headerIndexes[csvHeader[0]]])
		if err := record.DateTime.UnmarshalCSV(row[headerIndexes[csvHeader[1]]]); err != nil {
			return nil, fmt.Errorf("CSV row %d column %q: %w", rowNumber, csvHeader[1], err)
		}
		for i, field := range record.floatFields() {
			column := csvHeader[i+2]
			if err := field.UnmarshalCSV(row[headerIndexes[column]]); err != nil {
				return nil, fmt.Errorf("CSV row %d column %q: %w", rowNumber, column, err)
			}
		}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("CSV row %d: %w", rowNumber, err)
		}

		key := string(record.Key())
		if previousRow, exists := seen[key]; exists {
			return nil, fmt.Errorf("CSV row %d duplicates row %d (%q)", rowNumber, previousRow, key)
		}
		seen[key] = rowNumber
		records = append(records, record)
	}
	if len(records) == 0 {
		return nil, errors.New("CSV contains no measurements")
	}
	return records, nil
}

func readCSVHeader(reader *csv.Reader) (map[string]int, error) {
	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return nil, errors.New("CSV is empty")
	}
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}

	indexes := make(map[string]int, len(header))
	for index, name := range header {
		name = strings.TrimSpace(name)
		if _, exists := indexes[name]; exists {
			return nil, fmt.Errorf("CSV header %q appears more than once", name)
		}
		indexes[name] = index
	}
	for _, required := range csvHeader {
		if _, ok := indexes[required]; !ok {
			return nil, fmt.Errorf("CSV is missing required header %q", required)
		}
	}
	return indexes, nil
}
