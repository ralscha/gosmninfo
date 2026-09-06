// Package csvoutput streams stored measurements as CSV.
package csvoutput

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
	"gosmninfo.rasc.ch/internal/store"
)

// Write writes a header followed by all records matching query.
func Write(output io.Writer, db *pebble.DB, query store.Query) error {
	writer := csv.NewWriter(output)
	if err := writer.Write(data.CSVHeader()); err != nil {
		return fmt.Errorf("write CSV header: %w", err)
	}

	iterateErr := store.Iterate(db, query, func(record data.StationData) error {
		row, err := record.CSVRecord()
		if err != nil {
			return err
		}
		return writer.Write(row)
	})
	writer.Flush()
	return errors.Join(iterateErr, writer.Error())
}
