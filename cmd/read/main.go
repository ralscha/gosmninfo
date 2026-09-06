package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/csvoutput"
	"gosmninfo.rasc.ch/internal/store"
)

func main() {
	dbPath := flag.String("db", "smninfo", "Pebble database path")
	station := flag.String("station", "", "show only this station")
	limit := flag.Int("limit", 0, "maximum records to show (0 means all)")
	flag.Parse()

	if err := run(*dbPath, *station, *limit, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(dbPath, station string, limit int, output io.Writer) (err error) {
	if dbPath == "" {
		return fmt.Errorf("database path is empty")
	}
	db, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close database: %w", closeErr))
		}
	}()

	readErr := csvoutput.Write(output, db, store.Query{Station: station, Limit: limit})
	return readErr
}
