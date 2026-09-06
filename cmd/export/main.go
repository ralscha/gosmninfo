package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/csvoutput"
	"gosmninfo.rasc.ch/internal/fileutil"
	"gosmninfo.rasc.ch/internal/store"
)

func main() {
	dbPath := flag.String("db", "smninfo", "Pebble database path")
	outputPath := flag.String("out", "smninfo.csv", "output CSV path")
	station := flag.String("station", "", "export only this station")
	flag.Parse()

	if err := run(*dbPath, *outputPath, *station); err != nil {
		log.Fatal(err)
	}
}

func run(dbPath, outputPath, station string) (err error) {
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

	write := func(output io.Writer) error {
		return csvoutput.Write(output, db, store.Query{Station: station})
	}
	if err := fileutil.WriteAtomically(outputPath, 0o644, write); err != nil {
		return fmt.Errorf("export CSV: %w", err)
	}
	return nil
}
