package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
	"gosmninfo.rasc.ch/internal/store"
)

func TestRunExportsSelectedStation(t *testing.T) {
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	records := []data.StationData{
		{Station: "KLO", DateTime: data.DateTime{EpochSeconds: 1700000000}},
		{Station: "BER", DateTime: data.DateTime{EpochSeconds: 1700000000}},
	}
	if err := store.Put(db, records); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	outputPath := filepath.Join(directory, "klo.csv")
	if err := run(dbPath, outputPath, "KLO"); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	output, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(output), "\nKLO,") || strings.Contains(string(output), "\nBER,") {
		t.Fatalf("unexpected filtered export:\n%s", output)
	}
}
