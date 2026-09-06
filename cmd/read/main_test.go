package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
	"gosmninfo.rasc.ch/internal/store"
)

func TestRunFiltersAndLimitsOutput(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	records := []data.StationData{
		{Station: "KLO", DateTime: data.DateTime{EpochSeconds: 1700000000}},
		{Station: "KLO", DateTime: data.DateTime{EpochSeconds: 1700000600}},
		{Station: "BER", DateTime: data.DateTime{EpochSeconds: 1700000000}},
	}
	if err := store.Put(db, records); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var output bytes.Buffer
	if err := run(dbPath, "KLO", 1, &output); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("output has %d lines, want header and one row:\n%s", len(lines), output.String())
	}
	if !strings.HasPrefix(lines[1], "KLO,") {
		t.Fatalf("unexpected row: %s", lines[1])
	}
}
