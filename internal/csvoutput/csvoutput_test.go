package csvoutput

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"gosmninfo.rasc.ch/internal/data"
	"gosmninfo.rasc.ch/internal/store"
)

func TestWrite(t *testing.T) {
	db, err := pebble.Open("test", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	records := []data.StationData{{
		Station:        "KLO",
		DateTime:       data.DateTime{EpochSeconds: 1700000000},
		AirTemperature: data.NullFloat64{Float64: 12.345, Valid: true},
	}}
	if err := store.Put(db, records); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}

	var output bytes.Buffer
	if err := Write(&output, db, store.Query{Station: "KLO"}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("output has %d lines:\n%s", len(lines), output.String())
	}
	if !strings.HasPrefix(lines[0], "Station/Location,Date,tre200s0") {
		t.Errorf("unexpected header: %s", lines[0])
	}
	if !strings.HasPrefix(lines[1], "KLO,2023-11-14T22:13:20.000Z,12.35,-") {
		t.Errorf("unexpected row: %s", lines[1])
	}
}
