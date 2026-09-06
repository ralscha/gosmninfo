package store

import (
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"gosmninfo.rasc.ch/internal/data"
)

func TestPutAndIterate(t *testing.T) {
	db := openTestDB(t)
	records := []data.StationData{
		measurement("AB", 1700000000, 1),
		measurement("AB", 1700000600, 2),
		measurement("AB-C", 1700000000, 3),
		measurement("CD", 1700000000, 4),
	}
	if err := Put(db, records); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	var got []data.StationData
	err := Iterate(db, Query{Station: "AB", Limit: 1}, func(record data.StationData) error {
		got = append(got, record)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate() error = %v", err)
	}
	if len(got) != 1 || got[0].Station != "AB" {
		t.Fatalf("Iterate() records = %+v, want one exact AB match", got)
	}
}

func TestPutRejectsInvalidRecordWithoutWriting(t *testing.T) {
	db := openTestDB(t)
	records := []data.StationData{
		measurement("AB", 1700000000, 1),
		measurement("", 1700000600, 2),
	}
	if err := Put(db, records); err == nil {
		t.Fatal("Put() expected an error")
	}

	count := 0
	if err := Iterate(db, Query{}, func(data.StationData) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Iterate() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("database contains %d records after rejected batch", count)
	}
}

func TestIterateReportsCorruptRecord(t *testing.T) {
	db := openTestDB(t)
	if err := db.Set([]byte("AB-1700000000"), []byte("bad"), pebble.Sync); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	err := Iterate(db, Query{}, func(data.StationData) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "decode value") {
		t.Fatalf("Iterate() error = %v, want corrupt-value error", err)
	}
}

func TestIterateRejectsNegativeLimit(t *testing.T) {
	db := openTestDB(t)
	if err := Iterate(db, Query{Limit: -1}, func(data.StationData) error { return nil }); err == nil {
		t.Fatal("Iterate() expected an error")
	}
}

func openTestDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open("test", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return db
}

func measurement(station string, epoch int64, temperature float64) data.StationData {
	return data.StationData{
		Station:        station,
		DateTime:       data.DateTime{EpochSeconds: epoch},
		AirTemperature: data.NullFloat64{Float64: temperature, Valid: true},
	}
}
