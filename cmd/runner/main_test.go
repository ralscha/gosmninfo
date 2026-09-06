package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
	"gosmninfo.rasc.ch/internal/store"
)

const testCSV = "Station/Location;Date;tre200s0;rre150z0;sre000z0;gre000z0;ure200s0;tde200s0;dkl010z0;fu3010z0;fu3010z1;prestas0;pp0qffs0;pp0qnhs0;ppz850s0;ppz700s0;dv1towz0;fu3towz0;fu3towz1;ta1tows0;uretows0;tdetows0\n" +
	"KLO;202609061230;12.34;0.00;-;100.00;65.00;6.10;180.00;4.30;7.20;950.00;1010.00;1011.00;-;-;-;-;-;-;-;-\n"

func TestRunImportsAndSnapshotsValidatedDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(testCSV))
	}))
	defer server.Close()

	directory := t.TempDir()
	dbPath := filepath.Join(directory, "db")
	csvPath := filepath.Join(directory, "data.csv")
	count, err := run(context.Background(), server.Client(), config{
		URL: server.URL, DBPath: dbPath, CSVPath: csvPath,
	})
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("run() count = %d, want 1", count)
	}
	snapshot, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(snapshot) != testCSV {
		t.Fatal("saved snapshot differs from download")
	}

	db, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	stored := 0
	if err := store.Iterate(db, store.Query{}, func(record data.StationData) error {
		stored++
		if record.Station != "KLO" {
			t.Errorf("stored station = %q", record.Station)
		}
		return nil
	}); err != nil {
		t.Fatalf("store.Iterate() error = %v", err)
	}
	if stored != 1 {
		t.Fatalf("stored records = %d, want 1", stored)
	}
}

func TestRunDoesNotReplaceSnapshotOrCreateDBForInvalidCSV(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("not,a,meteoswiss,csv\n"))
	}))
	defer server.Close()

	directory := t.TempDir()
	dbPath := filepath.Join(directory, "db")
	csvPath := filepath.Join(directory, "data.csv")
	if err := os.WriteFile(csvPath, []byte("last-good"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := run(context.Background(), server.Client(), config{
		URL: server.URL, DBPath: dbPath, CSVPath: csvPath,
	}); err == nil {
		t.Fatal("run() expected an error")
	}
	snapshot, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(snapshot) != "last-good" {
		t.Fatalf("snapshot = %q, want last-good", snapshot)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("database path exists after validation failure: %v", err)
	}
}

func TestDownloadWithRetryRetriesServerErrors(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			http.Error(response, "try again", http.StatusServiceUnavailable)
			return
		}
		_, _ = response.Write([]byte(testCSV))
	}))
	defer server.Close()

	body, err := downloadWithRetry(context.Background(), server.Client(), config{
		URL: server.URL, Retries: 2,
	})
	if err != nil {
		t.Fatalf("downloadWithRetry() error = %v", err)
	}
	if string(body) != testCSV {
		t.Fatal("downloaded body differs")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestDownloadWithRetryDoesNotRetryClientError(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.NotFound(response, nil)
	}))
	defer server.Close()

	if _, err := downloadWithRetry(context.Background(), server.Client(), config{
		URL: server.URL, Retries: 3,
	}); err == nil {
		t.Fatal("downloadWithRetry() expected an error")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}
