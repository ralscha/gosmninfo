package fileutil

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAtomicallyReplacesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output.csv")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	err := WriteAtomically(path, 0o640, func(output io.Writer) error {
		_, err := io.WriteString(output, "new")
		return err
	})
	if err != nil {
		t.Fatalf("WriteAtomically() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("file = %q, want new", got)
	}
}

func TestWriteAtomicallyPreservesFileOnError(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "output.csv")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	wantErr := errors.New("write failed")
	err := WriteAtomically(path, 0o644, func(output io.Writer) error {
		_, _ = io.WriteString(output, "partial")
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("WriteAtomically() error = %v, want %v", err, wantErr)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != "old" {
		t.Fatalf("file = %q, want old", got)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("temporary file was not cleaned up: %v", entries)
	}
}
