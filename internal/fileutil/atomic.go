// Package fileutil contains small filesystem helpers shared by commands.
package fileutil

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// WriteAtomically replaces path only after write and sync both succeed.
func WriteAtomically(path string, mode fs.FileMode, write func(io.Writer) error) error {
	if path == "" {
		return fmt.Errorf("output path is empty")
	}
	if write == nil {
		return fmt.Errorf("write function is nil")
	}

	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryPath := file.Name()
	keepTemporary := true
	defer func() {
		if keepTemporary {
			_ = file.Close()
			_ = os.Remove(temporaryPath)
		}
	}()

	if err := file.Chmod(mode); err != nil {
		return fmt.Errorf("set output permissions: %w", err)
	}
	if err := write(file); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary output: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary output: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace output: %w", err)
	}
	keepTemporary = false
	return nil
}
