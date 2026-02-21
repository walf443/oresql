package engine

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// WAL implements write-ahead logging for SQL statements.
type WAL struct {
	file *os.File
}

// NewWAL opens (or creates) a WAL file at the given path.
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	return &WAL{file: f}, nil
}

// Append writes a SQL statement to the WAL file.
func (w *WAL) Append(sql string) error {
	_, err := fmt.Fprintln(w.file, sql)
	if err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay reads the WAL file from the beginning and executes each statement via execFn.
// Empty lines and lines starting with "--" are skipped.
func (w *WAL) Replay(execFn func(sql string) error) error {
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek WAL file: %w", err)
	}
	scanner := bufio.NewScanner(w.file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		if err := execFn(line); err != nil {
			return fmt.Errorf("WAL replay error at line %d (%q): %w", lineNo, line, err)
		}
	}
	return scanner.Err()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
