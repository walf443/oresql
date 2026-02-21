package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	// Phase 1: Execute statements with WAL
	wal1, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %s", err)
	}
	exec1 := NewExecutor(WithWAL(wal1))

	if _, err := exec1.ExecuteSQL("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error: %s", err)
	}
	if _, err := exec1.ExecuteSQL("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("INSERT error: %s", err)
	}
	if _, err := exec1.ExecuteSQL("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("INSERT error: %s", err)
	}
	wal1.Close()

	// Phase 2: Create new executor and replay WAL
	wal2, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %s", err)
	}
	defer wal2.Close()
	exec2 := NewExecutor(WithWAL(wal2))

	if err := exec2.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL error: %s", err)
	}

	// Verify state was restored
	result, err := exec2.ExecuteSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT error: %s", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("expected name='alice', got %v", result.Rows[0][1])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[1][0])
	}
}

func TestWALReplayEmpty(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "empty.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %s", err)
	}
	defer wal.Close()

	exec := NewExecutor(WithWAL(wal))
	if err := exec.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL on empty file should succeed, got: %s", err)
	}
}

func TestWALReplayError(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "bad.wal")

	// Write invalid SQL directly to the WAL file
	if err := os.WriteFile(walPath, []byte("INVALID SQL STATEMENT\n"), 0644); err != nil {
		t.Fatalf("failed to write WAL file: %s", err)
	}

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %s", err)
	}
	defer wal.Close()

	exec := NewExecutor(WithWAL(wal))
	if err := exec.ReplayWAL(); err == nil {
		t.Fatal("expected error during replay of invalid WAL, got nil")
	}
}

func TestWALSelectNotLogged(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %s", err)
	}

	exec := NewExecutor(WithWAL(wal))

	// Execute a SELECT (should not be logged)
	if _, err := exec.ExecuteSQL("SELECT 1"); err != nil {
		t.Fatalf("SELECT error: %s", err)
	}
	wal.Close()

	// Read WAL file and verify it's empty
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed to read WAL file: %s", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty WAL file, got %q", string(data))
	}
}

func TestNewExecutorBackwardCompat(t *testing.T) {
	exec := NewExecutor()

	result, err := exec.ExecuteSQL("SELECT 1")
	if err != nil {
		t.Fatalf("ExecuteSQL error: %s", err)
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected 1, got %v", result.Rows[0][0])
	}

	// ReplayWAL with no WAL should be a no-op
	if err := exec.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL without WAL should succeed, got: %s", err)
	}
}

func TestWALCommentLines(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "comments.wal")

	// Write WAL with comments and blank lines
	content := "-- This is a comment\n\nCREATE TABLE t (id INT)\n-- Another comment\nINSERT INTO t VALUES (42)\n\n"
	if err := os.WriteFile(walPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write WAL file: %s", err)
	}

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %s", err)
	}
	defer wal.Close()

	exec := NewExecutor(WithWAL(wal))
	if err := exec.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL error: %s", err)
	}

	result, err := exec.ExecuteSQL("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT error: %s", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(42) {
		t.Errorf("expected 42, got %v", result.Rows[0][0])
	}
}
