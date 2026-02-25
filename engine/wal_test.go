package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	// Phase 1: Execute statements with WAL
	wal1, err := NewWAL(walPath)
	require.NoError(t, err, "failed to create WAL")
	exec1 := NewExecutor(WithWAL(wal1))

	_, err = exec1.ExecuteSQL("CREATE TABLE users (id INT, name TEXT)")
	require.NoError(t, err, "CREATE TABLE error")
	_, err = exec1.ExecuteSQL("INSERT INTO users VALUES (1, 'alice')")
	require.NoError(t, err, "INSERT error")
	_, err = exec1.ExecuteSQL("INSERT INTO users VALUES (2, 'bob')")
	require.NoError(t, err, "INSERT error")
	wal1.Close()

	// Phase 2: Create new executor and replay WAL
	wal2, err := NewWAL(walPath)
	require.NoError(t, err, "failed to open WAL")
	defer wal2.Close()
	exec2 := NewExecutor(WithWAL(wal2))

	err = exec2.ReplayWAL()
	require.NoError(t, err, "ReplayWAL error")

	// Verify state was restored
	result, err := exec2.ExecuteSQL("SELECT * FROM users")
	require.NoError(t, err, "SELECT error")
	require.Len(t, result.Rows, 2, "expected 2 rows after WAL replay")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[1][0])
}

func TestWALReplayEmpty(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "empty.wal")

	wal, err := NewWAL(walPath)
	require.NoError(t, err, "failed to create WAL")
	defer wal.Close()

	exec := NewExecutor(WithWAL(wal))
	err = exec.ReplayWAL()
	require.NoError(t, err, "ReplayWAL on empty file should succeed")
}

func TestWALReplayError(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "bad.wal")

	// Write invalid SQL directly to the WAL file
	err := os.WriteFile(walPath, []byte("INVALID SQL STATEMENT\n"), 0644)
	require.NoError(t, err, "failed to write WAL file")

	wal, err := NewWAL(walPath)
	require.NoError(t, err, "failed to open WAL")
	defer wal.Close()

	exec := NewExecutor(WithWAL(wal))
	err = exec.ReplayWAL()
	require.Error(t, err, "expected error during replay of invalid WAL")
}

func TestWALSelectNotLogged(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(walPath)
	require.NoError(t, err, "failed to create WAL")

	exec := NewExecutor(WithWAL(wal))

	// Execute a SELECT (should not be logged)
	_, err = exec.ExecuteSQL("SELECT 1")
	require.NoError(t, err, "SELECT error")
	wal.Close()

	// Read WAL file and verify it's empty
	data, err := os.ReadFile(walPath)
	require.NoError(t, err, "failed to read WAL file")
	assert.Empty(t, data, "expected empty WAL file")
}

func TestNewExecutorBackwardCompat(t *testing.T) {
	exec := NewExecutor()

	result, err := exec.ExecuteSQL("SELECT 1")
	require.NoError(t, err, "ExecuteSQL error")
	assert.Equal(t, int64(1), result.Rows[0][0])

	// ReplayWAL with no WAL should be a no-op
	err = exec.ReplayWAL()
	require.NoError(t, err, "ReplayWAL without WAL should succeed")
}

func TestWALCommentLines(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "comments.wal")

	// Write WAL with comments and blank lines
	content := "-- This is a comment\n\nCREATE TABLE t (id INT)\n-- Another comment\nINSERT INTO t VALUES (42)\n\n"
	err := os.WriteFile(walPath, []byte(content), 0644)
	require.NoError(t, err, "failed to write WAL file")

	wal, err := NewWAL(walPath)
	require.NoError(t, err, "failed to open WAL")
	defer wal.Close()

	exec := NewExecutor(WithWAL(wal))
	err = exec.ReplayWAL()
	require.NoError(t, err, "ReplayWAL error")

	result, err := exec.ExecuteSQL("SELECT * FROM t")
	require.NoError(t, err, "SELECT error")
	require.Len(t, result.Rows, 1, "expected 1 row after replaying WAL with comments")
	assert.Equal(t, int64(42), result.Rows[0][0])
}
