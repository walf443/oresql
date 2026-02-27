package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseInMemory(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
}

func TestDatabaseWithDataDir(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create database, tables, and insert data
	db1 := NewDatabase("test", WithDataDir(dir))
	exec1 := NewExecutor(db1)

	run(t, exec1, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec1, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec1, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec1, "CREATE INDEX idx_name ON users (name)")

	result := run(t, exec1, "SELECT * FROM users ORDER BY id")
	require.Len(t, result.Rows, 2)

	// Phase 2: Create new database from same directory - data should persist
	db2 := NewDatabase("test", WithDataDir(dir))
	exec2 := NewExecutor(db2)

	result = run(t, exec2, "SELECT * FROM users ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, "bob", result.Rows[1][1])

	// Index should be restored
	result = run(t, exec2, "SELECT name FROM users WHERE name = 'alice'")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "alice", result.Rows[0][0])
}

func TestDatabasePersistDeleteUpdate(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create, insert, update, delete
	db1 := NewDatabase("test", WithDataDir(dir))
	exec1 := NewExecutor(db1)

	run(t, exec1, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec1, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec1, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec1, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec1, "UPDATE users SET name = 'ALICE' WHERE id = 1")
	run(t, exec1, "DELETE FROM users WHERE id = 2")

	// Phase 2: Reload and verify
	db2 := NewDatabase("test", WithDataDir(dir))
	exec2 := NewExecutor(db2)

	result := run(t, exec2, "SELECT * FROM users ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "ALICE", result.Rows[0][1])
	assert.Equal(t, int64(3), result.Rows[1][0])
	assert.Equal(t, "charlie", result.Rows[1][1])
}

func TestDatabasePersistMultipleTables(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create multiple tables
	db1 := NewDatabase("test", WithDataDir(dir))
	exec1 := NewExecutor(db1)

	run(t, exec1, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec1, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	run(t, exec1, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec1, "INSERT INTO orders VALUES (100, 1, 500)")

	// Phase 2: Reload and verify
	db2 := NewDatabase("test", WithDataDir(dir))
	exec2 := NewExecutor(db2)

	result := run(t, exec2, "SELECT * FROM users")
	require.Len(t, result.Rows, 1)

	result = run(t, exec2, "SELECT * FROM orders")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(500), result.Rows[0][2])
}

func TestDatabasePersistDropTable(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table, then drop it
	db1 := NewDatabase("test", WithDataDir(dir))
	exec1 := NewExecutor(db1)

	run(t, exec1, "CREATE TABLE temp (id INT PRIMARY KEY)")
	run(t, exec1, "INSERT INTO temp VALUES (1)")
	run(t, exec1, "DROP TABLE temp")

	// Phase 2: Reload - table should not exist
	db2 := NewDatabase("test", WithDataDir(dir))
	exec2 := NewExecutor(db2)

	_, err := runWithError(exec2, "SELECT * FROM temp")
	require.Error(t, err)
}

func TestDatabasePersistWithFloat(t *testing.T) {
	dir := t.TempDir()

	db1 := NewDatabase("test", WithDataDir(dir))
	exec1 := NewExecutor(db1)

	run(t, exec1, "CREATE TABLE measures (id INT PRIMARY KEY, value FLOAT)")
	run(t, exec1, "INSERT INTO measures VALUES (1, 3.14)")
	run(t, exec1, "INSERT INTO measures VALUES (2, -2.71)")

	db2 := NewDatabase("test", WithDataDir(dir))
	exec2 := NewExecutor(db2)

	result := run(t, exec2, "SELECT * FROM measures ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, float64(3.14), result.Rows[0][1])
	assert.Equal(t, float64(-2.71), result.Rows[1][1])
}

func TestDatabasePersistWithNull(t *testing.T) {
	dir := t.TempDir()

	db1 := NewDatabase("test", WithDataDir(dir))
	exec1 := NewExecutor(db1)

	run(t, exec1, "CREATE TABLE data (id INT PRIMARY KEY, value TEXT)")
	run(t, exec1, "INSERT INTO data VALUES (1, NULL)")
	run(t, exec1, "INSERT INTO data VALUES (2, 'hello')")

	db2 := NewDatabase("test", WithDataDir(dir))
	exec2 := NewExecutor(db2)

	result := run(t, exec2, "SELECT * FROM data ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Nil(t, result.Rows[0][1])
	assert.Equal(t, "hello", result.Rows[1][1])
}
