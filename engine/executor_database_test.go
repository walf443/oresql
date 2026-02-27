package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestExecutorWithDBManager() *Executor {
	mgr := NewDatabaseManager("")
	db, _ := mgr.GetDatabase("default")
	return NewExecutor(db, WithDatabaseManager(mgr))
}

func TestCreateDatabase(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	result := run(t, exec, "CREATE DATABASE testdb")
	assert.Equal(t, "database created", result.Message)

	// Verify it shows up in SHOW DATABASES
	result = run(t, exec, "SHOW DATABASES")
	require.Len(t, result.Rows, 2)
	names := []string{result.Rows[0][0].(string), result.Rows[1][0].(string)}
	assert.Contains(t, names, "default")
	assert.Contains(t, names, "testdb")
}

func TestCreateDatabaseDuplicate(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE testdb")
	runExpectError(t, exec, "CREATE DATABASE testdb")
}

func TestCreateDatabaseCaseInsensitive(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE TestDB")
	// Same name different case should fail
	runExpectError(t, exec, "CREATE DATABASE testdb")
}

func TestDropDatabase(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE testdb")
	result := run(t, exec, "DROP DATABASE testdb")
	assert.Equal(t, "database dropped", result.Message)

	// Verify it's gone from SHOW DATABASES
	result = run(t, exec, "SHOW DATABASES")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "default", result.Rows[0][0].(string))
}

func TestDropDatabaseNonExistent(t *testing.T) {
	exec := newTestExecutorWithDBManager()
	runExpectError(t, exec, "DROP DATABASE nonexistent")
}

func TestDropDatabaseDefault(t *testing.T) {
	exec := newTestExecutorWithDBManager()
	runExpectError(t, exec, "DROP DATABASE default")
}

func TestDropDatabaseActive(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE testdb")
	run(t, exec, "USE testdb")

	// Cannot drop the currently active database
	runExpectError(t, exec, "DROP DATABASE testdb")
}

func TestUseDatabase(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE testdb")

	result := run(t, exec, "USE testdb")
	assert.Contains(t, result.Message, "testdb")

	assert.Equal(t, "testdb", exec.CurrentDatabaseName())
}

func TestUseDatabaseNonExistent(t *testing.T) {
	exec := newTestExecutorWithDBManager()
	runExpectError(t, exec, "USE nonexistent")
}

func TestUseDatabaseCaseInsensitive(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE testdb")
	result := run(t, exec, "USE TESTDB")
	assert.Contains(t, result.Message, "testdb")
}

func TestShowDatabases(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	result := run(t, exec, "SHOW DATABASES")
	require.Len(t, result.Columns, 1)
	assert.Equal(t, "database", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "default", result.Rows[0][0].(string))
}

func TestDatabaseTableIsolation(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	// Create table in default database
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// Switch to new database
	run(t, exec, "CREATE DATABASE testdb")
	run(t, exec, "USE testdb")

	// Table should not exist in testdb
	runExpectError(t, exec, "SELECT * FROM users")

	// Create same table in testdb with different data
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])

	// Switch back to default
	run(t, exec, "USE default")
	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestShowTablesEmpty(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	result := run(t, exec, "SHOW TABLES")
	require.Len(t, result.Columns, 1)
	assert.Equal(t, "table", result.Columns[0])
	require.Len(t, result.Rows, 0)
}

func TestShowTables(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, total INT)")

	result := run(t, exec, "SHOW TABLES")
	require.Len(t, result.Columns, 1)
	assert.Equal(t, "table", result.Columns[0])
	require.Len(t, result.Rows, 2)
	// Results should be sorted
	assert.Equal(t, "orders", result.Rows[0][0].(string))
	assert.Equal(t, "users", result.Rows[1][0].(string))
}

func TestShowTablesAfterDrop(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	run(t, exec, "CREATE TABLE users (id INT)")
	run(t, exec, "CREATE TABLE orders (id INT)")
	run(t, exec, "DROP TABLE users")

	result := run(t, exec, "SHOW TABLES")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "orders", result.Rows[0][0].(string))
}

func TestDatabaseManagementNotEnabled(t *testing.T) {
	// Executor without DatabaseManager
	exec := NewExecutor(NewDatabase("test"))

	runExpectError(t, exec, "CREATE DATABASE testdb")
	runExpectError(t, exec, "DROP DATABASE testdb")
	runExpectError(t, exec, "USE testdb")
	runExpectError(t, exec, "SHOW DATABASES")
}

func TestDatabasePersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create database manager with persistence
	mgr := NewDatabaseManager(tmpDir)
	db, _ := mgr.GetDatabase("default")
	exec := NewExecutor(db, WithDatabaseManager(mgr))

	run(t, exec, "CREATE DATABASE testdb")
	run(t, exec, "USE testdb")
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, 'item1')")

	// Create a new manager from the same directory
	mgr2 := NewDatabaseManager(tmpDir)
	err := mgr2.LoadExistingDatabases()
	require.NoError(t, err)

	db2, err := mgr2.GetDatabase("testdb")
	require.NoError(t, err)
	exec2 := NewExecutor(db2, WithDatabaseManager(mgr2))

	result := run(t, exec2, "SELECT * FROM items")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestLegacyMigration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a .dat file in root (simulating legacy layout)
	legacyFile := filepath.Join(tmpDir, "users.dat")
	os.WriteFile(legacyFile, []byte("dummy"), 0644)

	mgr := NewDatabaseManager(tmpDir)
	mgr.LoadExistingDatabases()

	// The .dat file should be moved to default/
	_, err := os.Stat(legacyFile)
	assert.True(t, os.IsNotExist(err), "legacy file should have been moved")

	migratedFile := filepath.Join(tmpDir, "default", "users.dat")
	_, err = os.Stat(migratedFile)
	assert.NoError(t, err, "file should exist in default/ directory")
}
