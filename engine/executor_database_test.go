package engine

import (
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

func TestCrossDatabaseSelect(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	// Create table in default DB
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// Create another DB with its own table
	run(t, exec, "CREATE DATABASE otherdb")
	run(t, exec, "USE otherdb")
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// Cross-DB SELECT from otherdb to default (keyword DB name + DOT)
	result := run(t, exec, "SELECT * FROM default.users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])

	// Switch back and cross-DB SELECT the other way
	run(t, exec, "USE default")
	result = run(t, exec, "SELECT * FROM otherdb.users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])
	assert.Equal(t, "bob", result.Rows[0][1])
}

func TestCrossDatabaseInsert(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE otherdb")
	run(t, exec, "USE otherdb")
	run(t, exec, "CREATE TABLE logs (id INT, msg TEXT)")

	// Switch back to default and insert into otherdb
	run(t, exec, "USE default")
	run(t, exec, "INSERT INTO otherdb.logs VALUES (1, 'hello')")

	// Verify by selecting from otherdb
	result := run(t, exec, "SELECT * FROM otherdb.logs")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "hello", result.Rows[0][1])
}

func TestCrossDatabaseUpdate(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE otherdb")
	run(t, exec, "USE otherdb")
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// Switch back and update otherdb
	run(t, exec, "USE default")
	result := run(t, exec, "UPDATE otherdb.users SET name = 'bob' WHERE id = 1")
	assert.Equal(t, "1 row updated", result.Message)

	// Verify
	result = run(t, exec, "SELECT * FROM otherdb.users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "bob", result.Rows[0][1])
}

func TestCrossDatabaseDelete(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE otherdb")
	run(t, exec, "USE otherdb")
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// Switch back and delete from otherdb
	run(t, exec, "USE default")
	result := run(t, exec, "DELETE FROM otherdb.users WHERE id = 1")
	assert.Equal(t, "1 row deleted", result.Message)

	// Verify
	result = run(t, exec, "SELECT * FROM otherdb.users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCrossDatabaseJoin(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	// Create users in db1
	run(t, exec, "CREATE DATABASE db1")
	run(t, exec, "USE db1")
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// Create orders in db2
	run(t, exec, "CREATE DATABASE db2")
	run(t, exec, "USE db2")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, total INT)")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 200)")

	// Cross-DB JOIN using aliases
	result := run(t, exec, "SELECT u.name, o.total FROM db1.users u JOIN db2.orders o ON u.id = o.user_id")
	require.Len(t, result.Rows, 2)
}

func TestCrossDatabaseNonExistentDB(t *testing.T) {
	exec := newTestExecutorWithDBManager()
	runExpectError(t, exec, "SELECT * FROM nonexistent.users")
}

func TestCrossDatabaseWithoutDBManager(t *testing.T) {
	// Executor without DatabaseManager
	exec := NewExecutor(NewDatabase("test"))
	exec.ExecuteSQL("CREATE TABLE users (id INT)")
	runExpectError(t, exec, "SELECT * FROM otherdb.users")
}

func TestCrossDatabaseCreateTable(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE otherdb")
	run(t, exec, "CREATE TABLE otherdb.items (id INT, name TEXT)")

	// Verify by selecting from otherdb
	run(t, exec, "INSERT INTO otherdb.items VALUES (1, 'widget')")
	result := run(t, exec, "SELECT * FROM otherdb.items")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "widget", result.Rows[0][1])
}

func TestCrossDatabaseDropTable(t *testing.T) {
	exec := newTestExecutorWithDBManager()

	run(t, exec, "CREATE DATABASE otherdb")
	run(t, exec, "USE otherdb")
	run(t, exec, "CREATE TABLE items (id INT)")
	run(t, exec, "USE default")

	result := run(t, exec, "DROP TABLE otherdb.items")
	assert.Equal(t, "table dropped", result.Message)

	// Verify it's gone
	runExpectError(t, exec, "SELECT * FROM otherdb.items")
}
