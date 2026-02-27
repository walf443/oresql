package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteWithWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users WHERE id = 2")
	assert.Equal(t, "1 row deleted", result.Message)

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 2, "expected 2 rows after deleting one")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestDeleteMultipleRows(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users WHERE id > 1")
	assert.Equal(t, "2 rows deleted", result.Message)

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row after deleting two")
}

func TestDeleteNoMatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "DELETE FROM users WHERE id = 999")
	assert.Equal(t, "0 rows deleted", result.Message)

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row when no rows matched")
}

func TestDeleteNoWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "DELETE FROM users")
	assert.Equal(t, "2 rows deleted", result.Message)

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 0, "expected 0 rows after deleting all")
}

func TestDeleteWithIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users WHERE name = 'bob'")
	assert.Equal(t, "1 row deleted", result.Message)

	// Verify deletion
	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 2, "expected 2 rows after deleting bob")
	for _, row := range result.Rows {
		assert.NotEqual(t, "bob", row[1], "bob should have been deleted")
	}
}

func TestDeleteWithIndexIn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	result := run(t, exec, "DELETE FROM users WHERE name IN ('bob', 'dave')")
	assert.Equal(t, "2 rows deleted", result.Message)

	// Verify remaining rows
	result = run(t, exec, "SELECT * FROM users ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows after deleting bob and dave")
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, "charlie", result.Rows[1][1])
}

func TestDeleteOrderByLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	// DELETE with ORDER BY id ASC LIMIT 2 → deletes id=1 and id=2
	result := run(t, exec, "DELETE FROM users ORDER BY id LIMIT 2")
	assert.Equal(t, "2 rows deleted", result.Message)

	result = run(t, exec, "SELECT id, name FROM users ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows remaining after ORDER BY LIMIT delete")
	assert.Equal(t, int64(3), result.Rows[0][0])
	assert.Equal(t, int64(4), result.Rows[1][0])
}

func TestDeleteOrderByDescLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	// DELETE with ORDER BY id DESC LIMIT 1 → deletes id=3
	result := run(t, exec, "DELETE FROM users ORDER BY id DESC LIMIT 1")
	assert.Equal(t, "1 row deleted", result.Message)

	result = run(t, exec, "SELECT id FROM users ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows remaining after DESC LIMIT delete")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
}

func TestDeleteWhereOrderByLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	// WHERE id > 1 → {2,3,4}, ORDER BY id DESC → {4,3,2}, LIMIT 2 → deletes {4,3}
	result := run(t, exec, "DELETE FROM users WHERE id > 1 ORDER BY id DESC LIMIT 2")
	assert.Equal(t, "2 rows deleted", result.Message)

	result = run(t, exec, "SELECT id FROM users ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows remaining after WHERE ORDER BY LIMIT delete")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
}

func TestDeleteNoWhereWithIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users")
	assert.Equal(t, "3 rows deleted", result.Message)

	// Verify all rows deleted
	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 0, "expected 0 rows after deleting all with index")
}
