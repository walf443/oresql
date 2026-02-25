package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertMultipleRows(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")

	result := run(t, exec, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
	assert.Equal(t, "3 rows inserted", result.Message)

	result = run(t, exec, "SELECT COUNT(*) FROM users")
	assert.Equal(t, int64(3), result.Rows[0][0])

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][1])
	assert.Equal(t, "charlie", result.Rows[2][1])
}

func TestInsertNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	assert.Equal(t, "1 row inserted", result.Message)

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Nil(t, result.Rows[0][1])
}

func TestInsertNotNullSuccess(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	result := run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	assert.Equal(t, "1 row inserted", result.Message)
}

func TestInsertNotNullViolation(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES (NULL, 'alice')")
}

func TestInsertNullableColumnStillAllowsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	result := run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	assert.Equal(t, "1 row inserted", result.Message)

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Nil(t, result.Rows[0][1])
}

func TestErrorInsertNonexistentTable(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "INSERT INTO nonexistent VALUES (1)")
}

func TestErrorInsertWrongValueCount(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES (1)")
}

func TestErrorInsertTypeMismatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES ('not_int', 'alice')")
}

func TestInsertWithColumnsAllColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	assert.Equal(t, "1 row inserted", result.Message)

	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
}

func TestInsertWithColumnsReorder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users (name, id) VALUES ('alice', 1)")

	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
}

func TestInsertPartialColumnsWithDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT DEFAULT 'unknown')")
	run(t, exec, "INSERT INTO users (id) VALUES (1)")

	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "unknown", result.Rows[0][1])
}

func TestInsertPartialColumnsNoDefaultGetsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users (id) VALUES (1)")

	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Nil(t, result.Rows[0][1])
}

func TestErrorInsertPartialColumnsNotNullNoDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (name) VALUES ('alice')")
}

func TestInsertPartialColumnsNotNullWithDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL DEFAULT 0, name TEXT)")
	run(t, exec, "INSERT INTO users (name) VALUES ('alice')")

	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(0), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
}

func TestErrorInsertDuplicateColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (id, id) VALUES (1, 2)")
}

func TestErrorInsertNonexistentColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (id, foo) VALUES (1, 'bar')")
}

func TestErrorInsertColumnValueCountMismatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (id, name) VALUES (1)")
}

func TestInsertWithColumnsMultipleRows(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT DEFAULT 'unknown')")
	result := run(t, exec, "INSERT INTO users (id) VALUES (1), (2), (3)")
	assert.Equal(t, "3 rows inserted", result.Message)

	result = run(t, exec, "SELECT * FROM users ORDER BY id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	for i, row := range result.Rows {
		assert.Equal(t, int64(i+1), row[0], "row %d: expected id=%d", i, i+1)
		assert.Equal(t, "unknown", row[1], "row %d: expected name='unknown'", i)
	}
}

func TestInsertSelectBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE src (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE dst (id INT, name TEXT)")
	run(t, exec, "INSERT INTO src VALUES (1, 'alice'), (2, 'bob')")

	result := run(t, exec, "INSERT INTO dst SELECT * FROM src")
	assert.Equal(t, "2 rows inserted", result.Message)

	result = run(t, exec, "SELECT * FROM dst ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, "bob", result.Rows[1][1])
}

func TestInsertSelectWithColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE src (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE dst (id INT, name TEXT DEFAULT 'unknown')")
	run(t, exec, "INSERT INTO src VALUES (1, 'alice'), (2, 'bob')")

	result := run(t, exec, "INSERT INTO dst (id) SELECT id FROM src")
	assert.Equal(t, "2 rows inserted", result.Message)

	result = run(t, exec, "SELECT * FROM dst ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "unknown", result.Rows[0][1])
}

func TestInsertSelectWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE src (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE dst (id INT, name TEXT)")
	run(t, exec, "INSERT INTO src VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

	result := run(t, exec, "INSERT INTO dst SELECT * FROM src WHERE id >= 2")
	assert.Equal(t, "2 rows inserted", result.Message)

	result = run(t, exec, "SELECT * FROM dst ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestInsertSelectWithUnion(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT)")
	run(t, exec, "CREATE TABLE t2 (id INT)")
	run(t, exec, "CREATE TABLE dst (id INT)")
	run(t, exec, "INSERT INTO t1 VALUES (1), (2)")
	run(t, exec, "INSERT INTO t2 VALUES (3), (4)")

	result := run(t, exec, "INSERT INTO dst SELECT id FROM t1 UNION ALL SELECT id FROM t2")
	assert.Equal(t, "4 rows inserted", result.Message)

	result = run(t, exec, "SELECT * FROM dst ORDER BY id")
	require.Len(t, result.Rows, 4, "expected 4 rows")
}

func TestInsertSelectColumnCountMismatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE src (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE dst (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO src VALUES (1, 'alice')")

	runExpectError(t, exec, "INSERT INTO dst SELECT * FROM src")
}
