package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectWhereEq(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT name FROM users WHERE id = 1")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
}

func TestSelectWhereGt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id > 1")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestSelectWhereAnd(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id = 1 AND name = 'alice'")
	require.Len(t, result.Rows, 1, "expected 1 row")
}

func TestSelectWhereOr(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id = 1 OR id = 3")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestSelectNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "SELECT * FROM users WHERE id = 999")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestSelectQualifiedColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT users.id, users.name FROM users")
	require.Len(t, result.Columns, 2, "expected 2 columns")
	assert.Equal(t, "id", result.Columns[0])
	assert.Equal(t, "name", result.Columns[1])
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestSelectQualifiedWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE users.id = 1")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestErrorSelectWrongTableQualifier(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "SELECT other.id FROM users")
}

func TestErrorWhereWrongTableQualifier(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	runExpectError(t, exec, "SELECT * FROM users WHERE other.id = 1")
}

func TestSelectCountStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT COUNT(*) FROM users")
	assert.Equal(t, "COUNT(*)", result.Columns[0])
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestSelectCountStarWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT COUNT(*) FROM users WHERE id > 1")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectCountStarEmpty(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT)")

	result := run(t, exec, "SELECT COUNT(*) FROM users")
	assert.Equal(t, int64(0), result.Rows[0][0])
}

func TestSelectCountColumnExcludesNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, NULL)")

	// COUNT(*) counts all rows including NULLs
	result := run(t, exec, "SELECT COUNT(*) FROM users")
	assert.Equal(t, int64(4), result.Rows[0][0])

	// COUNT(name) excludes NULLs
	result = run(t, exec, "SELECT COUNT(name) FROM users")
	assert.Equal(t, "COUNT(name)", result.Columns[0])
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectCountLiteral(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, NULL)")

	// COUNT(1) should count all rows (same as COUNT(*))
	result := run(t, exec, "SELECT COUNT(1) FROM users")
	assert.Equal(t, "COUNT(1)", result.Columns[0])
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestSelectWithoutFrom(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1
	result := run(t, exec, "SELECT 1")
	assert.Equal(t, "1", result.Columns[0])
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])

	// SELECT 1, 'hello'
	result = run(t, exec, "SELECT 1, 'hello'")
	assert.Equal(t, "1", result.Columns[0])
	assert.Equal(t, "'hello'", result.Columns[1])
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "hello", result.Rows[0][1])
}

func TestSelectAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT id AS user_id FROM users")
	assert.Equal(t, "user_id", result.Columns[0])
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectCountAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT COUNT(*) AS total FROM users")
	assert.Equal(t, "total", result.Columns[0])
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectLiteralAlias(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, "SELECT 1 AS one")
	assert.Equal(t, "one", result.Columns[0])
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectQuotedIdent(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (`count` INT)")
	run(t, exec, "INSERT INTO t VALUES (42)")

	result := run(t, exec, "SELECT `count` FROM t")
	assert.Equal(t, "count", result.Columns[0])
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(42), result.Rows[0][0])
}

func TestSelectArithmetic(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1 * 2 → 2
	result := run(t, exec, "SELECT 1 * 2")
	assert.Equal(t, int64(2), result.Rows[0][0])

	// SELECT 1 + 2 * 3 → 7 (precedence)
	result = run(t, exec, "SELECT 1 + 2 * 3")
	assert.Equal(t, int64(7), result.Rows[0][0])

	// SELECT 10 / 3 → 3 (integer division)
	result = run(t, exec, "SELECT 10 / 3")
	assert.Equal(t, int64(3), result.Rows[0][0])

	// SELECT 10 - 3 → 7
	result = run(t, exec, "SELECT 10 - 3")
	assert.Equal(t, int64(7), result.Rows[0][0])
}

func TestSelectArithmeticWithTable(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (price INT)")
	run(t, exec, "INSERT INTO items VALUES (10)")
	run(t, exec, "INSERT INTO items VALUES (20)")

	result := run(t, exec, "SELECT price * 2 FROM items")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(20), result.Rows[0][0])
	assert.Equal(t, int64(40), result.Rows[1][0])
}

func TestSelectArithmeticInWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (price INT)")
	run(t, exec, "INSERT INTO items VALUES (5)")
	run(t, exec, "INSERT INTO items VALUES (10)")
	run(t, exec, "INSERT INTO items VALUES (20)")

	result := run(t, exec, "SELECT price FROM items WHERE price * 2 > 15")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(10), result.Rows[0][0])
}

func TestErrorDivisionByZero(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "SELECT 1 / 0")
}

func TestSelectUnaryMinus(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, "SELECT -1")
	assert.Equal(t, int64(-1), result.Rows[0][0])

	result = run(t, exec, "SELECT -2 + 5")
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestErrorSelectNonexistentTable(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "SELECT * FROM nonexistent")
}

func TestErrorSelectNonexistentColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT)")
	runExpectError(t, exec, "SELECT foo FROM users")
}

func TestSelectWhereIsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT id FROM users WHERE name IS NULL")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectWhereIsNotNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT id FROM users WHERE name IS NOT NULL")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestNullComparisonReturnsFalse(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// NULL = 'bob' should be false (SQL semantics)
	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectOrderByAsc(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users ORDER BY id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, int64(3), result.Rows[2][0])
}

func TestSelectOrderByDesc(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users ORDER BY id DESC")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, int64(3), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, int64(1), result.Rows[2][0])
}

func TestSelectOrderByMultipleColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 20)")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice', 20)")

	result := run(t, exec, "SELECT * FROM users ORDER BY name ASC, age ASC")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// alice age=20 first, then alice age=30, then bob age=20
	assert.Equal(t, int64(3), result.Rows[0][0])
	assert.Equal(t, int64(1), result.Rows[1][0])
	assert.Equal(t, int64(2), result.Rows[2][0])
}

func TestSelectWhereOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id > 1 ORDER BY id DESC")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(3), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
}

func TestSelectOrderByWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	// NULLs should sort last in ASC
	result := run(t, exec, "SELECT * FROM users ORDER BY name ASC")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][1])
	assert.Nil(t, result.Rows[2][1])

	// NULLs should sort last in DESC too
	result = run(t, exec, "SELECT * FROM users ORDER BY name DESC")
	assert.Equal(t, "bob", result.Rows[0][1])
	assert.Equal(t, "alice", result.Rows[1][1])
	assert.Nil(t, result.Rows[2][1])
}

func TestSelectLimitOnly(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
}

func TestSelectOffsetOnly(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users OFFSET 1")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(2), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestSelectLimitOffset(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	result := run(t, exec, "SELECT * FROM users LIMIT 2 OFFSET 1")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(2), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestSelectOrderByLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users ORDER BY id ASC LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
}

func TestSelectOffsetExceedsRowCount(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users OFFSET 10")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestSelectGroupByBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users GROUP BY name")
	require.Len(t, result.Columns, 2, "expected 2 columns")
	assert.Equal(t, "name", result.Columns[0])
	assert.Equal(t, "COUNT(*)", result.Columns[1])
	require.Len(t, result.Rows, 2, "expected 2 rows")
	// alice group first (insertion order), then bob
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, int64(1), result.Rows[1][1])
}

func TestSelectGroupByWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (4, 'bob')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users WHERE id > 1 GROUP BY name")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	// bob appears first because id=2 is the first row after WHERE filter with name='bob'
	assert.Equal(t, "bob", result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[0][1])
	assert.Equal(t, "alice", result.Rows[1][0])
	assert.Equal(t, int64(1), result.Rows[1][1])
}

func TestSelectGroupByOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users GROUP BY name ORDER BY name ASC")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "bob", result.Rows[1][0])
}

func TestSelectGroupByHaving(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (4, 'charlie')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[0][1])
}

func TestSelectGroupByMultipleColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (product TEXT, region TEXT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES ('A', 'east', 10)")
	run(t, exec, "INSERT INTO orders VALUES ('A', 'east', 20)")
	run(t, exec, "INSERT INTO orders VALUES ('A', 'west', 30)")
	run(t, exec, "INSERT INTO orders VALUES ('B', 'east', 40)")

	result := run(t, exec, "SELECT product, region, COUNT(*) FROM orders GROUP BY product, region")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// Order by first appearance: (A, east), (A, west), (B, east)
	assert.Equal(t, "A", result.Rows[0][0])
	assert.Equal(t, "east", result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[0][2])
	assert.Equal(t, "A", result.Rows[1][0])
	assert.Equal(t, "west", result.Rows[1][1])
	assert.Equal(t, int64(1), result.Rows[1][2])
	assert.Equal(t, "B", result.Rows[2][0])
	assert.Equal(t, "east", result.Rows[2][1])
}

func TestSelectSumBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 300)")

	result := run(t, exec, "SELECT SUM(amount) FROM orders")
	assert.Equal(t, "SUM(amount)", result.Columns[0])
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(600), result.Rows[0][0])
}

func TestSelectSumWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, region TEXT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 'east', 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 'west', 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 'east', 300)")

	result := run(t, exec, "SELECT SUM(amount) FROM orders WHERE region = 'east'")
	assert.Equal(t, int64(400), result.Rows[0][0])
}

func TestSelectAvgBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 10)")
	run(t, exec, "INSERT INTO orders VALUES (2, 20)")
	run(t, exec, "INSERT INTO orders VALUES (3, 30)")

	result := run(t, exec, "SELECT AVG(amount) FROM orders")
	assert.Equal(t, "AVG(amount)", result.Columns[0])
	assert.Equal(t, float64(20), result.Rows[0][0])
}

func TestSelectMinMaxInt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT MIN(id), MAX(id) FROM users")
	require.Len(t, result.Columns, 2, "expected 2 columns")
	assert.Equal(t, "MIN(id)", result.Columns[0])
	assert.Equal(t, "MAX(id)", result.Columns[1])
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[0][1])
}

func TestSelectMinMaxText(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	result := run(t, exec, "SELECT MIN(name), MAX(name) FROM users")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "charlie", result.Rows[0][1])
}

func TestSelectAggregatesWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, value INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 10)")
	run(t, exec, "INSERT INTO scores VALUES (2, NULL)")
	run(t, exec, "INSERT INTO scores VALUES (3, 30)")
	run(t, exec, "INSERT INTO scores VALUES (4, NULL)")

	// SUM should skip NULLs
	result := run(t, exec, "SELECT SUM(value) FROM scores")
	assert.Equal(t, int64(40), result.Rows[0][0])

	// AVG should skip NULLs (40 / 2 = 20)
	result = run(t, exec, "SELECT AVG(value) FROM scores")
	assert.Equal(t, float64(20), result.Rows[0][0])

	// MIN should skip NULLs
	result = run(t, exec, "SELECT MIN(value) FROM scores")
	assert.Equal(t, int64(10), result.Rows[0][0])

	// MAX should skip NULLs
	result = run(t, exec, "SELECT MAX(value) FROM scores")
	assert.Equal(t, int64(30), result.Rows[0][0])
}

func TestSelectAggregatesAllNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, value INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, NULL)")
	run(t, exec, "INSERT INTO scores VALUES (2, NULL)")

	// SUM of all NULLs should return NULL
	result := run(t, exec, "SELECT SUM(value) FROM scores")
	assert.Nil(t, result.Rows[0][0])

	// AVG of all NULLs should return NULL
	result = run(t, exec, "SELECT AVG(value) FROM scores")
	assert.Nil(t, result.Rows[0][0])

	// MIN of all NULLs should return NULL
	result = run(t, exec, "SELECT MIN(value) FROM scores")
	assert.Nil(t, result.Rows[0][0])

	// MAX of all NULLs should return NULL
	result = run(t, exec, "SELECT MAX(value) FROM scores")
	assert.Nil(t, result.Rows[0][0])
}

func TestSelectGroupBySumBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, name TEXT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 'alice', 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 'bob', 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 'alice', 300)")

	result := run(t, exec, "SELECT name, SUM(amount) FROM orders GROUP BY name")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	// alice first (insertion order)
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, int64(400), result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, int64(200), result.Rows[1][1])
}

func TestSelectSumEmpty(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, amount INT)")

	// SUM on empty table should return NULL
	result := run(t, exec, "SELECT SUM(amount) FROM orders")
	assert.Nil(t, result.Rows[0][0])
}

func TestFloatColumnInsertSelect(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (3.14)")

	result := run(t, exec, "SELECT val FROM t")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, float64(3.14), result.Rows[0][0])
}

func TestFloatColumnInsertIntAutoConvert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (42)")

	result := run(t, exec, "SELECT val FROM t")
	assert.Equal(t, float64(42), result.Rows[0][0])
}

func TestErrorIntColumnInsertFloat(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val INT)")
	runExpectError(t, exec, "INSERT INTO t VALUES (3.14)")
}

func TestFloatArithmetic(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1.5 + 2.5
	result := run(t, exec, "SELECT 1.5 + 2.5")
	assert.Equal(t, float64(4.0), result.Rows[0][0])
}

func TestFloatIntMixedArithmetic(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1 + 0.5
	result := run(t, exec, "SELECT 1 + 0.5")
	assert.Equal(t, float64(1.5), result.Rows[0][0])
}

func TestAvgReturnsFloat(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, value INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 10)")
	run(t, exec, "INSERT INTO scores VALUES (2, 20)")
	run(t, exec, "INSERT INTO scores VALUES (3, 20)")

	result := run(t, exec, "SELECT AVG(value) FROM scores")
	avg, ok := result.Rows[0][0].(float64)
	require.True(t, ok, "expected AVG to return float64, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	// AVG(10, 20, 20) = 50/3 ≈ 16.666...
	assert.InDelta(t, 16.666, avg, 0.01)
}

func TestSumFloatColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")
	run(t, exec, "INSERT INTO t VALUES (3.0)")

	result := run(t, exec, "SELECT SUM(val) FROM t")
	assert.Equal(t, float64(7.0), result.Rows[0][0])
}

func TestFloatComparison(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")
	run(t, exec, "INSERT INTO t VALUES (3.5)")

	result := run(t, exec, "SELECT val FROM t WHERE val > 2.0")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, float64(2.5), result.Rows[0][0])
	assert.Equal(t, float64(3.5), result.Rows[1][0])
}

func TestFloatOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (3.5)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")

	result := run(t, exec, "SELECT val FROM t ORDER BY val ASC")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, float64(1.5), result.Rows[0][0])
	assert.Equal(t, float64(2.5), result.Rows[1][0])
	assert.Equal(t, float64(3.5), result.Rows[2][0])
}

func TestFloatMinMax(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (3.5)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")

	result := run(t, exec, "SELECT MIN(val), MAX(val) FROM t")
	assert.Equal(t, float64(1.5), result.Rows[0][0])
	assert.Equal(t, float64(3.5), result.Rows[0][1])
}

func TestFloatIntMixedComparison(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1.5)")
	run(t, exec, "INSERT INTO t VALUES (2, 2.5)")

	result := run(t, exec, "SELECT id FROM t WHERE val > 2")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestFloatUpdateSet(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1.0)")

	run(t, exec, "UPDATE t SET val = 9.99")
	result := run(t, exec, "SELECT val FROM t")
	assert.Equal(t, float64(9.99), result.Rows[0][0])
}

func TestSelectDistinctBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")

	result := run(t, exec, "SELECT DISTINCT name FROM users")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "bob", result.Rows[1][0])
}

func TestSelectDistinctStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "SELECT DISTINCT * FROM users")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestSelectDistinctOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	result := run(t, exec, "SELECT DISTINCT name FROM users ORDER BY name")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "bob", result.Rows[1][0])
}

func TestSelectDistinctLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (4, 'charlie')")

	result := run(t, exec, "SELECT DISTINCT name FROM users LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestSelectWhereIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (1, 3)")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestSelectWhereInNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (10, 20)")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestSelectWhereNotIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id NOT IN (2)")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestSelectWhereInLeftNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name IN ('bob', 'alice')")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectWhereInWithNullValues(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name IN ('alice', NULL)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectWhereBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")
	run(t, exec, "INSERT INTO users VALUES (5, 'eve')")

	result := run(t, exec, "SELECT * FROM users WHERE id BETWEEN 2 AND 4")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, int64(2), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
	assert.Equal(t, int64(4), result.Rows[2][0])
}

func TestSelectWhereNotBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")
	run(t, exec, "INSERT INTO users VALUES (5, 'eve')")

	result := run(t, exec, "SELECT * FROM users WHERE id NOT BETWEEN 2 AND 4")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(5), result.Rows[1][0])
}

func TestSelectWhereBetweenNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id BETWEEN 10 AND 20")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestSelectWhereBetweenNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (NULL, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id BETWEEN 1 AND 10")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectWhereBetweenText(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	result := run(t, exec, "SELECT * FROM users WHERE name BETWEEN 'bob' AND 'dave'")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "bob", result.Rows[0][1])
	assert.Equal(t, "charlie", result.Rows[1][1])
	assert.Equal(t, "dave", result.Rows[2][1])
}

func TestSelectWhereLike(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice_smith')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE '%alice%'")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestSelectWhereNotLike(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice_smith')")

	result := run(t, exec, "SELECT * FROM users WHERE name NOT LIKE '%alice%'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectWhereLikeUnderscore(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'abc')")
	run(t, exec, "INSERT INTO users VALUES (2, 'aXc')")
	run(t, exec, "INSERT INTO users VALUES (3, 'abbc')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE 'a_c'")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
}

func TestSelectWhereLikeExact(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE 'alice'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectWhereLikeNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE '%ali%'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectWhereLikeEscapePercent(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, '100%')")
	run(t, exec, "INSERT INTO items VALUES (2, '100abc')")
	run(t, exec, "INSERT INTO items VALUES (3, '50%')")

	result := run(t, exec, "SELECT * FROM items WHERE name LIKE '%\\%'")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])
}

func TestSelectWhereLikeEscapeUnderscore(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, 'a_b')")
	run(t, exec, "INSERT INTO items VALUES (2, 'aXb')")
	run(t, exec, "INSERT INTO items VALUES (3, 'a_c')")

	result := run(t, exec, "SELECT * FROM items WHERE name LIKE 'a\\_b'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectWhereLikeEscapeBackslash(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, 'a\\b')")
	run(t, exec, "INSERT INTO items VALUES (2, 'aXb')")

	result := run(t, exec, "SELECT * FROM items WHERE name LIKE 'a\\\\b'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestNotOperator(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, active INT)")
	run(t, exec, "INSERT INTO items VALUES (1, 1)")
	run(t, exec, "INSERT INTO items VALUES (2, 0)")
	run(t, exec, "INSERT INTO items VALUES (3, 1)")

	// NOT (id = 1)
	result := run(t, exec, "SELECT id FROM items WHERE NOT (id = 1)")
	require.Len(t, result.Rows, 2, "expected 2 rows")

	// NOT (id > 1)
	result = run(t, exec, "SELECT id FROM items WHERE NOT (id > 1)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])

	// NOT with AND
	result = run(t, exec, "SELECT id FROM items WHERE NOT (id = 1) AND NOT (id = 3)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])

	// NOT with OR
	result = run(t, exec, "SELECT id FROM items WHERE NOT (id = 1 OR id = 2)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(3), result.Rows[0][0])

	// Double NOT
	result = run(t, exec, "SELECT id FROM items WHERE NOT NOT (id = 1)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestCast(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (i INT, f FLOAT, s TEXT)")
	run(t, exec, "INSERT INTO t VALUES (42, 3.14, 'hello')")
	run(t, exec, "INSERT INTO t VALUES (7, 2.0, '123')")

	// INT to TEXT
	result := run(t, exec, "SELECT CAST(i AS TEXT) FROM t WHERE i = 42")
	assert.Equal(t, "42", result.Rows[0][0])

	// INT to FLOAT
	result = run(t, exec, "SELECT CAST(i AS FLOAT) FROM t WHERE i = 42")
	assert.Equal(t, float64(42), result.Rows[0][0])

	// FLOAT to INT (truncate)
	result = run(t, exec, "SELECT CAST(f AS INT) FROM t WHERE i = 42")
	assert.Equal(t, int64(3), result.Rows[0][0])

	// FLOAT to TEXT
	result = run(t, exec, "SELECT CAST(f AS TEXT) FROM t WHERE i = 42")
	got, ok := result.Rows[0][0].(string)
	require.True(t, ok, "CAST(3.14 AS TEXT): expected string, got %T", result.Rows[0][0])
	assert.Equal(t, "3.14", got)

	// TEXT to INT
	result = run(t, exec, "SELECT CAST(s AS INT) FROM t WHERE i = 7")
	assert.Equal(t, int64(123), result.Rows[0][0])

	// TEXT to FLOAT
	result = run(t, exec, "SELECT CAST(s AS FLOAT) FROM t WHERE i = 7")
	assert.Equal(t, float64(123), result.Rows[0][0])

	// NULL stays NULL
	exec2 := NewExecutor()
	run(t, exec2, "CREATE TABLE t2 (val TEXT)")
	run(t, exec2, "INSERT INTO t2 VALUES (NULL)")
	result = run(t, exec2, "SELECT CAST(val AS INT) FROM t2")
	assert.Nil(t, result.Rows[0][0])
}

func TestCastError(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (s TEXT)")
	run(t, exec, "INSERT INTO t VALUES ('abc')")

	runExpectError(t, exec, "SELECT CAST(s AS INT) FROM t")
}
