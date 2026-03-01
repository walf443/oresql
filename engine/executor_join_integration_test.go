package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInnerJoinBasic(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
}

func TestInnerJoinNoMatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 99, 'laptop')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestInnerJoinWithWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE orders.product = 'laptop'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
}

func TestInnerJoinWithAlias(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
}

func TestInnerJoinQualifiedColumns(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'x')")

	result := run(t, exec, "SELECT t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id")
	require.Len(t, result.Columns, 2, "expected 2 columns")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "a", result.Rows[0][0])
	assert.Equal(t, "x", result.Rows[0][1])
}

func TestInnerJoinStar(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT * FROM users JOIN orders ON users.id = orders.user_id")
	require.Len(t, result.Columns, 5, "expected 5 columns")
	require.Len(t, result.Rows, 1, "expected 1 row")
	// users.id, users.name, orders.id, orders.user_id, orders.product
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, int64(10), result.Rows[0][2])
	assert.Equal(t, "laptop", result.Rows[0][4])
}

func TestInnerJoinMultiple(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t3 (id INT, t2_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'b')")
	run(t, exec, "INSERT INTO t3 VALUES (100, 10, 'c')")

	result := run(t, exec, "SELECT t1.val, t2.val, t3.val FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "a", result.Rows[0][0])
	assert.Equal(t, "b", result.Rows[0][1])
	assert.Equal(t, "c", result.Rows[0][2])
}

func TestInnerJoinOrderBy(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.name DESC")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "bob", result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[1][0])
}

func TestInnerJoinAmbiguousColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (1, 'b')")

	runExpectError(t, exec, "SELECT id FROM t1 JOIN t2 ON t1.id = t2.id")
}

func TestLeftJoinBasic(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
	assert.Equal(t, "charlie", result.Rows[2][0])
	assert.Nil(t, result.Rows[2][1])
}

func TestLeftJoinNoMatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Nil(t, result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Nil(t, result.Rows[1][1])
}

func TestLeftJoinAllMatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
}

func TestLeftJoinWithWhereIsNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT users.name FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE orders.id IS NULL ORDER BY users.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "bob", result.Rows[0][0])
	assert.Equal(t, "charlie", result.Rows[1][0])
}

func TestLeftJoinMultiple(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t3 (id INT, t2_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t1 VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'x')")
	// No t3 rows match

	result := run(t, exec, "SELECT t1.val, t2.val, t3.val FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = t3.t2_id ORDER BY t1.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "a", result.Rows[0][0])
	assert.Equal(t, "x", result.Rows[0][1])
	assert.Nil(t, result.Rows[0][2])
	assert.Equal(t, "b", result.Rows[1][0])
	assert.Nil(t, result.Rows[1][1])
	assert.Nil(t, result.Rows[1][2])
}

func TestMixedInnerAndLeftJoin(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "CREATE TABLE reviews (id INT, order_id INT, rating INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")
	run(t, exec, "INSERT INTO reviews VALUES (100, 10, 5)")
	// order 20 has no review

	result := run(t, exec, "SELECT users.name, orders.product, reviews.rating FROM users INNER JOIN orders ON users.id = orders.user_id LEFT JOIN reviews ON orders.id = reviews.order_id ORDER BY users.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, int64(5), result.Rows[0][2])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
	assert.Nil(t, result.Rows[1][2])
}

func TestLeftJoinWithIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders(user_id)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Nil(t, result.Rows[1][1])
}

func TestLeftJoinWithAlias(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Nil(t, result.Rows[1][1])
}

func TestRightJoinBasic(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")
	run(t, exec, "INSERT INTO orders VALUES (30, 3, 'tablet')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY orders.id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Nil(t, result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
	assert.Nil(t, result.Rows[2][0])
	assert.Equal(t, "tablet", result.Rows[2][1])
}

func TestRightJoinNoMatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY orders.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Nil(t, result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Nil(t, result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
}

func TestRightJoinAllMatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY orders.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
}

func TestRightJoinWithWhereIsNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")
	run(t, exec, "INSERT INTO orders VALUES (30, 3, 'tablet')")

	result := run(t, exec, "SELECT orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id WHERE users.id IS NULL ORDER BY orders.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "phone", result.Rows[0][0])
	assert.Equal(t, "tablet", result.Rows[1][0])
}

func TestCrossJoinBasic(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE colors (name TEXT)")
	run(t, exec, "CREATE TABLE sizes (name TEXT)")
	run(t, exec, "INSERT INTO colors VALUES ('red')")
	run(t, exec, "INSERT INTO colors VALUES ('blue')")
	run(t, exec, "INSERT INTO sizes VALUES ('S')")
	run(t, exec, "INSERT INTO sizes VALUES ('M')")
	run(t, exec, "INSERT INTO sizes VALUES ('L')")

	result := run(t, exec, "SELECT colors.name, sizes.name FROM colors CROSS JOIN sizes ORDER BY colors.name, sizes.name")
	require.Len(t, result.Rows, 6, "expected 6 rows")
	expected := [][2]string{
		{"blue", "L"}, {"blue", "M"}, {"blue", "S"},
		{"red", "L"}, {"red", "M"}, {"red", "S"},
	}
	for i, exp := range expected {
		assert.Equal(t, exp[0], result.Rows[i][0], fmt.Sprintf("row %d", i))
		assert.Equal(t, exp[1], result.Rows[i][1], fmt.Sprintf("row %d", i))
	}
}

func TestCrossJoinWithWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t1 VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'x')")
	run(t, exec, "INSERT INTO t2 VALUES (20, 2, 'y')")

	result := run(t, exec, "SELECT t1.val, t2.val FROM t1 CROSS JOIN t2 WHERE t1.id = t2.t1_id ORDER BY t1.id, t2.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "a", result.Rows[0][0])
	assert.Equal(t, "x", result.Rows[0][1])
	assert.Equal(t, "b", result.Rows[1][0])
	assert.Equal(t, "y", result.Rows[1][1])
}

func TestCrossJoinEmpty(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (id INT)")
	run(t, exec, "CREATE TABLE t2 (id INT)")
	run(t, exec, "INSERT INTO t1 VALUES (1)")

	result := run(t, exec, "SELECT * FROM t1 CROSS JOIN t2")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestTableAlias(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "SELECT u.name FROM users AS u")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
}

func TestInnerJoinUsingBasic(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users JOIN orders USING (id)")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
}

func TestLeftJoinUsingWithNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (1, 'laptop')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders USING (id) ORDER BY users.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Nil(t, result.Rows[1][1])
}

func TestJoinUsingMultipleColumns(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (a INT, b INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (a INT, b INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 10, 'x')")
	run(t, exec, "INSERT INTO t1 VALUES (2, 20, 'y')")
	run(t, exec, "INSERT INTO t2 VALUES (1, 10, 'p')")
	run(t, exec, "INSERT INTO t2 VALUES (2, 99, 'q')")

	result := run(t, exec, "SELECT t1.val, t2.val FROM t1 JOIN t2 USING (a, b)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "x", result.Rows[0][0])
	assert.Equal(t, "p", result.Rows[0][1])
}

func TestJoinUsingStarDedup(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (1, 'laptop')")

	result := run(t, exec, "SELECT * FROM users JOIN orders USING (id)")
	// Standard SQL: USING columns appear once. Expected: id, name, product (3 cols, not 4)
	require.Len(t, result.Columns, 3, "expected 3 columns (id should not be duplicated)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	// Columns should be: id, name, product
	assert.Equal(t, "id", result.Columns[0])
	assert.Equal(t, "name", result.Columns[1])
	assert.Equal(t, "product", result.Columns[2])
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, "laptop", result.Rows[0][2])
}
