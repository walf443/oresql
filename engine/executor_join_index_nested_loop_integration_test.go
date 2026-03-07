package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinWithIndexNestedLoop(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")
	run(t, exec, "INSERT INTO orders VALUES (30, 1, 'tablet')")

	q := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "phone", result.Rows[1][1])
	assert.Equal(t, "alice", result.Rows[2][0])
	assert.Equal(t, "tablet", result.Rows[2][1])

	assertExplain(t, exec, q, []planRow{
		{Table: "users", Type: "full scan"},
		{Table: "orders", Type: "ref", Key: "idx_orders_user_id"},
	})
}

func TestJoinWithWherePushdown(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 'active')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 'inactive')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")
	run(t, exec, "INSERT INTO orders VALUES (30, 3, 'tablet')")

	// WHERE u.status = 'active' should be pushed down to users table
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' ORDER BY u.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "charlie", result.Rows[1][0])
	assert.Equal(t, "tablet", result.Rows[1][1])
}

func TestJoinDrivingTableChoice(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, amount INT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")
	run(t, exec, "CREATE INDEX idx_orders_amount ON orders (amount)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop', 1000)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone', 500)")
	run(t, exec, "INSERT INTO orders VALUES (30, 1, 'tablet', 300)")

	// WHERE o.amount = 1000 with index on amount should cause orders to be considered as driving table
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount = 1000")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
}

func TestJoinReversedOrderSelectStar(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	// Even if driving table is switched, SELECT * should maintain FROM + JOIN column order
	result := run(t, exec, "SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
	require.Len(t, result.Columns, 5, "expected 5 columns")
	// Columns should be: id, name, id, user_id, product (users + orders)
	require.Len(t, result.Rows, 1, "expected 1 row")
	row := result.Rows[0]
	assert.Equal(t, int64(1), row[0], "users.id")
	assert.Equal(t, "alice", row[1], "users.name")
	assert.Equal(t, int64(10), row[2], "orders.id")
	assert.Equal(t, int64(1), row[3], "orders.user_id")
	assert.Equal(t, "laptop", row[4], "orders.product")
}

func TestJoinNullEquiJoinValue(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, NULL, 'orphan')")

	// NULL user_id should not match any user
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
}

func TestJoinCrossTableWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	// Cross-table WHERE: u.id + o.id > 15 (references both tables)
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id + o.id > 15")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "bob", result.Rows[0][0])
	assert.Equal(t, "phone", result.Rows[0][1])
}

func TestJoinOrWhereNotPushed(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 'active')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 'inactive')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 50)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 200)")

	// OR across tables should not be pushed down, but should still work correctly
	result := run(t, exec, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' OR o.amount > 100 ORDER BY u.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, int64(50), result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, int64(200), result.Rows[1][1])
}

func TestJoinBothTablesPushedDown(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, amount INT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 'active')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 'inactive')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop', 1000)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone', 500)")
	run(t, exec, "INSERT INTO orders VALUES (30, 3, 'tablet', 300)")

	// Both tables have pushdown conditions
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' AND o.amount > 500 ORDER BY u.id")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
}

func TestJoinInnerTableLocalWhereIndexScan(t *testing.T) {
	// Problem 1: JOINカラムにインデックスなし、WHERE条件のカラムにインデックスあり
	// orders.status にインデックスあり、orders.user_id にインデックスなし
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, status TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_status ON orders (status)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone', 'cancelled')")
	run(t, exec, "INSERT INTO orders VALUES (30, 3, 'tablet', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (40, 1, 'monitor', 'cancelled')")

	// No index on user_id (JOIN column), but index on status (WHERE column)
	// tryIndexScan should be used for LocalWhere on inner table
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active' ORDER BY o.id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "charlie", result.Rows[1][0])
	assert.Equal(t, "tablet", result.Rows[1][1])
}

func TestJoinInnerTableLocalWherePKWithJoinIndex(t *testing.T) {
	// Problem 2: JOINカラムにインデックスあり + WHERE条件がPKを指定
	// orders.user_id にインデックスあり、WHERE o.id = 10 はPKルックアップ
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")
	run(t, exec, "INSERT INTO orders VALUES (30, 1, 'tablet')")

	// JOIN index on user_id + PK WHERE on o.id = 10
	// innerWhereKeys should intersect with JOIN index lookup results
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 10")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
}

func TestJoinThreeTablesNaivePath(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t3 (id INT, t2_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'b')")
	run(t, exec, "INSERT INTO t3 VALUES (100, 10, 'c')")

	// 3-table JOIN should use the existing naive path
	result := run(t, exec, "SELECT t1.val, t2.val, t3.val FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "a", result.Rows[0][0])
	assert.Equal(t, "b", result.Rows[0][1])
	assert.Equal(t, "c", result.Rows[0][2])
}

func TestJoinCompositeIndexFullEquality(t *testing.T) {
	// Case A: Composite index (user_id, status) covers both JOIN + WHERE equality
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, status TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_uid_status ON orders (user_id, status)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (20, 1, 'phone', 'cancelled')")
	run(t, exec, "INSERT INTO orders VALUES (30, 2, 'tablet', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (40, 3, 'monitor', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (50, 3, 'keyboard', 'cancelled')")

	// Composite index (user_id, status) should allow single Lookup([joinVal, 'active'])
	q := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active' ORDER BY o.id"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "bob", result.Rows[1][0])
	assert.Equal(t, "tablet", result.Rows[1][1])
	assert.Equal(t, "charlie", result.Rows[2][0])
	assert.Equal(t, "monitor", result.Rows[2][1])

	assertExplain(t, exec, q, []planRow{
		{Table: "users", Type: "full scan"},
		{Table: "orders", Type: "ref", Key: "idx_orders_uid_status"},
	})
}

func TestJoinCompositeIndexPrefixRange(t *testing.T) {
	// Case B: Composite index (user_id, amount) with JOIN + range on amount
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, amount INT)")
	run(t, exec, "CREATE INDEX idx_orders_uid_amount ON orders (user_id, amount)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop', 1000)")
	run(t, exec, "INSERT INTO orders VALUES (20, 1, 'phone', 200)")
	run(t, exec, "INSERT INTO orders VALUES (30, 1, 'tablet', 500)")
	run(t, exec, "INSERT INTO orders VALUES (40, 2, 'monitor', 800)")
	run(t, exec, "INSERT INTO orders VALUES (50, 2, 'keyboard', 100)")

	// Composite index (user_id, amount): prefix=[joinVal], range amount >= 500
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount >= 500 ORDER BY o.id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "alice", result.Rows[1][0])
	assert.Equal(t, "tablet", result.Rows[1][1])
	assert.Equal(t, "bob", result.Rows[2][0])
	assert.Equal(t, "monitor", result.Rows[2][1])
}

func TestJoinCompositeIndex3ColPrefixScan(t *testing.T) {
	// Case C: 3-column composite index (user_id, status, amount), WHERE only has status
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, status TEXT, amount INT)")
	run(t, exec, "CREATE INDEX idx_orders_uid_status_amount ON orders (user_id, status, amount)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop', 'active', 1000)")
	run(t, exec, "INSERT INTO orders VALUES (20, 1, 'phone', 'cancelled', 200)")
	run(t, exec, "INSERT INTO orders VALUES (30, 1, 'tablet', 'active', 500)")
	run(t, exec, "INSERT INTO orders VALUES (40, 2, 'monitor', 'active', 800)")
	run(t, exec, "INSERT INTO orders VALUES (50, 2, 'keyboard', 'cancelled', 100)")

	// 3-col index (user_id, status, amount): prefix scan with [joinVal, 'active']
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active' ORDER BY o.id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "laptop", result.Rows[0][1])
	assert.Equal(t, "alice", result.Rows[1][0])
	assert.Equal(t, "tablet", result.Rows[1][1])
	assert.Equal(t, "bob", result.Rows[2][0])
	assert.Equal(t, "monitor", result.Rows[2][1])
}
