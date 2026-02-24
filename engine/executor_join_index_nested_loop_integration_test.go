package engine

import (
	"testing"
)

func TestJoinWithIndexNestedLoop(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")
	run(t, exec, "INSERT INTO orders VALUES (30, 1, 'tablet')")

	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != "phone" {
		t.Errorf("row 1: expected [bob phone], got %v", result.Rows[1])
	}
	if result.Rows[2][0] != "alice" || result.Rows[2][1] != "tablet" {
		t.Errorf("row 2: expected [alice tablet], got %v", result.Rows[2])
	}
}

func TestJoinWithWherePushdown(t *testing.T) {
	exec := NewExecutor()
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
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "charlie" || result.Rows[1][1] != "tablet" {
		t.Errorf("row 1: expected [charlie tablet], got %v", result.Rows[1])
	}
}

func TestJoinDrivingTableChoice(t *testing.T) {
	exec := NewExecutor()
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
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("expected [alice laptop], got %v", result.Rows[0])
	}
}

func TestJoinReversedOrderSelectStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	// Even if driving table is switched, SELECT * should maintain FROM + JOIN column order
	result := run(t, exec, "SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
	if len(result.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(result.Columns))
	}
	// Columns should be: id, name, id, user_id, product (users + orders)
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	row := result.Rows[0]
	if row[0] != int64(1) {
		t.Errorf("users.id = %v, want 1", row[0])
	}
	if row[1] != "alice" {
		t.Errorf("users.name = %v, want alice", row[1])
	}
	if row[2] != int64(10) {
		t.Errorf("orders.id = %v, want 10", row[2])
	}
	if row[3] != int64(1) {
		t.Errorf("orders.user_id = %v, want 1", row[3])
	}
	if row[4] != "laptop" {
		t.Errorf("orders.product = %v, want laptop", row[4])
	}
}

func TestJoinNullEquiJoinValue(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, NULL, 'orphan')")

	// NULL user_id should not match any user
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("expected [alice laptop], got %v", result.Rows[0])
	}
}

func TestJoinCrossTableWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	// Cross-table WHERE: u.id + o.id > 15 (references both tables)
	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id + o.id > 15")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "bob" || result.Rows[0][1] != "phone" {
		t.Errorf("expected [bob phone], got %v", result.Rows[0])
	}
}

func TestJoinOrWhereNotPushed(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 'active')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 'inactive')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 50)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 200)")

	// OR across tables should not be pushed down, but should still work correctly
	result := run(t, exec, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' OR o.amount > 100 ORDER BY u.id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != int64(50) {
		t.Errorf("row 0: expected [alice 50], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != int64(200) {
		t.Errorf("row 1: expected [bob 200], got %v", result.Rows[1])
	}
}

func TestJoinBothTablesPushedDown(t *testing.T) {
	exec := NewExecutor()
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
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("expected [alice laptop], got %v", result.Rows[0])
	}
}

func TestJoinInnerTableLocalWhereIndexScan(t *testing.T) {
	// Problem 1: JOINカラムにインデックスなし、WHERE条件のカラムにインデックスあり
	// orders.status にインデックスあり、orders.user_id にインデックスなし
	exec := NewExecutor()
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
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "charlie" || result.Rows[1][1] != "tablet" {
		t.Errorf("row 1: expected [charlie tablet], got %v", result.Rows[1])
	}
}

func TestJoinInnerTableLocalWherePKWithJoinIndex(t *testing.T) {
	// Problem 2: JOINカラムにインデックスあり + WHERE条件がPKを指定
	// orders.user_id にインデックスあり、WHERE o.id = 10 はPKルックアップ
	exec := NewExecutor()
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
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("expected [alice laptop], got %v", result.Rows[0])
	}
}

func TestJoinThreeTablesNaivePath(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t3 (id INT, t2_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'b')")
	run(t, exec, "INSERT INTO t3 VALUES (100, 10, 'c')")

	// 3-table JOIN should use the existing naive path
	result := run(t, exec, "SELECT t1.val, t2.val, t3.val FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "a" || result.Rows[0][1] != "b" || result.Rows[0][2] != "c" {
		t.Errorf("expected [a b c], got %v", result.Rows[0])
	}
}
