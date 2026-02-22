package engine

import (
	"testing"
)

func TestInnerJoinBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != "phone" {
		t.Errorf("row 1: expected [bob phone], got %v", result.Rows[1])
	}
}

func TestInnerJoinNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 99, 'laptop')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestInnerJoinWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE orders.product = 'laptop'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("expected [alice laptop], got %v", result.Rows[0])
	}
}

func TestInnerJoinWithAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("expected [alice laptop], got %v", result.Rows[0])
	}
}

func TestInnerJoinQualifiedColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'x')")

	result := run(t, exec, "SELECT t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id")
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "a" || result.Rows[0][1] != "x" {
		t.Errorf("expected [a x], got %v", result.Rows[0])
	}
}

func TestInnerJoinStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT * FROM users JOIN orders ON users.id = orders.user_id")
	if len(result.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	// users.id, users.name, orders.id, orders.user_id, orders.product
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected users.id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("expected users.name=alice, got %v", result.Rows[0][1])
	}
	if result.Rows[0][2] != int64(10) {
		t.Errorf("expected orders.id=10, got %v", result.Rows[0][2])
	}
	if result.Rows[0][4] != "laptop" {
		t.Errorf("expected orders.product=laptop, got %v", result.Rows[0][4])
	}
}

func TestInnerJoinMultiple(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t3 (id INT, t2_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'b')")
	run(t, exec, "INSERT INTO t3 VALUES (100, 10, 'c')")

	result := run(t, exec, "SELECT t1.val, t2.val, t3.val FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "a" || result.Rows[0][1] != "b" || result.Rows[0][2] != "c" {
		t.Errorf("expected [a b c], got %v", result.Rows[0])
	}
}

func TestInnerJoinOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.name DESC")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "bob" {
		t.Errorf("expected first row name=bob, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != "alice" {
		t.Errorf("expected second row name=alice, got %v", result.Rows[1][0])
	}
}

func TestInnerJoinAmbiguousColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (1, 'b')")

	runExpectError(t, exec, "SELECT id FROM t1 JOIN t2 ON t1.id = t2.id")
}

func TestLeftJoinBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != "phone" {
		t.Errorf("row 1: expected [bob phone], got %v", result.Rows[1])
	}
	if result.Rows[2][0] != "charlie" || result.Rows[2][1] != nil {
		t.Errorf("row 2: expected [charlie <nil>], got %v", result.Rows[2])
	}
}

func TestLeftJoinNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != nil {
		t.Errorf("row 0: expected [alice <nil>], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != nil {
		t.Errorf("row 1: expected [bob <nil>], got %v", result.Rows[1])
	}
}

func TestLeftJoinAllMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'phone')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != "phone" {
		t.Errorf("row 1: expected [bob phone], got %v", result.Rows[1])
	}
}

func TestLeftJoinWithWhereIsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT users.name FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE orders.id IS NULL ORDER BY users.id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "bob" {
		t.Errorf("row 0: expected bob, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != "charlie" {
		t.Errorf("row 1: expected charlie, got %v", result.Rows[1][0])
	}
}

func TestLeftJoinMultiple(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT, t1_id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t3 (id INT, t2_id INT, val TEXT)")
	run(t, exec, "INSERT INTO t1 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t1 VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t2 VALUES (10, 1, 'x')")
	// No t3 rows match

	result := run(t, exec, "SELECT t1.val, t2.val, t3.val FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = t3.t2_id ORDER BY t1.id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "a" || result.Rows[0][1] != "x" || result.Rows[0][2] != nil {
		t.Errorf("row 0: expected [a x <nil>], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "b" || result.Rows[1][1] != nil || result.Rows[1][2] != nil {
		t.Errorf("row 1: expected [b <nil> <nil>], got %v", result.Rows[1])
	}
}

func TestMixedInnerAndLeftJoin(t *testing.T) {
	exec := NewExecutor()
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
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" || result.Rows[0][2] != int64(5) {
		t.Errorf("row 0: expected [alice laptop 5], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != "phone" || result.Rows[1][2] != nil {
		t.Errorf("row 1: expected [bob phone <nil>], got %v", result.Rows[1])
	}
}

func TestLeftJoinWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders(user_id)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != nil {
		t.Errorf("row 1: expected [bob <nil>], got %v", result.Rows[1])
	}
}

func TestLeftJoinWithAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'laptop')")

	result := run(t, exec, "SELECT u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "laptop" {
		t.Errorf("row 0: expected [alice laptop], got %v", result.Rows[0])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != nil {
		t.Errorf("row 1: expected [bob <nil>], got %v", result.Rows[1])
	}
}

func TestTableAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "SELECT u.name FROM users AS u")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" {
		t.Errorf("expected alice, got %v", result.Rows[0][0])
	}
}
