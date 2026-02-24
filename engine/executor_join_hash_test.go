package engine

import (
	"testing"
)

// TestHashJoinInnerBasic tests basic INNER JOIN without indexes (hash join path).
func TestHashJoinInnerBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	// No indexes on user_id — forces hash join

	run(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'Bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'Widget')")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 'Gadget')")
	run(t, exec, "INSERT INTO orders VALUES (30, 1, 'Doohickey')")

	res := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id")
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "Alice" || res.Rows[0][1] != "Widget" {
		t.Errorf("row 0 = %v, want [Alice Widget]", res.Rows[0])
	}
	if res.Rows[1][0] != "Bob" || res.Rows[1][1] != "Gadget" {
		t.Errorf("row 1 = %v, want [Bob Gadget]", res.Rows[1])
	}
	if res.Rows[2][0] != "Alice" || res.Rows[2][1] != "Doohickey" {
		t.Errorf("row 2 = %v, want [Alice Doohickey]", res.Rows[2])
	}
}

// TestHashJoinInnerNoMatch tests INNER JOIN with no matching rows.
func TestHashJoinInnerNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE TABLE t2 (id INT PRIMARY KEY, ref INT)")

	run(t, exec, "INSERT INTO t1 VALUES (1, 100)")
	run(t, exec, "INSERT INTO t2 VALUES (1, 999)")

	res := run(t, exec, "SELECT t1.val, t2.ref FROM t1 JOIN t2 ON t1.id = t2.ref")
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(res.Rows))
	}
}

// TestHashJoinInnerWithWhere tests hash join with LocalWhere on inner table.
func TestHashJoinInnerWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, status TEXT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'Bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'active')")
	run(t, exec, "INSERT INTO orders VALUES (20, 1, 'cancelled')")
	run(t, exec, "INSERT INTO orders VALUES (30, 2, 'active')")

	res := run(t, exec, "SELECT u.name, o.status FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active' ORDER BY o.id")
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "Alice" {
		t.Errorf("row 0 name = %v, want Alice", res.Rows[0][0])
	}
	if res.Rows[1][0] != "Bob" {
		t.Errorf("row 1 name = %v, want Bob", res.Rows[1][0])
	}
}

// TestHashJoinLeftJoinBasic tests LEFT JOIN without indexes (hash join path).
func TestHashJoinLeftJoinBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")

	run(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'Bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'Charlie')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 'Widget')")

	res := run(t, exec, "SELECT u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id")
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "Alice" || res.Rows[0][1] != "Widget" {
		t.Errorf("row 0 = %v, want [Alice Widget]", res.Rows[0])
	}
	if res.Rows[1][0] != "Bob" || res.Rows[1][1] != nil {
		t.Errorf("row 1 = %v, want [Bob <nil>]", res.Rows[1])
	}
	if res.Rows[2][0] != "Charlie" || res.Rows[2][1] != nil {
		t.Errorf("row 2 = %v, want [Charlie <nil>]", res.Rows[2])
	}
}

// TestHashJoinWithResidualOn tests hash join with equi + non-equi ON conditions.
func TestHashJoinWithResidualOn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE TABLE t2 (id INT PRIMARY KEY, ref INT, val INT)")

	run(t, exec, "INSERT INTO t1 VALUES (1, 10)")
	run(t, exec, "INSERT INTO t1 VALUES (2, 20)")
	run(t, exec, "INSERT INTO t2 VALUES (100, 1, 5)")
	run(t, exec, "INSERT INTO t2 VALUES (200, 1, 15)")
	run(t, exec, "INSERT INTO t2 VALUES (300, 2, 25)")

	// equi: t1.id = t2.ref, residual: t2.val > 10
	res := run(t, exec, "SELECT t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.ref AND t2.val > 10 ORDER BY t2.id")
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != int64(10) || res.Rows[0][1] != int64(15) {
		t.Errorf("row 0 = %v, want [10 15]", res.Rows[0])
	}
	if res.Rows[1][0] != int64(20) || res.Rows[1][1] != int64(25) {
		t.Errorf("row 1 = %v, want [20 25]", res.Rows[1])
	}
}

// TestHashJoinMultiColumnKey tests multi-column equi-join.
func TestHashJoinMultiColumnKey(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (a INT, b INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (x INT, y INT, data TEXT)")

	run(t, exec, "INSERT INTO t1 VALUES (1, 2, 'match')")
	run(t, exec, "INSERT INTO t1 VALUES (1, 3, 'nomatch')")
	run(t, exec, "INSERT INTO t2 VALUES (1, 2, 'found')")
	run(t, exec, "INSERT INTO t2 VALUES (2, 2, 'wrong')")

	res := run(t, exec, "SELECT t1.val, t2.data FROM t1 JOIN t2 ON t1.a = t2.x AND t1.b = t2.y")
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "match" || res.Rows[0][1] != "found" {
		t.Errorf("row 0 = %v, want [match found]", res.Rows[0])
	}
}

// TestHashJoinWithNullJoinKey tests that NULL join keys don't match (SQL semantics).
func TestHashJoinWithNullJoinKey(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (ref INT, data TEXT)")

	run(t, exec, "INSERT INTO t1 VALUES (1, 'one')")
	run(t, exec, "INSERT INTO t1 VALUES (NULL, 'null_row')")
	run(t, exec, "INSERT INTO t2 VALUES (1, 'match')")
	run(t, exec, "INSERT INTO t2 VALUES (NULL, 'null_match')")

	res := run(t, exec, "SELECT t1.val, t2.data FROM t1 JOIN t2 ON t1.id = t2.ref")
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "one" || res.Rows[0][1] != "match" {
		t.Errorf("row 0 = %v, want [one match]", res.Rows[0])
	}
}

// TestHashJoinWithLimit tests hash join with LIMIT early termination.
func TestHashJoinWithLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
	run(t, exec, "CREATE TABLE t2 (id INT PRIMARY KEY, ref INT, data TEXT)")

	for i := 1; i <= 100; i++ {
		run(t, exec, "INSERT INTO t1 VALUES ("+itoa(i)+", 'v"+itoa(i)+"')")
		run(t, exec, "INSERT INTO t2 VALUES ("+itoa(i)+", "+itoa(i)+", 'd"+itoa(i)+"')")
	}

	res := run(t, exec, "SELECT t1.val, t2.data FROM t1 JOIN t2 ON t1.id = t2.ref LIMIT 5")
	if len(res.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(res.Rows))
	}
}

// TestHashJoinThreeTableNoIndex tests 3-table JOIN without any indexes.
func TestHashJoinThreeTableNoIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, total INT)")
	run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, order_id INT, product TEXT)")

	run(t, exec, "INSERT INTO customers VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO customers VALUES (2, 'Bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 200)")
	run(t, exec, "INSERT INTO items VALUES (100, 10, 'Widget')")
	run(t, exec, "INSERT INTO items VALUES (200, 20, 'Gadget')")
	run(t, exec, "INSERT INTO items VALUES (300, 10, 'Doohickey')")

	res := run(t, exec, "SELECT c.name, o.total, i.product FROM customers c JOIN orders o ON c.id = o.cust_id JOIN items i ON o.id = i.order_id ORDER BY i.id")
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "Alice" || res.Rows[0][1] != int64(100) || res.Rows[0][2] != "Widget" {
		t.Errorf("row 0 = %v, want [Alice 100 Widget]", res.Rows[0])
	}
	if res.Rows[1][0] != "Bob" || res.Rows[1][1] != int64(200) || res.Rows[1][2] != "Gadget" {
		t.Errorf("row 1 = %v, want [Bob 200 Gadget]", res.Rows[1])
	}
	if res.Rows[2][0] != "Alice" || res.Rows[2][1] != int64(100) || res.Rows[2][2] != "Doohickey" {
		t.Errorf("row 2 = %v, want [Alice 100 Doohickey]", res.Rows[2])
	}
}

// itoa is a simple int-to-string helper for test SQL generation.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	digits := make([]byte, 0, 10)
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	if neg {
		digits = append(digits, '-')
	}
	// reverse
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}
