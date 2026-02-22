package engine

import (
	"testing"
)

func TestThreeTableJoinWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, amount INT)")
	run(t, exec, "CREATE TABLE items (id INT, order_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_uid ON orders(user_id)")
	run(t, exec, "CREATE INDEX idx_items_oid ON items(order_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 200)")
	run(t, exec, "INSERT INTO items VALUES (100, 10, 'widget')")
	run(t, exec, "INSERT INTO items VALUES (101, 10, 'gadget')")
	run(t, exec, "INSERT INTO items VALUES (200, 20, 'doohickey')")

	result := run(t, exec, `
		SELECT u.name, o.amount, i.product
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		INNER JOIN items i ON o.id = i.order_id
		ORDER BY i.product
	`)

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Sorted by product: doohickey, gadget, widget
	expected := []struct {
		name    string
		amount  int64
		product string
	}{
		{"bob", int64(200), "doohickey"},
		{"alice", int64(100), "gadget"},
		{"alice", int64(100), "widget"},
	}

	for i, exp := range expected {
		row := result.Rows[i]
		if row[0] != exp.name {
			t.Errorf("row %d name: got %v, want %v", i, row[0], exp.name)
		}
		if row[1] != exp.amount {
			t.Errorf("row %d amount: got %v, want %v", i, row[1], exp.amount)
		}
		if row[2] != exp.product {
			t.Errorf("row %d product: got %v, want %v", i, row[2], exp.product)
		}
	}
}

func TestThreeTableJoinWithWherePushdown(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, amount INT)")
	run(t, exec, "CREATE TABLE items (id INT, order_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_uid ON orders(user_id)")
	run(t, exec, "CREATE INDEX idx_items_oid ON items(order_id)")

	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 'active')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 'inactive')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (20, 2, 200)")
	run(t, exec, "INSERT INTO items VALUES (100, 10, 'widget')")
	run(t, exec, "INSERT INTO items VALUES (200, 20, 'gadget')")

	result := run(t, exec, `
		SELECT u.name, o.amount, i.product
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		INNER JOIN items i ON o.id = i.order_id
		WHERE u.status = 'active'
	`)

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" {
		t.Errorf("expected alice, got %v", result.Rows[0][0])
	}
	if result.Rows[0][2] != "widget" {
		t.Errorf("expected widget, got %v", result.Rows[0][2])
	}
}

func TestThreeTableJoinStarSchema(t *testing.T) {
	// Fact table (sales) + two dimension tables (products, stores)
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE products (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE stores (id INT, city TEXT)")
	run(t, exec, "CREATE TABLE sales (id INT, product_id INT, store_id INT, quantity INT)")
	run(t, exec, "CREATE INDEX idx_sales_pid ON sales(product_id)")
	run(t, exec, "CREATE INDEX idx_sales_sid ON sales(store_id)")

	run(t, exec, "INSERT INTO products VALUES (1, 'widget')")
	run(t, exec, "INSERT INTO products VALUES (2, 'gadget')")
	run(t, exec, "INSERT INTO stores VALUES (1, 'tokyo')")
	run(t, exec, "INSERT INTO stores VALUES (2, 'osaka')")
	run(t, exec, "INSERT INTO sales VALUES (1, 1, 1, 10)")
	run(t, exec, "INSERT INTO sales VALUES (2, 1, 2, 20)")
	run(t, exec, "INSERT INTO sales VALUES (3, 2, 1, 30)")

	result := run(t, exec, `
		SELECT p.name, st.city, s.quantity
		FROM sales s
		INNER JOIN products p ON s.product_id = p.id
		INNER JOIN stores st ON s.store_id = st.id
		ORDER BY s.quantity
	`)

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Sorted by quantity: 10, 20, 30
	if result.Rows[0][2] != int64(10) {
		t.Errorf("row 0 quantity: got %v, want 10", result.Rows[0][2])
	}
	if result.Rows[2][2] != int64(30) {
		t.Errorf("row 2 quantity: got %v, want 30", result.Rows[2][2])
	}
}

func TestThreeTableJoinChainSchema(t *testing.T) {
	// A -> B -> C chain
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE a (id INT, val TEXT)")
	run(t, exec, "CREATE TABLE b (id INT, a_id INT, val TEXT)")
	run(t, exec, "CREATE TABLE c (id INT, b_id INT, val TEXT)")
	run(t, exec, "CREATE INDEX idx_b_aid ON b(a_id)")
	run(t, exec, "CREATE INDEX idx_c_bid ON c(b_id)")

	run(t, exec, "INSERT INTO a VALUES (1, 'a1')")
	run(t, exec, "INSERT INTO a VALUES (2, 'a2')")
	run(t, exec, "INSERT INTO b VALUES (10, 1, 'b1')")
	run(t, exec, "INSERT INTO b VALUES (20, 2, 'b2')")
	run(t, exec, "INSERT INTO c VALUES (100, 10, 'c1')")
	run(t, exec, "INSERT INTO c VALUES (200, 20, 'c2')")

	result := run(t, exec, `
		SELECT a.val, b.val, c.val
		FROM a
		INNER JOIN b ON a.id = b.a_id
		INNER JOIN c ON b.id = c.b_id
		ORDER BY a.val
	`)

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "a1" || result.Rows[0][1] != "b1" || result.Rows[0][2] != "c1" {
		t.Errorf("row 0: got %v, want [a1 b1 c1]", result.Rows[0])
	}
	if result.Rows[1][0] != "a2" || result.Rows[1][1] != "b2" || result.Rows[1][2] != "c2" {
		t.Errorf("row 1: got %v, want [a2 b2 c2]", result.Rows[1])
	}
}

func TestFourTableJoin(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE customers (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, customer_id INT)")
	run(t, exec, "CREATE TABLE items (id INT, order_id INT, product_id INT)")
	run(t, exec, "CREATE TABLE products (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_orders_cid ON orders(customer_id)")
	run(t, exec, "CREATE INDEX idx_items_oid ON items(order_id)")
	run(t, exec, "CREATE INDEX idx_items_pid ON items(product_id)")

	run(t, exec, "INSERT INTO customers VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (10, 1)")
	run(t, exec, "INSERT INTO items VALUES (100, 10, 1)")
	run(t, exec, "INSERT INTO products VALUES (1, 'widget')")

	result := run(t, exec, `
		SELECT c.name, p.name
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		INNER JOIN items i ON o.id = i.order_id
		INNER JOIN products p ON i.product_id = p.id
	`)

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != "widget" {
		t.Errorf("got %v, want [alice widget]", result.Rows[0])
	}
}

func TestThreeTableJoinSelectStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE a (id INT, aval TEXT)")
	run(t, exec, "CREATE TABLE b (id INT, a_id INT, bval TEXT)")
	run(t, exec, "CREATE TABLE c (id INT, b_id INT, cval TEXT)")

	run(t, exec, "INSERT INTO a VALUES (1, 'a1')")
	run(t, exec, "INSERT INTO b VALUES (10, 1, 'b1')")
	run(t, exec, "INSERT INTO c VALUES (100, 10, 'c1')")

	result := run(t, exec, `
		SELECT *
		FROM a
		INNER JOIN b ON a.id = b.a_id
		INNER JOIN c ON b.id = c.b_id
	`)

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// Column order should be: a.id, a.aval, b.id, b.a_id, b.bval, c.id, c.b_id, c.cval
	if len(result.Columns) != 8 {
		t.Fatalf("expected 8 columns, got %d: %v", len(result.Columns), result.Columns)
	}

	row := result.Rows[0]
	if row[0] != int64(1) { // a.id
		t.Errorf("a.id: got %v, want 1", row[0])
	}
	if row[1] != "a1" { // a.aval
		t.Errorf("a.aval: got %v, want a1", row[1])
	}
	if row[2] != int64(10) { // b.id
		t.Errorf("b.id: got %v, want 10", row[2])
	}
	if row[5] != int64(100) { // c.id
		t.Errorf("c.id: got %v, want 100", row[5])
	}
	if row[7] != "c1" { // c.cval
		t.Errorf("c.cval: got %v, want c1", row[7])
	}
}

func TestThreeTableJoinCrossTableWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE a (id INT, val INT)")
	run(t, exec, "CREATE TABLE b (id INT, a_id INT, val INT)")
	run(t, exec, "CREATE TABLE c (id INT, b_id INT, val INT)")

	run(t, exec, "INSERT INTO a VALUES (1, 10)")
	run(t, exec, "INSERT INTO a VALUES (2, 20)")
	run(t, exec, "INSERT INTO b VALUES (10, 1, 100)")
	run(t, exec, "INSERT INTO b VALUES (20, 2, 200)")
	run(t, exec, "INSERT INTO c VALUES (100, 10, 1000)")
	run(t, exec, "INSERT INTO c VALUES (200, 20, 2000)")

	// Cross-table WHERE: a.val + c.val > 1500
	result := run(t, exec, `
		SELECT a.val, b.val, c.val
		FROM a
		INNER JOIN b ON a.id = b.a_id
		INNER JOIN c ON b.id = c.b_id
		WHERE a.val + c.val > 1500
	`)

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(20) {
		t.Errorf("a.val: got %v, want 20", result.Rows[0][0])
	}
	if result.Rows[0][2] != int64(2000) {
		t.Errorf("c.val: got %v, want 2000", result.Rows[0][2])
	}
}
