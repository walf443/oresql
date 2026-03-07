package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThreeTableJoinWithIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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

	q := `SELECT u.name, o.amount, i.product
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		INNER JOIN items i ON o.id = i.order_id
		ORDER BY i.product`
	result := run(t, exec, q)

	require.Len(t, result.Rows, 3, "expected 3 rows from three-table join")

	assertExplain(t, exec, q, []planRow{
		{Table: "users", Type: "full scan"},
		{Table: "orders", Type: "ref", Key: "idx_orders_uid"},
		{Table: "items", Type: "ref", Key: "idx_items_oid"},
	})

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
		assert.Equal(t, exp.name, row[0], "row %d name", i)
		assert.Equal(t, exp.amount, row[1], "row %d amount", i)
		assert.Equal(t, exp.product, row[2], "row %d product", i)
	}
}

func TestThreeTableJoinWithWherePushdown(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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

	require.Len(t, result.Rows, 1, "expected 1 row after WHERE pushdown")
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "widget", result.Rows[0][2])
}

func TestThreeTableJoinStarSchema(t *testing.T) {
	// Fact table (sales) + two dimension tables (products, stores)
	exec := NewExecutor(NewDatabase("test"))
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

	require.Len(t, result.Rows, 3, "expected 3 rows from star schema join")

	// Sorted by quantity: 10, 20, 30
	assert.Equal(t, int64(10), result.Rows[0][2], "row 0 quantity")
	assert.Equal(t, int64(30), result.Rows[2][2], "row 2 quantity")
}

func TestThreeTableJoinChainSchema(t *testing.T) {
	// A -> B -> C chain
	exec := NewExecutor(NewDatabase("test"))
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

	require.Len(t, result.Rows, 2, "expected 2 rows from chain join")
	assert.Equal(t, Row{"a1", "b1", "c1"}, result.Rows[0], "row 0")
	assert.Equal(t, Row{"a2", "b2", "c2"}, result.Rows[1], "row 1")
}

func TestFourTableJoin(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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

	q := `SELECT c.name, p.name
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		INNER JOIN items i ON o.id = i.order_id
		INNER JOIN products p ON i.product_id = p.id`
	result := run(t, exec, q)

	require.Len(t, result.Rows, 1, "expected 1 row from four-table join")

	assertExplain(t, exec, q, []planRow{
		{Table: "customers", Type: "full scan"},
		{Table: "orders", Type: "ref", Key: "idx_orders_cid"},
		{Table: "items", Type: "ref", Key: "idx_items_oid"},
		{Table: "products", Type: "full scan"},
	})
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "widget", result.Rows[0][1])
}

func TestThreeTableJoinSelectStar(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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

	require.Len(t, result.Rows, 1, "expected 1 row from SELECT *")

	// Column order should be: a.id, a.aval, b.id, b.a_id, b.bval, c.id, c.b_id, c.cval
	require.Len(t, result.Columns, 8, "expected 8 columns from SELECT *")

	row := result.Rows[0]
	assert.Equal(t, int64(1), row[0], "a.id")
	assert.Equal(t, "a1", row[1], "a.aval")
	assert.Equal(t, int64(10), row[2], "b.id")
	assert.Equal(t, int64(100), row[5], "c.id")
	assert.Equal(t, "c1", row[7], "c.cval")
}

func TestThreeTableJoinCrossTableWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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

	require.Len(t, result.Rows, 1, "expected 1 row after cross-table WHERE")
	assert.Equal(t, int64(20), result.Rows[0][0], "a.val")
	assert.Equal(t, int64(2000), result.Rows[0][2], "c.val")
}
