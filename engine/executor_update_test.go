package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "UPDATE users SET name = 'ALICE' WHERE id = 1")
	assert.Equal(t, "1 row updated", result.Message)

	result = run(t, exec, "SELECT name FROM users WHERE id = 1")
	require.Len(t, result.Rows, 1, "expected 1 row for id=1")
	assert.Equal(t, "ALICE", result.Rows[0][0])
}

func TestUpdateMultipleRows(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "UPDATE users SET name = 'updated' WHERE id > 1")
	assert.Equal(t, "2 rows updated", result.Message)

	result = run(t, exec, "SELECT name FROM users WHERE id > 1")
	for _, row := range result.Rows {
		assert.Equal(t, "updated", row[0])
	}
}

func TestUpdateNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "UPDATE users SET name = 'bob' WHERE id = 999")
	assert.Equal(t, "0 rows updated", result.Message)
}

func TestUpdateNoWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "UPDATE users SET name = 'updated'")
	assert.Equal(t, "2 rows updated", result.Message)

	result = run(t, exec, "SELECT name FROM users")
	for _, row := range result.Rows {
		assert.Equal(t, "updated", row[0])
	}
}

func TestUpdateMultipleSets(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 20)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")

	result := run(t, exec, "UPDATE users SET name = 'ALICE', age = 30 WHERE id = 1")
	assert.Equal(t, "1 row updated", result.Message)

	result = run(t, exec, "SELECT name, age FROM users WHERE id = 1")
	assert.Equal(t, "ALICE", result.Rows[0][0])
	assert.Equal(t, int64(30), result.Rows[0][1])
}

func TestErrorUpdateTypeMismatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	runExpectError(t, exec, "UPDATE users SET id = 'not_int' WHERE id = 1")
}

func TestErrorUpdateNotNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	runExpectError(t, exec, "UPDATE users SET id = NULL WHERE id = 1")
}

func TestUpdateWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 20)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie', 30)")

	result := run(t, exec, "UPDATE users SET age = 21 WHERE name = 'alice'")
	assert.Equal(t, "1 row updated", result.Message)

	// Verify the update
	result = run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	require.Len(t, result.Rows, 1, "expected 1 row for alice")
	assert.Equal(t, int64(21), result.Rows[0][2])

	// Verify other rows unchanged
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	require.Len(t, result.Rows, 1, "expected 1 row for bob")
	assert.Equal(t, int64(25), result.Rows[0][2])
}

func TestUpdateWithIndexRange(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE products (id INT, price INT)")
	run(t, exec, "CREATE INDEX idx_price ON products(price)")
	run(t, exec, "INSERT INTO products VALUES (1, 100)")
	run(t, exec, "INSERT INTO products VALUES (2, 200)")
	run(t, exec, "INSERT INTO products VALUES (3, 300)")
	run(t, exec, "INSERT INTO products VALUES (4, 400)")

	result := run(t, exec, "UPDATE products SET price = 999 WHERE price >= 200 AND price <= 300")
	assert.Equal(t, "2 rows updated", result.Message)

	// Verify updates
	result = run(t, exec, "SELECT * FROM products ORDER BY id")
	require.Len(t, result.Rows, 4, "expected 4 rows in products")
	expected := []int64{100, 999, 999, 400}
	for i, row := range result.Rows {
		assert.Equal(t, expected[i], row[1], "row %d: unexpected price", i)
	}
}

func TestUpdateOrderByLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	// UPDATE with ORDER BY id ASC LIMIT 2 → updates only id=1 and id=2
	result := run(t, exec, "UPDATE users SET name = 'updated' ORDER BY id LIMIT 2")
	assert.Equal(t, "2 rows updated", result.Message)

	result = run(t, exec, "SELECT id, name FROM users ORDER BY id")
	require.Len(t, result.Rows, 4, "expected 4 rows in users")
	expected := []string{"updated", "updated", "charlie", "dave"}
	for i, row := range result.Rows {
		assert.Equal(t, expected[i], row[1], "row %d: unexpected name", i)
	}
}

func TestUpdateOrderByDescLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	// UPDATE with ORDER BY id DESC LIMIT 1 → updates only id=3
	result := run(t, exec, "UPDATE users SET name = 'updated' ORDER BY id DESC LIMIT 1")
	assert.Equal(t, "1 row updated", result.Message)

	result = run(t, exec, "SELECT id, name FROM users ORDER BY id")
	expected := []string{"alice", "bob", "updated"}
	for i, row := range result.Rows {
		assert.Equal(t, expected[i], row[1], "row %d: unexpected name", i)
	}
}

func TestUpdateWhereOrderByLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	// WHERE id > 1 → {2,3,4}, ORDER BY id DESC → {4,3,2}, LIMIT 2 → {4,3}
	result := run(t, exec, "UPDATE users SET name = 'updated' WHERE id > 1 ORDER BY id DESC LIMIT 2")
	assert.Equal(t, "2 rows updated", result.Message)

	result = run(t, exec, "SELECT id, name FROM users ORDER BY id")
	expected := []string{"alice", "bob", "updated", "updated"}
	for i, row := range result.Rows {
		assert.Equal(t, expected[i], row[1], "row %d: unexpected name", i)
	}
}

func TestUpdateNoWhereWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, active INT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 0)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 0)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie', 0)")

	result := run(t, exec, "UPDATE users SET active = 1")
	assert.Equal(t, "3 rows updated", result.Message)

	// Verify all rows updated
	result = run(t, exec, "SELECT * FROM users")
	for _, row := range result.Rows {
		assert.Equal(t, int64(1), row[2])
	}
}
