package engine

import (
	"testing"
)

func TestUpdateBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "UPDATE users SET name = 'ALICE' WHERE id = 1")
	if result.Message != "1 row updated" {
		t.Errorf("expected '1 row updated', got %q", result.Message)
	}

	result = run(t, exec, "SELECT name FROM users WHERE id = 1")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "ALICE" {
		t.Errorf("expected 'ALICE', got %v", result.Rows[0][0])
	}
}

func TestUpdateMultipleRows(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "UPDATE users SET name = 'updated' WHERE id > 1")
	if result.Message != "2 rows updated" {
		t.Errorf("expected '2 rows updated', got %q", result.Message)
	}

	result = run(t, exec, "SELECT name FROM users WHERE id > 1")
	for _, row := range result.Rows {
		if row[0] != "updated" {
			t.Errorf("expected 'updated', got %v", row[0])
		}
	}
}

func TestUpdateNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "UPDATE users SET name = 'bob' WHERE id = 999")
	if result.Message != "0 rows updated" {
		t.Errorf("expected '0 rows updated', got %q", result.Message)
	}
}

func TestUpdateNoWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "UPDATE users SET name = 'updated'")
	if result.Message != "2 rows updated" {
		t.Errorf("expected '2 rows updated', got %q", result.Message)
	}

	result = run(t, exec, "SELECT name FROM users")
	for _, row := range result.Rows {
		if row[0] != "updated" {
			t.Errorf("expected 'updated', got %v", row[0])
		}
	}
}

func TestUpdateMultipleSets(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 20)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")

	result := run(t, exec, "UPDATE users SET name = 'ALICE', age = 30 WHERE id = 1")
	if result.Message != "1 row updated" {
		t.Errorf("expected '1 row updated', got %q", result.Message)
	}

	result = run(t, exec, "SELECT name, age FROM users WHERE id = 1")
	if result.Rows[0][0] != "ALICE" {
		t.Errorf("expected 'ALICE', got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != int64(30) {
		t.Errorf("expected 30, got %v", result.Rows[0][1])
	}
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
	if result.Message != "1 row updated" {
		t.Errorf("expected '1 row updated', got %q", result.Message)
	}

	// Verify the update
	result = run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][2] != int64(21) {
		t.Errorf("expected age=21, got %v", result.Rows[0][2])
	}

	// Verify other rows unchanged
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][2] != int64(25) {
		t.Errorf("expected age=25, got %v", result.Rows[0][2])
	}
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
	if result.Message != "2 rows updated" {
		t.Errorf("expected '2 rows updated', got %q", result.Message)
	}

	// Verify updates
	result = run(t, exec, "SELECT * FROM products ORDER BY id")
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
	expected := []int64{100, 999, 999, 400}
	for i, row := range result.Rows {
		if row[1] != expected[i] {
			t.Errorf("row %d: expected price=%d, got %v", i, expected[i], row[1])
		}
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
	if result.Message != "3 rows updated" {
		t.Errorf("expected '3 rows updated', got %q", result.Message)
	}

	// Verify all rows updated
	result = run(t, exec, "SELECT * FROM users")
	for _, row := range result.Rows {
		if row[2] != int64(1) {
			t.Errorf("expected active=1, got %v", row[2])
		}
	}
}
