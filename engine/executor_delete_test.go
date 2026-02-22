package engine

import (
	"testing"
)

func TestDeleteWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users WHERE id = 2")
	if result.Message != "1 row deleted" {
		t.Errorf("expected '1 row deleted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(3) {
		t.Errorf("expected id=3, got %v", result.Rows[1][0])
	}
}

func TestDeleteMultipleRows(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users WHERE id > 1")
	if result.Message != "2 rows deleted" {
		t.Errorf("expected '2 rows deleted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestDeleteNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "DELETE FROM users WHERE id = 999")
	if result.Message != "0 rows deleted" {
		t.Errorf("expected '0 rows deleted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestDeleteNoWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "DELETE FROM users")
	if result.Message != "2 rows deleted" {
		t.Errorf("expected '2 rows deleted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestDeleteWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users WHERE name = 'bob'")
	if result.Message != "1 row deleted" {
		t.Errorf("expected '1 row deleted', got %q", result.Message)
	}

	// Verify deletion
	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	for _, row := range result.Rows {
		if row[1] == "bob" {
			t.Errorf("bob should have been deleted")
		}
	}
}

func TestDeleteWithIndexIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	result := run(t, exec, "DELETE FROM users WHERE name IN ('bob', 'dave')")
	if result.Message != "2 rows deleted" {
		t.Errorf("expected '2 rows deleted', got %q", result.Message)
	}

	// Verify remaining rows
	result = run(t, exec, "SELECT * FROM users ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("expected 'alice', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "charlie" {
		t.Errorf("expected 'charlie', got %v", result.Rows[1][1])
	}
}

func TestDeleteOrderByLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	// DELETE with ORDER BY id ASC LIMIT 2 → deletes id=1 and id=2
	result := run(t, exec, "DELETE FROM users ORDER BY id LIMIT 2")
	if result.Message != "2 rows deleted" {
		t.Errorf("expected '2 rows deleted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT id, name FROM users ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected id=3, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(4) {
		t.Errorf("expected id=4, got %v", result.Rows[1][0])
	}
}

func TestDeleteOrderByDescLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	// DELETE with ORDER BY id DESC LIMIT 1 → deletes id=3
	result := run(t, exec, "DELETE FROM users ORDER BY id DESC LIMIT 1")
	if result.Message != "1 row deleted" {
		t.Errorf("expected '1 row deleted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT id FROM users ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[1][0])
	}
}

func TestDeleteWhereOrderByLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	// WHERE id > 1 → {2,3,4}, ORDER BY id DESC → {4,3,2}, LIMIT 2 → deletes {4,3}
	result := run(t, exec, "DELETE FROM users WHERE id > 1 ORDER BY id DESC LIMIT 2")
	if result.Message != "2 rows deleted" {
		t.Errorf("expected '2 rows deleted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT id FROM users ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[1][0])
	}
}

func TestDeleteNoWhereWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "DELETE FROM users")
	if result.Message != "3 rows deleted" {
		t.Errorf("expected '3 rows deleted', got %q", result.Message)
	}

	// Verify all rows deleted
	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}
