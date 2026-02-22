package engine

import (
	"testing"
)

func TestInsertMultipleRows(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")

	result := run(t, exec, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
	if result.Message != "3 rows inserted" {
		t.Errorf("expected '3 rows inserted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT COUNT(*) FROM users")
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected COUNT(*)=3, got %v", result.Rows[0][0])
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("row 0: expected 'alice', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "bob" {
		t.Errorf("row 1: expected 'bob', got %v", result.Rows[1][1])
	}
	if result.Rows[2][1] != "charlie" {
		t.Errorf("row 2: expected 'charlie', got %v", result.Rows[2][1])
	}
}

func TestInsertNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	if result.Message != "1 row inserted" {
		t.Errorf("expected '1 row inserted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != nil {
		t.Errorf("expected name=nil, got %v", result.Rows[0][1])
	}
}

func TestInsertNotNullSuccess(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	result := run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	if result.Message != "1 row inserted" {
		t.Errorf("expected '1 row inserted', got %q", result.Message)
	}
}

func TestInsertNotNullViolation(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES (NULL, 'alice')")
}

func TestInsertNullableColumnStillAllowsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	result := run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	if result.Message != "1 row inserted" {
		t.Errorf("expected '1 row inserted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != nil {
		t.Errorf("expected name=nil, got %v", result.Rows[0][1])
	}
}

func TestErrorInsertNonexistentTable(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "INSERT INTO nonexistent VALUES (1)")
}

func TestErrorInsertWrongValueCount(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES (1)")
}

func TestErrorInsertTypeMismatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES ('not_int', 'alice')")
}

func TestInsertWithColumnsAllColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	if result.Message != "1 row inserted" {
		t.Errorf("expected '1 row inserted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("expected name='alice', got %v", result.Rows[0][1])
	}
}

func TestInsertWithColumnsReorder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users (name, id) VALUES ('alice', 1)")

	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("expected name='alice', got %v", result.Rows[0][1])
	}
}

func TestInsertPartialColumnsWithDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT DEFAULT 'unknown')")
	run(t, exec, "INSERT INTO users (id) VALUES (1)")

	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "unknown" {
		t.Errorf("expected name='unknown', got %v", result.Rows[0][1])
	}
}

func TestInsertPartialColumnsNoDefaultGetsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users (id) VALUES (1)")

	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != nil {
		t.Errorf("expected name=nil, got %v", result.Rows[0][1])
	}
}

func TestErrorInsertPartialColumnsNotNullNoDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (name) VALUES ('alice')")
}

func TestInsertPartialColumnsNotNullWithDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT NOT NULL DEFAULT 0, name TEXT)")
	run(t, exec, "INSERT INTO users (name) VALUES ('alice')")

	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(0) {
		t.Errorf("expected id=0, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("expected name='alice', got %v", result.Rows[0][1])
	}
}

func TestErrorInsertDuplicateColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (id, id) VALUES (1, 2)")
}

func TestErrorInsertNonexistentColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (id, foo) VALUES (1, 'bar')")
}

func TestErrorInsertColumnValueCountMismatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users (id, name) VALUES (1)")
}

func TestInsertWithColumnsMultipleRows(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT DEFAULT 'unknown')")
	result := run(t, exec, "INSERT INTO users (id) VALUES (1), (2), (3)")
	if result.Message != "3 rows inserted" {
		t.Errorf("expected '3 rows inserted', got %q", result.Message)
	}

	result = run(t, exec, "SELECT * FROM users ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		if row[0] != int64(i+1) {
			t.Errorf("row %d: expected id=%d, got %v", i, i+1, row[0])
		}
		if row[1] != "unknown" {
			t.Errorf("row %d: expected name='unknown', got %v", i, row[1])
		}
	}
}
