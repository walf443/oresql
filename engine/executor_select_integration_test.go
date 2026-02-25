package engine

import (
	"testing"
)

func TestSelectWhereEq(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT name FROM users WHERE id = 1")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" {
		t.Errorf("expected 'alice', got %v", result.Rows[0][0])
	}
}

func TestSelectWhereGt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id > 1")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSelectWhereAnd(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id = 1 AND name = 'alice'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestSelectWhereOr(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id = 1 OR id = 3")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSelectNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "SELECT * FROM users WHERE id = 999")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestSelectQualifiedColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT users.id, users.name FROM users")
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("columns: expected [id, name], got %v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSelectQualifiedWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE users.id = 1")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestErrorSelectWrongTableQualifier(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "SELECT other.id FROM users")
}

func TestErrorWhereWrongTableQualifier(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	runExpectError(t, exec, "SELECT * FROM users WHERE other.id = 1")
}

func TestSelectCountStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT COUNT(*) FROM users")
	if len(result.Columns) != 1 || result.Columns[0] != "COUNT(*)" {
		t.Errorf("expected columns [COUNT(*)], got %v", result.Columns)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected COUNT(*)=3, got %v", result.Rows[0][0])
	}
}

func TestSelectCountStarWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT COUNT(*) FROM users WHERE id > 1")
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected COUNT(*)=2, got %v", result.Rows[0][0])
	}
}

func TestSelectCountStarEmpty(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT)")

	result := run(t, exec, "SELECT COUNT(*) FROM users")
	if result.Rows[0][0] != int64(0) {
		t.Errorf("expected COUNT(*)=0, got %v", result.Rows[0][0])
	}
}

func TestSelectCountColumnExcludesNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, NULL)")

	// COUNT(*) counts all rows including NULLs
	result := run(t, exec, "SELECT COUNT(*) FROM users")
	if result.Rows[0][0] != int64(4) {
		t.Errorf("expected COUNT(*)=4, got %v", result.Rows[0][0])
	}

	// COUNT(name) excludes NULLs
	result = run(t, exec, "SELECT COUNT(name) FROM users")
	if result.Columns[0] != "COUNT(name)" {
		t.Errorf("expected column name COUNT(name), got %s", result.Columns[0])
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected COUNT(name)=2, got %v", result.Rows[0][0])
	}
}

func TestSelectCountLiteral(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, NULL)")

	// COUNT(1) should count all rows (same as COUNT(*))
	result := run(t, exec, "SELECT COUNT(1) FROM users")
	if result.Columns[0] != "COUNT(1)" {
		t.Errorf("expected column name COUNT(1), got %s", result.Columns[0])
	}
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected COUNT(1)=3, got %v", result.Rows[0][0])
	}
}

func TestSelectWithoutFrom(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1
	result := run(t, exec, "SELECT 1")
	if len(result.Columns) != 1 || result.Columns[0] != "1" {
		t.Errorf("expected columns [1], got %v", result.Columns)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(1) {
		t.Errorf("expected row [1], got %v", result.Rows)
	}

	// SELECT 1, 'hello'
	result = run(t, exec, "SELECT 1, 'hello'")
	if len(result.Columns) != 2 || result.Columns[0] != "1" || result.Columns[1] != "'hello'" {
		t.Errorf("expected columns [1, 'hello'], got %v", result.Columns)
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected first column 1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "hello" {
		t.Errorf("expected second column 'hello', got %v", result.Rows[0][1])
	}
}

func TestSelectAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT id AS user_id FROM users")
	if len(result.Columns) != 1 || result.Columns[0] != "user_id" {
		t.Errorf("expected columns [user_id], got %v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestSelectCountAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT COUNT(*) AS total FROM users")
	if len(result.Columns) != 1 || result.Columns[0] != "total" {
		t.Errorf("expected columns [total], got %v", result.Columns)
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected COUNT(*)=2, got %v", result.Rows[0][0])
	}
}

func TestSelectLiteralAlias(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, "SELECT 1 AS one")
	if len(result.Columns) != 1 || result.Columns[0] != "one" {
		t.Errorf("expected columns [one], got %v", result.Columns)
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected 1, got %v", result.Rows[0][0])
	}
}

func TestSelectQuotedIdent(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (`count` INT)")
	run(t, exec, "INSERT INTO t VALUES (42)")

	result := run(t, exec, "SELECT `count` FROM t")
	if len(result.Columns) != 1 || result.Columns[0] != "count" {
		t.Errorf("expected columns [count], got %v", result.Columns)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(42) {
		t.Errorf("expected 42, got %v", result.Rows[0][0])
	}
}

func TestSelectArithmetic(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1 * 2 → 2
	result := run(t, exec, "SELECT 1 * 2")
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected 1*2=2, got %v", result.Rows[0][0])
	}

	// SELECT 1 + 2 * 3 → 7 (precedence)
	result = run(t, exec, "SELECT 1 + 2 * 3")
	if result.Rows[0][0] != int64(7) {
		t.Errorf("expected 1+2*3=7, got %v", result.Rows[0][0])
	}

	// SELECT 10 / 3 → 3 (integer division)
	result = run(t, exec, "SELECT 10 / 3")
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected 10/3=3, got %v", result.Rows[0][0])
	}

	// SELECT 10 - 3 → 7
	result = run(t, exec, "SELECT 10 - 3")
	if result.Rows[0][0] != int64(7) {
		t.Errorf("expected 10-3=7, got %v", result.Rows[0][0])
	}
}

func TestSelectArithmeticWithTable(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (price INT)")
	run(t, exec, "INSERT INTO items VALUES (10)")
	run(t, exec, "INSERT INTO items VALUES (20)")

	result := run(t, exec, "SELECT price * 2 FROM items")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(20) {
		t.Errorf("expected 10*2=20, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(40) {
		t.Errorf("expected 20*2=40, got %v", result.Rows[1][0])
	}
}

func TestSelectArithmeticInWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (price INT)")
	run(t, exec, "INSERT INTO items VALUES (5)")
	run(t, exec, "INSERT INTO items VALUES (10)")
	run(t, exec, "INSERT INTO items VALUES (20)")

	result := run(t, exec, "SELECT price FROM items WHERE price * 2 > 15")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(10) {
		t.Errorf("expected 10, got %v", result.Rows[0][0])
	}
}

func TestErrorDivisionByZero(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "SELECT 1 / 0")
}

func TestSelectUnaryMinus(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, "SELECT -1")
	if result.Rows[0][0] != int64(-1) {
		t.Errorf("expected -1, got %v", result.Rows[0][0])
	}

	result = run(t, exec, "SELECT -2 + 5")
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected -2+5=3, got %v", result.Rows[0][0])
	}
}

func TestErrorSelectNonexistentTable(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "SELECT * FROM nonexistent")
}

func TestErrorSelectNonexistentColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT)")
	runExpectError(t, exec, "SELECT foo FROM users")
}

func TestSelectWhereIsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT id FROM users WHERE name IS NULL")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereIsNotNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT id FROM users WHERE name IS NOT NULL")
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

func TestNullComparisonReturnsFalse(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// NULL = 'bob' should be false (SQL semantics)
	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestSelectOrderByAsc(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("row 0: expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("row 1: expected id=2, got %v", result.Rows[1][0])
	}
	if result.Rows[2][0] != int64(3) {
		t.Errorf("row 2: expected id=3, got %v", result.Rows[2][0])
	}
}

func TestSelectOrderByDesc(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users ORDER BY id DESC")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(3) {
		t.Errorf("row 0: expected id=3, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("row 1: expected id=2, got %v", result.Rows[1][0])
	}
	if result.Rows[2][0] != int64(1) {
		t.Errorf("row 2: expected id=1, got %v", result.Rows[2][0])
	}
}

func TestSelectOrderByMultipleColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 20)")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice', 20)")

	result := run(t, exec, "SELECT * FROM users ORDER BY name ASC, age ASC")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	// alice age=20 first, then alice age=30, then bob age=20
	if result.Rows[0][0] != int64(3) {
		t.Errorf("row 0: expected id=3, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(1) {
		t.Errorf("row 1: expected id=1, got %v", result.Rows[1][0])
	}
	if result.Rows[2][0] != int64(2) {
		t.Errorf("row 2: expected id=2, got %v", result.Rows[2][0])
	}
}

func TestSelectWhereOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id > 1 ORDER BY id DESC")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(3) {
		t.Errorf("row 0: expected id=3, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("row 1: expected id=2, got %v", result.Rows[1][0])
	}
}

func TestSelectOrderByWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	// NULLs should sort last in ASC
	result := run(t, exec, "SELECT * FROM users ORDER BY name ASC")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("row 0: expected name='alice', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "bob" {
		t.Errorf("row 1: expected name='bob', got %v", result.Rows[1][1])
	}
	if result.Rows[2][1] != nil {
		t.Errorf("row 2: expected name=nil, got %v", result.Rows[2][1])
	}

	// NULLs should sort last in DESC too
	result = run(t, exec, "SELECT * FROM users ORDER BY name DESC")
	if result.Rows[0][1] != "bob" {
		t.Errorf("row 0: expected name='bob', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "alice" {
		t.Errorf("row 1: expected name='alice', got %v", result.Rows[1][1])
	}
	if result.Rows[2][1] != nil {
		t.Errorf("row 2: expected name=nil, got %v", result.Rows[2][1])
	}
}

func TestSelectLimitOnly(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("row 0: expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("row 1: expected id=2, got %v", result.Rows[1][0])
	}
}

func TestSelectOffsetOnly(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users OFFSET 1")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("row 0: expected id=2, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(3) {
		t.Errorf("row 1: expected id=3, got %v", result.Rows[1][0])
	}
}

func TestSelectLimitOffset(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	result := run(t, exec, "SELECT * FROM users LIMIT 2 OFFSET 1")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("row 0: expected id=2, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(3) {
		t.Errorf("row 1: expected id=3, got %v", result.Rows[1][0])
	}
}

func TestSelectOrderByLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users ORDER BY id ASC LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("row 0: expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("row 1: expected id=2, got %v", result.Rows[1][0])
	}
}

func TestSelectOffsetExceedsRowCount(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users OFFSET 10")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestSelectGroupByBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users GROUP BY name")
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "name" || result.Columns[1] != "COUNT(*)" {
		t.Errorf("expected columns [name, COUNT(*)], got %v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// alice group first (insertion order), then bob
	if result.Rows[0][0] != "alice" {
		t.Errorf("row 0: expected name='alice', got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != int64(2) {
		t.Errorf("row 0: expected COUNT(*)=2, got %v", result.Rows[0][1])
	}
	if result.Rows[1][0] != "bob" {
		t.Errorf("row 1: expected name='bob', got %v", result.Rows[1][0])
	}
	if result.Rows[1][1] != int64(1) {
		t.Errorf("row 1: expected COUNT(*)=1, got %v", result.Rows[1][1])
	}
}

func TestSelectGroupByWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (4, 'bob')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users WHERE id > 1 GROUP BY name")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// bob appears first because id=2 is the first row after WHERE filter with name='bob'
	if result.Rows[0][0] != "bob" {
		t.Errorf("row 0: expected name='bob', got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != int64(2) {
		t.Errorf("row 0: expected COUNT(*)=2, got %v", result.Rows[0][1])
	}
	if result.Rows[1][0] != "alice" {
		t.Errorf("row 1: expected name='alice', got %v", result.Rows[1][0])
	}
	if result.Rows[1][1] != int64(1) {
		t.Errorf("row 1: expected COUNT(*)=1, got %v", result.Rows[1][1])
	}
}

func TestSelectGroupByOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users GROUP BY name ORDER BY name ASC")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" {
		t.Errorf("row 0: expected name='alice', got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != "bob" {
		t.Errorf("row 1: expected name='bob', got %v", result.Rows[1][0])
	}
}

func TestSelectGroupByHaving(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (4, 'charlie')")

	result := run(t, exec, "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" {
		t.Errorf("row 0: expected name='alice', got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != int64(2) {
		t.Errorf("row 0: expected COUNT(*)=2, got %v", result.Rows[0][1])
	}
}

func TestSelectGroupByMultipleColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (product TEXT, region TEXT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES ('A', 'east', 10)")
	run(t, exec, "INSERT INTO orders VALUES ('A', 'east', 20)")
	run(t, exec, "INSERT INTO orders VALUES ('A', 'west', 30)")
	run(t, exec, "INSERT INTO orders VALUES ('B', 'east', 40)")

	result := run(t, exec, "SELECT product, region, COUNT(*) FROM orders GROUP BY product, region")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	// Order by first appearance: (A, east), (A, west), (B, east)
	if result.Rows[0][0] != "A" || result.Rows[0][1] != "east" {
		t.Errorf("row 0: expected (A, east), got (%v, %v)", result.Rows[0][0], result.Rows[0][1])
	}
	if result.Rows[0][2] != int64(2) {
		t.Errorf("row 0: expected COUNT(*)=2, got %v", result.Rows[0][2])
	}
	if result.Rows[1][0] != "A" || result.Rows[1][1] != "west" {
		t.Errorf("row 1: expected (A, west), got (%v, %v)", result.Rows[1][0], result.Rows[1][1])
	}
	if result.Rows[1][2] != int64(1) {
		t.Errorf("row 1: expected COUNT(*)=1, got %v", result.Rows[1][2])
	}
	if result.Rows[2][0] != "B" || result.Rows[2][1] != "east" {
		t.Errorf("row 2: expected (B, east), got (%v, %v)", result.Rows[2][0], result.Rows[2][1])
	}
}

func TestSelectSumBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 300)")

	result := run(t, exec, "SELECT SUM(amount) FROM orders")
	if len(result.Columns) != 1 || result.Columns[0] != "SUM(amount)" {
		t.Errorf("expected columns [SUM(amount)], got %v", result.Columns)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(600) {
		t.Errorf("expected SUM(amount)=600, got %v", result.Rows[0][0])
	}
}

func TestSelectSumWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, region TEXT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 'east', 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 'west', 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 'east', 300)")

	result := run(t, exec, "SELECT SUM(amount) FROM orders WHERE region = 'east'")
	if result.Rows[0][0] != int64(400) {
		t.Errorf("expected SUM(amount)=400, got %v", result.Rows[0][0])
	}
}

func TestSelectAvgBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 10)")
	run(t, exec, "INSERT INTO orders VALUES (2, 20)")
	run(t, exec, "INSERT INTO orders VALUES (3, 30)")

	result := run(t, exec, "SELECT AVG(amount) FROM orders")
	if len(result.Columns) != 1 || result.Columns[0] != "AVG(amount)" {
		t.Errorf("expected columns [AVG(amount)], got %v", result.Columns)
	}
	if result.Rows[0][0] != float64(20) {
		t.Errorf("expected AVG(amount)=20.0, got %v (%T)", result.Rows[0][0], result.Rows[0][0])
	}
}

func TestSelectMinMaxInt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT MIN(id), MAX(id) FROM users")
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "MIN(id)" || result.Columns[1] != "MAX(id)" {
		t.Errorf("expected columns [MIN(id), MAX(id)], got %v", result.Columns)
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected MIN(id)=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != int64(3) {
		t.Errorf("expected MAX(id)=3, got %v", result.Rows[0][1])
	}
}

func TestSelectMinMaxText(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	result := run(t, exec, "SELECT MIN(name), MAX(name) FROM users")
	if result.Rows[0][0] != "alice" {
		t.Errorf("expected MIN(name)='alice', got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "charlie" {
		t.Errorf("expected MAX(name)='charlie', got %v", result.Rows[0][1])
	}
}

func TestSelectAggregatesWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, value INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 10)")
	run(t, exec, "INSERT INTO scores VALUES (2, NULL)")
	run(t, exec, "INSERT INTO scores VALUES (3, 30)")
	run(t, exec, "INSERT INTO scores VALUES (4, NULL)")

	// SUM should skip NULLs
	result := run(t, exec, "SELECT SUM(value) FROM scores")
	if result.Rows[0][0] != int64(40) {
		t.Errorf("expected SUM(value)=40, got %v", result.Rows[0][0])
	}

	// AVG should skip NULLs (40 / 2 = 20)
	result = run(t, exec, "SELECT AVG(value) FROM scores")
	if result.Rows[0][0] != float64(20) {
		t.Errorf("expected AVG(value)=20.0, got %v (%T)", result.Rows[0][0], result.Rows[0][0])
	}

	// MIN should skip NULLs
	result = run(t, exec, "SELECT MIN(value) FROM scores")
	if result.Rows[0][0] != int64(10) {
		t.Errorf("expected MIN(value)=10, got %v", result.Rows[0][0])
	}

	// MAX should skip NULLs
	result = run(t, exec, "SELECT MAX(value) FROM scores")
	if result.Rows[0][0] != int64(30) {
		t.Errorf("expected MAX(value)=30, got %v", result.Rows[0][0])
	}
}

func TestSelectAggregatesAllNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, value INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, NULL)")
	run(t, exec, "INSERT INTO scores VALUES (2, NULL)")

	// SUM of all NULLs should return NULL
	result := run(t, exec, "SELECT SUM(value) FROM scores")
	if result.Rows[0][0] != nil {
		t.Errorf("expected SUM(value)=NULL, got %v", result.Rows[0][0])
	}

	// AVG of all NULLs should return NULL
	result = run(t, exec, "SELECT AVG(value) FROM scores")
	if result.Rows[0][0] != nil {
		t.Errorf("expected AVG(value)=NULL, got %v", result.Rows[0][0])
	}

	// MIN of all NULLs should return NULL
	result = run(t, exec, "SELECT MIN(value) FROM scores")
	if result.Rows[0][0] != nil {
		t.Errorf("expected MIN(value)=NULL, got %v", result.Rows[0][0])
	}

	// MAX of all NULLs should return NULL
	result = run(t, exec, "SELECT MAX(value) FROM scores")
	if result.Rows[0][0] != nil {
		t.Errorf("expected MAX(value)=NULL, got %v", result.Rows[0][0])
	}
}

func TestSelectGroupBySumBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, name TEXT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 'alice', 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 'bob', 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 'alice', 300)")

	result := run(t, exec, "SELECT name, SUM(amount) FROM orders GROUP BY name")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// alice first (insertion order)
	if result.Rows[0][0] != "alice" {
		t.Errorf("row 0: expected name='alice', got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != int64(400) {
		t.Errorf("row 0: expected SUM(amount)=400, got %v", result.Rows[0][1])
	}
	if result.Rows[1][0] != "bob" {
		t.Errorf("row 1: expected name='bob', got %v", result.Rows[1][0])
	}
	if result.Rows[1][1] != int64(200) {
		t.Errorf("row 1: expected SUM(amount)=200, got %v", result.Rows[1][1])
	}
}

func TestSelectSumEmpty(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (id INT, amount INT)")

	// SUM on empty table should return NULL
	result := run(t, exec, "SELECT SUM(amount) FROM orders")
	if result.Rows[0][0] != nil {
		t.Errorf("expected SUM(amount)=NULL for empty table, got %v", result.Rows[0][0])
	}
}

func TestFloatColumnInsertSelect(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (3.14)")

	result := run(t, exec, "SELECT val FROM t")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != float64(3.14) {
		t.Errorf("expected 3.14, got %v", result.Rows[0][0])
	}
}

func TestFloatColumnInsertIntAutoConvert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (42)")

	result := run(t, exec, "SELECT val FROM t")
	if result.Rows[0][0] != float64(42) {
		t.Errorf("expected 42.0, got %v (%T)", result.Rows[0][0], result.Rows[0][0])
	}
}

func TestErrorIntColumnInsertFloat(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val INT)")
	runExpectError(t, exec, "INSERT INTO t VALUES (3.14)")
}

func TestFloatArithmetic(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1.5 + 2.5
	result := run(t, exec, "SELECT 1.5 + 2.5")
	if result.Rows[0][0] != float64(4.0) {
		t.Errorf("expected 1.5+2.5=4.0, got %v", result.Rows[0][0])
	}
}

func TestFloatIntMixedArithmetic(t *testing.T) {
	exec := NewExecutor()

	// SELECT 1 + 0.5
	result := run(t, exec, "SELECT 1 + 0.5")
	if result.Rows[0][0] != float64(1.5) {
		t.Errorf("expected 1+0.5=1.5, got %v", result.Rows[0][0])
	}
}

func TestAvgReturnsFloat(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, value INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 10)")
	run(t, exec, "INSERT INTO scores VALUES (2, 20)")
	run(t, exec, "INSERT INTO scores VALUES (3, 20)")

	result := run(t, exec, "SELECT AVG(value) FROM scores")
	avg, ok := result.Rows[0][0].(float64)
	if !ok {
		t.Fatalf("expected AVG to return float64, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	}
	// AVG(10, 20, 20) = 50/3 ≈ 16.666...
	if avg < 16.66 || avg > 16.67 {
		t.Errorf("expected AVG(value)≈16.666, got %v", avg)
	}
}

func TestSumFloatColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")
	run(t, exec, "INSERT INTO t VALUES (3.0)")

	result := run(t, exec, "SELECT SUM(val) FROM t")
	if result.Rows[0][0] != float64(7.0) {
		t.Errorf("expected SUM(val)=7.0, got %v", result.Rows[0][0])
	}
}

func TestFloatComparison(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")
	run(t, exec, "INSERT INTO t VALUES (3.5)")

	result := run(t, exec, "SELECT val FROM t WHERE val > 2.0")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != float64(2.5) {
		t.Errorf("row 0: expected 2.5, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != float64(3.5) {
		t.Errorf("row 1: expected 3.5, got %v", result.Rows[1][0])
	}
}

func TestFloatOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (3.5)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")

	result := run(t, exec, "SELECT val FROM t ORDER BY val ASC")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != float64(1.5) {
		t.Errorf("row 0: expected 1.5, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != float64(2.5) {
		t.Errorf("row 1: expected 2.5, got %v", result.Rows[1][0])
	}
	if result.Rows[2][0] != float64(3.5) {
		t.Errorf("row 2: expected 3.5, got %v", result.Rows[2][0])
	}
}

func TestFloatMinMax(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (3.5)")
	run(t, exec, "INSERT INTO t VALUES (1.5)")
	run(t, exec, "INSERT INTO t VALUES (2.5)")

	result := run(t, exec, "SELECT MIN(val), MAX(val) FROM t")
	if result.Rows[0][0] != float64(1.5) {
		t.Errorf("expected MIN(val)=1.5, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != float64(3.5) {
		t.Errorf("expected MAX(val)=3.5, got %v", result.Rows[0][1])
	}
}

func TestFloatIntMixedComparison(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1.5)")
	run(t, exec, "INSERT INTO t VALUES (2, 2.5)")

	result := run(t, exec, "SELECT id FROM t WHERE val > 2")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestFloatUpdateSet(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1.0)")

	run(t, exec, "UPDATE t SET val = 9.99")
	result := run(t, exec, "SELECT val FROM t")
	if result.Rows[0][0] != float64(9.99) {
		t.Errorf("expected 9.99, got %v", result.Rows[0][0])
	}
}

func TestSelectDistinctBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")

	result := run(t, exec, "SELECT DISTINCT name FROM users")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" {
		t.Errorf("row 0: expected 'alice', got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != "bob" {
		t.Errorf("row 1: expected 'bob', got %v", result.Rows[1][0])
	}
}

func TestSelectDistinctStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "SELECT DISTINCT * FROM users")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSelectDistinctOrderBy(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")

	result := run(t, exec, "SELECT DISTINCT name FROM users ORDER BY name")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" {
		t.Errorf("row 0: expected 'alice', got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != "bob" {
		t.Errorf("row 1: expected 'bob', got %v", result.Rows[1][0])
	}
}

func TestSelectDistinctLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (4, 'charlie')")

	result := run(t, exec, "SELECT DISTINCT name FROM users LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSelectWhereIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (1, 3)")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("row 0: expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(3) {
		t.Errorf("row 1: expected id=3, got %v", result.Rows[1][0])
	}
}

func TestSelectWhereInNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (10, 20)")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestSelectWhereNotIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT * FROM users WHERE id NOT IN (2)")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("row 0: expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(3) {
		t.Errorf("row 1: expected id=3, got %v", result.Rows[1][0])
	}
}

func TestSelectWhereInLeftNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name IN ('bob', 'alice')")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereInWithNullValues(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name IN ('alice', NULL)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")
	run(t, exec, "INSERT INTO users VALUES (5, 'eve')")

	result := run(t, exec, "SELECT * FROM users WHERE id BETWEEN 2 AND 4")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("row 0: expected id=2, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(3) {
		t.Errorf("row 1: expected id=3, got %v", result.Rows[1][0])
	}
	if result.Rows[2][0] != int64(4) {
		t.Errorf("row 2: expected id=4, got %v", result.Rows[2][0])
	}
}

func TestSelectWhereNotBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")
	run(t, exec, "INSERT INTO users VALUES (5, 'eve')")

	result := run(t, exec, "SELECT * FROM users WHERE id NOT BETWEEN 2 AND 4")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("row 0: expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(5) {
		t.Errorf("row 1: expected id=5, got %v", result.Rows[1][0])
	}
}

func TestSelectWhereBetweenNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id BETWEEN 10 AND 20")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestSelectWhereBetweenNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (NULL, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE id BETWEEN 1 AND 10")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereBetweenText(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (4, 'dave')")

	result := run(t, exec, "SELECT * FROM users WHERE name BETWEEN 'bob' AND 'dave'")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "bob" {
		t.Errorf("row 0: expected name='bob', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "charlie" {
		t.Errorf("row 1: expected name='charlie', got %v", result.Rows[1][1])
	}
	if result.Rows[2][1] != "dave" {
		t.Errorf("row 2: expected name='dave', got %v", result.Rows[2][1])
	}
}

func TestSelectWhereLike(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice_smith')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE '%alice%'")
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

func TestSelectWhereNotLike(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice_smith')")

	result := run(t, exec, "SELECT * FROM users WHERE name NOT LIKE '%alice%'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereLikeUnderscore(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'abc')")
	run(t, exec, "INSERT INTO users VALUES (2, 'aXc')")
	run(t, exec, "INSERT INTO users VALUES (3, 'abbc')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE 'a_c'")
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

func TestSelectWhereLikeExact(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE 'alice'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereLikeNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE '%ali%'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereLikeEscapePercent(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, '100%')")
	run(t, exec, "INSERT INTO items VALUES (2, '100abc')")
	run(t, exec, "INSERT INTO items VALUES (3, '50%')")

	result := run(t, exec, "SELECT * FROM items WHERE name LIKE '%\\%'")
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

func TestSelectWhereLikeEscapeUnderscore(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, 'a_b')")
	run(t, exec, "INSERT INTO items VALUES (2, 'aXb')")
	run(t, exec, "INSERT INTO items VALUES (3, 'a_c')")

	result := run(t, exec, "SELECT * FROM items WHERE name LIKE 'a\\_b'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestSelectWhereLikeEscapeBackslash(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, 'a\\b')")
	run(t, exec, "INSERT INTO items VALUES (2, 'aXb')")

	result := run(t, exec, "SELECT * FROM items WHERE name LIKE 'a\\\\b'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestNotOperator(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, active INT)")
	run(t, exec, "INSERT INTO items VALUES (1, 1)")
	run(t, exec, "INSERT INTO items VALUES (2, 0)")
	run(t, exec, "INSERT INTO items VALUES (3, 1)")

	// NOT (id = 1)
	result := run(t, exec, "SELECT id FROM items WHERE NOT (id = 1)")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// NOT (id > 1)
	result = run(t, exec, "SELECT id FROM items WHERE NOT (id > 1)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}

	// NOT with AND
	result = run(t, exec, "SELECT id FROM items WHERE NOT (id = 1) AND NOT (id = 3)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}

	// NOT with OR
	result = run(t, exec, "SELECT id FROM items WHERE NOT (id = 1 OR id = 2)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected id=3, got %v", result.Rows[0][0])
	}

	// Double NOT
	result = run(t, exec, "SELECT id FROM items WHERE NOT NOT (id = 1)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestCast(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (i INT, f FLOAT, s TEXT)")
	run(t, exec, "INSERT INTO t VALUES (42, 3.14, 'hello')")
	run(t, exec, "INSERT INTO t VALUES (7, 2.0, '123')")

	// INT to TEXT
	result := run(t, exec, "SELECT CAST(i AS TEXT) FROM t WHERE i = 42")
	if result.Rows[0][0] != "42" {
		t.Errorf("CAST(42 AS TEXT): expected '42', got %v", result.Rows[0][0])
	}

	// INT to FLOAT
	result = run(t, exec, "SELECT CAST(i AS FLOAT) FROM t WHERE i = 42")
	if result.Rows[0][0] != float64(42) {
		t.Errorf("CAST(42 AS FLOAT): expected 42.0, got %v", result.Rows[0][0])
	}

	// FLOAT to INT (truncate)
	result = run(t, exec, "SELECT CAST(f AS INT) FROM t WHERE i = 42")
	if result.Rows[0][0] != int64(3) {
		t.Errorf("CAST(3.14 AS INT): expected 3, got %v", result.Rows[0][0])
	}

	// FLOAT to TEXT
	result = run(t, exec, "SELECT CAST(f AS TEXT) FROM t WHERE i = 42")
	got, ok := result.Rows[0][0].(string)
	if !ok {
		t.Fatalf("CAST(3.14 AS TEXT): expected string, got %T", result.Rows[0][0])
	}
	if got != "3.14" {
		t.Errorf("CAST(3.14 AS TEXT): expected '3.14', got %q", got)
	}

	// TEXT to INT
	result = run(t, exec, "SELECT CAST(s AS INT) FROM t WHERE i = 7")
	if result.Rows[0][0] != int64(123) {
		t.Errorf("CAST('123' AS INT): expected 123, got %v", result.Rows[0][0])
	}

	// TEXT to FLOAT
	result = run(t, exec, "SELECT CAST(s AS FLOAT) FROM t WHERE i = 7")
	if result.Rows[0][0] != float64(123) {
		t.Errorf("CAST('123' AS FLOAT): expected 123.0, got %v", result.Rows[0][0])
	}

	// NULL stays NULL
	exec2 := NewExecutor()
	run(t, exec2, "CREATE TABLE t2 (val TEXT)")
	run(t, exec2, "INSERT INTO t2 VALUES (NULL)")
	result = run(t, exec2, "SELECT CAST(val AS INT) FROM t2")
	if result.Rows[0][0] != nil {
		t.Errorf("CAST(NULL AS INT): expected nil, got %v", result.Rows[0][0])
	}
}

func TestCastError(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (s TEXT)")
	run(t, exec, "INSERT INTO t VALUES ('abc')")

	runExpectError(t, exec, "SELECT CAST(s AS INT) FROM t")
}
