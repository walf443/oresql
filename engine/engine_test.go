package engine

import (
	"testing"

	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

func run(t *testing.T, exec *Executor, sql string) *Result {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error for %q: %s", sql, err)
	}
	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("execute error for %q: %s", sql, err)
	}
	return result
}

func runExpectError(t *testing.T, exec *Executor, sql string) {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return // parse error is also acceptable
	}
	_, err = exec.Execute(stmt)
	if err == nil {
		t.Fatalf("expected error for %q, got nil", sql)
	}
}

func TestCreateInsertSelect(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	if result.Message != "table created" {
		t.Errorf("expected 'table created', got %q", result.Message)
	}

	result = run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	if result.Message != "1 row inserted" {
		t.Errorf("expected '1 row inserted', got %q", result.Message)
	}

	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// SELECT *
	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("columns: expected [id, name], got %v", result.Columns)
	}

	// SELECT specific columns
	result = run(t, exec, "SELECT name FROM users")
	if len(result.Columns) != 1 || result.Columns[0] != "name" {
		t.Errorf("expected [name], got %v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

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

func TestErrorDuplicateTable(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT)")
	runExpectError(t, exec, "CREATE TABLE users (id INT)")
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

func TestErrorSelectNonexistentTable(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "SELECT * FROM nonexistent")
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

func TestTruncateTable(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "TRUNCATE TABLE users")
	if result.Message != "table truncated" {
		t.Errorf("expected 'table truncated', got %q", result.Message)
	}

	// Table still exists but has no rows
	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}

	// Can insert again after truncate
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	result = run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(3) {
		t.Errorf("expected id=3, got %v", result.Rows[0][0])
	}
}

func TestTruncateTableNotExists(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "TRUNCATE TABLE nonexistent")
}

func TestDropTable(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "DROP TABLE users")
	if result.Message != "table dropped" {
		t.Errorf("expected 'table dropped', got %q", result.Message)
	}

	// SELECT after DROP TABLE should error
	runExpectError(t, exec, "SELECT * FROM users")
}

func TestDropTableNotExists(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "DROP TABLE nonexistent")
}

func TestDropTableRecreate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "DROP TABLE users")

	// Re-create the table
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
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

func TestErrorSelectNonexistentColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT)")
	runExpectError(t, exec, "SELECT foo FROM users")
}

func TestCreateTableWithDefault(t *testing.T) {
	exec := NewExecutor()
	result := run(t, exec, "CREATE TABLE t (id INT DEFAULT 0, name TEXT DEFAULT 'unknown')")
	if result.Message != "table created" {
		t.Errorf("expected 'table created', got %q", result.Message)
	}
}

func TestErrorCreateTableNotNullDefaultNull(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "CREATE TABLE t (id INT NOT NULL DEFAULT NULL)")
}

func TestErrorCreateTableDefaultTypeMismatch(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "CREATE TABLE t (id INT DEFAULT 'hello')")
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

func TestPrimaryKeyCreateInsertSelect(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// SELECT should return rows in PK order
	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("row 0: expected id=1, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("row 0: expected name='alice', got %v", result.Rows[0][1])
	}
	if result.Rows[1][0] != int64(2) {
		t.Errorf("row 1: expected id=2, got %v", result.Rows[1][0])
	}
	if result.Rows[2][0] != int64(3) {
		t.Errorf("row 2: expected id=3, got %v", result.Rows[2][0])
	}
}

func TestPrimaryKeyDuplicateInsertError(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	runExpectError(t, exec, "INSERT INTO users VALUES (1, 'bob')")
}

func TestPrimaryKeyDeleteAndReinsert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "DELETE FROM users WHERE id = 1")

	// Should be able to reinsert with the same PK
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	result := run(t, exec, "SELECT name FROM users WHERE id = 1")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "bob" {
		t.Errorf("expected 'bob', got %v", result.Rows[0][0])
	}
}

func TestPrimaryKeyImpliesNotNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES (NULL, 'alice')")
}

func TestTextPrimaryKeyAllowed(t *testing.T) {
	exec := NewExecutor()
	result := run(t, exec, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")
	if result.Message != "table created" {
		t.Errorf("expected 'table created', got %q", result.Message)
	}
}

func TestErrorMultiplePrimaryKeys(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, code INT PRIMARY KEY)")
}

func TestPrimaryKeyUpdate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	run(t, exec, "UPDATE users SET name = 'ALICE' WHERE id = 1")
	result := run(t, exec, "SELECT name FROM users WHERE id = 1")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "ALICE" {
		t.Errorf("expected 'ALICE', got %v", result.Rows[0][0])
	}
}

func TestPrimaryKeyTruncateAndReinsert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "TRUNCATE TABLE users")

	// Should be able to insert with same PK after truncate
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "bob" {
		t.Errorf("expected 'bob', got %v", result.Rows[0][1])
	}
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

func TestCreateIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "CREATE INDEX idx_name ON users(name)")
	if result.Message != "index created" {
		t.Errorf("expected 'index created', got %q", result.Message)
	}
}

func TestCreateIndexTableNotExists(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "CREATE INDEX idx_name ON nonexistent(name)")
}

func TestCreateIndexColumnNotExists(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "CREATE INDEX idx_foo ON users(foo)")
}

func TestCreateIndexDuplicateName(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	runExpectError(t, exec, "CREATE INDEX idx_name ON users(name)")
}

func TestDropIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	result := run(t, exec, "DROP INDEX idx_name")
	if result.Message != "index dropped" {
		t.Errorf("expected 'index dropped', got %q", result.Message)
	}
}

func TestDropIndexNotExists(t *testing.T) {
	exec := NewExecutor()
	runExpectError(t, exec, "DROP INDEX nonexistent")
}

func TestSelectWithIndexEquality(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "bob" {
		t.Errorf("expected name='bob', got %v", result.Rows[0][1])
	}
}

func TestSelectWithIndexNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'nonexistent'")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestIndexMaintainedOnInsert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// Insert after index creation
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestIndexMaintainedOnDelete(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	run(t, exec, "DELETE FROM users WHERE id = 2")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", len(result.Rows))
	}
}

func TestIndexMaintainedOnUpdate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	run(t, exec, "UPDATE users SET name = 'bobby' WHERE id = 2")

	// Old value should not be found
	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows for old value, got %d", len(result.Rows))
	}

	// New value should be found
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bobby'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for new value, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestIndexClearedOnTruncate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	run(t, exec, "TRUNCATE TABLE users")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows after truncate, got %d", len(result.Rows))
	}

	// Index still works after reinserting
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row after reinsert, got %d", len(result.Rows))
	}
}

func TestIndexOnIntColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id = 2")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "bob" {
		t.Errorf("expected name='bob', got %v", result.Rows[0][1])
	}
}

func TestIndexOnFloatColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1.5)")
	run(t, exec, "INSERT INTO t VALUES (2, 2.5)")
	run(t, exec, "INSERT INTO t VALUES (3, 3.5)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")

	result := run(t, exec, "SELECT * FROM t WHERE val = 2.5")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestIndexWithNullValues(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// NULL values are stored in the index via binary encoding
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}

	// WHERE name IS NULL should use index and find the NULL row
	result = run(t, exec, "SELECT * FROM users WHERE name IS NULL")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for IS NULL, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestCreateIndexOnExistingData(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")

	// Create index after data is inserted
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// Index should find multiple rows
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestDropTableRemovesIndexes(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "DROP TABLE users")

	// Re-create table and try to create index with same name
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "CREATE INDEX idx_name ON users(name)")
	if result.Message != "index created" {
		t.Errorf("expected 'index created', got %q", result.Message)
	}
}

func TestSelectWithIndexAndCondition(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 30)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// Index used for name = 'alice', then age > 28 is applied as filter
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age > 28")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestCreateCompositeIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	result := run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")
	if result.Message != "index created" {
		t.Errorf("expected 'index created', got %q", result.Message)
	}
}

func TestSelectWithCompositeIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Both columns in equality condition → use composite index
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age = 30")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestSelectWithCompositeIndexPartialMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Only one column → composite index not used, falls back to full scan
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestCompositeIndexMaintainedOnInsert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Insert after index creation
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 25")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestCompositeIndexMaintainedOnDelete(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	run(t, exec, "DELETE FROM users WHERE id = 2")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 25")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", len(result.Rows))
	}
}

func TestCompositeIndexMaintainedOnUpdate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	run(t, exec, "UPDATE users SET age = 35 WHERE id = 2")

	// Old value should not be found
	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 25")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows for old value, got %d", len(result.Rows))
	}

	// New value should be found
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 35")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for new value, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestCompositeIndexWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, NULL, 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', NULL)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// NULL values are stored in composite index; non-NULL row should still be found
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age = 30")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}

	// WHERE name IS NULL AND age = 25 should use composite index
	result = run(t, exec, "SELECT * FROM users WHERE name IS NULL AND age = 25")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for IS NULL composite, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestCreateCompositeIndexOnExistingData(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 25)")

	// Create composite index after data is inserted
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Index should find multiple matching rows
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age = 30")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSelectWithIndexIsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE products (id INT, name TEXT, category TEXT)")
	run(t, exec, "INSERT INTO products VALUES (1, 'Widget', 'tools')")
	run(t, exec, "INSERT INTO products VALUES (2, 'Gadget', NULL)")
	run(t, exec, "INSERT INTO products VALUES (3, 'Doohickey', NULL)")
	run(t, exec, "INSERT INTO products VALUES (4, 'Thingamajig', 'toys')")
	run(t, exec, "CREATE INDEX idx_category ON products(category)")

	// IS NULL should use index and find rows with NULL category
	result := run(t, exec, "SELECT * FROM products WHERE category IS NULL")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows for IS NULL, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[2] || !ids[3] {
		t.Errorf("expected ids 2 and 3, got %v", ids)
	}

	// Non-NULL lookup should still work
	result = run(t, exec, "SELECT * FROM products WHERE category = 'tools'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

// --- Index range scan tests ---

func TestSelectWithIndexRangeGt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id > 3")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[4] || !ids[5] {
		t.Errorf("expected ids 4 and 5, got %v", ids)
	}
}

func TestSelectWithIndexRangeGte(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id >= 3")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[3] || !ids[4] || !ids[5] {
		t.Errorf("expected ids 3,4,5, got %v", ids)
	}
}

func TestSelectWithIndexRangeLt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id < 3")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[1] || !ids[2] {
		t.Errorf("expected ids 1 and 2, got %v", ids)
	}
}

func TestSelectWithIndexRangeLte(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id <= 3")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[1] || !ids[2] || !ids[3] {
		t.Errorf("expected ids 1,2,3, got %v", ids)
	}
}

func TestSelectWithIndexRangeBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id BETWEEN 2 AND 4")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[2] || !ids[3] || !ids[4] {
		t.Errorf("expected ids 2,3,4, got %v", ids)
	}
}

func TestSelectWithIndexRangeCombined(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id >= 2 AND id < 5")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[2] || !ids[3] || !ids[4] {
		t.Errorf("expected ids 2,3,4, got %v", ids)
	}
}

func TestSelectWithIndexRangeNoMatchRange(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")

	result := run(t, exec, "SELECT * FROM t WHERE id > 10")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestSelectWithIndexRangeText(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON t(name)")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO t VALUES (4, 'dave')")

	result := run(t, exec, "SELECT * FROM t WHERE name > 'b'")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	names := map[string]bool{}
	for _, row := range result.Rows {
		names[row[1].(string)] = true
	}
	if !names["bob"] || !names["charlie"] || !names["dave"] {
		t.Errorf("expected bob, charlie, dave, got %v", names)
	}
}

func TestSelectWithIndexRangeNegativeInt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (-5, 'a')")
	run(t, exec, "INSERT INTO t VALUES (-2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (0, 'c')")
	run(t, exec, "INSERT INTO t VALUES (3, 'd')")
	run(t, exec, "INSERT INTO t VALUES (7, 'e')")

	// Range: id >= -2 AND id <= 3
	result := run(t, exec, "SELECT * FROM t WHERE id >= -2 AND id <= 3")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[-2] || !ids[0] || !ids[3] {
		t.Errorf("expected ids -2,0,3, got %v", ids)
	}
}

func TestSelectWithIndexRangeAndFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'x')")
	run(t, exec, "INSERT INTO t VALUES (2, 'y')")
	run(t, exec, "INSERT INTO t VALUES (3, 'x')")
	run(t, exec, "INSERT INTO t VALUES (4, 'y')")
	run(t, exec, "INSERT INTO t VALUES (5, 'x')")

	// Range scan on id, then filter by name
	result := run(t, exec, "SELECT * FROM t WHERE id > 2 AND name = 'x'")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[3] || !ids[5] {
		t.Errorf("expected ids 3 and 5, got %v", ids)
	}
}

func TestSelectWithCompositeIndexRangeGt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'a')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'b')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'c')")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 'd')")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 'e')")
	run(t, exec, "INSERT INTO t VALUES (2, 8, 'f')")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 > 5")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	col2s := map[int64]bool{}
	for _, row := range result.Rows {
		col2s[row[1].(int64)] = true
	}
	if !col2s[7] || !col2s[10] {
		t.Errorf("expected col2 7 and 10, got %v", col2s)
	}
}

func TestSelectWithCompositeIndexRangeBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'b')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'c')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'd')")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 'e')")
	run(t, exec, "INSERT INTO t VALUES (2, 5, 'f')")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 BETWEEN 3 AND 7")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	col2s := map[int64]bool{}
	for _, row := range result.Rows {
		col2s[row[1].(int64)] = true
	}
	if !col2s[3] || !col2s[5] || !col2s[7] {
		t.Errorf("expected col2 3,5,7, got %v", col2s)
	}
}

func TestSelectWithCompositeIndexRangeLt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 TEXT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES ('a', 3, 'x')")
	run(t, exec, "INSERT INTO t VALUES ('a', 7, 'y')")
	run(t, exec, "INSERT INTO t VALUES ('a', 10, 'z')")
	run(t, exec, "INSERT INTO t VALUES ('b', 5, 'w')")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 'a' AND col2 < 10")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	col2s := map[int64]bool{}
	for _, row := range result.Rows {
		col2s[row[1].(int64)] = true
	}
	if !col2s[3] || !col2s[7] {
		t.Errorf("expected col2 3 and 7, got %v", col2s)
	}
}

func TestSelectWithCompositeIndexRangeNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 3)")
	run(t, exec, "INSERT INTO t VALUES (1, 5)")
	run(t, exec, "INSERT INTO t VALUES (2, 1)")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 > 100")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestSelectWithCompositeIndexRangeWithPostFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'y')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 'y')")
	run(t, exec, "INSERT INTO t VALUES (2, 4, 'x')")

	// col1=1 AND col2>3 uses composite index, col3='x' is post-filtered
	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 > 3 AND col3 = 'x'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1].(int64) != 7 {
		t.Errorf("expected col2=7, got %v", result.Rows[0][1])
	}
	if result.Rows[0][2].(string) != "x" {
		t.Errorf("expected col3='x', got %v", result.Rows[0][2])
	}
}

func TestSelectWithCompositeIndexRangePartialPrefix(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c INT, d TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 3, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 5, 'y')")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 8, 'z')")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 10, 'w')")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 1, 'v')")
	run(t, exec, "INSERT INTO t VALUES (2, 2, 5, 'u')")

	// a=1 AND b=2 AND c>=5 uses 3-column composite index
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b = 2 AND c >= 5")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	cs := map[int64]bool{}
	for _, row := range result.Rows {
		cs[row[2].(int64)] = true
	}
	if !cs[5] || !cs[8] || !cs[10] {
		t.Errorf("expected c 5,8,10, got %v", cs)
	}
}

func TestSelectWithIndexIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (1, 3)")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	if !ids[1] || !ids[3] {
		t.Errorf("expected ids 1 and 3, got %v", ids)
	}
}

func TestSelectWithIndexInNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (100, 200)")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestSelectWithIndexInSingleValue(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (2)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestSelectWithCompositeIndexIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (a INT, b INT, c TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, 3, 'x')")
	run(t, exec, "INSERT INTO items VALUES (1, 5, 'y')")
	run(t, exec, "INSERT INTO items VALUES (1, 7, 'z')")
	run(t, exec, "INSERT INTO items VALUES (2, 3, 'w')")
	run(t, exec, "CREATE INDEX idx_ab ON items(a, b)")

	result := run(t, exec, "SELECT * FROM items WHERE a = 1 AND b IN (3, 7)")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	bs := map[int64]bool{}
	for _, row := range result.Rows {
		bs[row[1].(int64)] = true
	}
	if !bs[3] || !bs[7] {
		t.Errorf("expected b 3 and 7, got %v", bs)
	}
}

func TestSelectWithIndexInAndFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (1, 2, 3) AND name = 'alice'")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	for _, row := range result.Rows {
		if row[1] != "alice" {
			t.Errorf("expected name='alice', got %v", row[1])
		}
	}
}

func TestSelectWithCompositeIndexRangeMiddleColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'y')")
	run(t, exec, "INSERT INTO t VALUES (1, 8, 'z')")
	run(t, exec, "INSERT INTO t VALUES (2, 3, 'w')")

	// a=1 AND b>3 should use composite index with prefix a=1 and range on b
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b > 3")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	bs := map[int64]bool{}
	for _, row := range result.Rows {
		bs[row[1].(int64)] = true
	}
	if !bs[5] || !bs[8] {
		t.Errorf("expected b 5,8, got %v", bs)
	}
}

func TestSelectWithCompositeIndexRangeMiddleColumnBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'b')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'c')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'd')")
	run(t, exec, "INSERT INTO t VALUES (2, 4, 'e')")

	// a=1 AND b BETWEEN 2 AND 5 should use composite index
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b BETWEEN 2 AND 5")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	bs := map[int64]bool{}
	for _, row := range result.Rows {
		bs[row[1].(int64)] = true
	}
	if !bs[3] || !bs[5] {
		t.Errorf("expected b 3,5, got %v", bs)
	}
}

func TestSelectWithCompositeIndexRangeMiddleColumnWithPostFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 8, 'y')")
	run(t, exec, "INSERT INTO t VALUES (2, 3, 'x')")

	// a=1 AND b>3 AND c='x' — index handles a=1 + b>3, post-filter handles c='x'
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b > 3 AND c = 'x'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != int64(5) {
		t.Errorf("expected b=5, got %v", result.Rows[0][1])
	}
	if result.Rows[0][2] != "x" {
		t.Errorf("expected c='x', got %v", result.Rows[0][2])
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

func TestExtractLikePrefix(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"abc%", "abc"},
		{"a\\_b%", "a_b"},
		{"%abc", ""},
		{"_abc", ""},
		{"abc", "abc"},
		{"abc\\%def", "abc%def"},
		{"a\\\\b%", "a\\b"},
		{"", ""},
		{"\\%%", "%"},
	}
	for _, tt := range tests {
		got := extractLikePrefix(tt.pattern)
		if got != tt.want {
			t.Errorf("extractLikePrefix(%q) = %q, want %q", tt.pattern, got, tt.want)
		}
	}
}

func TestNextPrefix(t *testing.T) {
	tests := []struct {
		input  string
		want   string
		wantOK bool
	}{
		{"abc", "abd", true},
		{"ab" + string([]byte{0xFF}), "ac", true},
		{string([]byte{0xFF}), "", false},
		{string([]byte{0xFF, 0xFF}), "", false},
		{"", "", false},
		{"a", "b", true},
	}
	for _, tt := range tests {
		got, ok := nextPrefix(tt.input)
		if ok != tt.wantOK || got != tt.want {
			t.Errorf("nextPrefix(%q) = (%q, %v), want (%q, %v)", tt.input, got, ok, tt.want, tt.wantOK)
		}
	}
}

func TestSelectWhereLikeWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'albert')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (4, 'almond')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE 'al%'")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	// Verify all returned rows start with "al"
	for _, row := range result.Rows {
		name := row[1].(string)
		if name[:2] != "al" {
			t.Errorf("expected name starting with 'al', got %q", name)
		}
	}
}

func TestSelectWhereLikeWithIndexNoPrefix(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	// LIKE '%ice' has no prefix, so index should not be used, but results should be correct
	result := run(t, exec, "SELECT * FROM users WHERE name LIKE '%ice'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "alice" {
		t.Errorf("expected 'alice', got %v", result.Rows[0][1])
	}
}

func TestSelectWhereLikeWithIndexEscape(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON items(name)")
	run(t, exec, "INSERT INTO items VALUES (1, 'a_bcd')")
	run(t, exec, "INSERT INTO items VALUES (2, 'a_xyz')")
	run(t, exec, "INSERT INTO items VALUES (3, 'abcd')")

	// LIKE 'a\_b%' — escaped underscore, prefix is "a_b"
	result := run(t, exec, "SELECT * FROM items WHERE name LIKE 'a\\_b%'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
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

func TestAlterTableAddColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "ALTER TABLE users ADD COLUMN age INT")
	if result.Message != "table altered" {
		t.Errorf("expected 'table altered', got %q", result.Message)
	}

	// New column should be usable in INSERT
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie', 30)")

	// Existing rows should have NULL for the new column
	result = run(t, exec, "SELECT id, name, age FROM users ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][2] != nil {
		t.Errorf("expected nil for alice's age, got %v", result.Rows[0][2])
	}
	if result.Rows[2][2] != int64(30) {
		t.Errorf("expected 30 for charlie's age, got %v", result.Rows[2][2])
	}

	// COLUMN keyword should be optional
	result = run(t, exec, "ALTER TABLE users ADD email TEXT")
	if result.Message != "table altered" {
		t.Errorf("expected 'table altered', got %q", result.Message)
	}
}

func TestAlterTableAddColumnWithDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	run(t, exec, "ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'")

	// Existing rows should have the default value
	result := run(t, exec, "SELECT id, status FROM users ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "active" {
		t.Errorf("expected 'active', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "active" {
		t.Errorf("expected 'active', got %v", result.Rows[1][1])
	}
}

func TestAlterTableAddColumnNotNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// NOT NULL + DEFAULT should work: existing rows get the default
	run(t, exec, "ALTER TABLE users ADD COLUMN age INT NOT NULL DEFAULT 0")

	result := run(t, exec, "SELECT id, age FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != int64(0) {
		t.Errorf("expected 0, got %v", result.Rows[0][1])
	}
}

func TestAlterTableAddColumnNotNullNoDefault(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// NOT NULL without DEFAULT should error when rows exist
	runExpectError(t, exec, "ALTER TABLE users ADD COLUMN age INT NOT NULL")
}

func TestAlterTableDropColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")

	result := run(t, exec, "ALTER TABLE users DROP COLUMN age")
	if result.Message != "table altered" {
		t.Errorf("expected 'table altered', got %q", result.Message)
	}

	// Column should be gone
	result = run(t, exec, "SELECT * FROM users ORDER BY id")
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("expected [id, name], got %v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) || result.Rows[0][1] != "alice" {
		t.Errorf("unexpected row 0: %v", result.Rows[0])
	}
	if result.Rows[1][0] != int64(2) || result.Rows[1][1] != "bob" {
		t.Errorf("unexpected row 1: %v", result.Rows[1])
	}

	// COLUMN keyword should be optional
	result = run(t, exec, "ALTER TABLE users DROP name")
	if result.Message != "table altered" {
		t.Errorf("expected 'table altered', got %q", result.Message)
	}
}

func TestAlterTableDropColumnWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "CREATE INDEX idx_age ON users(age)")

	// Dropping a column with a single-column index should auto-delete the index
	run(t, exec, "ALTER TABLE users DROP COLUMN age")

	// Verify the index is gone by creating a new one with the same name
	run(t, exec, "ALTER TABLE users ADD COLUMN age INT")
	run(t, exec, "CREATE INDEX idx_age ON users(age)")
}

func TestAlterTableDropColumnCompositeIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Dropping a column used in a composite index should error
	runExpectError(t, exec, "ALTER TABLE users DROP COLUMN age")
	runExpectError(t, exec, "ALTER TABLE users DROP COLUMN name")
}

func TestAlterTableDropColumnAdjustsIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 20, 200)")
	run(t, exec, "CREATE INDEX idx_c ON t(c)")

	// Drop column b (index 1) — index on c should be adjusted
	run(t, exec, "ALTER TABLE t DROP COLUMN b")

	// Index on c should still work for lookups
	result := run(t, exec, "SELECT a, c FROM t WHERE c = 100")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected a=1, got %v", result.Rows[0][0])
	}
}

func TestAlterTableDropPrimaryKey(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// Cannot drop PK column
	runExpectError(t, exec, "ALTER TABLE users DROP COLUMN id")
}

func TestAlterTableDropLastColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT)")

	// Cannot drop the last column
	runExpectError(t, exec, "ALTER TABLE t DROP COLUMN a")
}

func TestAlterTableDuplicateColumnName(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")

	// Adding duplicate column name should error
	runExpectError(t, exec, "ALTER TABLE users ADD COLUMN name TEXT")
}

func TestUniqueColumnConstraint(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Duplicate email should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (3, 'alice@example.com')")

	// Verify existing data is intact
	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestUniqueColumnInsertNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")

	// Multiple NULLs should be allowed per SQL standard
	run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")

	result := run(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestCreateUniqueIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, email TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "CREATE UNIQUE INDEX idx_email ON users(email)")

	// Duplicate email should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (2, 'alice@example.com')")

	// Different email should succeed
	run(t, exec, "INSERT INTO users VALUES (3, 'bob@example.com')")
}

func TestCreateUniqueCompositeIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE orders (user_id INT, product_id INT, qty INT)")
	run(t, exec, "CREATE UNIQUE INDEX idx_user_product ON orders(user_id, product_id)")

	run(t, exec, "INSERT INTO orders VALUES (1, 100, 1)")
	run(t, exec, "INSERT INTO orders VALUES (1, 200, 2)")
	run(t, exec, "INSERT INTO orders VALUES (2, 100, 3)")

	// Duplicate (user_id, product_id) should fail
	runExpectError(t, exec, "INSERT INTO orders VALUES (1, 100, 5)")

	result := run(t, exec, "SELECT * FROM orders")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestCreateUniqueIndexOnExistingDuplicates(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, email TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice@example.com')")

	// Creating unique index on data with duplicates should fail
	runExpectError(t, exec, "CREATE UNIQUE INDEX idx_email ON users(email)")
}

func TestUniqueUpdateViolation(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Update that causes duplicate should fail
	runExpectError(t, exec, "UPDATE users SET email = 'alice@example.com' WHERE id = 2")

	// Verify data unchanged
	result := run(t, exec, "SELECT email FROM users WHERE id = 2")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "bob@example.com" {
		t.Errorf("expected 'bob@example.com', got %v", result.Rows[0][0])
	}
}

func TestUniqueUpdateSameRow(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Updating a row to the same value should succeed
	run(t, exec, "UPDATE users SET email = 'alice@example.com' WHERE id = 1")

	result := run(t, exec, "SELECT email FROM users WHERE id = 1")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice@example.com" {
		t.Errorf("expected 'alice@example.com', got %v", result.Rows[0][0])
	}
}

func TestAlterTableAddColumnUnique(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	run(t, exec, "ALTER TABLE users ADD COLUMN email TEXT UNIQUE")

	// Both existing rows have NULL for email (allowed by UNIQUE)
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 'bob@example.com')")

	// Duplicate email should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (3, 'charlie', 'bob@example.com')")
}

func TestUniqueColumnWithNotNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, email TEXT NOT NULL UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Duplicate should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (3, 'alice@example.com')")

	// NULL should fail (due to NOT NULL, not UNIQUE)
	runExpectError(t, exec, "INSERT INTO users VALUES (4, NULL)")
}

func TestCompositePrimaryKey(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 200, 'B')")
	run(t, exec, "INSERT INTO enrollment VALUES (2, 100, 'C')")

	result := run(t, exec, "SELECT * FROM enrollment")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestCompositePrimaryKeyDuplicate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")

	// Same composite key should fail
	runExpectError(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'B')")
}

func TestCompositePrimaryKeyNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")

	// NULL in PK column should fail
	runExpectError(t, exec, "INSERT INTO enrollment VALUES (NULL, 100, 'A')")
	runExpectError(t, exec, "INSERT INTO enrollment VALUES (1, NULL, 'A')")
}

func TestCompositePrimaryKeyWithTextColumns(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE tags (category TEXT, tag TEXT, value INT, PRIMARY KEY (category, tag))")
	run(t, exec, "INSERT INTO tags VALUES ('color', 'red', 1)")
	run(t, exec, "INSERT INTO tags VALUES ('color', 'blue', 2)")
	run(t, exec, "INSERT INTO tags VALUES ('size', 'red', 3)")

	result := run(t, exec, "SELECT * FROM tags")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Duplicate composite key with TEXT should fail
	runExpectError(t, exec, "INSERT INTO tags VALUES ('color', 'red', 99)")
}

func TestCompositePrimaryKeyUpdate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")
	run(t, exec, "INSERT INTO enrollment VALUES (2, 100, 'B')")

	// Update to duplicate composite key should fail
	runExpectError(t, exec, "UPDATE enrollment SET student_id = 1 WHERE student_id = 2 AND course_id = 100")

	// Update non-PK column should succeed
	run(t, exec, "UPDATE enrollment SET grade = 'A+' WHERE student_id = 1 AND course_id = 100")
	result := run(t, exec, "SELECT grade FROM enrollment WHERE student_id = 1 AND course_id = 100")
	if len(result.Rows) != 1 || result.Rows[0][0] != "A+" {
		t.Errorf("expected grade='A+', got %v", result.Rows)
	}
}

func TestCompositePrimaryKeyDropColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")

	// Dropping PK column should fail
	runExpectError(t, exec, "ALTER TABLE enrollment DROP COLUMN student_id")
	runExpectError(t, exec, "ALTER TABLE enrollment DROP COLUMN course_id")

	// Dropping non-PK column should succeed
	run(t, exec, "ALTER TABLE enrollment DROP COLUMN grade")
}

func TestBothColumnAndTablePrimaryKey(t *testing.T) {
	exec := NewExecutor()

	// Both column-level and table-level PK should error
	runExpectError(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, PRIMARY KEY (id, name))")
}

func TestTableLevelSinglePrimaryKey(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")

	// Duplicate should fail
	runExpectError(t, exec, "INSERT INTO t VALUES (1, 'charlie')")

	result := run(t, exec, "SELECT * FROM t")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestCompositePrimaryKeyDeleteAndReinsert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")

	// Delete the row
	run(t, exec, "DELETE FROM enrollment WHERE student_id = 1 AND course_id = 100")

	// Reinsert with the same PK should succeed
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'B')")

	result := run(t, exec, "SELECT grade FROM enrollment WHERE student_id = 1 AND course_id = 100")
	if len(result.Rows) != 1 || result.Rows[0][0] != "B" {
		t.Errorf("expected grade='B', got %v", result.Rows)
	}
}

func TestTextColumnPrimaryKey(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE codes (code TEXT PRIMARY KEY, description TEXT)")
	run(t, exec, "INSERT INTO codes VALUES ('A01', 'first')")
	run(t, exec, "INSERT INTO codes VALUES ('B02', 'second')")

	// Duplicate TEXT PK should fail
	runExpectError(t, exec, "INSERT INTO codes VALUES ('A01', 'duplicate')")

	// NULL in TEXT PK should fail
	runExpectError(t, exec, "INSERT INTO codes VALUES (NULL, 'null key')")

	result := run(t, exec, "SELECT * FROM codes")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestFloatColumnPrimaryKey(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE measurements (sensor_id FLOAT PRIMARY KEY, value TEXT)")
	run(t, exec, "INSERT INTO measurements VALUES (1.5, 'a')")
	run(t, exec, "INSERT INTO measurements VALUES (2.5, 'b')")

	// Duplicate FLOAT PK should fail
	runExpectError(t, exec, "INSERT INTO measurements VALUES (1.5, 'duplicate')")

	result := run(t, exec, "SELECT * FROM measurements")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}
