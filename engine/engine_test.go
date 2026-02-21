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

func TestErrorSelectNonexistentColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT)")
	runExpectError(t, exec, "SELECT foo FROM users")
}
