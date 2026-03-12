package engine

import (
	"testing"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

// optimizerExecSQL parses and executes a SQL statement, fatally failing on error.
func optimizerExecSQL(t *testing.T, exec *Executor, sql string) *Result {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("execute error for %q: %v", sql, err)
	}
	return result
}

func TestConstantFoldingIntegration(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Setup: create table and insert data
	optimizerExecSQL(t, exec, "CREATE TABLE items (id INT, val INT)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (1, 10)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (2, 20)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (3, 30)")

	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantMsg  string // for UPDATE/DELETE
	}{
		{
			name:     "WHERE 1 = 0 returns no rows",
			sql:      "SELECT * FROM items WHERE 1 = 0",
			wantRows: 0,
		},
		{
			name:     "WHERE 1 = 1 returns all rows",
			sql:      "SELECT * FROM items WHERE 1 = 1",
			wantRows: 3,
		},
		{
			name:     "WHERE id > 1 AND 1 = 1 returns id > 1",
			sql:      "SELECT * FROM items WHERE id > 1 AND 1 = 1",
			wantRows: 2,
		},
		{
			name:     "WHERE id > 1 AND 1 = 0 returns no rows",
			sql:      "SELECT * FROM items WHERE id > 1 AND 1 = 0",
			wantRows: 0,
		},
		{
			name:     "WHERE id > 1 OR 1 = 1 returns all rows",
			sql:      "SELECT * FROM items WHERE id > 1 OR 1 = 1",
			wantRows: 3,
		},
		{
			name:    "UPDATE WHERE 1 = 0 updates no rows",
			sql:     "UPDATE items SET val = 99 WHERE 1 = 0",
			wantMsg: "0 rows updated",
		},
		{
			name:    "DELETE WHERE 1 = 0 deletes no rows",
			sql:     "DELETE FROM items WHERE 1 = 0",
			wantMsg: "0 rows deleted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizerExecSQL(t, exec, tt.sql)
			if tt.wantMsg != "" {
				if result.Message != tt.wantMsg {
					t.Fatalf("expected message %q, got %q", tt.wantMsg, result.Message)
				}
			} else {
				if len(result.Rows) != tt.wantRows {
					t.Fatalf("expected %d rows, got %d", tt.wantRows, len(result.Rows))
				}
			}
		})
	}
}

func TestConstantFoldingShortCircuit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	optimizerExecSQL(t, exec, "CREATE TABLE items (id INT, val INT)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (1, 10)")

	// Short-circuit: WHERE 1 = 0 AND id / 0 > 0
	// With constant folding, 1 = 0 becomes false, AND false -> false (entire WHERE folded to false)
	result := optimizerExecSQL(t, exec, "SELECT * FROM items WHERE 1 = 0 AND id / 0 > 0")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}

	// Short-circuit: WHERE 1 = 1 OR id / 0 > 0
	// With constant folding, 1 = 1 becomes true, OR true -> true (entire WHERE folded to true)
	result = optimizerExecSQL(t, exec, "SELECT * FROM items WHERE 1 = 1 OR id / 0 > 0")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestConstantFoldingWithStringComparison(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	optimizerExecSQL(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (1, 'apple')")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (2, 'banana')")

	result := optimizerExecSQL(t, exec, "SELECT * FROM items WHERE 'a' = 'a'")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	result = optimizerExecSQL(t, exec, "SELECT * FROM items WHERE 'a' = 'b'")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestConstantFoldingComplex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	optimizerExecSQL(t, exec, "CREATE TABLE items (id INT, val INT)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (1, 10)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (2, 20)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (3, 30)")

	// (1 = 1 AND 2 > 1) OR 1 = 0 → true
	result := optimizerExecSQL(t, exec, "SELECT * FROM items WHERE (1 = 1 AND 2 > 1) OR 1 = 0")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// NOT (1 = 0) → true
	result = optimizerExecSQL(t, exec, "SELECT * FROM items WHERE NOT (1 = 0)")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestConstantFoldingSelectWithLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	optimizerExecSQL(t, exec, "CREATE TABLE items (id INT, val INT)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (1, 10)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (2, 20)")
	optimizerExecSQL(t, exec, "INSERT INTO items VALUES (3, 30)")

	result := optimizerExecSQL(t, exec, "SELECT * FROM items WHERE 1 = 1 LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	result = optimizerExecSQL(t, exec, "SELECT * FROM items WHERE 1 = 0 LIMIT 2")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestConstantFoldingBoolLitExprEval(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	info := &TableInfo{
		Name: "items",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "val", DataType: "INT", Index: 1},
		},
	}
	eval := newTableEvaluator(makeSubqueryRunner(exec), info)

	boolTrue := &ast.BoolLitExpr{Value: true}
	val, err := eval.Eval(boolTrue, Row{int64(1), int64(10)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != true {
		t.Fatalf("expected true, got %v", val)
	}

	boolFalse := &ast.BoolLitExpr{Value: false}
	val, err = eval.Eval(boolFalse, Row{int64(1), int64(10)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != false {
		t.Fatalf("expected false, got %v", val)
	}
}

func TestConstantFoldingFormatExpr(t *testing.T) {
	trueExpr := &ast.BoolLitExpr{Value: true}
	falseExpr := &ast.BoolLitExpr{Value: false}

	if got := formatExpr(trueExpr); got != "TRUE" {
		t.Fatalf("expected TRUE, got %s", got)
	}
	if got := formatExpr(falseExpr); got != "FALSE" {
		t.Fatalf("expected FALSE, got %s", got)
	}
}
