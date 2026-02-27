package engine

import (
	"testing"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

func TestOptimizeExpr(t *testing.T) {
	tests := []struct {
		name    string
		input   ast.Expr
		wantSQL string // expected SQL from ast.FormatSQL (empty string means nil result)
	}{
		{name: "1 = 1 -> TRUE", input: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 1}}, wantSQL: "TRUE"},
		{name: "1 = 0 -> FALSE", input: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 0}}, wantSQL: "FALSE"},
		{name: "5 > 3 -> TRUE", input: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 5}, Op: ">", Right: &ast.IntLitExpr{Value: 3}}, wantSQL: "TRUE"},
		{name: "'a' = 'b' -> FALSE", input: &ast.BinaryExpr{Left: &ast.StringLitExpr{Value: "a"}, Op: "=", Right: &ast.StringLitExpr{Value: "b"}}, wantSQL: "FALSE"},
		{name: "1 + 2 -> 3", input: &ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "+", Right: &ast.IntLitExpr{Value: 2}}, wantSQL: "3"},
		{name: "false AND col -> FALSE", input: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: false}, Op: "AND", Right: &ast.IdentExpr{Name: "col"}}, wantSQL: "FALSE"},
		{name: "true AND col -> col", input: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: true}, Op: "AND", Right: &ast.IdentExpr{Name: "col"}}, wantSQL: "col"},
		{name: "true OR col -> TRUE", input: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: true}, Op: "OR", Right: &ast.IdentExpr{Name: "col"}}, wantSQL: "TRUE"},
		{name: "false OR col -> col", input: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: false}, Op: "OR", Right: &ast.IdentExpr{Name: "col"}}, wantSQL: "col"},
		{name: "NOT true -> FALSE", input: &ast.NotExpr{Expr: &ast.BoolLitExpr{Value: true}}, wantSQL: "FALSE"},
		{name: "NOT false -> TRUE", input: &ast.NotExpr{Expr: &ast.BoolLitExpr{Value: false}}, wantSQL: "TRUE"},
		{name: "NULL IS NULL -> TRUE", input: &ast.IsNullExpr{Expr: &ast.NullLitExpr{}, Not: false}, wantSQL: "TRUE"},
		{name: "1 IS NULL -> FALSE", input: &ast.IsNullExpr{Expr: &ast.IntLitExpr{Value: 1}, Not: false}, wantSQL: "FALSE"},
		{name: "1 IN (1,2,3) -> TRUE", input: &ast.InExpr{Left: &ast.IntLitExpr{Value: 1}, Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}, &ast.IntLitExpr{Value: 3}}}, wantSQL: "TRUE"},
		{name: "5 NOT IN (1,2,3) -> TRUE", input: &ast.InExpr{Left: &ast.IntLitExpr{Value: 5}, Not: true, Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}, &ast.IntLitExpr{Value: 3}}}, wantSQL: "TRUE"},
		{name: "5 BETWEEN 1 AND 10 -> TRUE", input: &ast.BetweenExpr{Left: &ast.IntLitExpr{Value: 5}, Low: &ast.IntLitExpr{Value: 1}, High: &ast.IntLitExpr{Value: 10}}, wantSQL: "TRUE"},
		{name: "15 BETWEEN 1 AND 10 -> FALSE", input: &ast.BetweenExpr{Left: &ast.IntLitExpr{Value: 15}, Low: &ast.IntLitExpr{Value: 1}, High: &ast.IntLitExpr{Value: 10}}, wantSQL: "FALSE"},
		{name: "nil -> nil", input: nil, wantSQL: ""},
		{name: "column reference unchanged", input: &ast.IdentExpr{Name: "col"}, wantSQL: "col"},
		{name: "col AND true -> col", input: &ast.LogicalExpr{Left: &ast.IdentExpr{Name: "col"}, Op: "AND", Right: &ast.BoolLitExpr{Value: true}}, wantSQL: "col"},
		{name: "col OR false -> col", input: &ast.LogicalExpr{Left: &ast.IdentExpr{Name: "col"}, Op: "OR", Right: &ast.BoolLitExpr{Value: false}}, wantSQL: "col"},
		{
			name: "nested: 1 = 1 AND col > 5 -> (col > 5)",
			input: &ast.LogicalExpr{
				Left:  &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
				Op:    "AND",
				Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "col"}, Op: ">", Right: &ast.IntLitExpr{Value: 5}},
			},
			wantSQL: "(col > 5)",
		},
		{
			name: "nested: 1 = 0 AND col > 5 -> FALSE",
			input: &ast.LogicalExpr{
				Left:  &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 0}},
				Op:    "AND",
				Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "col"}, Op: ">", Right: &ast.IntLitExpr{Value: 5}},
			},
			wantSQL: "FALSE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizeExpr(tt.input)
			got := ast.FormatSQL(result)
			if got != tt.wantSQL {
				t.Fatalf("optimizeExpr() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(n int64) *int64 { return &n }

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
	eval := newTableEvaluator(exec, info)

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

func TestOptimizeCaseExpr(t *testing.T) {
	// CASE WHEN false THEN 1 WHEN true THEN 2 ELSE 3 END → 2
	expr := &ast.CaseExpr{
		Whens: []ast.CaseWhen{
			{When: &ast.BoolLitExpr{Value: false}, Then: &ast.IntLitExpr{Value: 1}},
			{When: &ast.BoolLitExpr{Value: true}, Then: &ast.IntLitExpr{Value: 2}},
		},
		Else: &ast.IntLitExpr{Value: 3},
	}
	if got := ast.FormatSQL(optimizeExpr(expr)); got != "2" {
		t.Fatalf("expected %q, got %q", "2", got)
	}

	// CASE WHEN false THEN 1 ELSE 3 END → 3
	expr2 := &ast.CaseExpr{
		Whens: []ast.CaseWhen{
			{When: &ast.BoolLitExpr{Value: false}, Then: &ast.IntLitExpr{Value: 1}},
		},
		Else: &ast.IntLitExpr{Value: 3},
	}
	if got := ast.FormatSQL(optimizeExpr(expr2)); got != "3" {
		t.Fatalf("expected %q, got %q", "3", got)
	}
}
