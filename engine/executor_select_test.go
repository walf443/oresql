package engine

import (
	"testing"

	"github.com/walf443/oresql/ast"
)

func TestHasAggregate(t *testing.T) {
	tests := []struct {
		name    string
		columns []ast.Expr
		want    bool
	}{
		{
			"CallExpr present",
			[]ast.Expr{&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}}},
			true,
		},
		{
			"IdentExpr only",
			[]ast.Expr{&ast.IdentExpr{Name: "id"}, &ast.IdentExpr{Name: "name"}},
			false,
		},
		{
			"AliasExpr wrapping CallExpr",
			[]ast.Expr{&ast.AliasExpr{Expr: &ast.CallExpr{Name: "SUM", Args: []ast.Expr{&ast.IdentExpr{Name: "amount"}}}, Alias: "total"}},
			true,
		},
		{
			"empty",
			[]ast.Expr{},
			false,
		},
		{
			"mixed with non-aggregate first",
			[]ast.Expr{&ast.IdentExpr{Name: "id"}, &ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasAggregate(tt.columns)
			if got != tt.want {
				t.Errorf("hasAggregate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatCallExpr(t *testing.T) {
	tests := []struct {
		name string
		call *ast.CallExpr
		want string
	}{
		{
			"COUNT(*)",
			&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}},
			"COUNT(*)",
		},
		{
			"SUM(col)",
			&ast.CallExpr{Name: "SUM", Args: []ast.Expr{&ast.IdentExpr{Name: "amount"}}},
			"SUM(amount)",
		},
		{
			"COUNT(1)",
			&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.IntLitExpr{Value: 1}}},
			"COUNT(1)",
		},
		{
			"table.col argument",
			&ast.CallExpr{Name: "SUM", Args: []ast.Expr{&ast.IdentExpr{Table: "t", Name: "amount"}}},
			"SUM(t.amount)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatCallExpr(tt.call)
			if got != tt.want {
				t.Errorf("formatCallExpr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDedup(t *testing.T) {
	tests := []struct {
		name string
		rows []Row
		want int // expected number of rows after dedup
	}{
		{
			"with duplicates",
			[]Row{
				{int64(1), "alice"},
				{int64(2), "bob"},
				{int64(1), "alice"},
			},
			2,
		},
		{
			"no duplicates",
			[]Row{
				{int64(1), "alice"},
				{int64(2), "bob"},
			},
			2,
		},
		{
			"empty",
			[]Row{},
			0,
		},
		{
			"with NULL values",
			[]Row{
				{int64(1), nil},
				{int64(1), nil},
				{int64(2), nil},
			},
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dedup(tt.rows)
			if len(got) != tt.want {
				t.Errorf("dedup() returned %d rows, want %d", len(got), tt.want)
			}
		})
	}
}

func TestCaseSearched(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, -5)")

	result := run(t, exec, "SELECT id, CASE WHEN val > 0 THEN 'positive' ELSE 'non-positive' END FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "positive" {
		t.Errorf("row 0: expected 'positive', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "non-positive" {
		t.Errorf("row 1: expected 'non-positive', got %v", result.Rows[1][1])
	}
}

func TestCaseSimple(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, status INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 0)")

	result := run(t, exec, "SELECT id, CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "active" {
		t.Errorf("row 0: expected 'active', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "inactive" {
		t.Errorf("row 1: expected 'inactive', got %v", result.Rows[1][1])
	}
}

func TestCaseNoElse(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, -5)")

	result := run(t, exec, "SELECT id, CASE WHEN val > 0 THEN 'positive' END FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "positive" {
		t.Errorf("row 0: expected 'positive', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != nil {
		t.Errorf("row 1: expected nil (NULL), got %v", result.Rows[1][1])
	}
}

func TestCaseMultipleWhens(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 2)")
	run(t, exec, "INSERT INTO t VALUES (3, 3)")

	result := run(t, exec, "SELECT id, CASE WHEN val = 1 THEN 'one' WHEN val = 2 THEN 'two' ELSE 'other' END FROM t ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "one" {
		t.Errorf("row 0: expected 'one', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "two" {
		t.Errorf("row 1: expected 'two', got %v", result.Rows[1][1])
	}
	if result.Rows[2][1] != "other" {
		t.Errorf("row 2: expected 'other', got %v", result.Rows[2][1])
	}
}

func TestCaseInWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, -5)")
	run(t, exec, "INSERT INTO t VALUES (3, 20)")

	result := run(t, exec, "SELECT id FROM t WHERE CASE WHEN val > 0 THEN 1 ELSE 0 END = 1 ORDER BY id")
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

func TestCaseWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL)")
	run(t, exec, "INSERT INTO t VALUES (2, 5)")

	// NULL in WHEN condition should be treated as false
	result := run(t, exec, "SELECT id, CASE WHEN val > 0 THEN 'positive' ELSE 'other' END FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "other" {
		t.Errorf("row 0 (NULL val): expected 'other', got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != "positive" {
		t.Errorf("row 1: expected 'positive', got %v", result.Rows[1][1])
	}
}
