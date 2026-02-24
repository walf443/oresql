package engine

import (
	"fmt"
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

// --- LIMIT early termination tests ---

func TestSelectLimitNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	result := run(t, exec, "SELECT * FROM t LIMIT 3")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestSelectWhereLimitNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	// WHERE val > 50 matches ids 6,7,8,9,10 → LIMIT 2 returns first 2
	result := run(t, exec, "SELECT * FROM t WHERE val > 50 LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSelectLimitOffsetNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	result := run(t, exec, "SELECT * FROM t LIMIT 2 OFFSET 3")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestJoinLimitNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	for i := 1; i <= 5; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i))
	}
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, 'product_%d')", i, (i-1)%5+1, i))
	}

	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id LIMIT 5")
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}
}

func TestLeftJoinLimitNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'Bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'Charlie')")
	run(t, exec, "INSERT INTO orders VALUES (1, 1, 'apple')")
	run(t, exec, "INSERT INTO orders VALUES (2, 1, 'banana')")

	// user 1 has 2 orders, users 2,3 have 0 → LEFT JOIN gives 4 rows total
	result := run(t, exec, "SELECT u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id LIMIT 3")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestGroupByLimitNoEarlyTermination(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%3))
	}

	// GROUP BY val produces 3 groups (0,1,2), LIMIT 2 should return 2 groups
	result := run(t, exec, "SELECT val, COUNT(*) FROM t GROUP BY val LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestDistinctLimitNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	// 10 rows with 3 distinct val values: 0, 1, 2
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%3))
	}

	// DISTINCT val produces 3 unique values, LIMIT 2 should return 2
	result := run(t, exec, "SELECT DISTINCT val FROM t LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// Verify all rows are distinct
	seen := make(map[interface{}]bool)
	for _, row := range result.Rows {
		if seen[row[0]] {
			t.Errorf("duplicate value found: %v", row[0])
		}
		seen[row[0]] = true
	}
}

func TestDistinctLimitOffsetNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	// 20 rows with 5 distinct val values: 0, 1, 2, 3, 4
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%5))
	}

	// DISTINCT val produces 5 unique values, OFFSET 2 LIMIT 2 should return 2
	result := run(t, exec, "SELECT DISTINCT val FROM t LIMIT 2 OFFSET 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// Verify all rows are distinct
	seen := make(map[interface{}]bool)
	for _, row := range result.Rows {
		if seen[row[0]] {
			t.Errorf("duplicate value found: %v", row[0])
		}
		seen[row[0]] = true
	}
}

func TestDistinctWhereLimitNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%5))
	}

	// WHERE val > 1 keeps values 2,3,4 → DISTINCT produces 3, LIMIT 2 returns 2
	result := run(t, exec, "SELECT DISTINCT val FROM t WHERE val > 1 LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	seen := make(map[interface{}]bool)
	for _, row := range result.Rows {
		if seen[row[0]] {
			t.Errorf("duplicate value found: %v", row[0])
		}
		seen[row[0]] = true
	}
}

func TestDistinctStarLimitNoOrder(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	// Insert duplicate rows: (1,10) appears twice
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	// DISTINCT * produces 3 unique rows, LIMIT 2 should return 2
	result := run(t, exec, "SELECT DISTINCT * FROM t LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
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

func TestCoalesceFirstNonNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, a INT, b INT, c INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL, 20, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, 10, NULL, 30)")

	result := run(t, exec, "SELECT id, COALESCE(a, b, c) FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != int64(20) {
		t.Errorf("row 0: expected 20, got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != int64(10) {
		t.Errorf("row 1: expected 10, got %v", result.Rows[1][1])
	}
}

func TestCoalesceAllNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, a INT, b INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL, NULL)")

	result := run(t, exec, "SELECT COALESCE(a, b) FROM t")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != nil {
		t.Errorf("expected NULL, got %v", result.Rows[0][0])
	}
}

func TestCoalesceSingleArg(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, a INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 42)")
	run(t, exec, "INSERT INTO t VALUES (2, NULL)")

	result := run(t, exec, "SELECT id, COALESCE(a) FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != int64(42) {
		t.Errorf("row 0: expected 42, got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != nil {
		t.Errorf("row 1: expected NULL, got %v", result.Rows[1][1])
	}
}

func TestCoalesceInWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL)")
	run(t, exec, "INSERT INTO t VALUES (2, 5)")
	run(t, exec, "INSERT INTO t VALUES (3, NULL)")

	result := run(t, exec, "SELECT id FROM t WHERE COALESCE(val, 0) > 0 ORDER BY id")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}

func TestCoalesceWithoutFrom(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, "SELECT COALESCE(NULL, NULL, 'hello')")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "hello" {
		t.Errorf("expected 'hello', got %v", result.Rows[0][0])
	}
}

func TestNullifEqual(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1)")

	result := run(t, exec, "SELECT NULLIF(val, 1) FROM t")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != nil {
		t.Errorf("expected NULL, got %v", result.Rows[0][0])
	}
}

func TestNullifNotEqual(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 5)")

	result := run(t, exec, "SELECT NULLIF(val, 1) FROM t")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(5) {
		t.Errorf("expected 5, got %v", result.Rows[0][0])
	}
}

func TestNullifWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL)")

	result := run(t, exec, "SELECT NULLIF(val, 1) FROM t")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != nil {
		t.Errorf("expected NULL (first arg is NULL), got %v", result.Rows[0][0])
	}
}

func TestNullifWithoutFrom(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, "SELECT NULLIF(1, 1)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != nil {
		t.Errorf("expected NULL, got %v", result.Rows[0][0])
	}

	result = run(t, exec, "SELECT NULLIF(1, 2)")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected 1, got %v", result.Rows[0][0])
	}
}

func TestAbsFunction(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, -5)")
	run(t, exec, "INSERT INTO t VALUES (2, 3)")
	run(t, exec, "INSERT INTO t VALUES (3, NULL)")

	result := run(t, exec, "SELECT id, ABS(val) FROM t ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != int64(5) {
		t.Errorf("row 0: expected 5, got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != int64(3) {
		t.Errorf("row 1: expected 3, got %v", result.Rows[1][1])
	}
	if result.Rows[2][1] != nil {
		t.Errorf("row 2: expected NULL, got %v", result.Rows[2][1])
	}
}

func TestAbsFunctionFloat(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, -3.14)")

	result := run(t, exec, "SELECT ABS(val) FROM t")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != float64(3.14) {
		t.Errorf("expected 3.14, got %v", result.Rows[0][0])
	}
}

func TestAbsFunctionWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, -5)")
	run(t, exec, "INSERT INTO t VALUES (2, 3)")
	run(t, exec, "INSERT INTO t VALUES (3, -1)")

	result := run(t, exec, "SELECT id FROM t WHERE ABS(val) > 3 ORDER BY id")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", result.Rows[0][0])
	}
}

func TestRoundFunction(t *testing.T) {
	exec := NewExecutor()

	// ROUND with integer (no change)
	result := run(t, exec, "SELECT ROUND(5)")
	if result.Rows[0][0] != int64(5) {
		t.Errorf("ROUND(5): expected 5, got %v", result.Rows[0][0])
	}

	// ROUND float without precision
	result = run(t, exec, "SELECT ROUND(3.7)")
	if result.Rows[0][0] != float64(4.0) {
		t.Errorf("ROUND(3.7): expected 4, got %v", result.Rows[0][0])
	}

	// ROUND float with precision
	result = run(t, exec, "SELECT ROUND(3.14159, 2)")
	if result.Rows[0][0] != float64(3.14) {
		t.Errorf("ROUND(3.14159, 2): expected 3.14, got %v", result.Rows[0][0])
	}

	// ROUND negative float
	result = run(t, exec, "SELECT ROUND(-2.5)")
	if result.Rows[0][0] != float64(-3.0) {
		t.Errorf("ROUND(-2.5): expected -3, got %v", result.Rows[0][0])
	}

	// ROUND NULL
	result = run(t, exec, "SELECT ROUND(NULL)")
	if result.Rows[0][0] != nil {
		t.Errorf("ROUND(NULL): expected NULL, got %v", result.Rows[0][0])
	}
}

func TestModFunction(t *testing.T) {
	exec := NewExecutor()

	// Basic MOD
	result := run(t, exec, "SELECT MOD(10, 3)")
	if result.Rows[0][0] != int64(1) {
		t.Errorf("MOD(10,3): expected 1, got %v", result.Rows[0][0])
	}

	// MOD with NULL
	result = run(t, exec, "SELECT MOD(10, NULL)")
	if result.Rows[0][0] != nil {
		t.Errorf("MOD(10,NULL): expected NULL, got %v", result.Rows[0][0])
	}

	// MOD division by zero
	_, err := runWithError(exec, "SELECT MOD(10, 0)")
	if err == nil {
		t.Error("MOD(10,0): expected error, got nil")
	}
}

func TestCeilFunction(t *testing.T) {
	exec := NewExecutor()

	// Positive float
	result := run(t, exec, "SELECT CEIL(2.3)")
	if result.Rows[0][0] != int64(3) {
		t.Errorf("CEIL(2.3): expected 3, got %v", result.Rows[0][0])
	}

	// Negative float
	result = run(t, exec, "SELECT CEIL(-2.3)")
	if result.Rows[0][0] != int64(-2) {
		t.Errorf("CEIL(-2.3): expected -2, got %v", result.Rows[0][0])
	}

	// Integer (unchanged)
	result = run(t, exec, "SELECT CEIL(5)")
	if result.Rows[0][0] != int64(5) {
		t.Errorf("CEIL(5): expected 5, got %v", result.Rows[0][0])
	}

	// NULL
	result = run(t, exec, "SELECT CEIL(NULL)")
	if result.Rows[0][0] != nil {
		t.Errorf("CEIL(NULL): expected NULL, got %v", result.Rows[0][0])
	}
}

func TestFloorFunction(t *testing.T) {
	exec := NewExecutor()

	// Positive float
	result := run(t, exec, "SELECT FLOOR(2.7)")
	if result.Rows[0][0] != int64(2) {
		t.Errorf("FLOOR(2.7): expected 2, got %v", result.Rows[0][0])
	}

	// Negative float
	result = run(t, exec, "SELECT FLOOR(-2.3)")
	if result.Rows[0][0] != int64(-3) {
		t.Errorf("FLOOR(-2.3): expected -3, got %v", result.Rows[0][0])
	}

	// Integer (unchanged)
	result = run(t, exec, "SELECT FLOOR(5)")
	if result.Rows[0][0] != int64(5) {
		t.Errorf("FLOOR(5): expected 5, got %v", result.Rows[0][0])
	}

	// NULL
	result = run(t, exec, "SELECT FLOOR(NULL)")
	if result.Rows[0][0] != nil {
		t.Errorf("FLOOR(NULL): expected NULL, got %v", result.Rows[0][0])
	}
}

func TestPowerFunction(t *testing.T) {
	exec := NewExecutor()

	// Basic power
	result := run(t, exec, "SELECT POWER(2, 10)")
	if result.Rows[0][0] != float64(1024) {
		t.Errorf("POWER(2,10): expected 1024, got %v", result.Rows[0][0])
	}

	// Zero exponent
	result = run(t, exec, "SELECT POWER(5, 0)")
	if result.Rows[0][0] != float64(1) {
		t.Errorf("POWER(5,0): expected 1, got %v", result.Rows[0][0])
	}

	// NULL
	result = run(t, exec, "SELECT POWER(2, NULL)")
	if result.Rows[0][0] != nil {
		t.Errorf("POWER(2,NULL): expected NULL, got %v", result.Rows[0][0])
	}
}

func TestNumericFunctionsWithTable(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, -3.7)")
	run(t, exec, "INSERT INTO t VALUES (2, 2.3)")

	result := run(t, exec, "SELECT id, ABS(val), CEIL(val), FLOOR(val) FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// Row 0: val=-3.7 -> ABS=3.7, CEIL=-3, FLOOR=-4
	if result.Rows[0][1] != float64(3.7) {
		t.Errorf("row 0 ABS: expected 3.7, got %v", result.Rows[0][1])
	}
	if result.Rows[0][2] != int64(-3) {
		t.Errorf("row 0 CEIL: expected -3, got %v", result.Rows[0][2])
	}
	if result.Rows[0][3] != int64(-4) {
		t.Errorf("row 0 FLOOR: expected -4, got %v", result.Rows[0][3])
	}
	// Row 1: val=2.3 -> ABS=2.3, CEIL=3, FLOOR=2
	if result.Rows[1][1] != float64(2.3) {
		t.Errorf("row 1 ABS: expected 2.3, got %v", result.Rows[1][1])
	}
	if result.Rows[1][2] != int64(3) {
		t.Errorf("row 1 CEIL: expected 3, got %v", result.Rows[1][2])
	}
	if result.Rows[1][3] != int64(2) {
		t.Errorf("row 1 FLOOR: expected 2, got %v", result.Rows[1][3])
	}
}

func TestDoubleQuoteStringLiteral(t *testing.T) {
	exec := NewExecutor()

	result := run(t, exec, `SELECT "hello"`)
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "hello" {
		t.Errorf("expected 'hello', got %v", result.Rows[0][0])
	}
}

func TestStringFunctionsWithoutTable(t *testing.T) {
	exec := NewExecutor()

	tests := []struct {
		name string
		sql  string
		want interface{}
	}{
		// LENGTH
		{"LENGTH basic", "SELECT LENGTH('hello')", int64(5)},
		{"LENGTH empty", "SELECT LENGTH('')", int64(0)},
		{"LENGTH multibyte", "SELECT LENGTH('日本語')", int64(3)},
		{"LENGTH NULL", "SELECT LENGTH(NULL)", nil},

		// UPPER
		{"UPPER basic", "SELECT UPPER('hello')", "HELLO"},
		{"UPPER NULL", "SELECT UPPER(NULL)", nil},

		// LOWER
		{"LOWER basic", "SELECT LOWER('HELLO')", "hello"},
		{"LOWER NULL", "SELECT LOWER(NULL)", nil},

		// SUBSTRING 2 args
		{"SUBSTRING 2 args", "SELECT SUBSTRING('hello', 2)", "ello"},
		// SUBSTRING 3 args
		{"SUBSTRING 3 args", "SELECT SUBSTRING('hello', 2, 3)", "ell"},
		// SUBSTRING out of range
		{"SUBSTRING pos beyond length", "SELECT SUBSTRING('hello', 10)", ""},
		{"SUBSTRING NULL", "SELECT SUBSTRING(NULL, 1, 2)", nil},
		// SUBSTRING multibyte
		{"SUBSTRING multibyte", "SELECT SUBSTRING('日本語', 2, 2)", "本語"},

		// TRIM
		{"TRIM basic", "SELECT TRIM('  hello  ')", "hello"},
		{"TRIM NULL", "SELECT TRIM(NULL)", nil},

		// CONCAT
		{"CONCAT 2 args", "SELECT CONCAT('hello', ' world')", "hello world"},
		{"CONCAT 3 args", "SELECT CONCAT('a', 'b', 'c')", "abc"},
		{"CONCAT NULL", "SELECT CONCAT('hello', NULL)", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := run(t, exec, tt.sql)
			if len(result.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(result.Rows))
			}
			got := result.Rows[0][0]
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestStringFunctionsWithTable(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO t VALUES (2, '  Bob  ')")

	// LENGTH with column
	result := run(t, exec, "SELECT id, LENGTH(name) FROM t ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != int64(5) {
		t.Errorf("row 0 LENGTH: expected 5, got %v", result.Rows[0][1])
	}
	if result.Rows[1][1] != int64(7) {
		t.Errorf("row 1 LENGTH: expected 7, got %v", result.Rows[1][1])
	}

	// UPPER with column
	result = run(t, exec, "SELECT UPPER(name) FROM t WHERE id = 1")
	if result.Rows[0][0] != "ALICE" {
		t.Errorf("UPPER: expected 'ALICE', got %v", result.Rows[0][0])
	}

	// LOWER with column
	result = run(t, exec, "SELECT LOWER(name) FROM t WHERE id = 1")
	if result.Rows[0][0] != "alice" {
		t.Errorf("LOWER: expected 'alice', got %v", result.Rows[0][0])
	}

	// TRIM with column
	result = run(t, exec, "SELECT TRIM(name) FROM t WHERE id = 2")
	if result.Rows[0][0] != "Bob" {
		t.Errorf("TRIM: expected 'Bob', got %v", result.Rows[0][0])
	}

	// SUBSTRING with column
	result = run(t, exec, "SELECT SUBSTRING(name, 1, 3) FROM t WHERE id = 1")
	if result.Rows[0][0] != "Ali" {
		t.Errorf("SUBSTRING: expected 'Ali', got %v", result.Rows[0][0])
	}

	// CONCAT with column
	result = run(t, exec, "SELECT CONCAT(name, '!') FROM t WHERE id = 1")
	if result.Rows[0][0] != "Alice!" {
		t.Errorf("CONCAT: expected 'Alice!', got %v", result.Rows[0][0])
	}

	// WHERE with string function
	result = run(t, exec, "SELECT id FROM t WHERE LENGTH(name) > 5")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", result.Rows[0][0])
	}
}
