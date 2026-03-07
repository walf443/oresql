package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			assert.Equal(t, tt.want, got, "hasAggregate() = %v, want %v", got, tt.want)
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
			assert.Equal(t, tt.want, got, "formatCallExpr() = %q, want %q", got, tt.want)
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
			assert.Len(t, got, tt.want, "dedup() returned unexpected number of rows")
		})
	}
}

// --- LIMIT early termination tests ---

func TestSelectLimitNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	result := run(t, exec, "SELECT * FROM t LIMIT 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
}

func TestSelectWhereLimitNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	// WHERE val > 50 matches ids 6,7,8,9,10 → LIMIT 2 returns first 2
	result := run(t, exec, "SELECT * FROM t WHERE val > 50 LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestSelectLimitOffsetNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	result := run(t, exec, "SELECT * FROM t LIMIT 2 OFFSET 3")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestJoinLimitNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	for i := 1; i <= 5; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i))
	}
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, 'product_%d')", i, (i-1)%5+1, i))
	}

	result := run(t, exec, "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id LIMIT 5")
	require.Len(t, result.Rows, 5, "expected 5 rows")
}

func TestLeftJoinLimitNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'Bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'Charlie')")
	run(t, exec, "INSERT INTO orders VALUES (1, 1, 'apple')")
	run(t, exec, "INSERT INTO orders VALUES (2, 1, 'banana')")

	// user 1 has 2 orders, users 2,3 have 0 → LEFT JOIN gives 4 rows total
	result := run(t, exec, "SELECT u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id LIMIT 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
}

func TestGroupByLimitNoEarlyTermination(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%3))
	}

	// GROUP BY val produces 3 groups (0,1,2), LIMIT 2 should return 2 groups
	result := run(t, exec, "SELECT val, COUNT(*) FROM t GROUP BY val LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestDistinctLimitNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	// 10 rows with 3 distinct val values: 0, 1, 2
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%3))
	}

	// DISTINCT val produces 3 unique values, LIMIT 2 should return 2
	result := run(t, exec, "SELECT DISTINCT val FROM t LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	// Verify all rows are distinct
	seen := make(map[interface{}]bool)
	for _, row := range result.Rows {
		assert.False(t, seen[row[0]], "duplicate value found: %v", row[0])
		seen[row[0]] = true
	}
}

func TestDistinctLimitOffsetNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	// 20 rows with 5 distinct val values: 0, 1, 2, 3, 4
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%5))
	}

	// DISTINCT val produces 5 unique values, OFFSET 2 LIMIT 2 should return 2
	result := run(t, exec, "SELECT DISTINCT val FROM t LIMIT 2 OFFSET 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	// Verify all rows are distinct
	seen := make(map[interface{}]bool)
	for _, row := range result.Rows {
		assert.False(t, seen[row[0]], "duplicate value found: %v", row[0])
		seen[row[0]] = true
	}
}

func TestDistinctWhereLimitNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%5))
	}

	// WHERE val > 1 keeps values 2,3,4 → DISTINCT produces 3, LIMIT 2 returns 2
	result := run(t, exec, "SELECT DISTINCT val FROM t WHERE val > 1 LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	seen := make(map[interface{}]bool)
	for _, row := range result.Rows {
		assert.False(t, seen[row[0]], "duplicate value found: %v", row[0])
		seen[row[0]] = true
	}
}

func TestDistinctStarLimitNoOrder(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	// Insert duplicate rows: (1,10) appears twice
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	// DISTINCT * produces 3 unique rows, LIMIT 2 should return 2
	result := run(t, exec, "SELECT DISTINCT * FROM t LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestCaseSearched(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, -5)")

	result := run(t, exec, "SELECT id, CASE WHEN val > 0 THEN 'positive' ELSE 'non-positive' END FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "positive", result.Rows[0][1], "row 0: expected 'positive'")
	assert.Equal(t, "non-positive", result.Rows[1][1], "row 1: expected 'non-positive'")
}

func TestCaseSimple(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, status INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 0)")

	result := run(t, exec, "SELECT id, CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "active", result.Rows[0][1], "row 0: expected 'active'")
	assert.Equal(t, "inactive", result.Rows[1][1], "row 1: expected 'inactive'")
}

func TestCaseNoElse(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, -5)")

	result := run(t, exec, "SELECT id, CASE WHEN val > 0 THEN 'positive' END FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "positive", result.Rows[0][1], "row 0: expected 'positive'")
	assert.Nil(t, result.Rows[1][1], "row 1: expected nil (NULL)")
}

func TestCaseMultipleWhens(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 2)")
	run(t, exec, "INSERT INTO t VALUES (3, 3)")

	result := run(t, exec, "SELECT id, CASE WHEN val = 1 THEN 'one' WHEN val = 2 THEN 'two' ELSE 'other' END FROM t ORDER BY id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, "one", result.Rows[0][1], "row 0: expected 'one'")
	assert.Equal(t, "two", result.Rows[1][1], "row 1: expected 'two'")
	assert.Equal(t, "other", result.Rows[2][1], "row 2: expected 'other'")
}

func TestCaseInWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, -5)")
	run(t, exec, "INSERT INTO t VALUES (3, 20)")

	result := run(t, exec, "SELECT id FROM t WHERE CASE WHEN val > 0 THEN 1 ELSE 0 END = 1 ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0], "row 0: expected id=1")
	assert.Equal(t, int64(3), result.Rows[1][0], "row 1: expected id=3")
}

func TestCaseWithNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL)")
	run(t, exec, "INSERT INTO t VALUES (2, 5)")

	// NULL in WHEN condition should be treated as false
	result := run(t, exec, "SELECT id, CASE WHEN val > 0 THEN 'positive' ELSE 'other' END FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "other", result.Rows[0][1], "row 0 (NULL val): expected 'other'")
	assert.Equal(t, "positive", result.Rows[1][1], "row 1: expected 'positive'")
}

func TestCoalesceFirstNonNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, a INT, b INT, c INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL, 20, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, 10, NULL, 30)")

	result := run(t, exec, "SELECT id, COALESCE(a, b, c) FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(20), result.Rows[0][1], "row 0: expected 20")
	assert.Equal(t, int64(10), result.Rows[1][1], "row 1: expected 10")
}

func TestCoalesceAllNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, a INT, b INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL, NULL)")

	result := run(t, exec, "SELECT COALESCE(a, b) FROM t")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Nil(t, result.Rows[0][0], "expected NULL")
}

func TestCoalesceSingleArg(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, a INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 42)")
	run(t, exec, "INSERT INTO t VALUES (2, NULL)")

	result := run(t, exec, "SELECT id, COALESCE(a) FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(42), result.Rows[0][1], "row 0: expected 42")
	assert.Nil(t, result.Rows[1][1], "row 1: expected NULL")
}

func TestCoalesceInWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL)")
	run(t, exec, "INSERT INTO t VALUES (2, 5)")
	run(t, exec, "INSERT INTO t VALUES (3, NULL)")

	result := run(t, exec, "SELECT id FROM t WHERE COALESCE(val, 0) > 0 ORDER BY id")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0], "expected id=2")
}

func TestCoalesceWithoutFrom(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	result := run(t, exec, "SELECT COALESCE(NULL, NULL, 'hello')")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "hello", result.Rows[0][0], "expected 'hello'")
}

func TestNullifEqual(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1)")

	result := run(t, exec, "SELECT NULLIF(val, 1) FROM t")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Nil(t, result.Rows[0][0], "expected NULL")
}

func TestNullifNotEqual(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 5)")

	result := run(t, exec, "SELECT NULLIF(val, 1) FROM t")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(5), result.Rows[0][0], "expected 5")
}

func TestNullifWithNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL)")

	result := run(t, exec, "SELECT NULLIF(val, 1) FROM t")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Nil(t, result.Rows[0][0], "expected NULL (first arg is NULL)")
}

func TestNullifWithoutFrom(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	result := run(t, exec, "SELECT NULLIF(1, 1)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Nil(t, result.Rows[0][0], "expected NULL")

	result = run(t, exec, "SELECT NULLIF(1, 2)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0], "expected 1")
}

func TestAbsFunction(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, -5)")
	run(t, exec, "INSERT INTO t VALUES (2, 3)")
	run(t, exec, "INSERT INTO t VALUES (3, NULL)")

	result := run(t, exec, "SELECT id, ABS(val) FROM t ORDER BY id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, int64(5), result.Rows[0][1], "row 0: expected 5")
	assert.Equal(t, int64(3), result.Rows[1][1], "row 1: expected 3")
	assert.Nil(t, result.Rows[2][1], "row 2: expected NULL")
}

func TestAbsFunctionFloat(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, -3.14)")

	result := run(t, exec, "SELECT ABS(val) FROM t")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, float64(3.14), result.Rows[0][0], "expected 3.14")
}

func TestAbsFunctionWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, -5)")
	run(t, exec, "INSERT INTO t VALUES (2, 3)")
	run(t, exec, "INSERT INTO t VALUES (3, -1)")

	result := run(t, exec, "SELECT id FROM t WHERE ABS(val) > 3 ORDER BY id")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0], "expected id=1")
}

func TestRoundFunction(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// ROUND with integer (no change)
	result := run(t, exec, "SELECT ROUND(5)")
	assert.Equal(t, int64(5), result.Rows[0][0], "ROUND(5): expected 5")

	// ROUND float without precision
	result = run(t, exec, "SELECT ROUND(3.7)")
	assert.Equal(t, float64(4.0), result.Rows[0][0], "ROUND(3.7): expected 4")

	// ROUND float with precision
	result = run(t, exec, "SELECT ROUND(3.14159, 2)")
	assert.Equal(t, float64(3.14), result.Rows[0][0], "ROUND(3.14159, 2): expected 3.14")

	// ROUND negative float
	result = run(t, exec, "SELECT ROUND(-2.5)")
	assert.Equal(t, float64(-3.0), result.Rows[0][0], "ROUND(-2.5): expected -3")

	// ROUND NULL
	result = run(t, exec, "SELECT ROUND(NULL)")
	assert.Nil(t, result.Rows[0][0], "ROUND(NULL): expected NULL")
}

func TestModFunction(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Basic MOD
	result := run(t, exec, "SELECT MOD(10, 3)")
	assert.Equal(t, int64(1), result.Rows[0][0], "MOD(10,3): expected 1")

	// MOD with NULL
	result = run(t, exec, "SELECT MOD(10, NULL)")
	assert.Nil(t, result.Rows[0][0], "MOD(10,NULL): expected NULL")

	// MOD division by zero
	_, err := runWithError(exec, "SELECT MOD(10, 0)")
	require.Error(t, err, "MOD(10,0): expected error")
}

func TestCeilFunction(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Positive float
	result := run(t, exec, "SELECT CEIL(2.3)")
	assert.Equal(t, int64(3), result.Rows[0][0], "CEIL(2.3): expected 3")

	// Negative float
	result = run(t, exec, "SELECT CEIL(-2.3)")
	assert.Equal(t, int64(-2), result.Rows[0][0], "CEIL(-2.3): expected -2")

	// Integer (unchanged)
	result = run(t, exec, "SELECT CEIL(5)")
	assert.Equal(t, int64(5), result.Rows[0][0], "CEIL(5): expected 5")

	// NULL
	result = run(t, exec, "SELECT CEIL(NULL)")
	assert.Nil(t, result.Rows[0][0], "CEIL(NULL): expected NULL")
}

func TestFloorFunction(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Positive float
	result := run(t, exec, "SELECT FLOOR(2.7)")
	assert.Equal(t, int64(2), result.Rows[0][0], "FLOOR(2.7): expected 2")

	// Negative float
	result = run(t, exec, "SELECT FLOOR(-2.3)")
	assert.Equal(t, int64(-3), result.Rows[0][0], "FLOOR(-2.3): expected -3")

	// Integer (unchanged)
	result = run(t, exec, "SELECT FLOOR(5)")
	assert.Equal(t, int64(5), result.Rows[0][0], "FLOOR(5): expected 5")

	// NULL
	result = run(t, exec, "SELECT FLOOR(NULL)")
	assert.Nil(t, result.Rows[0][0], "FLOOR(NULL): expected NULL")
}

func TestPowerFunction(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Basic power
	result := run(t, exec, "SELECT POWER(2, 10)")
	assert.Equal(t, float64(1024), result.Rows[0][0], "POWER(2,10): expected 1024")

	// Zero exponent
	result = run(t, exec, "SELECT POWER(5, 0)")
	assert.Equal(t, float64(1), result.Rows[0][0], "POWER(5,0): expected 1")

	// NULL
	result = run(t, exec, "SELECT POWER(2, NULL)")
	assert.Nil(t, result.Rows[0][0], "POWER(2,NULL): expected NULL")
}

func TestNumericFunctionsWithTable(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, -3.7)")
	run(t, exec, "INSERT INTO t VALUES (2, 2.3)")

	result := run(t, exec, "SELECT id, ABS(val), CEIL(val), FLOOR(val) FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	// Row 0: val=-3.7 -> ABS=3.7, CEIL=-3, FLOOR=-4
	assert.Equal(t, float64(3.7), result.Rows[0][1], "row 0 ABS: expected 3.7")
	assert.Equal(t, int64(-3), result.Rows[0][2], "row 0 CEIL: expected -3")
	assert.Equal(t, int64(-4), result.Rows[0][3], "row 0 FLOOR: expected -4")
	// Row 1: val=2.3 -> ABS=2.3, CEIL=3, FLOOR=2
	assert.Equal(t, float64(2.3), result.Rows[1][1], "row 1 ABS: expected 2.3")
	assert.Equal(t, int64(3), result.Rows[1][2], "row 1 CEIL: expected 3")
	assert.Equal(t, int64(2), result.Rows[1][3], "row 1 FLOOR: expected 2")
}

func TestDoubleQuoteStringLiteral(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	result := run(t, exec, `SELECT "hello"`)
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "hello", result.Rows[0][0], "expected 'hello'")
}

func TestStringFunctionsWithoutTable(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

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
			require.Len(t, result.Rows, 1, "expected 1 row")
			got := result.Rows[0][0]
			assert.Equal(t, tt.want, got, "got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
		})
	}
}

func TestStringFunctionsWithTable(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t VALUES (1, 'Alice')")
	run(t, exec, "INSERT INTO t VALUES (2, '  Bob  ')")

	// LENGTH with column
	result := run(t, exec, "SELECT id, LENGTH(name) FROM t ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(5), result.Rows[0][1], "row 0 LENGTH: expected 5")
	assert.Equal(t, int64(7), result.Rows[1][1], "row 1 LENGTH: expected 7")

	// UPPER with column
	result = run(t, exec, "SELECT UPPER(name) FROM t WHERE id = 1")
	assert.Equal(t, "ALICE", result.Rows[0][0], "UPPER: expected 'ALICE'")

	// LOWER with column
	result = run(t, exec, "SELECT LOWER(name) FROM t WHERE id = 1")
	assert.Equal(t, "alice", result.Rows[0][0], "LOWER: expected 'alice'")

	// TRIM with column
	result = run(t, exec, "SELECT TRIM(name) FROM t WHERE id = 2")
	assert.Equal(t, "Bob", result.Rows[0][0], "TRIM: expected 'Bob'")

	// SUBSTRING with column
	result = run(t, exec, "SELECT SUBSTRING(name, 1, 3) FROM t WHERE id = 1")
	assert.Equal(t, "Ali", result.Rows[0][0], "SUBSTRING: expected 'Ali'")

	// CONCAT with column
	result = run(t, exec, "SELECT CONCAT(name, '!') FROM t WHERE id = 1")
	assert.Equal(t, "Alice!", result.Rows[0][0], "CONCAT: expected 'Alice!'")

	// WHERE with string function
	result = run(t, exec, "SELECT id FROM t WHERE LENGTH(name) > 5")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0], "expected id=2")
}

func TestOrderByLimitTopK(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (val INT)")
	run(t, exec, "INSERT INTO t VALUES (5)")
	run(t, exec, "INSERT INTO t VALUES (3)")
	run(t, exec, "INSERT INTO t VALUES (1)")
	run(t, exec, "INSERT INTO t VALUES (4)")
	run(t, exec, "INSERT INTO t VALUES (2)")

	// Case A: ORDER BY ASC + LIMIT
	result := run(t, exec, "SELECT val FROM t ORDER BY val ASC LIMIT 3")
	require.Len(t, result.Rows, 3, "Case A: expected 3 rows")
	expected := []int64{1, 2, 3}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "Case A[%d]: expected %d", i, exp)
	}

	// Case B: ORDER BY DESC + LIMIT
	result = run(t, exec, "SELECT val FROM t ORDER BY val DESC LIMIT 2")
	require.Len(t, result.Rows, 2, "Case B: expected 2 rows")
	expected = []int64{5, 4}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "Case B[%d]: expected %d", i, exp)
	}

	// Case C: ORDER BY + LIMIT + OFFSET
	result = run(t, exec, "SELECT val FROM t ORDER BY val ASC LIMIT 2 OFFSET 1")
	require.Len(t, result.Rows, 2, "Case C: expected 2 rows")
	expected = []int64{2, 3}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "Case C[%d]: expected %d", i, exp)
	}

	// Case F: LIMIT larger than row count (returns all rows sorted)
	result = run(t, exec, "SELECT val FROM t ORDER BY val ASC LIMIT 100")
	require.Len(t, result.Rows, 5, "Case F: expected 5 rows")
	expected = []int64{1, 2, 3, 4, 5}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "Case F[%d]: expected %d", i, exp)
	}
}

func TestOrderByLimitTopKMultiColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t2 (col1 INT, col2 TEXT)")
	run(t, exec, "INSERT INTO t2 VALUES (1, 'b')")
	run(t, exec, "INSERT INTO t2 VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t2 VALUES (2, 'c')")
	run(t, exec, "INSERT INTO t2 VALUES (2, 'a')")

	// Case D: Multi-column ORDER BY + LIMIT
	result := run(t, exec, "SELECT col1, col2 FROM t2 ORDER BY col1 ASC, col2 ASC LIMIT 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	type row struct {
		col1 int64
		col2 string
	}
	expectedRows := []row{{1, "a"}, {1, "b"}, {2, "a"}}
	for i, exp := range expectedRows {
		assert.Equal(t, exp.col1, result.Rows[i][0], "row[%d] col1: expected %d", i, exp.col1)
		assert.Equal(t, exp.col2, result.Rows[i][1], "row[%d] col2: expected %s", i, exp.col2)
	}
}

func TestOrderByLimitTopKWithNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE tn (val INT)")
	run(t, exec, "INSERT INTO tn VALUES (3)")
	run(t, exec, "INSERT INTO tn VALUES (NULL)")
	run(t, exec, "INSERT INTO tn VALUES (1)")
	run(t, exec, "INSERT INTO tn VALUES (NULL)")
	run(t, exec, "INSERT INTO tn VALUES (2)")

	// Case E: NULLs sort last for ASC
	result := run(t, exec, "SELECT val FROM tn ORDER BY val ASC LIMIT 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []interface{}{int64(1), int64(2), int64(3)}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "ASC[%d]: expected %v", i, exp)
	}

	// NULLs sort last for DESC too
	result = run(t, exec, "SELECT val FROM tn ORDER BY val DESC LIMIT 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected = []interface{}{int64(3), int64(2), int64(1)}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "DESC[%d]: expected %v", i, exp)
	}
}

// --- Index ORDER BY tests ---

func TestIndexOrderByAsc(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, (11-i)*10))
	}

	q := "SELECT id, val FROM t ORDER BY val ASC"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 10, "expected 10 rows")
	for i := 0; i < 10; i++ {
		expected := int64((i + 1) * 10)
		assert.Equal(t, expected, result.Rows[i][1], "row %d: expected val=%d", i, expected)
	}

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "idx_val", Extra: "Using index for ORDER BY (ASC)"}})
}

func TestIndexOrderByDesc(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	q := "SELECT id, val FROM t ORDER BY val DESC"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 10, "expected 10 rows")
	for i := 0; i < 10; i++ {
		expected := int64((10 - i) * 10)
		assert.Equal(t, expected, result.Rows[i][1], "row %d: expected val=%d", i, expected)
	}

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "idx_val", Extra: "Using index for ORDER BY (DESC)"}})
}

func TestPKOrderByAscDesc(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	for i := 1; i <= 5; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	// ASC
	qAsc := "SELECT id FROM t ORDER BY id ASC"
	result := run(t, exec, qAsc)
	expected := []int64{1, 2, 3, 4, 5}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "ASC[%d]: expected %d", i, exp)
	}

	assertExplain(t, exec, qAsc, []planRow{{Type: "index scan", Key: "PRIMARY", Extra: "Using index for ORDER BY (ASC)"}})

	// DESC
	qDesc := "SELECT id FROM t ORDER BY id DESC"
	result = run(t, exec, qDesc)
	expected = []int64{5, 4, 3, 2, 1}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "DESC[%d]: expected %d", i, exp)
	}

	assertExplain(t, exec, qDesc, []planRow{{Type: "index scan", Key: "PRIMARY", Extra: "Using index for ORDER BY (DESC)"}})
}

func TestIndexOrderByLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	for i := 1; i <= 100; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	qAsc := "SELECT val FROM t ORDER BY val ASC LIMIT 5"
	result := run(t, exec, qAsc)
	require.Len(t, result.Rows, 5, "expected 5 rows")
	expected := []int64{10, 20, 30, 40, 50}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "row %d: expected %d", i, exp)
	}

	// ASC + LIMIT on nullable column falls back to filesort (NULL ordering issue)
	assertExplain(t, exec, qAsc, []planRow{{Extra: "Using filesort"}})

	qDesc := "SELECT val FROM t ORDER BY val DESC LIMIT 5"
	result = run(t, exec, qDesc)
	require.Len(t, result.Rows, 5, "expected 5 rows")
	expected = []int64{1000, 990, 980, 970, 960}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "DESC row %d: expected %d", i, exp)
	}

	// DESC + LIMIT on nullable column uses index (NULLs sort last naturally)
	assertExplain(t, exec, qDesc, []planRow{{Type: "index scan", Key: "idx_val", Extra: "Using index for ORDER BY (DESC)"}})
}

func TestIndexOrderByWithWhereRange(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	for i := 1; i <= 100; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	q := "SELECT val FROM t WHERE val > 50 ORDER BY val ASC LIMIT 5"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 5, "expected 5 rows")
	expected := []int64{51, 52, 53, 54, 55}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "row %d: expected %d", i, exp)
	}

	// ASC + LIMIT on nullable column falls back to filesort
	assertExplain(t, exec, q, []planRow{{Extra: "Using filesort"}})
}

func TestIndexOrderByOffsetLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	q := "SELECT val FROM t ORDER BY val ASC LIMIT 3 OFFSET 5"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []int64{60, 70, 80}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "row %d: expected %d", i, exp)
	}

	// ASC + LIMIT on nullable column falls back to filesort
	assertExplain(t, exec, q, []planRow{{Extra: "Using filesort"}})
}

func TestIndexOrderByDuplicates(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	// Insert duplicate val values
	run(t, exec, "INSERT INTO t VALUES (1, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, 10)")
	run(t, exec, "INSERT INTO t VALUES (3, 20)")
	run(t, exec, "INSERT INTO t VALUES (4, 10)")
	run(t, exec, "INSERT INTO t VALUES (5, 30)")
	run(t, exec, "INSERT INTO t VALUES (6, 20)")

	result := run(t, exec, "SELECT val FROM t ORDER BY val ASC")
	require.Len(t, result.Rows, 6, "expected 6 rows")
	expected := []int64{10, 10, 20, 20, 30, 30}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "row %d: expected %d", i, exp)
	}
}

func TestIndexOrderByMultiColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, col1 INT, col2 INT)")
	run(t, exec, "CREATE INDEX idx_col1 ON t(col1)")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 2, 10)")
	run(t, exec, "INSERT INTO t VALUES (4, 1, 40)")
	run(t, exec, "INSERT INTO t VALUES (5, 3, 50)")

	q := "SELECT col1, col2 FROM t ORDER BY col1 ASC, col2 ASC"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 5, "expected 5 rows")
	type pair struct{ c1, c2 int64 }
	expected := []pair{{1, 20}, {1, 40}, {2, 10}, {2, 30}, {3, 50}}
	for i, exp := range expected {
		assert.Equal(t, exp.c1, result.Rows[i][0], "row %d col1: expected %d", i, exp.c1)
		assert.Equal(t, exp.c2, result.Rows[i][1], "row %d col2: expected %d", i, exp.c2)
	}

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "idx_col1", Extra: "Using index for partial ORDER BY (ASC)"}})
}

func TestIndexOrderByMultiColumnLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, col1 INT, col2 INT)")
	run(t, exec, "CREATE INDEX idx_col1 ON t(col1)")
	// col1: 1,1,1,2,2,2,3,3,3
	for i := 1; i <= 9; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, (i-1)/3+1, (10-i)*10))
	}

	result := run(t, exec, "SELECT col1, col2 FROM t ORDER BY col1 ASC, col2 ASC LIMIT 4")
	require.Len(t, result.Rows, 4, "expected 4 rows")
	type pair struct{ c1, c2 int64 }
	expected := []pair{{1, 70}, {1, 80}, {1, 90}, {2, 40}}
	for i, exp := range expected {
		assert.Equal(t, exp.c1, result.Rows[i][0], "row %d col1: expected %d", i, exp.c1)
		assert.Equal(t, exp.c2, result.Rows[i][1], "row %d col2: expected %d", i, exp.c2)
	}
}

func TestIndexOrderByNullableDescLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	run(t, exec, "INSERT INTO t VALUES (1, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, NULL)")
	run(t, exec, "INSERT INTO t VALUES (3, 10)")
	run(t, exec, "INSERT INTO t VALUES (4, NULL)")
	run(t, exec, "INSERT INTO t VALUES (5, 20)")

	// DESC + LIMIT on nullable column: NULLs should sort last
	result := run(t, exec, "SELECT id, val FROM t ORDER BY val DESC LIMIT 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []int64{30, 20, 10}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][1], "row %d: expected val=%d", i, exp)
	}
}

func TestIndexOrderByNullableAscLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	run(t, exec, "INSERT INTO t VALUES (1, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, NULL)")
	run(t, exec, "INSERT INTO t VALUES (3, 10)")
	run(t, exec, "INSERT INTO t VALUES (4, NULL)")
	run(t, exec, "INSERT INTO t VALUES (5, 20)")

	// ASC + LIMIT on nullable column: NULLs should sort last
	result := run(t, exec, "SELECT id, val FROM t ORDER BY val ASC LIMIT 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []int64{10, 20, 30}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][1], "row %d: expected val=%d", i, exp)
	}
}

func TestIndexOrderByNullableDescNoLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	run(t, exec, "INSERT INTO t VALUES (1, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, NULL)")
	run(t, exec, "INSERT INTO t VALUES (3, 10)")
	run(t, exec, "INSERT INTO t VALUES (4, NULL)")
	run(t, exec, "INSERT INTO t VALUES (5, 20)")

	// DESC without LIMIT on nullable column: NULLs should sort last
	result := run(t, exec, "SELECT id, val FROM t ORDER BY val DESC")
	require.Len(t, result.Rows, 5, "expected 5 rows")
	expectedVals := []interface{}{int64(30), int64(20), int64(10), nil, nil}
	for i, exp := range expectedVals {
		assert.Equal(t, exp, result.Rows[i][1], "row %d: expected val=%v", i, exp)
	}
}

func TestOrderByNonIndexedFallback(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, other INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	for i := 1; i <= 5; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i*10, (6-i)*10))
	}

	// ORDER BY other (no index) should still work via normal sort path
	q := "SELECT other FROM t ORDER BY other ASC"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 5, "expected 5 rows")
	expected := []int64{10, 20, 30, 40, 50}
	for i, exp := range expected {
		assert.Equal(t, exp, result.Rows[i][0], "row %d: expected %d", i, exp)
	}

	assertExplain(t, exec, q, []planRow{{Type: "full scan", Extra: "Using filesort"}})
}

func TestOrderByWithGroupByFallback(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, grp INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i*10, i%3))
	}

	// GROUP BY + ORDER BY should use fallback (not index order)
	q := "SELECT grp, COUNT(*) FROM t GROUP BY grp ORDER BY grp ASC"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, int64(0), result.Rows[0][0], "row 0: expected grp=0")
	assert.Equal(t, int64(1), result.Rows[1][0], "row 1: expected grp=1")
	assert.Equal(t, int64(2), result.Rows[2][0], "row 2: expected grp=2")

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Extra: "Using filesort"}})
}

// --- Index scan streaming (WHERE + LIMIT without ORDER BY) ---

func TestIndexScanStreamingEquality(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, category INT)")
	run(t, exec, "CREATE INDEX idx_cat ON t(category)")
	for i := 1; i <= 100; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i*10, i%5))
	}

	// category = 3 has 20 rows (3,8,13,...,98), LIMIT 10 should return first 10
	q := "SELECT id, val, category FROM t WHERE category = 3 LIMIT 10"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 10, "expected 10 rows")
	for _, row := range result.Rows {
		assert.Equal(t, int64(3), row[2], "all rows should have category=3")
	}

	assertExplain(t, exec, q, []planRow{{Type: "ref", Key: "idx_cat"}})
}

func TestIndexScanStreamingRange(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	for i := 1; i <= 100; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	// val > 50 LIMIT 3 should return 3 rows with val > 50
	q := "SELECT id, val FROM t WHERE val > 50 LIMIT 3"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 3, "expected 3 rows")
	for _, row := range result.Rows {
		v := row[1].(int64)
		assert.Greater(t, v, int64(50), "all rows should have val > 50")
	}

	assertExplain(t, exec, q, []planRow{{Type: "range", Key: "idx_val"}})
}

func TestIndexScanStreamingPostFilter(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, category INT)")
	run(t, exec, "CREATE INDEX idx_cat ON t(category)")
	for i := 1; i <= 1000; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i, i%5))
	}

	// category = 3 AND val > 500 LIMIT 5: index on category, post-filter on val
	result := run(t, exec, "SELECT id, val, category FROM t WHERE category = 3 AND val > 500 LIMIT 5")
	require.Len(t, result.Rows, 5, "expected 5 rows")
	for _, row := range result.Rows {
		assert.Equal(t, int64(3), row[2], "all rows should have category=3")
		v := row[1].(int64)
		assert.Greater(t, v, int64(500), "all rows should have val > 500")
	}
}

func TestIndexScanStreamingOffsetLimit(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, category INT)")
	run(t, exec, "CREATE INDEX idx_cat ON t(category)")
	for i := 1; i <= 50; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%3))
	}

	// category = 1 rows: 1,4,7,10,13,16,19,22,25,28,31,34,37,40,43,46,49
	// OFFSET 5 LIMIT 3 should skip first 5 and return next 3
	result := run(t, exec, "SELECT id FROM t WHERE category = 1 LIMIT 3 OFFSET 5")
	require.Len(t, result.Rows, 3, "expected 3 rows")
}

func TestIndexScanStreamingDistinct(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, category INT)")
	run(t, exec, "CREATE INDEX idx_cat ON t(category)")
	for i := 1; i <= 100; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i%10, i%5))
	}

	// DISTINCT val WHERE category = 0 LIMIT 5
	// category = 0 rows have val values: 0,5,0,5,... → distinct = {0, 5} (only 2 unique)
	result := run(t, exec, "SELECT DISTINCT val FROM t WHERE category = 0 LIMIT 5")
	require.LessOrEqual(t, len(result.Rows), 5, "at most 5 rows")
	// Verify uniqueness
	seen := make(map[int64]bool)
	for _, row := range result.Rows {
		v := row[0].(int64)
		assert.False(t, seen[v], "duplicate val %d", v)
		seen[v] = true
	}
}

func TestIndexScanStreamingINFallthrough(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, category INT)")
	run(t, exec, "CREATE INDEX idx_cat ON t(category)")
	for i := 1; i <= 50; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i*10, i%5))
	}

	// IN condition should fall through to batch path but still produce correct results
	result := run(t, exec, "SELECT id, category FROM t WHERE category IN (1, 3) LIMIT 5")
	require.Len(t, result.Rows, 5, "expected 5 rows")
	for _, row := range result.Rows {
		cat := row[1].(int64)
		assert.True(t, cat == 1 || cat == 3, "category should be 1 or 3, got %d", cat)
	}
}

func TestMinWithIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	run(t, exec, "INSERT INTO t VALUES (1, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, 10)")
	run(t, exec, "INSERT INTO t VALUES (3, 20)")

	q := "SELECT MIN(val) FROM t"
	result := run(t, exec, q)
	assert.Equal(t, "MIN(val)", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(10), result.Rows[0][0])

	assertExplain(t, exec, q, []planRow{{Type: "index"}})
}

func TestMaxWithIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	run(t, exec, "INSERT INTO t VALUES (1, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, 10)")
	run(t, exec, "INSERT INTO t VALUES (3, 20)")

	q := "SELECT MAX(val) FROM t"
	result := run(t, exec, q)
	assert.Equal(t, "MAX(val)", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(30), result.Rows[0][0])

	assertExplain(t, exec, q, []planRow{{Type: "index"}})
}

// --- COUNT(*) RowCount optimization tests ---

func TestCountStarOptimization(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	q := "SELECT COUNT(*) FROM t"
	result := run(t, exec, q)
	assert.Equal(t, "COUNT(*)", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(3), result.Rows[0][0])

	assertExplain(t, exec, q, []planRow{{Type: "row count"}})
}

func TestCountStarEmpty(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")

	result := run(t, exec, "SELECT COUNT(*) FROM t")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(0), result.Rows[0][0])
}

func TestCountStarWithAlias(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")

	result := run(t, exec, "SELECT COUNT(*) AS cnt FROM t")
	assert.Equal(t, "cnt", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCountLiteralOptimization(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	result := run(t, exec, "SELECT COUNT(1) FROM t")
	assert.Equal(t, "COUNT(1)", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestCountStarWithWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	// WHERE condition → optimization should NOT apply, but result should be correct
	result := run(t, exec, "SELECT COUNT(*) FROM t WHERE val > 10")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCountStarWithGroupBy(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, category INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 20, 2)")
	run(t, exec, "INSERT INTO t VALUES (3, 30, 1)")

	// GROUP BY → optimization should NOT apply, but result should be correct
	result := run(t, exec, "SELECT category, COUNT(*) FROM t GROUP BY category ORDER BY category")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, int64(1), result.Rows[1][1])
}

func TestCountColumnNoOptimization(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, NULL)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	// COUNT(col) counts non-NULL values → optimization should NOT apply
	result := run(t, exec, "SELECT COUNT(val) FROM t")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestMinMaxWithPK(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "INSERT INTO t VALUES (5, 50)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (8, 80)")

	// MIN on PK column
	qMin := "SELECT MIN(id) FROM t"
	result := run(t, exec, qMin)
	assert.Equal(t, "MIN(id)", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])

	assertExplain(t, exec, qMin, []planRow{{Type: "index"}})

	// MAX on PK column
	qMax := "SELECT MAX(id) FROM t"
	result = run(t, exec, qMax)
	assert.Equal(t, "MAX(id)", result.Columns[0])
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(8), result.Rows[0][0])

	assertExplain(t, exec, qMax, []planRow{{Type: "index"}})
}

func TestMinMaxWithNulls(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	run(t, exec, "INSERT INTO t VALUES (1, NULL)")
	run(t, exec, "INSERT INTO t VALUES (2, 30)")
	run(t, exec, "INSERT INTO t VALUES (3, NULL)")
	run(t, exec, "INSERT INTO t VALUES (4, 10)")

	// MIN should skip NULLs
	result := run(t, exec, "SELECT MIN(val) FROM t")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(10), result.Rows[0][0])

	// MAX should skip NULLs
	result = run(t, exec, "SELECT MAX(val) FROM t")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(30), result.Rows[0][0])
}

func TestMinMaxWithEmptyTable(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")

	// MIN on empty table → NULL
	result := run(t, exec, "SELECT MIN(val) FROM t")
	require.Len(t, result.Rows, 1)
	assert.Nil(t, result.Rows[0][0])

	// MAX on empty table → NULL
	result = run(t, exec, "SELECT MAX(val) FROM t")
	require.Len(t, result.Rows, 1)
	assert.Nil(t, result.Rows[0][0])
}

func TestMinMaxWithWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, category INT)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 20, 2)")
	run(t, exec, "INSERT INTO t VALUES (3, 30, 1)")
	run(t, exec, "INSERT INTO t VALUES (4, 40, 2)")

	// WHERE condition → optimization should NOT apply (falls back to full scan)
	// but should still produce correct results
	result := run(t, exec, "SELECT MIN(val) FROM t WHERE category = 1")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(10), result.Rows[0][0])

	result = run(t, exec, "SELECT MAX(val) FROM t WHERE category = 2")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(40), result.Rows[0][0])
}

func TestMinMaxNoIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	// No index on val — should fall back to full scan and still work
	run(t, exec, "INSERT INTO t VALUES (1, 30)")
	run(t, exec, "INSERT INTO t VALUES (2, 10)")
	run(t, exec, "INSERT INTO t VALUES (3, 20)")

	qMin := "SELECT MIN(val) FROM t"
	result := run(t, exec, qMin)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(10), result.Rows[0][0])

	assertExplain(t, exec, qMin, []planRow{{Type: "full scan"}})

	qMax := "SELECT MAX(val) FROM t"
	result = run(t, exec, qMax)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(30), result.Rows[0][0])

	assertExplain(t, exec, qMax, []planRow{{Type: "full scan"}})
}

// --- GROUP BY Index Optimization Tests ---

func TestGroupByIndexOptimization(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, category INT, amount INT)")
	run(t, exec, "INSERT INTO orders VALUES (1, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 2, 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 1, 150)")
	run(t, exec, "INSERT INTO orders VALUES (4, 3, 300)")
	run(t, exec, "INSERT INTO orders VALUES (5, 2, 250)")

	// GROUP BY PK column with COUNT(*)
	q := "SELECT id, COUNT(*) FROM orders GROUP BY id"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 5)
	// Each id appears once, so COUNT(*) = 1 for each
	for _, row := range result.Rows {
		assert.Equal(t, int64(1), row[1])
	}

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "PRIMARY", Extra: "Using index for GROUP BY"}})
}

func TestGroupBySecondaryIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, category INT, amount INT)")
	run(t, exec, "CREATE INDEX idx_category ON orders(category)")
	run(t, exec, "INSERT INTO orders VALUES (1, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 2, 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 1, 150)")
	run(t, exec, "INSERT INTO orders VALUES (4, 3, 300)")
	run(t, exec, "INSERT INTO orders VALUES (5, 2, 250)")

	q := "SELECT category, COUNT(*) FROM orders GROUP BY category"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 3)

	// Collect results into a map for order-independent assertion
	counts := make(map[int64]int64)
	for _, row := range result.Rows {
		cat := row[0].(int64)
		cnt := row[1].(int64)
		counts[cat] = cnt
	}
	assert.Equal(t, int64(2), counts[1])
	assert.Equal(t, int64(2), counts[2])
	assert.Equal(t, int64(1), counts[3])

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "idx_category", Extra: "Using index for GROUP BY"}})
}

func TestGroupByIndexWithSum(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, category INT, amount INT)")
	run(t, exec, "CREATE INDEX idx_category ON orders(category)")
	run(t, exec, "INSERT INTO orders VALUES (1, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 2, 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 1, 150)")
	run(t, exec, "INSERT INTO orders VALUES (4, 2, 250)")

	q := "SELECT category, SUM(amount) FROM orders GROUP BY category"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 2)

	sums := make(map[int64]int64)
	for _, row := range result.Rows {
		sums[row[0].(int64)] = row[1].(int64)
	}
	assert.Equal(t, int64(250), sums[1])
	assert.Equal(t, int64(450), sums[2])

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "idx_category", Extra: "Using index for GROUP BY"}})
}

func TestGroupByIndexWithAvg(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, category INT, amount INT)")
	run(t, exec, "CREATE INDEX idx_category ON orders(category)")
	run(t, exec, "INSERT INTO orders VALUES (1, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 2, 200)")
	run(t, exec, "INSERT INTO orders VALUES (3, 1, 300)")

	q := "SELECT category, AVG(amount) FROM orders GROUP BY category"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 2)

	avgs := make(map[int64]float64)
	for _, row := range result.Rows {
		avgs[row[0].(int64)] = row[1].(float64)
	}
	assert.InDelta(t, 200.0, avgs[1], 0.001)
	assert.InDelta(t, 200.0, avgs[2], 0.001)

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "idx_category", Extra: "Using index for GROUP BY"}})
}

func TestGroupByIndexWithMinMax(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, category INT, amount INT)")
	run(t, exec, "CREATE INDEX idx_category ON orders(category)")
	run(t, exec, "INSERT INTO orders VALUES (1, 1, 100)")
	run(t, exec, "INSERT INTO orders VALUES (2, 1, 300)")
	run(t, exec, "INSERT INTO orders VALUES (3, 2, 200)")
	run(t, exec, "INSERT INTO orders VALUES (4, 2, 400)")

	q := "SELECT category, MIN(amount), MAX(amount) FROM orders GROUP BY category"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 2)

	type minMax struct{ min, max int64 }
	mm := make(map[int64]minMax)
	for _, row := range result.Rows {
		mm[row[0].(int64)] = minMax{row[1].(int64), row[2].(int64)}
	}
	assert.Equal(t, int64(100), mm[1].min)
	assert.Equal(t, int64(300), mm[1].max)
	assert.Equal(t, int64(200), mm[2].min)
	assert.Equal(t, int64(400), mm[2].max)

	assertExplain(t, exec, q, []planRow{{Type: "index scan", Key: "idx_category", Extra: "Using index for GROUP BY"}})
}

func TestGroupByIndexWithCountCol(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, NULL)")
	run(t, exec, "INSERT INTO t VALUES (3, 2, 20)")

	result := run(t, exec, "SELECT grp, COUNT(val) FROM t GROUP BY grp")
	require.Len(t, result.Rows, 2)

	counts := make(map[int64]int64)
	for _, row := range result.Rows {
		counts[row[0].(int64)] = row[1].(int64)
	}
	// grp=1 has 2 rows but val is NULL in one → COUNT(val) = 1
	assert.Equal(t, int64(1), counts[1])
	assert.Equal(t, int64(1), counts[2])
}

func TestGroupByIndexWithCountDistinct(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (3, 1, 20)")
	run(t, exec, "INSERT INTO t VALUES (4, 2, 30)")
	run(t, exec, "INSERT INTO t VALUES (5, 2, 30)")

	result := run(t, exec, "SELECT grp, COUNT(DISTINCT val) FROM t GROUP BY grp")
	require.Len(t, result.Rows, 2)

	counts := make(map[int64]int64)
	for _, row := range result.Rows {
		counts[row[0].(int64)] = row[1].(int64)
	}
	assert.Equal(t, int64(2), counts[1]) // 10, 20
	assert.Equal(t, int64(1), counts[2]) // 30
}

func TestGroupByIndexMultipleAggregates(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 2, 30)")

	result := run(t, exec, "SELECT grp, COUNT(*), SUM(val), MIN(val), MAX(val) FROM t GROUP BY grp")
	require.Len(t, result.Rows, 2)

	for _, row := range result.Rows {
		grp := row[0].(int64)
		if grp == 1 {
			assert.Equal(t, int64(2), row[1])  // COUNT(*)
			assert.Equal(t, int64(30), row[2]) // SUM
			assert.Equal(t, int64(10), row[3]) // MIN
			assert.Equal(t, int64(20), row[4]) // MAX
		} else {
			assert.Equal(t, int64(1), row[1])
			assert.Equal(t, int64(30), row[2])
			assert.Equal(t, int64(30), row[3])
			assert.Equal(t, int64(30), row[4])
		}
	}
}

func TestGroupByIndexEmpty(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")

	result := run(t, exec, "SELECT grp, COUNT(*) FROM t GROUP BY grp")
	require.Len(t, result.Rows, 0)
}

func TestGroupByIndexWithAlias(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 2, 30)")

	result := run(t, exec, "SELECT grp AS g, COUNT(*) AS cnt FROM t GROUP BY grp")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, "g", result.Columns[0])
	assert.Equal(t, "cnt", result.Columns[1])
}

func TestGroupByIndexWithWhere(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 2, 30)")
	run(t, exec, "INSERT INTO t VALUES (4, 2, 40)")
	run(t, exec, "INSERT INTO t VALUES (5, 3, 50)")

	// WHERE val > 15 filters out (1,1,10)
	result := run(t, exec, "SELECT grp, COUNT(*) FROM t WHERE val > 15 GROUP BY grp")
	require.Len(t, result.Rows, 3)

	counts := make(map[int64]int64)
	for _, row := range result.Rows {
		counts[row[0].(int64)] = row[1].(int64)
	}
	assert.Equal(t, int64(1), counts[1]) // only (1,20) passes
	assert.Equal(t, int64(2), counts[2])
	assert.Equal(t, int64(1), counts[3])
}

func TestGroupByNoIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	// No index on grp — should fall back to hash-based GROUP BY
	run(t, exec, "INSERT INTO t VALUES (1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 1, 30)")

	q := "SELECT grp, COUNT(*) FROM t GROUP BY grp"
	result := run(t, exec, q)
	require.Len(t, result.Rows, 2)

	counts := make(map[int64]int64)
	for _, row := range result.Rows {
		counts[row[0].(int64)] = row[1].(int64)
	}
	assert.Equal(t, int64(2), counts[1])
	assert.Equal(t, int64(1), counts[2])

	assertExplain(t, exec, q, []planRow{{Type: "full scan", Extra: "Using group by"}})
}

func TestGroupByWithHaving(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT)")
	run(t, exec, "CREATE INDEX idx_grp ON t(grp)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 2, 30)")

	// HAVING should fall back to traditional path
	result := run(t, exec, "SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) > 1")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[0][1])
}

func TestGroupByMultipleColumns(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, val INT)")
	run(t, exec, "CREATE INDEX idx_a ON t(a)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 1, 1, 30)")

	// Multiple GROUP BY columns should fall back to traditional path
	result := run(t, exec, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b")
	require.Len(t, result.Rows, 2)
}
