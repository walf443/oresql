package engine

import (
	"fmt"
	"testing"

	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

// execSQL parses and executes a SQL statement, returning an error if any.
func execSQL(exec *Executor, sql string) error {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}
	_, err = exec.Execute(stmt)
	return err
}

// setupBenchTable creates a table with N rows and optionally creates indexes.
// Schema: bench (id INT PRIMARY KEY, val INT, name TEXT, category INT)
func setupBenchTable(b *testing.B, n int, withIndex bool) *Executor {
	b.Helper()
	exec := NewExecutor()
	if err := execSQL(exec, "CREATE TABLE bench (id INT PRIMARY KEY, val INT, name TEXT, category INT)"); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		sql := fmt.Sprintf("INSERT INTO bench VALUES (%d, %d, 'name_%d', %d)", i, i*10, i, i%100)
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}

	if withIndex {
		if err := execSQL(exec, "CREATE INDEX idx_val ON bench(val)"); err != nil {
			b.Fatal(err)
		}
		if err := execSQL(exec, "CREATE INDEX idx_category ON bench(category)"); err != nil {
			b.Fatal(err)
		}
		if err := execSQL(exec, "CREATE INDEX idx_name ON bench(name)"); err != nil {
			b.Fatal(err)
		}
		if err := execSQL(exec, "CREATE INDEX idx_cat_val ON bench(category, val)"); err != nil {
			b.Fatal(err)
		}
	}

	return exec
}

// --- Primary Key vs Secondary Index ---
// id は PRIMARY KEY（B-tree キーとして直接格納）だがセカンダリインデックスは自動作成されない。
// val にはセカンダリインデックスを作成。両方の等値検索を比較する。

func BenchmarkPrimaryKeyLookup_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench WHERE id = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSecondaryIndexLookup_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPrimaryKeyLookup_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench WHERE id = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSecondaryIndexLookup_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, true)
	sql := "SELECT * FROM bench WHERE val = 500000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// PK に手動でセカンダリインデックスを追加した場合
func BenchmarkPrimaryKeyWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	if err := execSQL(exec, "CREATE INDEX idx_id ON bench(id)"); err != nil {
		b.Fatal(err)
	}
	sql := "SELECT * FROM bench WHERE id = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPrimaryKeyWithIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	if err := execSQL(exec, "CREATE INDEX idx_id ON bench(id)"); err != nil {
		b.Fatal(err)
	}
	sql := "SELECT * FROM bench WHERE id = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Equality lookup (WHERE val = X) ---

func BenchmarkEqualityNoIndex_1000(b *testing.B) {
	exec := setupBenchTable(b, 1000, false)
	sql := "SELECT * FROM bench WHERE val = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEqualityWithIndex_1000(b *testing.B) {
	exec := setupBenchTable(b, 1000, true)
	sql := "SELECT * FROM bench WHERE val = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEqualityNoIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEqualityWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEqualityNoIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench WHERE val = 500000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEqualityWithIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, true)
	sql := "SELECT * FROM bench WHERE val = 500000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Range scan (WHERE val >= X AND val <= Y) ---

func BenchmarkRangeNoIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench WHERE val >= 40000 AND val <= 60000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRangeWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val >= 40000 AND val <= 60000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRangeNoIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench WHERE val >= 400000 AND val <= 600000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRangeWithIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, true)
	sql := "SELECT * FROM bench WHERE val >= 400000 AND val <= 600000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- IN lookup (WHERE category IN (...)) ---
// category = i%100 なので、1値あたり n/100 行ヒット

// IN 3値: 10,000行 → 300行ヒット (3%)
func BenchmarkInNoIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench WHERE category IN (5, 10, 15)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench WHERE category IN (5, 10, 15)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- IN lookup on unique column (WHERE val IN (...)) ---
// val = i*10 なのでユニーク。1値あたり最大1行ヒット

// IN 3値: 10,000行 → 3行ヒット (0.03%)
func BenchmarkInUniqueNoIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench WHERE val IN (50, 100, 150)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInUniqueWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val IN (50, 100, 150)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// IN 3値: 100,000行 → 3行ヒット (0.003%)
func BenchmarkInUniqueNoIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench WHERE val IN (50, 100, 150)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInUniqueWithIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, true)
	sql := "SELECT * FROM bench WHERE val IN (50, 100, 150)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInNoIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench WHERE category IN (5, 10, 15)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInWithIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, true)
	sql := "SELECT * FROM bench WHERE category IN (5, 10, 15)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- LIKE prefix (WHERE name LIKE 'name\_50%') ---
// name = 'name_N' なのでエスケープ 'name\_50%' → prefix='name_50' → 11行ヒット
func BenchmarkLikeEscapedNoIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := `SELECT * FROM bench WHERE name LIKE 'name\_50%'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLikeEscapedWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := `SELECT * FROM bench WHERE name LIKE 'name\_50%'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLikeEscapedNoIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := `SELECT * FROM bench WHERE name LIKE 'name\_500%'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLikeEscapedWithIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, true)
	sql := `SELECT * FROM bench WHERE name LIKE 'name\_500%'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Composite index: equality + range (WHERE category = X AND val >= Y) ---

func BenchmarkCompositeNoIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench WHERE category = 50 AND val >= 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompositeWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench WHERE category = 50 AND val >= 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompositeNoIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench WHERE category = 50 AND val >= 500000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompositeWithIndex_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, true)
	sql := "SELECT * FROM bench WHERE category = 50 AND val >= 500000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- High selectivity: category has many distinct values (10000 rows, 100 categories = 100 rows each) ---
// --- Low selectivity: few categories, many rows per category ---

func setupLowSelectivityTable(b *testing.B, n int, withIndex bool) *Executor {
	b.Helper()
	exec := NewExecutor()
	if err := execSQL(exec, "CREATE TABLE bench_low (id INT PRIMARY KEY, val INT, grp INT)"); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		sql := fmt.Sprintf("INSERT INTO bench_low VALUES (%d, %d, %d)", i, i*10, i%5)
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}

	if withIndex {
		if err := execSQL(exec, "CREATE INDEX idx_grp ON bench_low(grp)"); err != nil {
			b.Fatal(err)
		}
	}

	return exec
}

func BenchmarkLowSelectivityNoIndex_10000(b *testing.B) {
	exec := setupLowSelectivityTable(b, 10000, false)
	sql := "SELECT * FROM bench_low WHERE grp = 3"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLowSelectivityWithIndex_10000(b *testing.B) {
	exec := setupLowSelectivityTable(b, 10000, true)
	sql := "SELECT * FROM bench_low WHERE grp = 3"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLowSelectivityNoIndex_100000(b *testing.B) {
	exec := setupLowSelectivityTable(b, 100000, false)
	sql := "SELECT * FROM bench_low WHERE grp = 3"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLowSelectivityWithIndex_100000(b *testing.B) {
	exec := setupLowSelectivityTable(b, 100000, true)
	sql := "SELECT * FROM bench_low WHERE grp = 3"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- JOIN: inner table LocalWhere index scan ---

// setupJoinBenchTables creates users and orders tables for JOIN benchmarks.
// users: N/10 rows, orders: N rows (each user has ~10 orders).
// Orders have status column with 5 values: 'active', 'pending', 'shipped', 'cancelled', 'returned'.
// setupJoinBenchTablesComposite creates users and orders tables for composite index JOIN benchmarks.
// indexMode: "none", "separate" (user_id + status separately), "composite" (user_id, status)
func setupJoinBenchTablesComposite(b *testing.B, n int, indexMode string) *Executor {
	b.Helper()
	exec := NewExecutor()
	if err := execSQL(exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"); err != nil {
		b.Fatal(err)
	}
	if err := execSQL(exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, status TEXT)"); err != nil {
		b.Fatal(err)
	}

	statuses := []string{"active", "pending", "shipped", "cancelled", "returned"}
	numUsers := n / 10
	if numUsers < 1 {
		numUsers = 1
	}
	for i := 0; i < numUsers; i++ {
		sql := fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i)
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		sql := fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, 'product_%d', '%s')", i, i%numUsers, i, statuses[i%5])
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}

	switch indexMode {
	case "separate":
		if err := execSQL(exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)"); err != nil {
			b.Fatal(err)
		}
		if err := execSQL(exec, "CREATE INDEX idx_orders_status ON orders (status)"); err != nil {
			b.Fatal(err)
		}
	case "composite":
		if err := execSQL(exec, "CREATE INDEX idx_orders_uid_status ON orders (user_id, status)"); err != nil {
			b.Fatal(err)
		}
	}

	return exec
}

func setupJoinBenchTables(b *testing.B, n int, joinIndex bool, whereIndex bool) *Executor {
	b.Helper()
	exec := NewExecutor()
	if err := execSQL(exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"); err != nil {
		b.Fatal(err)
	}
	if err := execSQL(exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, status TEXT)"); err != nil {
		b.Fatal(err)
	}

	statuses := []string{"active", "pending", "shipped", "cancelled", "returned"}
	numUsers := n / 10
	if numUsers < 1 {
		numUsers = 1
	}
	for i := 0; i < numUsers; i++ {
		sql := fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i)
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		sql := fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, 'product_%d', '%s')", i, i%numUsers, i, statuses[i%5])
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}

	if joinIndex {
		if err := execSQL(exec, "CREATE INDEX idx_orders_user_id ON orders (user_id)"); err != nil {
			b.Fatal(err)
		}
	}
	if whereIndex {
		if err := execSQL(exec, "CREATE INDEX idx_orders_status ON orders (status)"); err != nil {
			b.Fatal(err)
		}
	}

	return exec
}

// Case 1: No JOIN index + inner table WHERE with index
// JOINカラムにインデックスなし、WHERE条件のカラムにインデックスあり
// orders.status = 'active' でインデックス使用可能 (全体の20%)

func BenchmarkJoinInnerWhereNoIndex_1000(b *testing.B) {
	exec := setupJoinBenchTables(b, 1000, false, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinInnerWhereWithIndex_1000(b *testing.B) {
	exec := setupJoinBenchTables(b, 1000, false, true)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinInnerWhereNoIndex_10000(b *testing.B) {
	exec := setupJoinBenchTables(b, 10000, false, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinInnerWhereWithIndex_10000(b *testing.B) {
	exec := setupJoinBenchTables(b, 10000, false, true)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// Case 2: JOIN index + inner table WHERE with PK
// JOINカラムにインデックスあり + WHERE条件がPKを指定
// orders.user_id にインデックスあり、WHERE o.id = X はPKルックアップ

func BenchmarkJoinWithIndexPKWhereNoOpt_1000(b *testing.B) {
	// JOIN index only, no WHERE index optimization baseline
	exec := setupJoinBenchTables(b, 1000, true, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 50"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinWithIndexPKWhere_1000(b *testing.B) {
	exec := setupJoinBenchTables(b, 1000, true, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 50"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinWithIndexPKWhere_10000(b *testing.B) {
	exec := setupJoinBenchTables(b, 10000, true, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 500"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinWithIndexPKWhere_100000(b *testing.B) {
	exec := setupJoinBenchTables(b, 100000, true, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// Case 2 without JOIN index (no optimization path, full scan baseline)

func BenchmarkJoinNoIndexPKWhere_1000(b *testing.B) {
	exec := setupJoinBenchTables(b, 1000, false, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 50"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinNoIndexPKWhere_10000(b *testing.B) {
	exec := setupJoinBenchTables(b, 10000, false, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 500"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinNoIndexPKWhere_100000(b *testing.B) {
	exec := setupJoinBenchTables(b, 100000, false, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- JOIN: composite index vs separate indexes ---
// 複合インデックス (user_id, status) と個別インデックス (user_id) + (status) の比較。
// u.id < X で users を driving テーブルに固定し、orders を inner テーブルとして
// JOIN ON u.id = o.user_id + WHERE o.status = 'active' の条件で inner テーブルルックアップを比較。
// composite: 1回の B-tree 走査で user_id + status を同時に絞り込む
// separate: user_id index lookup → keys1, status index lookup → keys2, keys1 ∩ keys2

func BenchmarkJoinCompositeIndex_1000(b *testing.B) {
	exec := setupJoinBenchTablesComposite(b, 1000, "composite")
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id < 50 AND o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinCompositeIndex_10000(b *testing.B) {
	exec := setupJoinBenchTablesComposite(b, 10000, "composite")
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id < 500 AND o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinSeparateIndexes_1000(b *testing.B) {
	exec := setupJoinBenchTablesComposite(b, 1000, "separate")
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id < 50 AND o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinSeparateIndexes_10000(b *testing.B) {
	exec := setupJoinBenchTablesComposite(b, 10000, "separate")
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id < 500 AND o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- LIMIT early termination benchmarks ---

func BenchmarkSelectLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectLimitNoOrder_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectLimitWithOrder_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinLimitNoOrder_1000(b *testing.B) {
	exec := setupJoinBenchTables(b, 1000, true, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinLimitNoOrder_10000(b *testing.B) {
	exec := setupJoinBenchTables(b, 10000, true, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- DISTINCT + LIMIT early termination benchmarks ---
// category = i%100 なので 100 種類の distinct 値がある

func BenchmarkDistinctNoLimit_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT DISTINCT category FROM bench"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDistinctLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT DISTINCT category FROM bench LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDistinctNoLimit_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT DISTINCT category FROM bench"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDistinctLimitNoOrder_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT DISTINCT category FROM bench LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDistinctWithOrder_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT DISTINCT category FROM bench ORDER BY category LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- ORDER BY + LIMIT heap top-K benchmarks ---

func BenchmarkSelectOrderByLimit_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectOrderByLimit_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectOrderByNoLimit_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY val"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectOrderByNoLimit_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench ORDER BY val"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- ORDER BY + index benchmarks ---

func BenchmarkOrderByWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByNoIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByDescWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench ORDER BY val DESC LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByWhereRangeWithIndex_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val > 50000 ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByPKAsc_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByPKAsc_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench ORDER BY id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByPKDesc_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY id DESC LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByPKDesc_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench ORDER BY id DESC LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByPKNoLimit_10000(b *testing.B) {
	exec := setupBenchTable(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY id"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderByPKNoLimit_100000(b *testing.B) {
	exec := setupBenchTable(b, 100000, false)
	sql := "SELECT * FROM bench ORDER BY id"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}
