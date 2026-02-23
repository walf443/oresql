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
