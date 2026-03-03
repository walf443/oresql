package engine

import (
	"fmt"
	"strings"
	"testing"
)

// setupBenchTableDisk creates a table with N rows using disk storage.
// Schema: bench (id INT PRIMARY KEY, val INT, name TEXT, category INT)
func setupBenchTableDisk(b *testing.B, n int, withIndex bool) *Executor {
	b.Helper()
	tmpDir := b.TempDir()
	db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec := NewExecutor(db)
	if err := execSQL(exec, "CREATE TABLE bench (id INT PRIMARY KEY, val INT, name TEXT, category INT)"); err != nil {
		b.Fatal(err)
	}

	batchSize := 1000
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var buf strings.Builder
		buf.WriteString("INSERT INTO bench VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d, 'name_%d', %d)", i, i*10, i, i%100)
		}
		if err := execSQL(exec, buf.String()); err != nil {
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

// --- Primary Key lookup (disk) ---

func BenchmarkDiskPrimaryKeyLookup_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE id = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskSecondaryIndexLookup_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Equality lookup (disk) ---

func BenchmarkDiskEqualityNoIndex_1000(b *testing.B) {
	exec := setupBenchTableDisk(b, 1000, false)
	sql := "SELECT * FROM bench WHERE val = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskEqualityWithIndex_1000(b *testing.B) {
	exec := setupBenchTableDisk(b, 1000, true)
	sql := "SELECT * FROM bench WHERE val = 5000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskEqualityNoIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskEqualityWithIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Range scan (disk) ---

func BenchmarkDiskRangeNoIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE val >= 40000 AND val <= 60000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskRangeWithIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val >= 40000 AND val <= 60000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- IN lookup (disk) ---

func BenchmarkDiskInNoIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE category IN (5, 10, 15)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskInWithIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE category IN (5, 10, 15)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskInUniqueNoIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE val IN (50, 100, 150)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskInUniqueWithIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val IN (50, 100, 150)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- LIKE prefix (disk) ---

func BenchmarkDiskLikeEscapedNoIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := `SELECT * FROM bench WHERE name LIKE 'name\_50%'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskLikeEscapedWithIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := `SELECT * FROM bench WHERE name LIKE 'name\_50%'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Composite index (disk) ---

func BenchmarkDiskCompositeNoIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE category = 50 AND val >= 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskCompositeWithIndex_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE category = 50 AND val >= 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Low selectivity (disk) ---

func setupLowSelectivityTableDisk(b *testing.B, n int, withIndex bool) *Executor {
	b.Helper()
	tmpDir := b.TempDir()
	db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec := NewExecutor(db)
	if err := execSQL(exec, "CREATE TABLE bench_low (id INT PRIMARY KEY, val INT, grp INT)"); err != nil {
		b.Fatal(err)
	}

	batchSize := 1000
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var buf strings.Builder
		buf.WriteString("INSERT INTO bench_low VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d, %d)", i, i*10, i%5)
		}
		if err := execSQL(exec, buf.String()); err != nil {
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

func BenchmarkDiskLowSelectivityNoIndex_10000(b *testing.B) {
	exec := setupLowSelectivityTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench_low WHERE grp = 3"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskLowSelectivityWithIndex_10000(b *testing.B) {
	exec := setupLowSelectivityTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench_low WHERE grp = 3"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- LIMIT early termination (disk) ---

func BenchmarkDiskSelectLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskSelectLimitWithOrder_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- ORDER BY + LIMIT (disk) ---

func BenchmarkDiskSelectOrderByLimit_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY val LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskSelectOrderByNoLimit_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY val"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- ORDER BY with index (disk) ---

func BenchmarkDiskOrderByPKAsc_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskOrderByPKDesc_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY id DESC LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskOrderByPKNoLimit_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY id"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- JOIN (disk) ---

func setupJoinBenchTablesDisk(b *testing.B, n int, joinIndex bool, whereIndex bool) *Executor {
	b.Helper()
	tmpDir := b.TempDir()
	db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec := NewExecutor(db)
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
	batchSize := 1000
	for start := 0; start < numUsers; start += batchSize {
		end := start + batchSize
		if end > numUsers {
			end = numUsers
		}
		var buf strings.Builder
		buf.WriteString("INSERT INTO users VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, 'user_%d')", i, i)
		}
		if err := execSQL(exec, buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var buf strings.Builder
		buf.WriteString("INSERT INTO orders VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d, 'product_%d', '%s')", i, i%numUsers, i, statuses[i%5])
		}
		if err := execSQL(exec, buf.String()); err != nil {
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

func setupJoinBenchTablesCompositeDisk(b *testing.B, n int, indexMode string) *Executor {
	b.Helper()
	tmpDir := b.TempDir()
	db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec := NewExecutor(db)
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
	batchSize := 1000
	for start := 0; start < numUsers; start += batchSize {
		end := start + batchSize
		if end > numUsers {
			end = numUsers
		}
		var buf strings.Builder
		buf.WriteString("INSERT INTO users VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, 'user_%d')", i, i)
		}
		if err := execSQL(exec, buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var buf strings.Builder
		buf.WriteString("INSERT INTO orders VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d, 'product_%d', '%s')", i, i%numUsers, i, statuses[i%5])
		}
		if err := execSQL(exec, buf.String()); err != nil {
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

func BenchmarkDiskJoinInnerWhereNoIndex_1000(b *testing.B) {
	exec := setupJoinBenchTablesDisk(b, 1000, false, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskJoinInnerWhereWithIndex_1000(b *testing.B) {
	exec := setupJoinBenchTablesDisk(b, 1000, false, true)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskJoinCompositeIndex_1000(b *testing.B) {
	exec := setupJoinBenchTablesCompositeDisk(b, 1000, "composite")
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id < 50 AND o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskJoinSeparateIndexes_1000(b *testing.B) {
	exec := setupJoinBenchTablesCompositeDisk(b, 1000, "separate")
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id < 50 AND o.status = 'active'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskJoinLimitNoOrder_1000(b *testing.B) {
	exec := setupJoinBenchTablesDisk(b, 1000, true, false)
	sql := "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- DISTINCT + LIMIT (disk) ---

func BenchmarkDiskDistinctNoLimit_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT DISTINCT category FROM bench"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskWhereLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE category = 3 LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskDistinctLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT DISTINCT category FROM bench LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmarks for index scan streaming (WHERE + LIMIT without ORDER BY)

func BenchmarkDiskIndexWhereLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE category = 3 LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskIndexWhereRangeLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val > 5000 LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskIndexWherePostFilterLimitNoOrder_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE category = 3 AND val > 5000 LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Covering index benchmarks (disk) ---

func BenchmarkDiskCoveringIndexLookup_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT val FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskNonCoveringIndexLookup_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench WHERE val = 50000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskCoveringStreamingLimit_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT val FROM bench WHERE val > 5000 LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskCoveringOrderBy_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT val FROM bench ORDER BY val DESC LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskNonCoveringOrderBy_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, true)
	sql := "SELECT * FROM bench ORDER BY val DESC LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- PK Covering Index benchmarks (disk) ---

// --- PK ORDER BY + WHERE filter (disk) ---

func BenchmarkDiskPKOrderByWhereFilter_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE id > 9990 ORDER BY id"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskPKOrderByWhereFilterDesc_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench WHERE id > 9990 ORDER BY id DESC"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskPKCoveringOrderByLimit_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT id FROM bench ORDER BY id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskFullRowOrderByLimit_10000(b *testing.B) {
	exec := setupBenchTableDisk(b, 10000, false)
	sql := "SELECT * FROM bench ORDER BY id LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}
