package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupUnionTables(t *testing.T, e *Executor) {
	t.Helper()
	stmts := []string{
		"CREATE TABLE t1 (id INT, name TEXT)",
		"INSERT INTO t1 VALUES (1, 'alice')",
		"INSERT INTO t1 VALUES (2, 'bob')",
		"INSERT INTO t1 VALUES (3, 'charlie')",
		"CREATE TABLE t2 (id INT, name TEXT)",
		"INSERT INTO t2 VALUES (2, 'bob')",
		"INSERT INTO t2 VALUES (3, 'charlie')",
		"INSERT INTO t2 VALUES (4, 'dave')",
		"CREATE TABLE t3 (id INT, name TEXT)",
		"INSERT INTO t3 VALUES (4, 'dave')",
		"INSERT INTO t3 VALUES (5, 'eve')",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}
}

func TestUnionBasic(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2")
	require.NoError(t, err)

	// t1: (1,alice), (2,bob), (3,charlie)
	// t2: (2,bob), (3,charlie), (4,dave)
	// UNION dedup: 4 unique rows
	if !assert.Len(t, result.Rows, 4, "UNION dedup should produce 4 unique rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionAll(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2")
	require.NoError(t, err)

	// UNION ALL: 3 + 3 = 6 rows
	assert.Len(t, result.Rows, 6, "UNION ALL should produce 6 rows")
}

func TestUnionColumnCountMismatch(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	_, err := e.ExecuteSQL("SELECT id FROM t1 UNION SELECT id, name FROM t2")
	require.Error(t, err, "expected error for column count mismatch")
}

func TestUnionWithOrderBy(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id")
	require.NoError(t, err)

	require.Len(t, result.Rows, 4, "UNION with ORDER BY should produce 4 rows")

	// Verify order: 1, 2, 3, 4
	expectedIDs := []int64{1, 2, 3, 4}
	for i, expectedID := range expectedIDs {
		assert.Equal(t, expectedID, result.Rows[i][0], "row %d id mismatch", i)
	}
}

func TestUnionWithLimitOffset(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id LIMIT 2 OFFSET 1")
	require.NoError(t, err)

	require.Len(t, result.Rows, 2, "UNION with LIMIT 2 OFFSET 1 should produce 2 rows")

	// Sorted by id: 1, 2, 3, 4 → OFFSET 1 LIMIT 2 → 2, 3
	assert.Equal(t, int64(2), result.Rows[0][0], "row 0 id mismatch")
	assert.Equal(t, int64(3), result.Rows[1][0], "row 1 id mismatch")
}

func TestUnionChain(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 UNION SELECT id, name FROM t3")
	require.NoError(t, err)

	// t1: 1,2,3  t2: 2,3,4  t3: 4,5
	// UNION dedup: 1,2,3,4,5 → 5 unique rows
	if !assert.Len(t, result.Rows, 5, "UNION chain should produce 5 unique rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionAllChain(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 UNION ALL SELECT id, name FROM t3")
	require.NoError(t, err)

	// UNION ALL: 3 + 3 + 2 = 8 rows
	assert.Len(t, result.Rows, 8, "UNION ALL chain should produce 8 rows")
}

func TestUnionWithWhere(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 WHERE id >= 2 UNION SELECT id, name FROM t2 WHERE id <= 3")
	require.NoError(t, err)

	// t1 WHERE id>=2: (2,bob), (3,charlie)
	// t2 WHERE id<=3: (2,bob), (3,charlie)
	// UNION dedup: 2 unique rows
	if !assert.Len(t, result.Rows, 2, "UNION with WHERE should produce 2 unique rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionColumnNames(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2")
	require.NoError(t, err)

	// Column names should come from the left SELECT
	require.Len(t, result.Columns, 2, "UNION result should have 2 columns")
	assert.Equal(t, "id", result.Columns[0], "column 0 name mismatch")
	assert.Equal(t, "name", result.Columns[1], "column 1 name mismatch")
}

func TestUnionWithJoin(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"CREATE TABLE orders (id INT, user_id INT, amount INT)",
		"INSERT INTO orders VALUES (1, 1, 100)",
		"INSERT INTO orders VALUES (2, 2, 200)",
		"CREATE TABLE returns (id INT, user_id INT, amount INT)",
		"INSERT INTO returns VALUES (1, 1, 50)",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL(
		"SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id " +
			"UNION " +
			"SELECT users.name, returns.amount FROM users JOIN returns ON users.id = returns.user_id")
	require.NoError(t, err)

	// JOIN 1: (alice,100), (bob,200)
	// JOIN 2: (alice,50)
	// UNION: 3 unique rows
	if !assert.Len(t, result.Rows, 3, "UNION with JOIN should produce 3 unique rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionParenthesizedLimit(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	// Each SELECT limited individually, then combined
	result, err := e.ExecuteSQL("(SELECT id, name FROM t1 ORDER BY id LIMIT 2) UNION ALL (SELECT id, name FROM t2 ORDER BY id LIMIT 2)")
	require.NoError(t, err)

	// t1 LIMIT 2: (1,alice), (2,bob)
	// t2 LIMIT 2: (2,bob), (3,charlie)
	// UNION ALL: 4 rows
	if !assert.Len(t, result.Rows, 4, "parenthesized UNION ALL should produce 4 rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionParenthesizedLimitWithOverallOrderBy(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("(SELECT id, name FROM t1 LIMIT 2) UNION (SELECT id, name FROM t2 LIMIT 2) ORDER BY id")
	require.NoError(t, err)

	// t1 LIMIT 2: (1,alice), (2,bob)
	// t2 LIMIT 2: (2,bob), (3,charlie)
	// UNION dedup: (1,alice), (2,bob), (3,charlie) → 3 rows
	require.Len(t, result.Rows, 3, "parenthesized UNION with ORDER BY should produce 3 rows")

	// Verify ORDER BY id
	expectedIDs := []int64{1, 2, 3}
	for i, expectedID := range expectedIDs {
		assert.Equal(t, expectedID, result.Rows[i][0], "row %d id mismatch", i)
	}
}

func TestUnionWhereAppliesToEachSelect(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	// WHERE on right SELECT only: left gets all rows, right gets filtered
	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 WHERE id = 4")
	require.NoError(t, err)

	// t1 (no WHERE): (1,alice), (2,bob), (3,charlie) → 3 rows
	// t2 WHERE id=4: (4,dave) → 1 row
	// UNION ALL: 4 rows
	require.Len(t, result.Rows, 4, "UNION ALL with WHERE on right SELECT should produce 4 rows")

	// Verify that all t1 rows are present and only id=4 from t2
	ids := make(map[int64]bool)
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	for _, id := range []int64{1, 2, 3, 4} {
		assert.True(t, ids[id], "expected id=%d to be present", id)
	}
}

func TestUnionGroupByAppliesToEachSelect(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE sales (region TEXT, amount INT)",
		"INSERT INTO sales VALUES ('east', 100)",
		"INSERT INTO sales VALUES ('east', 200)",
		"INSERT INTO sales VALUES ('west', 300)",
		"CREATE TABLE returns (region TEXT, amount INT)",
		"INSERT INTO returns VALUES ('east', 50)",
		"INSERT INTO returns VALUES ('west', 100)",
		"INSERT INTO returns VALUES ('west', 150)",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL(
		"SELECT region, SUM(amount) FROM sales GROUP BY region " +
			"UNION ALL " +
			"SELECT region, SUM(amount) FROM returns GROUP BY region")
	require.NoError(t, err)

	// sales GROUP BY: (east,300), (west,300)
	// returns GROUP BY: (east,50), (west,250)
	// UNION ALL: 4 rows
	require.Len(t, result.Rows, 4, "UNION ALL with GROUP BY should produce 4 rows")

	// Collect results as region→amounts map
	type regionAmount struct {
		region string
		amount int64
	}
	var got []regionAmount
	for _, row := range result.Rows {
		got = append(got, regionAmount{region: row[0].(string), amount: row[1].(int64)})
	}

	expected := []regionAmount{
		{"east", 300},
		{"west", 300},
		{"east", 50},
		{"west", 250},
	}
	for _, e := range expected {
		found := false
		for _, g := range got {
			if g.region == e.region && g.amount == e.amount {
				found = true
				break
			}
		}
		assert.True(t, found, "expected (%s, %d) in result, got %v", e.region, e.amount, got)
	}
}

func TestUnionHavingAppliesToEachSelect(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE orders1 (category TEXT, amount INT)",
		"INSERT INTO orders1 VALUES ('a', 100)",
		"INSERT INTO orders1 VALUES ('a', 200)",
		"INSERT INTO orders1 VALUES ('b', 50)",
		"CREATE TABLE orders2 (category TEXT, amount INT)",
		"INSERT INTO orders2 VALUES ('a', 10)",
		"INSERT INTO orders2 VALUES ('b', 400)",
		"INSERT INTO orders2 VALUES ('b', 500)",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL(
		"SELECT category, SUM(amount) FROM orders1 GROUP BY category HAVING SUM(amount) >= 100 " +
			"UNION ALL " +
			"SELECT category, SUM(amount) FROM orders2 GROUP BY category HAVING SUM(amount) >= 100")
	require.NoError(t, err)

	// orders1: a=300 (pass), b=50 (fail) → 1 row
	// orders2: a=10 (fail), b=900 (pass) → 1 row
	// UNION ALL: 2 rows
	require.Len(t, result.Rows, 2, "UNION ALL with HAVING should produce 2 rows")

	// Verify: (a, 300) from orders1 and (b, 900) from orders2
	row0Cat := result.Rows[0][0].(string)
	row0Sum := result.Rows[0][1].(int64)
	row1Cat := result.Rows[1][0].(string)
	row1Sum := result.Rows[1][1].(int64)

	assert.True(t,
		(row0Cat == "a" && row0Sum == 300 && row1Cat == "b" && row1Sum == 900) ||
			(row0Cat == "b" && row0Sum == 900 && row1Cat == "a" && row1Sum == 300),
		"expected (a,300) and (b,900), got (%s,%d) and (%s,%d)",
		row0Cat, row0Sum, row1Cat, row1Sum)
}

func TestUnionBareLimitError(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	_, err := e.ExecuteSQL("SELECT id FROM t1 LIMIT 2 UNION SELECT id FROM t2")
	require.Error(t, err, "expected error for bare LIMIT before UNION")
}

func TestUnionTypeMismatchError(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE ti (id INT, val INT)",
		"INSERT INTO ti VALUES (1, 100)",
		"CREATE TABLE tt (id INT, val TEXT)",
		"INSERT INTO tt VALUES (2, 'hello')",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	_, err := e.ExecuteSQL("SELECT id, val FROM ti UNION SELECT id, val FROM tt")
	require.Error(t, err, "expected error for type mismatch (INT vs TEXT)")
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestUnionAllTypeMismatchError(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE ti2 (id INT, val INT)",
		"INSERT INTO ti2 VALUES (1, 100)",
		"CREATE TABLE tt2 (id INT, val TEXT)",
		"INSERT INTO tt2 VALUES (2, 'hello')",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	_, err := e.ExecuteSQL("SELECT id, val FROM ti2 UNION ALL SELECT id, val FROM tt2")
	require.Error(t, err, "expected error for type mismatch (INT vs TEXT)")
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestUnionSameTypesOK(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE sa (id INT, val INT)",
		"INSERT INTO sa VALUES (1, 100)",
		"INSERT INTO sa VALUES (2, 200)",
		"CREATE TABLE sb (id INT, val INT)",
		"INSERT INTO sb VALUES (2, 200)",
		"INSERT INTO sb VALUES (3, 300)",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL("SELECT id, val FROM sa UNION SELECT id, val FROM sb")
	require.NoError(t, err)

	// (1,100), (2,200), (3,300) → 3 unique rows
	assert.Len(t, result.Rows, 3, "UNION same types should produce 3 unique rows")
}

func TestIntersectBasic(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2")
	require.NoError(t, err)

	// t1: (1,alice), (2,bob), (3,charlie)
	// t2: (2,bob), (3,charlie), (4,dave)
	// INTERSECT: common rows → (2,bob), (3,charlie) → 2 rows
	if !assert.Len(t, result.Rows, 2, "INTERSECT should produce 2 common rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectAll(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE ia (id INT)",
		"INSERT INTO ia VALUES (1)",
		"INSERT INTO ia VALUES (2)",
		"INSERT INTO ia VALUES (2)",
		"INSERT INTO ia VALUES (3)",
		"CREATE TABLE ib (id INT)",
		"INSERT INTO ib VALUES (2)",
		"INSERT INTO ib VALUES (2)",
		"INSERT INTO ib VALUES (2)",
		"INSERT INTO ib VALUES (3)",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL("SELECT id FROM ia INTERSECT ALL SELECT id FROM ib")
	require.NoError(t, err)

	// ia: 1, 2, 2, 3
	// ib: 2, 2, 2, 3
	// INTERSECT ALL: 2 appears min(2,3)=2 times, 3 appears min(1,1)=1 time → 3 rows
	if !assert.Len(t, result.Rows, 3, "INTERSECT ALL should produce 3 rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectNoCommon(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t3")
	require.NoError(t, err)

	// t1: (1,alice), (2,bob), (3,charlie)
	// t3: (4,dave), (5,eve)
	// No common rows → empty
	if !assert.Len(t, result.Rows, 0, "INTERSECT with no common rows should produce 0 rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectWithOrderBy(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2 ORDER BY id")
	require.NoError(t, err)

	require.Len(t, result.Rows, 2, "INTERSECT with ORDER BY should produce 2 rows")

	// Verify order: 2, 3
	expectedIDs := []int64{2, 3}
	for i, expectedID := range expectedIDs {
		assert.Equal(t, expectedID, result.Rows[i][0], "row %d id mismatch", i)
	}
}

func TestIntersectChain(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	// t1: 1,2,3  t2: 2,3,4  t3: 4,5
	// t1 INTERSECT t2 = {2,3}, then {2,3} INTERSECT t3 = {} → 0 rows
	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2 INTERSECT SELECT id, name FROM t3")
	require.NoError(t, err)

	if !assert.Len(t, result.Rows, 0, "INTERSECT chain should produce 0 rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectTypeMismatch(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE it1 (id INT, val INT)",
		"INSERT INTO it1 VALUES (1, 100)",
		"CREATE TABLE it2 (id INT, val TEXT)",
		"INSERT INTO it2 VALUES (1, 'hello')",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	_, err := e.ExecuteSQL("SELECT id, val FROM it1 INTERSECT SELECT id, val FROM it2")
	require.Error(t, err, "expected error for type mismatch")
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestExceptBasic(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2")
	require.NoError(t, err)

	// t1: (1,alice), (2,bob), (3,charlie)
	// t2: (2,bob), (3,charlie), (4,dave)
	// EXCEPT: rows in t1 but not in t2 → (1,alice) → 1 row
	if !assert.Len(t, result.Rows, 1, "EXCEPT should produce 1 row") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
	if len(result.Rows) == 1 {
		assert.Equal(t, int64(1), result.Rows[0][0], "expected id=1")
		assert.Equal(t, "alice", result.Rows[0][1], "expected name=alice")
	}
}

func TestExceptAll(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE ea (id INT)",
		"INSERT INTO ea VALUES (1)",
		"INSERT INTO ea VALUES (2)",
		"INSERT INTO ea VALUES (2)",
		"INSERT INTO ea VALUES (2)",
		"INSERT INTO ea VALUES (3)",
		"CREATE TABLE eb (id INT)",
		"INSERT INTO eb VALUES (2)",
		"INSERT INTO eb VALUES (2)",
		"INSERT INTO eb VALUES (3)",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL("SELECT id FROM ea EXCEPT ALL SELECT id FROM eb")
	require.NoError(t, err)

	// ea: 1, 2, 2, 2, 3
	// eb: 2, 2, 3
	// EXCEPT ALL: 1 remains (not in eb), 2 appears 3-2=1 time, 3 appears 1-1=0 times → 2 rows
	if !assert.Len(t, result.Rows, 2, "EXCEPT ALL should produce 2 rows") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestExceptNoMatch(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	// All t2 rows are in t1 or not — here t1 subset check
	result, err := e.ExecuteSQL("SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t3")
	require.NoError(t, err)

	// t1: (1,alice), (2,bob), (3,charlie)
	// t3: (4,dave), (5,eve)
	// No overlap → all t1 rows remain → 3 rows
	assert.Len(t, result.Rows, 3, "EXCEPT with no overlap should keep all rows")
}

func TestExceptAllRemoved(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	// t2 contains all of t1's common rows plus more
	result, err := e.ExecuteSQL("SELECT id, name FROM t2 EXCEPT SELECT id, name FROM t1")
	require.NoError(t, err)

	// t2: (2,bob), (3,charlie), (4,dave)
	// t1: (1,alice), (2,bob), (3,charlie)
	// EXCEPT: (4,dave) → 1 row
	assert.Len(t, result.Rows, 1, "EXCEPT should produce 1 row")
	if len(result.Rows) == 1 {
		assert.Equal(t, int64(4), result.Rows[0][0], "expected id=4")
		assert.Equal(t, "dave", result.Rows[0][1], "expected name=dave")
	}
}

func TestExceptWithOrderBy(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 EXCEPT SELECT id, name FROM t3 ORDER BY id")
	require.NoError(t, err)

	// t1 UNION t2 = {1,2,3,4}, then {1,2,3,4} EXCEPT t3{4,5} = {1,2,3} → 3 rows
	require.Len(t, result.Rows, 3, "UNION then EXCEPT should produce 3 rows")

	expectedIDs := []int64{1, 2, 3}
	for i, expectedID := range expectedIDs {
		assert.Equal(t, expectedID, result.Rows[i][0], "row %d id mismatch", i)
	}
}

func TestExceptChain(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))
	setupUnionTables(t, e)

	// t1: 1,2,3  t2: 2,3,4
	// t1 EXCEPT t2 = {1}, then {1} EXCEPT t3{4,5} = {1} → 1 row
	result, err := e.ExecuteSQL("SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2 EXCEPT SELECT id, name FROM t3")
	require.NoError(t, err)

	if !assert.Len(t, result.Rows, 1, "EXCEPT chain should produce 1 row") {
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestExceptTypeMismatch(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	stmts := []string{
		"CREATE TABLE et1 (id INT, val INT)",
		"INSERT INTO et1 VALUES (1, 100)",
		"CREATE TABLE et2 (id INT, val TEXT)",
		"INSERT INTO et2 VALUES (1, 'hello')",
	}
	for _, sql := range stmts {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	_, err := e.ExecuteSQL("SELECT id, val FROM et1 EXCEPT SELECT id, val FROM et2")
	require.Error(t, err, "expected error for type mismatch")
	assert.Contains(t, err.Error(), "type mismatch")
}
