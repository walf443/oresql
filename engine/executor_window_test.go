package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowRowNumber(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// Ordered by id: alice(1), bob(2), charlie(3)
	expected := []struct {
		name string
		rn   int64
	}{
		{"alice", 1},
		{"bob", 2},
		{"charlie", 3},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.name, result.Rows[i][0], "row %d name", i)
		assert.Equal(t, exp.rn, result.Rows[i][1], "row %d rn", i)
	}
}

func TestWindowRowNumberPartitioned(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, name TEXT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 'alice')")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 'bob')")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales', 'charlie')")
	run(t, exec, "INSERT INTO emp VALUES (4, 'sales', 'diana')")
	run(t, exec, "INSERT INTO emp VALUES (5, 'eng', 'eve')")

	result := run(t, exec, "SELECT dept, name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn FROM emp")
	require.Len(t, result.Rows, 5, "expected 5 rows")
	// eng partition: alice(1), bob(2), eve(3); sales partition: charlie(1), diana(2)
	expected := []struct {
		dept string
		name string
		rn   int64
	}{
		{"eng", "alice", 1},
		{"eng", "bob", 2},
		{"eng", "eve", 3},
		{"sales", "charlie", 1},
		{"sales", "diana", 2},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.dept, result.Rows[i][0], "row %d dept", i)
		assert.Equal(t, exp.name, result.Rows[i][1], "row %d name", i)
		assert.Equal(t, exp.rn, result.Rows[i][2], "row %d rn", i)
	}
}

func TestWindowRank(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, score INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 100)")
	run(t, exec, "INSERT INTO scores VALUES (2, 90)")
	run(t, exec, "INSERT INTO scores VALUES (3, 100)")
	run(t, exec, "INSERT INTO scores VALUES (4, 80)")

	result := run(t, exec, "SELECT id, RANK() OVER (ORDER BY score DESC) AS r FROM scores")
	require.Len(t, result.Rows, 4, "expected 4 rows")
	// score DESC: 100,100,90,80 → ranks: 1,1,3,4
	expected := []struct {
		id   int64
		rank int64
	}{
		{1, 1},
		{3, 1},
		{2, 3},
		{4, 4},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.id, result.Rows[i][0], "row %d id", i)
		assert.Equal(t, exp.rank, result.Rows[i][1], "row %d rank", i)
	}
}

func TestWindowDenseRank(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, score INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 100)")
	run(t, exec, "INSERT INTO scores VALUES (2, 90)")
	run(t, exec, "INSERT INTO scores VALUES (3, 100)")
	run(t, exec, "INSERT INTO scores VALUES (4, 80)")

	result := run(t, exec, "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM scores")
	require.Len(t, result.Rows, 4, "expected 4 rows")
	// score DESC: 100,100,90,80 → dense_ranks: 1,1,2,3
	expected := []struct {
		id        int64
		denseRank int64
	}{
		{1, 1},
		{3, 1},
		{2, 2},
		{4, 3},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.id, result.Rows[i][0], "row %d id", i)
		assert.Equal(t, exp.denseRank, result.Rows[i][1], "row %d dense_rank", i)
	}
}

func TestWindowRankPartitioned(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, salary INT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 100)")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 100)")
	run(t, exec, "INSERT INTO emp VALUES (3, 'eng', 80)")
	run(t, exec, "INSERT INTO emp VALUES (4, 'sales', 90)")
	run(t, exec, "INSERT INTO emp VALUES (5, 'sales', 90)")

	result := run(t, exec, "SELECT dept, id, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS r FROM emp")
	require.Len(t, result.Rows, 5, "expected 5 rows")
	expected := []struct {
		dept string
		id   int64
		rank int64
	}{
		{"eng", 1, 1},
		{"eng", 2, 1},
		{"eng", 3, 3},
		{"sales", 4, 1},
		{"sales", 5, 1},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.dept, result.Rows[i][0], "row %d dept", i)
		assert.Equal(t, exp.id, result.Rows[i][1], "row %d id", i)
		assert.Equal(t, exp.rank, result.Rows[i][2], "row %d rank", i)
	}
}

func TestWindowMultipleFunctions(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, score INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 100)")
	run(t, exec, "INSERT INTO t VALUES (3, 90)")

	result := run(t, exec, "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn, RANK() OVER (ORDER BY score DESC) AS r, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM t")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// score DESC: 100(id=1),100(id=2),90(id=3)
	// row_number: 1,2,3; rank: 1,1,3; dense_rank: 1,1,2
	expected := []struct {
		id        int64
		rn        int64
		rank      int64
		denseRank int64
	}{
		{1, 1, 1, 1},
		{2, 2, 1, 1},
		{3, 3, 3, 2},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.id, result.Rows[i][0], "row %d id", i)
		assert.Equal(t, exp.rn, result.Rows[i][1], "row %d rn", i)
		assert.Equal(t, exp.rank, result.Rows[i][2], "row %d rank", i)
		assert.Equal(t, exp.denseRank, result.Rows[i][3], "row %d dense_rank", i)
	}
}

func TestWindowWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT, active INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice', 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob', 0)")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie', 1)")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t WHERE active = 1")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0], "row 0 name")
	assert.Equal(t, int64(1), result.Rows[0][1], "row 0 rn")
	assert.Equal(t, "charlie", result.Rows[1][0], "row 1 name")
	assert.Equal(t, int64(2), result.Rows[1][1], "row 1 rn")
}

func TestWindowWithLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t LIMIT 2")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "alice", result.Rows[0][0], "row 0 name")
	assert.Equal(t, int64(1), result.Rows[0][1], "row 0 rn")
	assert.Equal(t, "bob", result.Rows[1][0], "row 1 name")
	assert.Equal(t, int64(2), result.Rows[1][1], "row 1 rn")
}

func TestWindowEmptyOver(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT)")
	run(t, exec, "INSERT INTO t VALUES (1)")
	run(t, exec, "INSERT INTO t VALUES (2)")
	run(t, exec, "INSERT INTO t VALUES (3)")

	result := run(t, exec, "SELECT id, ROW_NUMBER() OVER () AS rn FROM t")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// Without ORDER BY, all rows are in one partition; ROW_NUMBER assigned in scan order
	for i, row := range result.Rows {
		rn, ok := row[1].(int64)
		require.True(t, ok, "row %d rn: expected int64, got %T", i, row[1])
		assert.True(t, rn >= 1 && rn <= 3, "row %d rn: expected 1-3, got %d", i, rn)
	}
}

func TestWindowSumPartition(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, salary INT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 100)")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 200)")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales', 300)")
	run(t, exec, "INSERT INTO emp VALUES (4, 'sales', 400)")

	result := run(t, exec, "SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM emp")
	require.Len(t, result.Rows, 4, "expected 4 rows")
	// eng partition: sum=300, sales partition: sum=700
	expected := []struct {
		dept  string
		sal   int64
		total int64
	}{
		{"eng", 100, 300},
		{"eng", 200, 300},
		{"sales", 300, 700},
		{"sales", 400, 700},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.dept, result.Rows[i][0], "row %d dept", i)
		assert.Equal(t, exp.sal, result.Rows[i][1], "row %d salary", i)
		assert.Equal(t, exp.total, result.Rows[i][2], "row %d dept_total", i)
	}
}

func TestWindowSumRunning(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	result := run(t, exec, "SELECT id, SUM(val) OVER (ORDER BY id) AS running FROM t")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// Running total: 10, 30, 60
	expected := []struct {
		id      int64
		running int64
	}{
		{1, 10},
		{2, 30},
		{3, 60},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.id, result.Rows[i][0], "row %d id", i)
		assert.Equal(t, exp.running, result.Rows[i][1], "row %d running", i)
	}
}

func TestWindowCountStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng')")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng')")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales')")

	result := run(t, exec, "SELECT dept, COUNT(*) OVER (PARTITION BY dept) AS cnt FROM emp")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []struct {
		dept string
		cnt  int64
	}{
		{"eng", 2},
		{"eng", 2},
		{"sales", 1},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.dept, result.Rows[i][0], "row %d dept", i)
		assert.Equal(t, exp.cnt, result.Rows[i][1], "row %d cnt", i)
	}
}

func TestWindowAvgPartition(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, dept TEXT, score INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 'a', 10)")
	run(t, exec, "INSERT INTO scores VALUES (2, 'a', 20)")
	run(t, exec, "INSERT INTO scores VALUES (3, 'b', 30)")

	result := run(t, exec, "SELECT dept, AVG(score) OVER (PARTITION BY dept) AS avg_score FROM scores")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// a: avg=15.0, b: avg=30.0
	expected := []struct {
		dept string
		avg  float64
	}{
		{"a", 15.0},
		{"a", 15.0},
		{"b", 30.0},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.dept, result.Rows[i][0], "row %d dept", i)
		assert.Equal(t, exp.avg, result.Rows[i][1], "row %d avg", i)
	}
}

func TestWindowMinMaxPartition(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, grp TEXT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a', 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 'a', 30)")
	run(t, exec, "INSERT INTO t VALUES (3, 'a', 20)")
	run(t, exec, "INSERT INTO t VALUES (4, 'b', 50)")

	result := run(t, exec, "SELECT grp, MIN(val) OVER (PARTITION BY grp) AS mn, MAX(val) OVER (PARTITION BY grp) AS mx FROM t")
	require.Len(t, result.Rows, 4, "expected 4 rows")
	expected := []struct {
		grp string
		mn  int64
		mx  int64
	}{
		{"a", 10, 30},
		{"a", 10, 30},
		{"a", 10, 30},
		{"b", 50, 50},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.grp, result.Rows[i][0], "row %d grp", i)
		assert.Equal(t, exp.mn, result.Rows[i][1], "row %d min", i)
		assert.Equal(t, exp.mx, result.Rows[i][2], "row %d max", i)
	}
}

func TestWindowAggregateWithAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 200)")

	result := run(t, exec, "SELECT id, SUM(val) OVER () AS total FROM t")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "total", result.Columns[1], "expected column name 'total'")
	for i, row := range result.Rows {
		assert.Equal(t, int64(300), row[1], "row %d total", i)
	}
}

func TestWindowAggregateWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT, active INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 20, 0)")
	run(t, exec, "INSERT INTO t VALUES (3, 30, 1)")

	result := run(t, exec, "SELECT id, SUM(val) OVER () AS total FROM t WHERE active = 1")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	// Only active rows: val=10,30 → sum=40
	for i, row := range result.Rows {
		assert.Equal(t, int64(40), row[1], "row %d total", i)
	}
}

func TestWindowMixedRankingAndAggregate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 200)")
	run(t, exec, "INSERT INTO t VALUES (3, 100)")

	result := run(t, exec, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, SUM(val) OVER () AS total FROM t")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []struct {
		id    int64
		rn    int64
		total int64
	}{
		{1, 1, 400},
		{2, 2, 400},
		{3, 3, 400},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.id, result.Rows[i][0], "row %d id", i)
		assert.Equal(t, exp.rn, result.Rows[i][1], "row %d rn", i)
		assert.Equal(t, exp.total, result.Rows[i][2], "row %d total", i)
	}
}

func TestNamedWindowBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, name TEXT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 'alice')")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 'bob')")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales', 'charlie')")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER w AS rn FROM emp WINDOW w AS (ORDER BY id)")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []struct {
		name string
		rn   int64
	}{
		{"alice", 1},
		{"bob", 2},
		{"charlie", 3},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.name, result.Rows[i][0], "row %d name", i)
		assert.Equal(t, exp.rn, result.Rows[i][1], "row %d rn", i)
	}
}

func TestNamedWindowMultiple(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, salary INT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 100)")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 200)")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales', 300)")
	run(t, exec, "INSERT INTO emp VALUES (4, 'sales', 400)")

	result := run(t, exec, "SELECT dept, SUM(salary) OVER w1 AS dept_total, RANK() OVER w2 AS salary_rank FROM emp WINDOW w1 AS (PARTITION BY dept), w2 AS (PARTITION BY dept ORDER BY salary DESC)")
	require.Len(t, result.Rows, 4, "expected 4 rows")
	// Rows are reordered by the first window function (w1: PARTITION BY dept, no ORDER BY).
	// Within each partition, original order is preserved.
	// w2 ranks by salary DESC: eng(200→rank1, 100→rank2), sales(400→rank1, 300→rank2)
	expected := []struct {
		dept  string
		total int64
		rank  int64
	}{
		{"eng", 300, 2},   // id=1, salary=100
		{"eng", 300, 1},   // id=2, salary=200
		{"sales", 700, 2}, // id=3, salary=300
		{"sales", 700, 1}, // id=4, salary=400
	}
	for i, exp := range expected {
		assert.Equal(t, exp.dept, result.Rows[i][0], "row %d dept", i)
		assert.Equal(t, exp.total, result.Rows[i][1], "row %d dept_total", i)
		assert.Equal(t, exp.rank, result.Rows[i][2], "row %d salary_rank", i)
	}
}

func TestNamedWindowAggregate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, salary INT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 100)")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 200)")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales', 300)")

	result := run(t, exec, "SELECT dept, salary, SUM(salary) OVER w AS total, COUNT(*) OVER w AS cnt FROM emp WINDOW w AS (PARTITION BY dept)")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []struct {
		dept  string
		sal   int64
		total int64
		cnt   int64
	}{
		{"eng", 100, 300, 2},
		{"eng", 200, 300, 2},
		{"sales", 300, 300, 1},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.dept, result.Rows[i][0], "row %d dept", i)
		assert.Equal(t, exp.sal, result.Rows[i][1], "row %d salary", i)
		assert.Equal(t, exp.total, result.Rows[i][2], "row %d total", i)
		assert.Equal(t, exp.cnt, result.Rows[i][3], "row %d cnt", i)
	}
}

func TestNamedWindowMixed(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 200)")
	run(t, exec, "INSERT INTO t VALUES (3, 100)")

	// Mix named window and inline OVER clause
	result := run(t, exec, "SELECT id, ROW_NUMBER() OVER w AS rn, SUM(val) OVER () AS total FROM t WINDOW w AS (ORDER BY id)")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	expected := []struct {
		id    int64
		rn    int64
		total int64
	}{
		{1, 1, 400},
		{2, 2, 400},
		{3, 3, 400},
	}
	for i, exp := range expected {
		assert.Equal(t, exp.id, result.Rows[i][0], "row %d id", i)
		assert.Equal(t, exp.rn, result.Rows[i][1], "row %d rn", i)
		assert.Equal(t, exp.total, result.Rows[i][2], "row %d total", i)
	}
}
