package engine

import (
	"testing"
)

func TestWindowRowNumber(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.name {
			t.Errorf("row %d name: expected %q, got %v", i, exp.name, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.rn {
			t.Errorf("row %d rn: expected %d, got %v", i, exp.rn, result.Rows[i][1])
		}
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
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.dept {
			t.Errorf("row %d dept: expected %q, got %v", i, exp.dept, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.name {
			t.Errorf("row %d name: expected %q, got %v", i, exp.name, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.rn {
			t.Errorf("row %d rn: expected %d, got %v", i, exp.rn, result.Rows[i][2])
		}
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
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.id {
			t.Errorf("row %d id: expected %d, got %v", i, exp.id, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.rank {
			t.Errorf("row %d rank: expected %d, got %v", i, exp.rank, result.Rows[i][1])
		}
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
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.id {
			t.Errorf("row %d id: expected %d, got %v", i, exp.id, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.denseRank {
			t.Errorf("row %d dense_rank: expected %d, got %v", i, exp.denseRank, result.Rows[i][1])
		}
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
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.dept {
			t.Errorf("row %d dept: expected %q, got %v", i, exp.dept, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.id {
			t.Errorf("row %d id: expected %d, got %v", i, exp.id, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.rank {
			t.Errorf("row %d rank: expected %d, got %v", i, exp.rank, result.Rows[i][2])
		}
	}
}

func TestWindowMultipleFunctions(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, score INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 100)")
	run(t, exec, "INSERT INTO t VALUES (3, 90)")

	result := run(t, exec, "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn, RANK() OVER (ORDER BY score DESC) AS r, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM t")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.id {
			t.Errorf("row %d id: expected %d, got %v", i, exp.id, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.rn {
			t.Errorf("row %d rn: expected %d, got %v", i, exp.rn, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.rank {
			t.Errorf("row %d rank: expected %d, got %v", i, exp.rank, result.Rows[i][2])
		}
		if result.Rows[i][3] != exp.denseRank {
			t.Errorf("row %d dense_rank: expected %d, got %v", i, exp.denseRank, result.Rows[i][3])
		}
	}
}

func TestWindowWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT, active INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice', 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob', 0)")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie', 1)")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t WHERE active = 1")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != int64(1) {
		t.Errorf("row 0: expected (alice, 1), got (%v, %v)", result.Rows[0][0], result.Rows[0][1])
	}
	if result.Rows[1][0] != "charlie" || result.Rows[1][1] != int64(2) {
		t.Errorf("row 1: expected (charlie, 2), got (%v, %v)", result.Rows[1][0], result.Rows[1][1])
	}
}

func TestWindowWithLimit(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie')")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "alice" || result.Rows[0][1] != int64(1) {
		t.Errorf("row 0: expected (alice, 1), got (%v, %v)", result.Rows[0][0], result.Rows[0][1])
	}
	if result.Rows[1][0] != "bob" || result.Rows[1][1] != int64(2) {
		t.Errorf("row 1: expected (bob, 2), got (%v, %v)", result.Rows[1][0], result.Rows[1][1])
	}
}

func TestWindowEmptyOver(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT)")
	run(t, exec, "INSERT INTO t VALUES (1)")
	run(t, exec, "INSERT INTO t VALUES (2)")
	run(t, exec, "INSERT INTO t VALUES (3)")

	result := run(t, exec, "SELECT id, ROW_NUMBER() OVER () AS rn FROM t")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	// Without ORDER BY, all rows are in one partition; ROW_NUMBER assigned in scan order
	for i, row := range result.Rows {
		rn, ok := row[1].(int64)
		if !ok {
			t.Errorf("row %d rn: expected int64, got %T", i, row[1])
			continue
		}
		if rn < 1 || rn > 3 {
			t.Errorf("row %d rn: expected 1-3, got %d", i, rn)
		}
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
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.dept {
			t.Errorf("row %d dept: expected %q, got %v", i, exp.dept, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.sal {
			t.Errorf("row %d salary: expected %d, got %v", i, exp.sal, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.total {
			t.Errorf("row %d dept_total: expected %d, got %v", i, exp.total, result.Rows[i][2])
		}
	}
}

func TestWindowSumRunning(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10)")
	run(t, exec, "INSERT INTO t VALUES (2, 20)")
	run(t, exec, "INSERT INTO t VALUES (3, 30)")

	result := run(t, exec, "SELECT id, SUM(val) OVER (ORDER BY id) AS running FROM t")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.id {
			t.Errorf("row %d id: expected %d, got %v", i, exp.id, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.running {
			t.Errorf("row %d running: expected %d, got %v", i, exp.running, result.Rows[i][1])
		}
	}
}

func TestWindowCountStar(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng')")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng')")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales')")

	result := run(t, exec, "SELECT dept, COUNT(*) OVER (PARTITION BY dept) AS cnt FROM emp")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	expected := []struct {
		dept string
		cnt  int64
	}{
		{"eng", 2},
		{"eng", 2},
		{"sales", 1},
	}
	for i, exp := range expected {
		if result.Rows[i][0] != exp.dept {
			t.Errorf("row %d dept: expected %q, got %v", i, exp.dept, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.cnt {
			t.Errorf("row %d cnt: expected %d, got %v", i, exp.cnt, result.Rows[i][1])
		}
	}
}

func TestWindowAvgPartition(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE scores (id INT, dept TEXT, score INT)")
	run(t, exec, "INSERT INTO scores VALUES (1, 'a', 10)")
	run(t, exec, "INSERT INTO scores VALUES (2, 'a', 20)")
	run(t, exec, "INSERT INTO scores VALUES (3, 'b', 30)")

	result := run(t, exec, "SELECT dept, AVG(score) OVER (PARTITION BY dept) AS avg_score FROM scores")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.dept {
			t.Errorf("row %d dept: expected %q, got %v", i, exp.dept, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.avg {
			t.Errorf("row %d avg: expected %v, got %v", i, exp.avg, result.Rows[i][1])
		}
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
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.grp {
			t.Errorf("row %d grp: expected %q, got %v", i, exp.grp, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.mn {
			t.Errorf("row %d min: expected %d, got %v", i, exp.mn, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.mx {
			t.Errorf("row %d max: expected %d, got %v", i, exp.mx, result.Rows[i][2])
		}
	}
}

func TestWindowAggregateWithAlias(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 200)")

	result := run(t, exec, "SELECT id, SUM(val) OVER () AS total FROM t")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Columns[1] != "total" {
		t.Errorf("expected column name 'total', got %q", result.Columns[1])
	}
	for i, row := range result.Rows {
		if row[1] != int64(300) {
			t.Errorf("row %d total: expected 300, got %v", i, row[1])
		}
	}
}

func TestWindowAggregateWithWhere(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT, active INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 1)")
	run(t, exec, "INSERT INTO t VALUES (2, 20, 0)")
	run(t, exec, "INSERT INTO t VALUES (3, 30, 1)")

	result := run(t, exec, "SELECT id, SUM(val) OVER () AS total FROM t WHERE active = 1")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// Only active rows: val=10,30 → sum=40
	for i, row := range result.Rows {
		if row[1] != int64(40) {
			t.Errorf("row %d total: expected 40, got %v", i, row[1])
		}
	}
}

func TestWindowMixedRankingAndAggregate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 200)")
	run(t, exec, "INSERT INTO t VALUES (3, 100)")

	result := run(t, exec, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, SUM(val) OVER () AS total FROM t")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.id {
			t.Errorf("row %d id: expected %d, got %v", i, exp.id, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.rn {
			t.Errorf("row %d rn: expected %d, got %v", i, exp.rn, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.total {
			t.Errorf("row %d total: expected %d, got %v", i, exp.total, result.Rows[i][2])
		}
	}
}

func TestNamedWindowBasic(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, name TEXT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 'alice')")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 'bob')")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales', 'charlie')")

	result := run(t, exec, "SELECT name, ROW_NUMBER() OVER w AS rn FROM emp WINDOW w AS (ORDER BY id)")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	expected := []struct {
		name string
		rn   int64
	}{
		{"alice", 1},
		{"bob", 2},
		{"charlie", 3},
	}
	for i, exp := range expected {
		if result.Rows[i][0] != exp.name {
			t.Errorf("row %d name: expected %q, got %v", i, exp.name, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.rn {
			t.Errorf("row %d rn: expected %d, got %v", i, exp.rn, result.Rows[i][1])
		}
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
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.dept {
			t.Errorf("row %d dept: expected %q, got %v", i, exp.dept, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.total {
			t.Errorf("row %d dept_total: expected %d, got %v", i, exp.total, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.rank {
			t.Errorf("row %d salary_rank: expected %d, got %v", i, exp.rank, result.Rows[i][2])
		}
	}
}

func TestNamedWindowAggregate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE emp (id INT, dept TEXT, salary INT)")
	run(t, exec, "INSERT INTO emp VALUES (1, 'eng', 100)")
	run(t, exec, "INSERT INTO emp VALUES (2, 'eng', 200)")
	run(t, exec, "INSERT INTO emp VALUES (3, 'sales', 300)")

	result := run(t, exec, "SELECT dept, salary, SUM(salary) OVER w AS total, COUNT(*) OVER w AS cnt FROM emp WINDOW w AS (PARTITION BY dept)")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.dept {
			t.Errorf("row %d dept: expected %q, got %v", i, exp.dept, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.sal {
			t.Errorf("row %d salary: expected %d, got %v", i, exp.sal, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.total {
			t.Errorf("row %d total: expected %d, got %v", i, exp.total, result.Rows[i][2])
		}
		if result.Rows[i][3] != exp.cnt {
			t.Errorf("row %d cnt: expected %d, got %v", i, exp.cnt, result.Rows[i][3])
		}
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
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
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
		if result.Rows[i][0] != exp.id {
			t.Errorf("row %d id: expected %d, got %v", i, exp.id, result.Rows[i][0])
		}
		if result.Rows[i][1] != exp.rn {
			t.Errorf("row %d rn: expected %d, got %v", i, exp.rn, result.Rows[i][1])
		}
		if result.Rows[i][2] != exp.total {
			t.Errorf("row %d total: expected %d, got %v", i, exp.total, result.Rows[i][2])
		}
	}
}
