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
