package engine

import (
	"strings"
	"testing"
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
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}
}

func TestUnionBasic(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1: (1,alice), (2,bob), (3,charlie)
	// t2: (2,bob), (3,charlie), (4,dave)
	// UNION dedup: 4 unique rows
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionAll(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// UNION ALL: 3 + 3 = 6 rows
	if len(result.Rows) != 6 {
		t.Errorf("expected 6 rows, got %d", len(result.Rows))
	}
}

func TestUnionColumnCountMismatch(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	_, err := e.ExecuteSQL("SELECT id FROM t1 UNION SELECT id, name FROM t2")
	if err == nil {
		t.Fatal("expected error for column count mismatch, got nil")
	}
}

func TestUnionWithOrderBy(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}

	// Verify order: 1, 2, 3, 4
	expectedIDs := []int64{1, 2, 3, 4}
	for i, expectedID := range expectedIDs {
		if result.Rows[i][0] != expectedID {
			t.Errorf("row %d: expected id=%d, got %v", i, expectedID, result.Rows[i][0])
		}
	}
}

func TestUnionWithLimitOffset(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// Sorted by id: 1, 2, 3, 4 → OFFSET 1 LIMIT 2 → 2, 3
	if result.Rows[0][0] != int64(2) {
		t.Errorf("row 0: expected id=2, got %v", result.Rows[0][0])
	}
	if result.Rows[1][0] != int64(3) {
		t.Errorf("row 1: expected id=3, got %v", result.Rows[1][0])
	}
}

func TestUnionChain(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 UNION SELECT id, name FROM t3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1: 1,2,3  t2: 2,3,4  t3: 4,5
	// UNION dedup: 1,2,3,4,5 → 5 unique rows
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionAllChain(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 UNION ALL SELECT id, name FROM t3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// UNION ALL: 3 + 3 + 2 = 8 rows
	if len(result.Rows) != 8 {
		t.Errorf("expected 8 rows, got %d", len(result.Rows))
	}
}

func TestUnionWithWhere(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 WHERE id >= 2 UNION SELECT id, name FROM t2 WHERE id <= 3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1 WHERE id>=2: (2,bob), (3,charlie)
	// t2 WHERE id<=3: (2,bob), (3,charlie)
	// UNION dedup: 2 unique rows
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionColumnNames(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION SELECT id, name FROM t2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Column names should come from the left SELECT
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "id" {
		t.Errorf("column 0: expected %q, got %q", "id", result.Columns[0])
	}
	if result.Columns[1] != "name" {
		t.Errorf("column 1: expected %q, got %q", "name", result.Columns[1])
	}
}

func TestUnionWithJoin(t *testing.T) {
	e := NewExecutor()

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
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL(
		"SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id " +
			"UNION " +
			"SELECT users.name, returns.amount FROM users JOIN returns ON users.id = returns.user_id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// JOIN 1: (alice,100), (bob,200)
	// JOIN 2: (alice,50)
	// UNION: 3 unique rows
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionParenthesizedLimit(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	// Each SELECT limited individually, then combined
	result, err := e.ExecuteSQL("(SELECT id, name FROM t1 ORDER BY id LIMIT 2) UNION ALL (SELECT id, name FROM t2 ORDER BY id LIMIT 2)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1 LIMIT 2: (1,alice), (2,bob)
	// t2 LIMIT 2: (2,bob), (3,charlie)
	// UNION ALL: 4 rows
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestUnionParenthesizedLimitWithOverallOrderBy(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("(SELECT id, name FROM t1 LIMIT 2) UNION (SELECT id, name FROM t2 LIMIT 2) ORDER BY id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1 LIMIT 2: (1,alice), (2,bob)
	// t2 LIMIT 2: (2,bob), (3,charlie)
	// UNION dedup: (1,alice), (2,bob), (3,charlie) → 3 rows
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Verify ORDER BY id
	expectedIDs := []int64{1, 2, 3}
	for i, expectedID := range expectedIDs {
		if result.Rows[i][0] != expectedID {
			t.Errorf("row %d: expected id=%d, got %v", i, expectedID, result.Rows[i][0])
		}
	}
}

func TestUnionWhereAppliesToEachSelect(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	// WHERE on right SELECT only: left gets all rows, right gets filtered
	result, err := e.ExecuteSQL("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 WHERE id = 4")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1 (no WHERE): (1,alice), (2,bob), (3,charlie) → 3 rows
	// t2 WHERE id=4: (4,dave) → 1 row
	// UNION ALL: 4 rows
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}

	// Verify that all t1 rows are present and only id=4 from t2
	ids := make(map[int64]bool)
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	for _, id := range []int64{1, 2, 3, 4} {
		if !ids[id] {
			t.Errorf("expected id=%d to be present", id)
		}
	}
}

func TestUnionGroupByAppliesToEachSelect(t *testing.T) {
	e := NewExecutor()

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
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL(
		"SELECT region, SUM(amount) FROM sales GROUP BY region " +
			"UNION ALL " +
			"SELECT region, SUM(amount) FROM returns GROUP BY region")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// sales GROUP BY: (east,300), (west,300)
	// returns GROUP BY: (east,50), (west,250)
	// UNION ALL: 4 rows
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}

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
		if !found {
			t.Errorf("expected (%s, %d) in result, got %v", e.region, e.amount, got)
		}
	}
}

func TestUnionHavingAppliesToEachSelect(t *testing.T) {
	e := NewExecutor()

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
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL(
		"SELECT category, SUM(amount) FROM orders1 GROUP BY category HAVING SUM(amount) >= 100 " +
			"UNION ALL " +
			"SELECT category, SUM(amount) FROM orders2 GROUP BY category HAVING SUM(amount) >= 100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// orders1: a=300 (pass), b=50 (fail) → 1 row
	// orders2: a=10 (fail), b=900 (pass) → 1 row
	// UNION ALL: 2 rows
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// Verify: (a, 300) from orders1 and (b, 900) from orders2
	row0Cat := result.Rows[0][0].(string)
	row0Sum := result.Rows[0][1].(int64)
	row1Cat := result.Rows[1][0].(string)
	row1Sum := result.Rows[1][1].(int64)

	if !((row0Cat == "a" && row0Sum == 300 && row1Cat == "b" && row1Sum == 900) ||
		(row0Cat == "b" && row0Sum == 900 && row1Cat == "a" && row1Sum == 300)) {
		t.Errorf("expected (a,300) and (b,900), got (%s,%d) and (%s,%d)",
			row0Cat, row0Sum, row1Cat, row1Sum)
	}
}

func TestUnionBareLimitError(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	_, err := e.ExecuteSQL("SELECT id FROM t1 LIMIT 2 UNION SELECT id FROM t2")
	if err == nil {
		t.Fatal("expected error for bare LIMIT before UNION, got nil")
	}
}

func TestUnionTypeMismatchError(t *testing.T) {
	e := NewExecutor()

	stmts := []string{
		"CREATE TABLE ti (id INT, val INT)",
		"INSERT INTO ti VALUES (1, 100)",
		"CREATE TABLE tt (id INT, val TEXT)",
		"INSERT INTO tt VALUES (2, 'hello')",
	}
	for _, sql := range stmts {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	_, err := e.ExecuteSQL("SELECT id, val FROM ti UNION SELECT id, val FROM tt")
	if err == nil {
		t.Fatal("expected error for type mismatch (INT vs TEXT), got nil")
	}
	if !strings.Contains(err.Error(), "type mismatch") {
		t.Errorf("expected type mismatch error, got: %v", err)
	}
}

func TestUnionAllTypeMismatchError(t *testing.T) {
	e := NewExecutor()

	stmts := []string{
		"CREATE TABLE ti2 (id INT, val INT)",
		"INSERT INTO ti2 VALUES (1, 100)",
		"CREATE TABLE tt2 (id INT, val TEXT)",
		"INSERT INTO tt2 VALUES (2, 'hello')",
	}
	for _, sql := range stmts {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	_, err := e.ExecuteSQL("SELECT id, val FROM ti2 UNION ALL SELECT id, val FROM tt2")
	if err == nil {
		t.Fatal("expected error for type mismatch (INT vs TEXT), got nil")
	}
	if !strings.Contains(err.Error(), "type mismatch") {
		t.Errorf("expected type mismatch error, got: %v", err)
	}
}

func TestUnionSameTypesOK(t *testing.T) {
	e := NewExecutor()

	stmts := []string{
		"CREATE TABLE sa (id INT, val INT)",
		"INSERT INTO sa VALUES (1, 100)",
		"INSERT INTO sa VALUES (2, 200)",
		"CREATE TABLE sb (id INT, val INT)",
		"INSERT INTO sb VALUES (2, 200)",
		"INSERT INTO sb VALUES (3, 300)",
	}
	for _, sql := range stmts {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL("SELECT id, val FROM sa UNION SELECT id, val FROM sb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// (1,100), (2,200), (3,300) → 3 unique rows
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestIntersectBasic(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1: (1,alice), (2,bob), (3,charlie)
	// t2: (2,bob), (3,charlie), (4,dave)
	// INTERSECT: common rows → (2,bob), (3,charlie) → 2 rows
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectAll(t *testing.T) {
	e := NewExecutor()

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
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL("SELECT id FROM ia INTERSECT ALL SELECT id FROM ib")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ia: 1, 2, 2, 3
	// ib: 2, 2, 2, 3
	// INTERSECT ALL: 2 appears min(2,3)=2 times, 3 appears min(1,1)=1 time → 3 rows
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectNoCommon(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// t1: (1,alice), (2,bob), (3,charlie)
	// t3: (4,dave), (5,eve)
	// No common rows → empty
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectWithOrderBy(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2 ORDER BY id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// Verify order: 2, 3
	expectedIDs := []int64{2, 3}
	for i, expectedID := range expectedIDs {
		if result.Rows[i][0] != expectedID {
			t.Errorf("row %d: expected id=%d, got %v", i, expectedID, result.Rows[i][0])
		}
	}
}

func TestIntersectChain(t *testing.T) {
	e := NewExecutor()
	setupUnionTables(t, e)

	// t1: 1,2,3  t2: 2,3,4  t3: 4,5
	// t1 INTERSECT t2 = {2,3}, then {2,3} INTERSECT t3 = {} → 0 rows
	result, err := e.ExecuteSQL("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2 INTERSECT SELECT id, name FROM t3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  row: %v", row)
		}
	}
}

func TestIntersectTypeMismatch(t *testing.T) {
	e := NewExecutor()

	stmts := []string{
		"CREATE TABLE it1 (id INT, val INT)",
		"INSERT INTO it1 VALUES (1, 100)",
		"CREATE TABLE it2 (id INT, val TEXT)",
		"INSERT INTO it2 VALUES (1, 'hello')",
	}
	for _, sql := range stmts {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	_, err := e.ExecuteSQL("SELECT id, val FROM it1 INTERSECT SELECT id, val FROM it2")
	if err == nil {
		t.Fatal("expected error for type mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "type mismatch") {
		t.Errorf("expected type mismatch error, got: %v", err)
	}
}
