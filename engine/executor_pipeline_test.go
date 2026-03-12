package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/storage"
)

func TestApplyOffset(t *testing.T) {
	rows := []Row{
		{int64(1)},
		{int64(2)},
		{int64(3)},
	}

	tests := []struct {
		name   string
		offset *int64
		want   int
	}{
		{"nil offset", nil, 3},
		{"offset 0", ptr(int64(0)), 3},
		{"offset 1", ptr(int64(1)), 2},
		{"offset 3", ptr(int64(3)), 0},
		{"offset beyond", ptr(int64(10)), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyOffset(rows, tt.offset)
			assert.Len(t, result, tt.want, "unexpected number of rows after offset")
		})
	}
}

func TestApplyLimit(t *testing.T) {
	rows := []Row{
		{int64(1)},
		{int64(2)},
		{int64(3)},
	}

	tests := []struct {
		name  string
		limit *int64
		want  int
	}{
		{"nil limit", nil, 3},
		{"limit 0", ptr(int64(0)), 0},
		{"limit 2", ptr(int64(2)), 2},
		{"limit 5", ptr(int64(5)), 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyLimit(rows, tt.limit)
			assert.Len(t, result, tt.want, "unexpected number of rows after limit")
		})
	}
}

func ptr(v int64) *int64 {
	return &v
}

func TestFilterWhere(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(nil, info)
	rows := []Row{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Alice"},
	}

	// Filter by name = 'Alice'
	where := &ast.BinaryExpr{
		Left:  &ast.IdentExpr{Name: "name"},
		Op:    "=",
		Right: &ast.StringLitExpr{Value: "Alice"},
	}
	result, err := filterWhere(rows, where, eval, rowIdentity)
	require.NoError(t, err)
	assert.Len(t, result, 2, "expected 2 rows matching name='Alice'")

	// Nil where returns all rows
	result, err = filterWhere(rows, nil, eval, rowIdentity)
	require.NoError(t, err)
	assert.Len(t, result, 3, "expected all 3 rows with nil where")
}

func TestFilterWhereLimit(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(nil, info)
	rows := []Row{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Alice"},
		{int64(4), "Charlie"},
		{int64(5), "Alice"},
	}

	// Filter by name = 'Alice' with limit 2
	where := &ast.BinaryExpr{
		Left:  &ast.IdentExpr{Name: "name"},
		Op:    "=",
		Right: &ast.StringLitExpr{Value: "Alice"},
	}
	result, err := filterWhereLimit(rows, where, eval, rowIdentity, 2)
	require.NoError(t, err)
	assert.Len(t, result, 2, "expected 2 rows matching name='Alice' with limit 2")

	// Nil where with limit 3 returns first 3 rows
	result, err = filterWhereLimit(rows, nil, eval, rowIdentity, 3)
	require.NoError(t, err)
	assert.Len(t, result, 3, "expected 3 rows with nil where and limit 3")

	// Limit larger than matching rows returns all matching
	result, err = filterWhereLimit(rows, where, eval, rowIdentity, 10)
	require.NoError(t, err)
	assert.Len(t, result, 3, "expected all 3 matching rows when limit exceeds matches")
}

func TestSortRows(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(nil, info)
	rows := []Row{
		{int64(3), "Charlie"},
		{int64(1), "Alice"},
		{int64(2), "Bob"},
	}

	// Sort by id ASC
	orderBy := []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
	}
	result, err := sortRows(rows, orderBy, eval, rowIdentity)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result[0][0])
	assert.Equal(t, int64(2), result[1][0])
	assert.Equal(t, int64(3), result[2][0])

	// Sort by id DESC
	orderBy = []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "id"}, Desc: true},
	}
	result, err = sortRows(rows, orderBy, eval, rowIdentity)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result[0][0])
	assert.Equal(t, int64(2), result[1][0])
	assert.Equal(t, int64(1), result[2][0])

	// Empty orderBy returns unchanged
	result, err = sortRows(rows, nil, eval, rowIdentity)
	require.NoError(t, err)
	assert.Len(t, result, 3, "expected 3 rows with nil orderBy")
}

func TestProjectRows(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
			{Name: "age", DataType: "INT", Index: 2},
		},
	}
	eval := newTableEvaluator(nil, info)
	rows := []Row{
		{int64(1), "Alice", int64(30)},
		{int64(2), "Bob", int64(25)},
	}

	// Project specific columns
	colExprs := []ast.Expr{
		&ast.IdentExpr{Name: "name"},
		&ast.IdentExpr{Name: "age"},
	}
	result, err := projectRows(rows, colExprs, false, eval)
	require.NoError(t, err)
	require.Len(t, result, 2, "expected 2 projected rows")
	assert.Equal(t, "Alice", result[0][0])
	assert.Equal(t, int64(30), result[0][1])

	// Star projection
	result, err = projectRows(rows, nil, true, eval)
	require.NoError(t, err)
	assert.Len(t, result[0], 3, "expected 3 columns in star projection")
}

func TestCompareSortKeys(t *testing.T) {
	ascOrder := []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
	}
	descOrder := []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "id"}, Desc: true},
	}
	multiOrder := []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "col1"}, Desc: false},
		{Expr: &ast.IdentExpr{Name: "col2"}, Desc: false},
	}

	tests := []struct {
		name    string
		a, b    []Value
		orderBy []ast.OrderByClause
		want    int // -1, 0, 1 (sign)
	}{
		{"equal int ASC", []Value{int64(1)}, []Value{int64(1)}, ascOrder, 0},
		{"less int ASC", []Value{int64(1)}, []Value{int64(2)}, ascOrder, -1},
		{"greater int ASC", []Value{int64(3)}, []Value{int64(2)}, ascOrder, 1},
		{"less int DESC", []Value{int64(1)}, []Value{int64(2)}, descOrder, 1},
		{"greater int DESC", []Value{int64(3)}, []Value{int64(2)}, descOrder, -1},
		{"equal string ASC", []Value{"abc"}, []Value{"abc"}, ascOrder, 0},
		{"less string ASC", []Value{"abc"}, []Value{"def"}, ascOrder, -1},
		{"both NULL", []Value{nil}, []Value{nil}, ascOrder, 0},
		{"left NULL ASC", []Value{nil}, []Value{int64(1)}, ascOrder, 1},
		{"right NULL ASC", []Value{int64(1)}, []Value{nil}, ascOrder, -1},
		{"left NULL DESC", []Value{nil}, []Value{int64(1)}, descOrder, 1},
		{"right NULL DESC", []Value{int64(1)}, []Value{nil}, descOrder, -1},
		{"multi first differs", []Value{int64(1), "b"}, []Value{int64(2), "a"}, multiOrder, -1},
		{"multi first equal second differs", []Value{int64(1), "a"}, []Value{int64(1), "b"}, multiOrder, -1},
		{"multi all equal", []Value{int64(1), "a"}, []Value{int64(1), "a"}, multiOrder, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareSortKeys(tt.a, tt.b, tt.orderBy)
			assert.Equal(t, tt.want, sign(got), "compareSortKeys(%v, %v)", tt.a, tt.b)
		})
	}
}

func sign(v int) int {
	if v < 0 {
		return -1
	}
	if v > 0 {
		return 1
	}
	return 0
}

func TestTopKHeapInvariant(t *testing.T) {
	// Build a max-heap and verify the top element is always the largest
	orderBy := []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
	}
	h := &topKHeap{orderBy: orderBy}

	// Push values: 3, 1, 4, 1, 5, 9, 2, 6
	values := []int64{3, 1, 4, 1, 5, 9, 2, 6}
	for i, v := range values {
		h.Push(sortKey{values: []Value{v}, index: i})
	}
	// Re-establish heap property after manual pushes
	heapInit(h)

	// Pop should yield values in descending order (max-heap)
	prev := int64(100)
	for h.Len() > 0 {
		sk := heapPop(h)
		val := sk.values[0].(int64)
		assert.LessOrEqual(t, val, prev, "heap invariant violated: got %d after %d", val, prev)
		prev = val
	}
}

func heapInit(h *topKHeap) {
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		heapDown(h, i, n)
	}
}

func heapPop(h *topKHeap) sortKey {
	n := h.Len() - 1
	h.Swap(0, n)
	heapDown(h, 0, n)
	return h.Pop().(sortKey)
}

func heapDown(h *topKHeap, i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h.Less(right, left) {
			j = right
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
}

func TestSortRowsTopK(t *testing.T) {
	info := &TableInfo{
		Name: "t",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(nil, info)

	t.Run("basic ASC limit", func(t *testing.T) {
		rows := []Row{
			{int64(5), "e"},
			{int64(3), "c"},
			{int64(1), "a"},
			{int64(4), "d"},
			{int64(2), "b"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		// limit=1, rows=5 → 1*4=4 < 5, heap path
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 1)
		require.NoError(t, err)
		require.Len(t, result, 1, "expected 1 row")
		assert.Equal(t, int64(1), result[0][0])
	})

	t.Run("basic DESC limit", func(t *testing.T) {
		rows := []Row{
			{int64(5), "e"},
			{int64(3), "c"},
			{int64(1), "a"},
			{int64(4), "d"},
			{int64(2), "b"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: true},
		}
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 1)
		require.NoError(t, err)
		require.Len(t, result, 1, "expected 1 row")
		assert.Equal(t, int64(5), result[0][0])
	})

	t.Run("fallback when limit*4 >= len", func(t *testing.T) {
		rows := []Row{
			{int64(3), "c"},
			{int64(1), "a"},
			{int64(2), "b"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		// limit=1, rows=3 → 1*4=4 >= 3, falls back to sortRows
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 1)
		require.NoError(t, err)
		// Falls back to full sort, returns all rows
		require.Len(t, result, 3, "expected 3 rows (fallback)")
		assert.Equal(t, int64(1), result[0][0])
	})

	t.Run("empty rows", func(t *testing.T) {
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		result, err := sortRowsTopK([]Row{}, orderBy, eval, rowIdentity, 3)
		require.NoError(t, err)
		assert.Len(t, result, 0, "expected 0 rows")
	})

	t.Run("empty orderBy", func(t *testing.T) {
		rows := []Row{{int64(2), "b"}, {int64(1), "a"}}
		result, err := sortRowsTopK(rows, nil, eval, rowIdentity, 1)
		require.NoError(t, err)
		assert.Len(t, result, 2, "expected 2 rows unchanged")
	})

	t.Run("limit zero", func(t *testing.T) {
		rows := []Row{
			{int64(3), "c"},
			{int64(1), "a"},
			{int64(2), "b"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		// limit <= 0 falls back to sortRows
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result[0][0])
	})

	t.Run("with duplicates", func(t *testing.T) {
		rows := []Row{
			{int64(3), "c"},
			{int64(1), "a"},
			{int64(3), "c2"},
			{int64(2), "b"},
			{int64(1), "a2"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 1)
		require.NoError(t, err)
		require.Len(t, result, 1, "expected 1 row")
		assert.Equal(t, int64(1), result[0][0])
	})

	t.Run("multi column ORDER BY", func(t *testing.T) {
		rows := []Row{
			{int64(2), "b"},
			{int64(1), "b"},
			{int64(1), "a"},
			{int64(2), "a"},
			{int64(3), "a"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
			{Expr: &ast.IdentExpr{Name: "name"}, Desc: false},
		}
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 1)
		require.NoError(t, err)
		require.Len(t, result, 1, "expected 1 row")
		assert.Equal(t, int64(1), result[0][0])
		assert.Equal(t, "a", result[0][1])
	})

	t.Run("with NULLs", func(t *testing.T) {
		rows := []Row{
			{nil, "x"},
			{int64(3), "c"},
			{int64(1), "a"},
			{nil, "y"},
			{int64(2), "b"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		// Top 2 ASC: should be 1, 2 (NULLs sort last)
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 1)
		require.NoError(t, err)
		require.Len(t, result, 1, "expected 1 row")
		assert.Equal(t, int64(1), result[0][0])
	})

	t.Run("with NULLs DESC", func(t *testing.T) {
		rows := []Row{
			{nil, "x"},
			{int64(3), "c"},
			{int64(1), "a"},
			{nil, "y"},
			{int64(2), "b"},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: true},
		}
		// Top 1 DESC: should be 3 (NULLs sort last even for DESC)
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 1)
		require.NoError(t, err)
		require.Len(t, result, 1, "expected 1 row")
		assert.Equal(t, int64(3), result[0][0])
	})

	t.Run("storage.KeyRow generic type", func(t *testing.T) {
		krows := []storage.KeyRow{
			{Key: 10, Row: Row{int64(5), "e"}},
			{Key: 20, Row: Row{int64(3), "c"}},
			{Key: 30, Row: Row{int64(1), "a"}},
			{Key: 40, Row: Row{int64(4), "d"}},
			{Key: 50, Row: Row{int64(2), "b"}},
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		result, err := sortRowsTopK(krows, orderBy, eval, rowOfKeyRow, 1)
		require.NoError(t, err)
		require.Len(t, result, 1, "expected 1 row")
		assert.Equal(t, int64(1), result[0].Row[0])
		assert.Equal(t, int64(30), result[0].Key)
	})

	t.Run("result order with limit 3", func(t *testing.T) {
		// 10 rows to ensure heap path (limit*4=12 < 10? no, need more rows)
		rows := make([]Row, 20)
		for i := 0; i < 20; i++ {
			rows[i] = Row{int64(20 - i), "x"}
		}
		orderBy := []ast.OrderByClause{
			{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
		}
		// limit=3, rows=20 → 3*4=12 < 20, heap path
		result, err := sortRowsTopK(rows, orderBy, eval, rowIdentity, 3)
		require.NoError(t, err)
		require.Len(t, result, 3, "expected 3 rows")
		// Should be sorted: 1, 2, 3
		for i, exp := range []int64{1, 2, 3} {
			assert.Equal(t, exp, result[i][0], "result[%d]", i)
		}
	})
}

func TestResolveSelectColumns(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(nil, info)

	// Star
	colNames, colExprs, isStar, err := resolveSelectColumns([]ast.Expr{&ast.StarExpr{}}, eval)
	require.NoError(t, err)
	assert.True(t, isStar, "expected isStar=true")
	require.Len(t, colNames, 2, "expected 2 column names for star")
	assert.Equal(t, "id", colNames[0])
	assert.Equal(t, "name", colNames[1])
	assert.Nil(t, colExprs, "expected nil colExprs for star")

	// Named columns
	columns := []ast.Expr{
		&ast.IdentExpr{Name: "name"},
		&ast.AliasExpr{Expr: &ast.IdentExpr{Name: "id"}, Alias: "user_id"},
	}
	colNames, colExprs, isStar, err = resolveSelectColumns(columns, eval)
	require.NoError(t, err)
	assert.False(t, isStar, "expected isStar=false")
	require.Len(t, colNames, 2, "expected 2 column names")
	assert.Equal(t, "name", colNames[0])
	assert.Equal(t, "user_id", colNames[1])
	assert.Len(t, colExprs, 2, "expected 2 colExprs")
}
