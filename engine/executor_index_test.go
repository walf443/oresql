package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/ast"
)

func TestExtractEqualityConditions(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want map[string]Value
	}{
		{
			"single equality",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "id"},
				Op:    "=",
				Right: &ast.IntLitExpr{Value: 42},
			},
			map[string]Value{"id": int64(42)},
		},
		{
			"AND chain",
			&ast.LogicalExpr{
				Left: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "id"},
					Op:    "=",
					Right: &ast.IntLitExpr{Value: 1},
				},
				Op: "AND",
				Right: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "name"},
					Op:    "=",
					Right: &ast.StringLitExpr{Value: "alice"},
				},
			},
			map[string]Value{"id": int64(1), "name": "alice"},
		},
		{
			"IS NULL",
			&ast.IsNullExpr{
				Expr: &ast.IdentExpr{Name: "name"},
				Not:  false,
			},
			map[string]Value{"name": nil},
		},
		{
			"IS NOT NULL skipped",
			&ast.IsNullExpr{
				Expr: &ast.IdentExpr{Name: "name"},
				Not:  true,
			},
			map[string]Value{},
		},
		{
			"non-equality skipped",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "id"},
				Op:    ">",
				Right: &ast.IntLitExpr{Value: 5},
			},
			map[string]Value{},
		},
		{
			"OR does not collect",
			&ast.LogicalExpr{
				Left: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "id"},
					Op:    "=",
					Right: &ast.IntLitExpr{Value: 1},
				},
				Op: "OR",
				Right: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "id"},
					Op:    "=",
					Right: &ast.IntLitExpr{Value: 2},
				},
			},
			map[string]Value{},
		},
		{
			"float equality",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "score"},
				Op:    "=",
				Right: &ast.FloatLitExpr{Value: 3.14},
			},
			map[string]Value{"score": float64(3.14)},
		},
		{
			"case insensitive column",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "Name"},
				Op:    "=",
				Right: &ast.StringLitExpr{Value: "alice"},
			},
			map[string]Value{"name": "alice"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractEqualityConditions(tt.expr)
			require.Len(t, got, len(tt.want), "extractEqualityConditions() returned unexpected number of entries")
			for k, wantVal := range tt.want {
				gotVal, ok := got[k]
				assert.True(t, ok, "missing key %q", k)
				if ok {
					assert.Equal(t, wantVal, gotVal, "key %q", k)
				}
			}
		})
	}
}

func TestExtractInConditions(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want map[string][]Value
	}{
		{
			"single IN",
			&ast.InExpr{
				Left: &ast.IdentExpr{Name: "id"},
				Values: []ast.Expr{
					&ast.IntLitExpr{Value: 1},
					&ast.IntLitExpr{Value: 2},
					&ast.IntLitExpr{Value: 3},
				},
				Not: false,
			},
			map[string][]Value{"id": {int64(1), int64(2), int64(3)}},
		},
		{
			"NOT IN skipped",
			&ast.InExpr{
				Left: &ast.IdentExpr{Name: "id"},
				Values: []ast.Expr{
					&ast.IntLitExpr{Value: 1},
				},
				Not: true,
			},
			map[string][]Value{},
		},
		{
			"AND chain with IN",
			&ast.LogicalExpr{
				Left: &ast.InExpr{
					Left: &ast.IdentExpr{Name: "id"},
					Values: []ast.Expr{
						&ast.IntLitExpr{Value: 1},
						&ast.IntLitExpr{Value: 2},
					},
					Not: false,
				},
				Op: "AND",
				Right: &ast.InExpr{
					Left: &ast.IdentExpr{Name: "status"},
					Values: []ast.Expr{
						&ast.StringLitExpr{Value: "active"},
						&ast.StringLitExpr{Value: "pending"},
					},
					Not: false,
				},
			},
			map[string][]Value{
				"id":     {int64(1), int64(2)},
				"status": {"active", "pending"},
			},
		},
		{
			"non-literal value skipped",
			&ast.InExpr{
				Left: &ast.IdentExpr{Name: "id"},
				Values: []ast.Expr{
					&ast.IdentExpr{Name: "other_col"},
				},
				Not: false,
			},
			map[string][]Value{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractInConditions(tt.expr)
			require.Len(t, got, len(tt.want), "extractInConditions() returned unexpected number of entries")
			for k, wantVals := range tt.want {
				gotVals, ok := got[k]
				if !assert.True(t, ok, "missing key %q", k) {
					continue
				}
				require.Len(t, gotVals, len(wantVals), "key %q: unexpected number of values", k)
				for i, wv := range wantVals {
					assert.Equal(t, wv, gotVals[i], "key %q[%d]", k, i)
				}
			}
		})
	}
}

func TestDedupKeys(t *testing.T) {
	tests := []struct {
		name string
		keys []int64
		want []int64
	}{
		{
			"with duplicates",
			[]int64{1, 2, 3, 2, 1},
			[]int64{1, 2, 3},
		},
		{
			"no duplicates",
			[]int64{1, 2, 3},
			[]int64{1, 2, 3},
		},
		{
			"empty",
			[]int64{},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dedupKeys(tt.keys)
			require.Len(t, got, len(tt.want), "dedupKeys() returned unexpected number of keys")
			for i, w := range tt.want {
				assert.Equal(t, w, got[i], "dedupKeys()[%d]", i)
			}
		})
	}
}

func TestExtractRangeConditions(t *testing.T) {
	tests := []struct {
		name          string
		expr          ast.Expr
		wantCols      []string
		checkFromVal  map[string]Value
		checkFromIncl map[string]bool
		checkToVal    map[string]Value
		checkToIncl   map[string]bool
	}{
		{
			"greater than",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "id"},
				Op:    ">",
				Right: &ast.IntLitExpr{Value: 5},
			},
			[]string{"id"},
			map[string]Value{"id": int64(5)},
			map[string]bool{"id": false},
			map[string]Value{},
			map[string]bool{},
		},
		{
			"greater or equal",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "id"},
				Op:    ">=",
				Right: &ast.IntLitExpr{Value: 5},
			},
			[]string{"id"},
			map[string]Value{"id": int64(5)},
			map[string]bool{"id": true},
			map[string]Value{},
			map[string]bool{},
		},
		{
			"less than",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "id"},
				Op:    "<",
				Right: &ast.IntLitExpr{Value: 10},
			},
			[]string{"id"},
			map[string]Value{},
			map[string]bool{},
			map[string]Value{"id": int64(10)},
			map[string]bool{"id": false},
		},
		{
			"less or equal",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "id"},
				Op:    "<=",
				Right: &ast.IntLitExpr{Value: 10},
			},
			[]string{"id"},
			map[string]Value{},
			map[string]bool{},
			map[string]Value{"id": int64(10)},
			map[string]bool{"id": true},
		},
		{
			"BETWEEN",
			&ast.BetweenExpr{
				Left: &ast.IdentExpr{Name: "age"},
				Low:  &ast.IntLitExpr{Value: 18},
				High: &ast.IntLitExpr{Value: 65},
				Not:  false,
			},
			[]string{"age"},
			map[string]Value{"age": int64(18)},
			map[string]bool{"age": true},
			map[string]Value{"age": int64(65)},
			map[string]bool{"age": true},
		},
		{
			"NOT BETWEEN skipped",
			&ast.BetweenExpr{
				Left: &ast.IdentExpr{Name: "age"},
				Low:  &ast.IntLitExpr{Value: 18},
				High: &ast.IntLitExpr{Value: 65},
				Not:  true,
			},
			[]string{},
			map[string]Value{},
			map[string]bool{},
			map[string]Value{},
			map[string]bool{},
		},
		{
			"LIKE with prefix",
			&ast.LikeExpr{
				Left:    &ast.IdentExpr{Name: "name"},
				Pattern: &ast.StringLitExpr{Value: "abc%"},
				Not:     false,
			},
			[]string{"name"},
			map[string]Value{"name": "abc"},
			map[string]bool{"name": true},
			map[string]Value{"name": "abd"},
			map[string]bool{"name": false},
		},
		{
			"AND chain merge",
			&ast.LogicalExpr{
				Left: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "id"},
					Op:    ">",
					Right: &ast.IntLitExpr{Value: 5},
				},
				Op: "AND",
				Right: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "id"},
					Op:    "<",
					Right: &ast.IntLitExpr{Value: 10},
				},
			},
			[]string{"id"},
			map[string]Value{"id": int64(5)},
			map[string]bool{"id": false},
			map[string]Value{"id": int64(10)},
			map[string]bool{"id": false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractRangeConditions(tt.expr)
			require.Len(t, got, len(tt.wantCols), "extractRangeConditions() returned unexpected number of entries")
			for _, col := range tt.wantCols {
				rc, ok := got[col]
				if !assert.True(t, ok, "missing column %q", col) {
					continue
				}
				if wantFrom, ok := tt.checkFromVal[col]; ok {
					require.NotNil(t, rc.fromVal, "col %q: fromVal is nil, want %v", col, wantFrom)
					assert.Equal(t, wantFrom, *rc.fromVal, "col %q: fromVal", col)
				}
				if wantIncl, ok := tt.checkFromIncl[col]; ok {
					assert.Equal(t, wantIncl, rc.fromInclusive, "col %q: fromInclusive", col)
				}
				if wantTo, ok := tt.checkToVal[col]; ok {
					require.NotNil(t, rc.toVal, "col %q: toVal is nil, want %v", col, wantTo)
					assert.Equal(t, wantTo, *rc.toVal, "col %q: toVal", col)
				}
				if wantIncl, ok := tt.checkToIncl[col]; ok {
					assert.Equal(t, wantIncl, rc.toInclusive, "col %q: toInclusive", col)
				}
			}
		})
	}
}

// --- collectColumnRefs / collectNeededColumns / isIndexCovering tests ---

func TestCollectColumnRefs(t *testing.T) {
	info := &TableInfo{
		Name: "t",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "val", DataType: "INT", Index: 1},
			{Name: "name", DataType: "TEXT", Index: 2},
			{Name: "category", DataType: "INT", Index: 3},
		},
		PrimaryKeyCol: 0,
	}

	tests := []struct {
		name string
		expr ast.Expr
		want map[int]bool
	}{
		{
			"ident expr",
			&ast.IdentExpr{Name: "val"},
			map[int]bool{1: true},
		},
		{
			"star expr",
			&ast.StarExpr{},
			map[int]bool{0: true, 1: true, 2: true, 3: true},
		},
		{
			"alias expr",
			&ast.AliasExpr{Expr: &ast.IdentExpr{Name: "name"}, Alias: "n"},
			map[int]bool{2: true},
		},
		{
			"binary expr",
			&ast.BinaryExpr{
				Left:  &ast.IdentExpr{Name: "val"},
				Op:    "=",
				Right: &ast.IntLitExpr{Value: 42},
			},
			map[int]bool{1: true},
		},
		{
			"logical expr",
			&ast.LogicalExpr{
				Left: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "val"},
					Op:    ">",
					Right: &ast.IntLitExpr{Value: 5},
				},
				Op: "AND",
				Right: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Name: "category"},
					Op:    "=",
					Right: &ast.IntLitExpr{Value: 1},
				},
			},
			map[int]bool{1: true, 3: true},
		},
		{
			"arithmetic expr",
			&ast.ArithmeticExpr{
				Left:  &ast.IdentExpr{Name: "val"},
				Op:    "+",
				Right: &ast.IntLitExpr{Value: 1},
			},
			map[int]bool{1: true},
		},
		{
			"not expr",
			&ast.NotExpr{Expr: &ast.IdentExpr{Name: "val"}},
			map[int]bool{1: true},
		},
		{
			"is null expr",
			&ast.IsNullExpr{Expr: &ast.IdentExpr{Name: "name"}},
			map[int]bool{2: true},
		},
		{
			"in expr",
			&ast.InExpr{
				Left:   &ast.IdentExpr{Name: "category"},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}},
			},
			map[int]bool{3: true},
		},
		{
			"between expr",
			&ast.BetweenExpr{
				Left: &ast.IdentExpr{Name: "val"},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			map[int]bool{1: true},
		},
		{
			"like expr",
			&ast.LikeExpr{
				Left:    &ast.IdentExpr{Name: "name"},
				Pattern: &ast.StringLitExpr{Value: "abc%"},
			},
			map[int]bool{2: true},
		},
		{
			"case expr",
			&ast.CaseExpr{
				Operand: &ast.IdentExpr{Name: "category"},
				Whens: []ast.CaseWhen{
					{When: &ast.IntLitExpr{Value: 1}, Then: &ast.IdentExpr{Name: "name"}},
				},
				Else: &ast.IdentExpr{Name: "val"},
			},
			map[int]bool{3: true, 2: true, 1: true},
		},
		{
			"call expr",
			&ast.CallExpr{
				Name: "ABS",
				Args: []ast.Expr{&ast.IdentExpr{Name: "val"}},
			},
			map[int]bool{1: true},
		},
		{
			"cast expr",
			&ast.CastExpr{
				Expr:       &ast.IdentExpr{Name: "val"},
				TargetType: "TEXT",
			},
			map[int]bool{1: true},
		},
		{
			"int literal (no columns)",
			&ast.IntLitExpr{Value: 42},
			map[int]bool{},
		},
		{
			"string literal (no columns)",
			&ast.StringLitExpr{Value: "hello"},
			map[int]bool{},
		},
		{
			"null literal (no columns)",
			&ast.NullLitExpr{},
			map[int]bool{},
		},
		{
			"bool literal (no columns)",
			&ast.BoolLitExpr{Value: true},
			map[int]bool{},
		},
		{
			"nil expr (no columns)",
			nil,
			map[int]bool{},
		},
		{
			"unknown column (ignored)",
			&ast.IdentExpr{Name: "nonexistent"},
			map[int]bool{},
		},
		{
			"case insensitive column",
			&ast.IdentExpr{Name: "VAL"},
			map[int]bool{1: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := make(map[int]bool)
			collectColumnRefs(tt.expr, info, got)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCollectNeededColumns(t *testing.T) {
	info := &TableInfo{
		Name: "t",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "val", DataType: "INT", Index: 1},
			{Name: "name", DataType: "TEXT", Index: 2},
		},
		PrimaryKeyCol: 0,
	}

	t.Run("select and where", func(t *testing.T) {
		columns := []ast.Expr{&ast.IdentExpr{Name: "val"}}
		where := &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Name: "name"},
			Op:    "=",
			Right: &ast.StringLitExpr{Value: "test"},
		}
		got := collectNeededColumns(columns, where, nil, info)
		assert.Equal(t, map[int]bool{1: true, 2: true}, got)
	})

	t.Run("select with order by", func(t *testing.T) {
		columns := []ast.Expr{&ast.IdentExpr{Name: "val"}}
		orderBy := []ast.OrderByClause{{Expr: &ast.IdentExpr{Name: "id"}}}
		got := collectNeededColumns(columns, nil, orderBy, info)
		assert.Equal(t, map[int]bool{1: true, 0: true}, got)
	})
}

func TestIsIndexCovering(t *testing.T) {
	t.Run("single column covering", func(t *testing.T) {
		exec := NewExecutor(NewDatabase("test"))
		run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, name TEXT)")
		run(t, exec, "CREATE INDEX idx_val ON t(val)")

		db, _ := exec.resolveDatabase("")
		info, _ := db.catalog.GetTable("t")
		idx := db.storage.LookupSingleColumnIndex("t", 1) // val
		assert.NotNil(t, idx)

		// val + pk is covering
		assert.True(t, isIndexCovering(idx, map[int]bool{1: true}, info.PrimaryKeyCol))
		// val + pk + id is covering (pk = id)
		assert.True(t, isIndexCovering(idx, map[int]bool{0: true, 1: true}, info.PrimaryKeyCol))
		// val + name is not covering
		assert.False(t, isIndexCovering(idx, map[int]bool{1: true, 2: true}, info.PrimaryKeyCol))
		// all columns is not covering
		assert.False(t, isIndexCovering(idx, map[int]bool{0: true, 1: true, 2: true}, info.PrimaryKeyCol))
	})
}

func TestPrimaryKeyLookup(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO items VALUES (%d, 'item%d')", i, i))
	}

	// WHERE id = 5 should return exactly 1 row via PK lookup
	result := run(t, exec, "SELECT * FROM items WHERE id = 5")
	require.Len(t, result.Rows, 1, "expected 1 row for PK lookup")
	assert.Equal(t, int64(5), result.Rows[0][0], "expected id=5")
	assert.Equal(t, "item5", result.Rows[0][1], "expected name='item5'")
}

func TestPrimaryKeyLookupNotFound(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO items VALUES (%d, 'item%d')", i, i))
	}

	// WHERE id = 999 should return 0 rows
	result := run(t, exec, "SELECT * FROM items WHERE id = 999")
	require.Len(t, result.Rows, 0, "expected 0 rows for non-existent PK")
}
