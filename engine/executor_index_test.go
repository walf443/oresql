package engine

import (
	"fmt"
	"testing"

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
			if len(got) != len(tt.want) {
				t.Errorf("extractEqualityConditions() returned %d entries, want %d", len(got), len(tt.want))
				return
			}
			for k, wantVal := range tt.want {
				gotVal, ok := got[k]
				if !ok {
					t.Errorf("missing key %q", k)
					continue
				}
				if gotVal != wantVal {
					t.Errorf("key %q = %v, want %v", k, gotVal, wantVal)
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
			if len(got) != len(tt.want) {
				t.Errorf("extractInConditions() returned %d entries, want %d", len(got), len(tt.want))
				return
			}
			for k, wantVals := range tt.want {
				gotVals, ok := got[k]
				if !ok {
					t.Errorf("missing key %q", k)
					continue
				}
				if len(gotVals) != len(wantVals) {
					t.Errorf("key %q: got %d values, want %d", k, len(gotVals), len(wantVals))
					continue
				}
				for i, wv := range wantVals {
					if gotVals[i] != wv {
						t.Errorf("key %q[%d] = %v, want %v", k, i, gotVals[i], wv)
					}
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
			if len(got) != len(tt.want) {
				t.Errorf("dedupKeys() returned %d keys, want %d", len(got), len(tt.want))
				return
			}
			for i, w := range tt.want {
				if got[i] != w {
					t.Errorf("dedupKeys()[%d] = %d, want %d", i, got[i], w)
				}
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
			if len(got) != len(tt.wantCols) {
				t.Errorf("extractRangeConditions() returned %d entries, want %d", len(got), len(tt.wantCols))
				return
			}
			for _, col := range tt.wantCols {
				rc, ok := got[col]
				if !ok {
					t.Errorf("missing column %q", col)
					continue
				}
				if wantFrom, ok := tt.checkFromVal[col]; ok {
					if rc.fromVal == nil {
						t.Errorf("col %q: fromVal is nil, want %v", col, wantFrom)
					} else if *rc.fromVal != wantFrom {
						t.Errorf("col %q: fromVal = %v, want %v", col, *rc.fromVal, wantFrom)
					}
				}
				if wantIncl, ok := tt.checkFromIncl[col]; ok {
					if rc.fromInclusive != wantIncl {
						t.Errorf("col %q: fromInclusive = %v, want %v", col, rc.fromInclusive, wantIncl)
					}
				}
				if wantTo, ok := tt.checkToVal[col]; ok {
					if rc.toVal == nil {
						t.Errorf("col %q: toVal is nil, want %v", col, wantTo)
					} else if *rc.toVal != wantTo {
						t.Errorf("col %q: toVal = %v, want %v", col, *rc.toVal, wantTo)
					}
				}
				if wantIncl, ok := tt.checkToIncl[col]; ok {
					if rc.toInclusive != wantIncl {
						t.Errorf("col %q: toInclusive = %v, want %v", col, rc.toInclusive, wantIncl)
					}
				}
			}
		})
	}
}

func TestPrimaryKeyLookup(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO items VALUES (%d, 'item%d')", i, i))
	}

	// WHERE id = 5 should return exactly 1 row via PK lookup
	result := run(t, exec, "SELECT * FROM items WHERE id = 5")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(5) {
		t.Errorf("expected id=5, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "item5" {
		t.Errorf("expected name='item5', got %v", result.Rows[0][1])
	}
}

func TestPrimaryKeyLookupNotFound(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	for i := 1; i <= 10; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO items VALUES (%d, 'item%d')", i, i))
	}

	// WHERE id = 999 should return 0 rows
	result := run(t, exec, "SELECT * FROM items WHERE id = 999")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}
