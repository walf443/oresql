package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/ast"
)

func TestTableEvaluator_ResolveColumn(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(nil, info)

	// Resolve unqualified column
	col, err := eval.ResolveColumn("", "id")
	require.NoError(t, err)
	assert.Equal(t, 0, col.Index)

	// Resolve qualified column
	col, err = eval.ResolveColumn("users", "name")
	require.NoError(t, err)
	assert.Equal(t, 1, col.Index)

	// Error on wrong table
	_, err = eval.ResolveColumn("orders", "id")
	assert.Error(t, err, "expected error for wrong table")

	// Error on unknown column
	_, err = eval.ResolveColumn("", "missing")
	assert.Error(t, err, "expected error for unknown column")
}

func TestTableEvaluator_Eval(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(nil, info)
	row := Row{int64(1), "Alice"}

	tests := []struct {
		name string
		expr ast.Expr
		want Value
	}{
		{
			name: "ident",
			expr: &ast.IdentExpr{Name: "id"},
			want: int64(1),
		},
		{
			name: "int literal",
			expr: &ast.IntLitExpr{Value: int64(42)},
			want: int64(42),
		},
		{
			name: "string literal",
			expr: &ast.StringLitExpr{Value: "hello"},
			want: "hello",
		},
		{
			name: "binary comparison",
			expr: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "id"}, Op: "=", Right: &ast.IntLitExpr{Value: int64(1)}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eval.Eval(tt.expr, row)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJoinEvaluator_Eval(t *testing.T) {
	jc := newJoinContext([]struct {
		info  *TableInfo
		alias string
	}{
		{
			info: &TableInfo{
				Name: "users",
				Columns: []ColumnInfo{
					{Name: "id", DataType: "INT", Index: 0},
					{Name: "name", DataType: "TEXT", Index: 1},
				},
			},
		},
		{
			info: &TableInfo{
				Name: "orders",
				Columns: []ColumnInfo{
					{Name: "id", DataType: "INT", Index: 0},
					{Name: "user_id", DataType: "INT", Index: 1},
				},
			},
		},
	}, nil)
	eval := newJoinEvaluator(nil, jc)
	row := Row{int64(1), "Alice", int64(10), int64(1)}

	// Qualified access
	val, err := eval.Eval(&ast.IdentExpr{Table: "users", Name: "name"}, row)
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)

	// Qualified access to second table
	val, err = eval.Eval(&ast.IdentExpr{Table: "orders", Name: "id"}, row)
	require.NoError(t, err)
	assert.Equal(t, int64(10), val)

	// Unqualified unambiguous access
	val, err = eval.Eval(&ast.IdentExpr{Name: "user_id"}, row)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)

	// Ambiguous column
	_, err = eval.Eval(&ast.IdentExpr{Name: "id"}, row)
	assert.Error(t, err, "expected error for ambiguous column")
}

func TestGroupEvaluator_Eval(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	groupRows := []Row{
		{int64(1), "Alice"},
		{int64(2), "Alice"},
		{int64(3), "Alice"},
	}
	eval := newGroupEvaluator(nil, info, groupRows)
	row := groupRows[0]

	// Regular column access
	val, err := eval.Eval(&ast.IdentExpr{Name: "name"}, row)
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)

	// COUNT(*)
	val, err = eval.Eval(&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}}, row)
	require.NoError(t, err)
	assert.Equal(t, int64(3), val)

	// SUM(id)
	val, err = eval.Eval(&ast.CallExpr{Name: "SUM", Args: []ast.Expr{&ast.IdentExpr{Name: "id"}}}, row)
	require.NoError(t, err)
	assert.Equal(t, int64(6), val)
}

func TestResultEvaluator_Eval(t *testing.T) {
	selectCols := []ast.Expr{
		&ast.AliasExpr{Expr: &ast.IdentExpr{Name: "name"}, Alias: "user_name"},
		&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}},
	}
	colNames := []string{"user_name", "COUNT(*)"}
	eval := newResultEvaluator(nil, selectCols, colNames)
	row := Row{"Alice", int64(3)}

	// Match by alias
	val, err := eval.Eval(&ast.IdentExpr{Name: "user_name"}, row)
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)

	// Match by original column name
	val, err = eval.Eval(&ast.IdentExpr{Name: "name"}, row)
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)

	// Match aggregate by function name
	val, err = eval.Eval(&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}}, row)
	require.NoError(t, err)
	assert.Equal(t, int64(3), val)
}
