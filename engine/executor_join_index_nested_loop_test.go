package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/ast"
)

func TestFlattenAND(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want int // expected number of flattened expressions
	}{
		{
			name: "single expression",
			expr: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
			want: 1,
		},
		{
			name: "two ANDs",
			expr: &ast.LogicalExpr{
				Left:  &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
				Op:    "AND",
				Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "b"}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
			},
			want: 2,
		},
		{
			name: "nested AND",
			expr: &ast.LogicalExpr{
				Left: &ast.LogicalExpr{
					Left:  &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
					Op:    "AND",
					Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "b"}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
				},
				Op:    "AND",
				Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "c"}, Op: "=", Right: &ast.IntLitExpr{Value: 3}},
			},
			want: 3,
		},
		{
			name: "OR is not flattened",
			expr: &ast.LogicalExpr{
				Left:  &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
				Op:    "OR",
				Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "b"}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
			},
			want: 1,
		},
		{
			name: "nil expression",
			expr: nil,
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := flattenAND(tt.expr)
			assert.Len(t, got, tt.want, "flattenAND() returned expression count")
		})
	}
}

func TestCollectTableRefs(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want map[string]bool
	}{
		{
			name: "qualified ident",
			expr: &ast.IdentExpr{Table: "u", Name: "id"},
			want: map[string]bool{"u": true},
		},
		{
			name: "unqualified ident",
			expr: &ast.IdentExpr{Name: "id"},
			want: map[string]bool{},
		},
		{
			name: "binary expr with two tables",
			expr: &ast.BinaryExpr{
				Left:  &ast.IdentExpr{Table: "u", Name: "id"},
				Op:    "=",
				Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
			},
			want: map[string]bool{"u": true, "o": true},
		},
		{
			name: "logical expr",
			expr: &ast.LogicalExpr{
				Left:  &ast.BinaryExpr{Left: &ast.IdentExpr{Table: "u", Name: "id"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
				Op:    "AND",
				Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Table: "o", Name: "amount"}, Op: ">", Right: &ast.IntLitExpr{Value: 100}},
			},
			want: map[string]bool{"u": true, "o": true},
		},
		{
			name: "literal only",
			expr: &ast.IntLitExpr{Value: 42},
			want: map[string]bool{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectTableRefs(tt.expr)
			require.Len(t, got, len(tt.want), "collectTableRefs() result count")
			for k := range tt.want {
				assert.True(t, got[k], "collectTableRefs() missing key %q", k)
			}
		})
	}
}

func TestStripTableQualifier(t *testing.T) {
	tests := []struct {
		name      string
		expr      ast.Expr
		tableName string
		alias     string
		wantTable string // expected Table field after stripping
	}{
		{
			name:      "strip alias",
			expr:      &ast.IdentExpr{Table: "u", Name: "id"},
			tableName: "users",
			alias:     "u",
			wantTable: "",
		},
		{
			name:      "strip table name",
			expr:      &ast.IdentExpr{Table: "users", Name: "id"},
			tableName: "users",
			alias:     "",
			wantTable: "",
		},
		{
			name:      "no strip other table",
			expr:      &ast.IdentExpr{Table: "orders", Name: "id"},
			tableName: "users",
			alias:     "u",
			wantTable: "orders",
		},
		{
			name:      "unqualified stays unqualified",
			expr:      &ast.IdentExpr{Name: "id"},
			tableName: "users",
			alias:     "u",
			wantTable: "",
		},
		{
			name:      "case insensitive strip",
			expr:      &ast.IdentExpr{Table: "Users", Name: "id"},
			tableName: "users",
			alias:     "",
			wantTable: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripTableQualifier(tt.expr, tt.tableName, tt.alias)
			ident, ok := got.(*ast.IdentExpr)
			require.True(t, ok, "stripTableQualifier() returned %T, want *ast.IdentExpr", got)
			assert.Equal(t, tt.wantTable, ident.Table)
		})
	}
}

func TestStripTableQualifierBinaryExpr(t *testing.T) {
	// Test stripping in nested binary expression
	expr := &ast.BinaryExpr{
		Left:  &ast.IdentExpr{Table: "u", Name: "status"},
		Op:    "=",
		Right: &ast.StringLitExpr{Value: "active"},
	}
	got := stripTableQualifier(expr, "users", "u")
	binExpr, ok := got.(*ast.BinaryExpr)
	require.True(t, ok, "expected *ast.BinaryExpr, got %T", got)
	ident, ok := binExpr.Left.(*ast.IdentExpr)
	require.True(t, ok, "expected *ast.IdentExpr, got %T", binExpr.Left)
	assert.Equal(t, "", ident.Table, "expected Table to be stripped")
}

func TestCombineExprsAND(t *testing.T) {
	tests := []struct {
		name string
		in   []ast.Expr
		want bool // true if result is non-nil
	}{
		{"empty", nil, false},
		{"one", []ast.Expr{&ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}}}, true},
		{"two", []ast.Expr{
			&ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
			&ast.BinaryExpr{Left: &ast.IdentExpr{Name: "b"}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := combineExprsAND(tt.in)
			if tt.want {
				assert.NotNil(t, got, "combineExprsAND() should return non-nil")
			} else {
				assert.Nil(t, got, "combineExprsAND() should return nil")
			}
		})
	}

	// Verify two expressions become a LogicalExpr
	two := combineExprsAND([]ast.Expr{
		&ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
		&ast.BinaryExpr{Left: &ast.IdentExpr{Name: "b"}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
	})
	assert.IsType(t, &ast.LogicalExpr{}, two, "combineExprsAND(2 exprs) should return *ast.LogicalExpr")
}
