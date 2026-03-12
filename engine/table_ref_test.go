package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/ast"
)

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
