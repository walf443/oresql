package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
