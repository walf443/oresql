package engine

import (
	"testing"

	"github.com/walf443/oresql/ast"
)

func TestHasAggregate(t *testing.T) {
	tests := []struct {
		name    string
		columns []ast.Expr
		want    bool
	}{
		{
			"CallExpr present",
			[]ast.Expr{&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}}},
			true,
		},
		{
			"IdentExpr only",
			[]ast.Expr{&ast.IdentExpr{Name: "id"}, &ast.IdentExpr{Name: "name"}},
			false,
		},
		{
			"AliasExpr wrapping CallExpr",
			[]ast.Expr{&ast.AliasExpr{Expr: &ast.CallExpr{Name: "SUM", Args: []ast.Expr{&ast.IdentExpr{Name: "amount"}}}, Alias: "total"}},
			true,
		},
		{
			"empty",
			[]ast.Expr{},
			false,
		},
		{
			"mixed with non-aggregate first",
			[]ast.Expr{&ast.IdentExpr{Name: "id"}, &ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasAggregate(tt.columns)
			if got != tt.want {
				t.Errorf("hasAggregate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatCallExpr(t *testing.T) {
	tests := []struct {
		name string
		call *ast.CallExpr
		want string
	}{
		{
			"COUNT(*)",
			&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}},
			"COUNT(*)",
		},
		{
			"SUM(col)",
			&ast.CallExpr{Name: "SUM", Args: []ast.Expr{&ast.IdentExpr{Name: "amount"}}},
			"SUM(amount)",
		},
		{
			"COUNT(1)",
			&ast.CallExpr{Name: "COUNT", Args: []ast.Expr{&ast.IntLitExpr{Value: 1}}},
			"COUNT(1)",
		},
		{
			"table.col argument",
			&ast.CallExpr{Name: "SUM", Args: []ast.Expr{&ast.IdentExpr{Table: "t", Name: "amount"}}},
			"SUM(t.amount)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatCallExpr(tt.call)
			if got != tt.want {
				t.Errorf("formatCallExpr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDedup(t *testing.T) {
	tests := []struct {
		name string
		rows []Row
		want int // expected number of rows after dedup
	}{
		{
			"with duplicates",
			[]Row{
				{int64(1), "alice"},
				{int64(2), "bob"},
				{int64(1), "alice"},
			},
			2,
		},
		{
			"no duplicates",
			[]Row{
				{int64(1), "alice"},
				{int64(2), "bob"},
			},
			2,
		},
		{
			"empty",
			[]Row{},
			0,
		},
		{
			"with NULL values",
			[]Row{
				{int64(1), nil},
				{int64(1), nil},
				{int64(2), nil},
			},
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dedup(tt.rows)
			if len(got) != tt.want {
				t.Errorf("dedup() returned %d rows, want %d", len(got), tt.want)
			}
		})
	}
}
