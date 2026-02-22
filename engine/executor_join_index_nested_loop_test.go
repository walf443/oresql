package engine

import (
	"testing"

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
			if len(got) != tt.want {
				t.Errorf("flattenAND() returned %d exprs, want %d", len(got), tt.want)
			}
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
			if len(got) != len(tt.want) {
				t.Errorf("collectTableRefs() = %v, want %v", got, tt.want)
				return
			}
			for k := range tt.want {
				if !got[k] {
					t.Errorf("collectTableRefs() missing key %q", k)
				}
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
			if !ok {
				t.Fatalf("stripTableQualifier() returned %T, want *ast.IdentExpr", got)
			}
			if ident.Table != tt.wantTable {
				t.Errorf("stripTableQualifier().Table = %q, want %q", ident.Table, tt.wantTable)
			}
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
	if !ok {
		t.Fatalf("expected *ast.BinaryExpr, got %T", got)
	}
	ident, ok := binExpr.Left.(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected *ast.IdentExpr, got %T", binExpr.Left)
	}
	if ident.Table != "" {
		t.Errorf("expected Table to be stripped, got %q", ident.Table)
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
			if tt.want && got == nil {
				t.Errorf("combineExprsAND() = nil, want non-nil")
			}
			if !tt.want && got != nil {
				t.Errorf("combineExprsAND() = %v, want nil", got)
			}
		})
	}

	// Verify two expressions become a LogicalExpr
	two := combineExprsAND([]ast.Expr{
		&ast.BinaryExpr{Left: &ast.IdentExpr{Name: "a"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
		&ast.BinaryExpr{Left: &ast.IdentExpr{Name: "b"}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
	})
	if _, ok := two.(*ast.LogicalExpr); !ok {
		t.Errorf("combineExprsAND(2 exprs) = %T, want *ast.LogicalExpr", two)
	}
}
