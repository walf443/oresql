package optimize

import (
	"reflect"
	"testing"

	"github.com/walf443/oresql/ast"
)

func TestExpr_BinaryExpr(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "1 = 1 folds to true",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "1 = 2 folds to false",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "2 != 2 folds to false",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 2}, Op: "!=", Right: &ast.IntLitExpr{Value: 2}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "2 != 3 folds to true",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 2}, Op: "!=", Right: &ast.IntLitExpr{Value: 3}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "\"a\" < \"b\" folds to true",
			expr: &ast.BinaryExpr{Left: &ast.StringLitExpr{Value: "a"}, Op: "<", Right: &ast.StringLitExpr{Value: "b"}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "\"b\" < \"a\" folds to false",
			expr: &ast.BinaryExpr{Left: &ast.StringLitExpr{Value: "b"}, Op: "<", Right: &ast.StringLitExpr{Value: "a"}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "3 > 1 folds to true",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 3}, Op: ">", Right: &ast.IntLitExpr{Value: 1}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "1 >= 1 folds to true",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: ">=", Right: &ast.IntLitExpr{Value: 1}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "1 <= 2 folds to true",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "<=", Right: &ast.IntLitExpr{Value: 2}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "non-constant left stays unchanged",
			expr: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "x"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
			want: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "x"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
		},
		{
			name: "non-constant right stays unchanged",
			expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IdentExpr{Name: "y"}},
			want: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IdentExpr{Name: "y"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_ArithmeticExpr(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "2 + 3 folds to 5",
			expr: &ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 2}, Op: "+", Right: &ast.IntLitExpr{Value: 3}},
			want: &ast.IntLitExpr{Value: 5},
		},
		{
			name: "10 - 4 folds to 6",
			expr: &ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 10}, Op: "-", Right: &ast.IntLitExpr{Value: 4}},
			want: &ast.IntLitExpr{Value: 6},
		},
		{
			name: "3 * 4 folds to 12",
			expr: &ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 3}, Op: "*", Right: &ast.IntLitExpr{Value: 4}},
			want: &ast.IntLitExpr{Value: 12},
		},
		{
			name: "2.0 * 3.0 folds to 6.0",
			expr: &ast.ArithmeticExpr{Left: &ast.FloatLitExpr{Value: 2.0}, Op: "*", Right: &ast.FloatLitExpr{Value: 3.0}},
			want: &ast.FloatLitExpr{Value: 6.0},
		},
		{
			name: "1.5 + 2.5 folds to 4.0",
			expr: &ast.ArithmeticExpr{Left: &ast.FloatLitExpr{Value: 1.5}, Op: "+", Right: &ast.FloatLitExpr{Value: 2.5}},
			want: &ast.FloatLitExpr{Value: 4.0},
		},
		{
			name: "non-constant arithmetic stays unchanged",
			expr: &ast.ArithmeticExpr{Left: &ast.IdentExpr{Name: "x"}, Op: "+", Right: &ast.IntLitExpr{Value: 1}},
			want: &ast.ArithmeticExpr{Left: &ast.IdentExpr{Name: "x"}, Op: "+", Right: &ast.IntLitExpr{Value: 1}},
		},
		{
			name: "nested constant (1 + 2) * 3 folds to 9",
			expr: &ast.ArithmeticExpr{
				Left: &ast.ArithmeticExpr{
					Left: &ast.IntLitExpr{Value: 1}, Op: "+", Right: &ast.IntLitExpr{Value: 2},
				},
				Op:    "*",
				Right: &ast.IntLitExpr{Value: 3},
			},
			want: &ast.IntLitExpr{Value: 9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_LogicalExpr(t *testing.T) {
	identX := &ast.IdentExpr{Name: "x"}
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "true AND false folds to false",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: true}, Op: "AND", Right: &ast.BoolLitExpr{Value: false}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "true AND true folds to true",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: true}, Op: "AND", Right: &ast.BoolLitExpr{Value: true}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "false AND true folds to false",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: false}, Op: "AND", Right: &ast.BoolLitExpr{Value: true}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "false OR true folds to true",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: false}, Op: "OR", Right: &ast.BoolLitExpr{Value: true}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "true OR false folds to true",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: true}, Op: "OR", Right: &ast.BoolLitExpr{Value: false}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "false OR false folds to false",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: false}, Op: "OR", Right: &ast.BoolLitExpr{Value: false}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "true AND x simplifies to x",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: true}, Op: "AND", Right: identX},
			want: identX,
		},
		{
			name: "x AND true simplifies to x",
			expr: &ast.LogicalExpr{Left: identX, Op: "AND", Right: &ast.BoolLitExpr{Value: true}},
			want: identX,
		},
		{
			name: "false AND x short-circuits to false",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: false}, Op: "AND", Right: identX},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "x AND false short-circuits to false",
			expr: &ast.LogicalExpr{Left: identX, Op: "AND", Right: &ast.BoolLitExpr{Value: false}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "true OR x short-circuits to true",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: true}, Op: "OR", Right: identX},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "x OR true short-circuits to true",
			expr: &ast.LogicalExpr{Left: identX, Op: "OR", Right: &ast.BoolLitExpr{Value: true}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "false OR x simplifies to x",
			expr: &ast.LogicalExpr{Left: &ast.BoolLitExpr{Value: false}, Op: "OR", Right: identX},
			want: identX,
		},
		{
			name: "x OR false simplifies to x",
			expr: &ast.LogicalExpr{Left: identX, Op: "OR", Right: &ast.BoolLitExpr{Value: false}},
			want: identX,
		},
		{
			name: "non-constant logical stays unchanged",
			expr: &ast.LogicalExpr{Left: identX, Op: "AND", Right: &ast.IdentExpr{Name: "y"}},
			want: &ast.LogicalExpr{Left: identX, Op: "AND", Right: &ast.IdentExpr{Name: "y"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_NotExpr(t *testing.T) {
	identX := &ast.IdentExpr{Name: "x"}
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "NOT true folds to false",
			expr: &ast.NotExpr{Expr: &ast.BoolLitExpr{Value: true}},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "NOT false folds to true",
			expr: &ast.NotExpr{Expr: &ast.BoolLitExpr{Value: false}},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "NOT x stays unchanged",
			expr: &ast.NotExpr{Expr: identX},
			want: &ast.NotExpr{Expr: identX},
		},
		{
			name: "NOT (1 = 1) folds to false",
			expr: &ast.NotExpr{Expr: &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 1}}},
			want: &ast.BoolLitExpr{Value: false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_IsNullExpr(t *testing.T) {
	identX := &ast.IdentExpr{Name: "x"}
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "NULL IS NULL folds to true",
			expr: &ast.IsNullExpr{Expr: &ast.NullLitExpr{}, Not: false},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "1 IS NULL folds to false",
			expr: &ast.IsNullExpr{Expr: &ast.IntLitExpr{Value: 1}, Not: false},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "NULL IS NOT NULL folds to false",
			expr: &ast.IsNullExpr{Expr: &ast.NullLitExpr{}, Not: true},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "1 IS NOT NULL folds to true",
			expr: &ast.IsNullExpr{Expr: &ast.IntLitExpr{Value: 1}, Not: true},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "\"hello\" IS NULL folds to false",
			expr: &ast.IsNullExpr{Expr: &ast.StringLitExpr{Value: "hello"}, Not: false},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "x IS NULL stays unchanged",
			expr: &ast.IsNullExpr{Expr: identX, Not: false},
			want: &ast.IsNullExpr{Expr: identX, Not: false},
		},
		{
			name: "x IS NOT NULL stays unchanged",
			expr: &ast.IsNullExpr{Expr: identX, Not: true},
			want: &ast.IsNullExpr{Expr: identX, Not: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_InExpr(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "1 IN (1, 2, 3) folds to true",
			expr: &ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 1},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}, &ast.IntLitExpr{Value: 3}},
			},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "4 IN (1, 2, 3) folds to false",
			expr: &ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 4},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}, &ast.IntLitExpr{Value: 3}},
			},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "1 NOT IN (1, 2, 3) folds to false",
			expr: &ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 1},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}, &ast.IntLitExpr{Value: 3}},
				Not:    true,
			},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "4 NOT IN (1, 2, 3) folds to true",
			expr: &ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 4},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}, &ast.IntLitExpr{Value: 3}},
				Not:    true,
			},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "NULL IN (1, 2) folds to false",
			expr: &ast.InExpr{
				Left:   &ast.NullLitExpr{},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}},
			},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "x IN (1, 2) stays unchanged (non-constant left)",
			expr: &ast.InExpr{
				Left:   &ast.IdentExpr{Name: "x"},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}},
			},
			want: &ast.InExpr{
				Left:   &ast.IdentExpr{Name: "x"},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_BetweenExpr(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "5 BETWEEN 1 AND 10 folds to true",
			expr: &ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 5},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "15 BETWEEN 1 AND 10 folds to false",
			expr: &ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 15},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "5 NOT BETWEEN 1 AND 10 folds to false",
			expr: &ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 5},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
				Not:  true,
			},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "15 NOT BETWEEN 1 AND 10 folds to true",
			expr: &ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 15},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
				Not:  true,
			},
			want: &ast.BoolLitExpr{Value: true},
		},
		{
			name: "NULL BETWEEN 1 AND 10 folds to false",
			expr: &ast.BetweenExpr{
				Left: &ast.NullLitExpr{},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			want: &ast.BoolLitExpr{Value: false},
		},
		{
			name: "x BETWEEN 1 AND 10 stays unchanged",
			expr: &ast.BetweenExpr{
				Left: &ast.IdentExpr{Name: "x"},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			want: &ast.BetweenExpr{
				Left: &ast.IdentExpr{Name: "x"},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_CaseExpr(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want ast.Expr
	}{
		{
			name: "searched CASE with true WHEN returns THEN directly",
			expr: &ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.BoolLitExpr{Value: true}, Then: &ast.IntLitExpr{Value: 42}},
				},
				Else: &ast.IntLitExpr{Value: 0},
			},
			want: &ast.IntLitExpr{Value: 42},
		},
		{
			name: "searched CASE with all false WHENs returns ELSE",
			expr: &ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.BoolLitExpr{Value: false}, Then: &ast.IntLitExpr{Value: 1}},
				},
				Else: &ast.IntLitExpr{Value: 99},
			},
			want: &ast.IntLitExpr{Value: 99},
		},
		{
			name: "searched CASE with all false WHENs and no ELSE returns NULL",
			expr: &ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.BoolLitExpr{Value: false}, Then: &ast.IntLitExpr{Value: 1}},
				},
			},
			want: &ast.NullLitExpr{},
		},
		{
			name: "searched CASE with non-constant WHEN stays as CaseExpr",
			expr: &ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.IdentExpr{Name: "x"}, Then: &ast.IntLitExpr{Value: 1}},
				},
				Else: &ast.IntLitExpr{Value: 0},
			},
			want: &ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.IdentExpr{Name: "x"}, Then: &ast.IntLitExpr{Value: 1}},
				},
				Else: &ast.IntLitExpr{Value: 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpr_Nil(t *testing.T) {
	got := Expr(nil)
	if got != nil {
		t.Errorf("Expr(nil) = %#v, want nil", got)
	}
}

func TestExpr_LiteralPassthrough(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
	}{
		{"IntLitExpr", &ast.IntLitExpr{Value: 42}},
		{"FloatLitExpr", &ast.FloatLitExpr{Value: 3.14}},
		{"StringLitExpr", &ast.StringLitExpr{Value: "hello"}},
		{"BoolLitExpr", &ast.BoolLitExpr{Value: true}},
		{"NullLitExpr", &ast.NullLitExpr{}},
		{"IdentExpr", &ast.IdentExpr{Name: "col"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Expr(tt.expr)
			if !reflect.DeepEqual(got, tt.expr) {
				t.Errorf("Expr() = %#v, want %#v", got, tt.expr)
			}
		})
	}
}

func TestStatement_SelectStmt(t *testing.T) {
	t.Run("WHERE clause gets optimized", func(t *testing.T) {
		stmt := &ast.SelectStmt{
			TableName: "users",
			Columns:   []ast.Expr{&ast.StarExpr{}},
			Where:     &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
		}
		Statement(stmt)
		want := &ast.BoolLitExpr{Value: true}
		if !reflect.DeepEqual(stmt.Where, want) {
			t.Errorf("WHERE = %#v, want %#v", stmt.Where, want)
		}
	})

	t.Run("HAVING clause gets optimized", func(t *testing.T) {
		stmt := &ast.SelectStmt{
			TableName: "users",
			Columns:   []ast.Expr{&ast.StarExpr{}},
			Having:    &ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 2}, Op: "!=", Right: &ast.IntLitExpr{Value: 2}},
		}
		Statement(stmt)
		want := &ast.BoolLitExpr{Value: false}
		if !reflect.DeepEqual(stmt.Having, want) {
			t.Errorf("HAVING = %#v, want %#v", stmt.Having, want)
		}
	})

	t.Run("nil WHERE stays nil", func(t *testing.T) {
		stmt := &ast.SelectStmt{
			TableName: "users",
			Columns:   []ast.Expr{&ast.StarExpr{}},
		}
		Statement(stmt)
		if stmt.Where != nil {
			t.Errorf("WHERE = %#v, want nil", stmt.Where)
		}
	})
}

func TestStatement_UpdateStmt(t *testing.T) {
	t.Run("WHERE clause gets optimized", func(t *testing.T) {
		stmt := &ast.UpdateStmt{
			TableName: "users",
			Sets:      []ast.SetClause{{Column: "name", Value: &ast.StringLitExpr{Value: "Alice"}}},
			Where: &ast.LogicalExpr{
				Left:  &ast.BoolLitExpr{Value: true},
				Op:    "AND",
				Right: &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "id"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
			},
		}
		Statement(stmt)
		// true AND (id = 1) should simplify to (id = 1)
		want := &ast.BinaryExpr{Left: &ast.IdentExpr{Name: "id"}, Op: "=", Right: &ast.IntLitExpr{Value: 1}}
		if !reflect.DeepEqual(stmt.Where, want) {
			t.Errorf("WHERE = %#v, want %#v", stmt.Where, want)
		}
	})

	t.Run("nil WHERE stays nil", func(t *testing.T) {
		stmt := &ast.UpdateStmt{
			TableName: "users",
			Sets:      []ast.SetClause{{Column: "name", Value: &ast.StringLitExpr{Value: "Alice"}}},
		}
		Statement(stmt)
		if stmt.Where != nil {
			t.Errorf("WHERE = %#v, want nil", stmt.Where)
		}
	})
}

func TestStatement_DeleteStmt(t *testing.T) {
	t.Run("WHERE clause gets optimized", func(t *testing.T) {
		stmt := &ast.DeleteStmt{
			TableName: "users",
			Where:     &ast.NotExpr{Expr: &ast.BoolLitExpr{Value: false}},
		}
		Statement(stmt)
		want := &ast.BoolLitExpr{Value: true}
		if !reflect.DeepEqual(stmt.Where, want) {
			t.Errorf("WHERE = %#v, want %#v", stmt.Where, want)
		}
	})

	t.Run("nil WHERE stays nil", func(t *testing.T) {
		stmt := &ast.DeleteStmt{TableName: "users"}
		Statement(stmt)
		if stmt.Where != nil {
			t.Errorf("WHERE = %#v, want nil", stmt.Where)
		}
	})
}

func TestStatement_UnsupportedStmt(t *testing.T) {
	// Statement should not panic on unsupported statement types
	stmt := &ast.CreateTableStmt{TableName: "users"}
	Statement(stmt) // should be a no-op, no panic
}
