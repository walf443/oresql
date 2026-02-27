package ast

import "testing"

func TestFormatSQL(t *testing.T) {
	tests := []struct {
		name string
		expr Expr
		want string
	}{
		// Literals
		{name: "int", expr: &IntLitExpr{Value: 42}, want: "42"},
		{name: "negative int", expr: &IntLitExpr{Value: -1}, want: "-1"},
		{name: "float", expr: &FloatLitExpr{Value: 3.14}, want: "3.14"},
		{name: "string", expr: &StringLitExpr{Value: "hello"}, want: "'hello'"},
		{name: "null", expr: &NullLitExpr{}, want: "NULL"},
		{name: "true", expr: &BoolLitExpr{Value: true}, want: "TRUE"},
		{name: "false", expr: &BoolLitExpr{Value: false}, want: "FALSE"},

		// Ident
		{name: "ident", expr: &IdentExpr{Name: "col"}, want: "col"},
		{name: "qualified ident", expr: &IdentExpr{Table: "t", Name: "col"}, want: "t.col"},

		// Star
		{name: "star", expr: &StarExpr{}, want: "*"},

		// Alias
		{name: "alias", expr: &AliasExpr{Expr: &IdentExpr{Name: "id"}, Alias: "user_id"}, want: "id AS user_id"},

		// Arithmetic
		{name: "arithmetic add", expr: &ArithmeticExpr{Left: &IntLitExpr{Value: 1}, Op: "+", Right: &IntLitExpr{Value: 2}}, want: "(1 + 2)"},
		{name: "arithmetic mul", expr: &ArithmeticExpr{Left: &IdentExpr{Name: "a"}, Op: "*", Right: &IntLitExpr{Value: 3}}, want: "(a * 3)"},

		// Binary
		{name: "binary eq", expr: &BinaryExpr{Left: &IdentExpr{Name: "id"}, Op: "=", Right: &IntLitExpr{Value: 5}}, want: "(id = 5)"},
		{name: "binary gt", expr: &BinaryExpr{Left: &IdentExpr{Name: "id"}, Op: ">", Right: &IntLitExpr{Value: 5}}, want: "(id > 5)"},

		// Logical
		{name: "logical and", expr: &LogicalExpr{Left: &IdentExpr{Name: "a"}, Op: "AND", Right: &IdentExpr{Name: "b"}}, want: "(a AND b)"},
		{name: "logical or", expr: &LogicalExpr{Left: &IdentExpr{Name: "a"}, Op: "OR", Right: &IdentExpr{Name: "b"}}, want: "(a OR b)"},

		// Not
		{name: "not", expr: &NotExpr{Expr: &IdentExpr{Name: "flag"}}, want: "NOT flag"},

		// IsNull
		{name: "is null", expr: &IsNullExpr{Expr: &IdentExpr{Name: "col"}, Not: false}, want: "col IS NULL"},
		{name: "is not null", expr: &IsNullExpr{Expr: &IdentExpr{Name: "col"}, Not: true}, want: "col IS NOT NULL"},

		// In
		{
			name: "in values",
			expr: &InExpr{Left: &IdentExpr{Name: "id"}, Values: []Expr{&IntLitExpr{Value: 1}, &IntLitExpr{Value: 2}}},
			want: "id IN (1, 2)",
		},
		{
			name: "not in values",
			expr: &InExpr{Left: &IdentExpr{Name: "id"}, Not: true, Values: []Expr{&IntLitExpr{Value: 1}}},
			want: "id NOT IN (1)",
		},
		{
			name: "in subquery",
			expr: &InExpr{Left: &IdentExpr{Name: "id"}, Subquery: &SelectStmt{}},
			want: "id IN (SELECT ...)",
		},

		// Between
		{
			name: "between",
			expr: &BetweenExpr{Left: &IdentExpr{Name: "val"}, Low: &IntLitExpr{Value: 1}, High: &IntLitExpr{Value: 10}},
			want: "val BETWEEN 1 AND 10",
		},
		{
			name: "not between",
			expr: &BetweenExpr{Left: &IdentExpr{Name: "val"}, Low: &IntLitExpr{Value: 1}, High: &IntLitExpr{Value: 10}, Not: true},
			want: "val NOT BETWEEN 1 AND 10",
		},

		// Like
		{
			name: "like",
			expr: &LikeExpr{Left: &IdentExpr{Name: "name"}, Pattern: &StringLitExpr{Value: "%foo%"}},
			want: "name LIKE '%foo%'",
		},
		{
			name: "not like",
			expr: &LikeExpr{Left: &IdentExpr{Name: "name"}, Pattern: &StringLitExpr{Value: "%bar"}, Not: true},
			want: "name NOT LIKE '%bar'",
		},

		// Call
		{
			name: "count star",
			expr: &CallExpr{Name: "COUNT", Args: []Expr{&StarExpr{}}},
			want: "COUNT(*)",
		},
		{
			name: "sum col",
			expr: &CallExpr{Name: "SUM", Args: []Expr{&IdentExpr{Name: "amount"}}},
			want: "SUM(amount)",
		},
		{
			name: "coalesce",
			expr: &CallExpr{Name: "COALESCE", Args: []Expr{&IdentExpr{Name: "a"}, &IntLitExpr{Value: 0}}},
			want: "COALESCE(a, 0)",
		},

		// Cast
		{
			name: "cast",
			expr: &CastExpr{Expr: &IdentExpr{Name: "val"}, TargetType: "INT"},
			want: "CAST(val AS INT)",
		},

		// Case (searched)
		{
			name: "case searched",
			expr: &CaseExpr{
				Whens: []CaseWhen{
					{When: &BinaryExpr{Left: &IdentExpr{Name: "x"}, Op: ">", Right: &IntLitExpr{Value: 0}}, Then: &StringLitExpr{Value: "pos"}},
				},
				Else: &StringLitExpr{Value: "neg"},
			},
			want: "CASE WHEN (x > 0) THEN 'pos' ELSE 'neg' END",
		},
		// Case (simple)
		{
			name: "case simple",
			expr: &CaseExpr{
				Operand: &IdentExpr{Name: "status"},
				Whens: []CaseWhen{
					{When: &IntLitExpr{Value: 1}, Then: &StringLitExpr{Value: "active"}},
					{When: &IntLitExpr{Value: 0}, Then: &StringLitExpr{Value: "inactive"}},
				},
			},
			want: "CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END",
		},

		// Exists
		{name: "exists", expr: &ExistsExpr{Subquery: &SelectStmt{}}, want: "EXISTS (SELECT ...)"},
		{name: "not exists", expr: &ExistsExpr{Subquery: &SelectStmt{}, Not: true}, want: "NOT EXISTS (SELECT ...)"},

		// Window
		{
			name: "window row_number",
			expr: &WindowExpr{
				Name:    "ROW_NUMBER",
				OrderBy: []OrderByClause{{Expr: &IdentExpr{Name: "id"}}},
			},
			want: "ROW_NUMBER() OVER (ORDER BY id)",
		},
		{
			name: "window sum with partition",
			expr: &WindowExpr{
				Name:        "SUM",
				Args:        []Expr{&IdentExpr{Name: "amount"}},
				PartitionBy: []Expr{&IdentExpr{Name: "dept"}},
				OrderBy:     []OrderByClause{{Expr: &IdentExpr{Name: "id"}, Desc: true}},
			},
			want: "SUM(amount) OVER (PARTITION BY dept ORDER BY id DESC)",
		},
		{
			name: "window with named window",
			expr: &WindowExpr{
				Name:       "RANK",
				WindowName: "w",
			},
			want: "RANK() OVER w",
		},

		// Scalar subquery
		{name: "scalar subquery", expr: &ScalarExpr{Subquery: &SelectStmt{}}, want: "(SELECT ...)"},

		// Nil
		{name: "nil", expr: nil, want: ""},

		// Nested expression
		{
			name: "nested logical and binary",
			expr: &LogicalExpr{
				Left:  &BinaryExpr{Left: &IdentExpr{Name: "a"}, Op: ">", Right: &IntLitExpr{Value: 1}},
				Op:    "AND",
				Right: &BinaryExpr{Left: &IdentExpr{Name: "b"}, Op: "<", Right: &IntLitExpr{Value: 10}},
			},
			want: "((a > 1) AND (b < 10))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatSQL(tt.expr)
			if got != tt.want {
				t.Fatalf("FormatSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}
