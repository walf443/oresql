package eval

import (
	"fmt"
	"testing"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/storage"
)

// mockEvaluator implements ExprEvaluator for testing.
type mockEvaluator struct {
	columns        []storage.ColumnInfo
	subqueryRunner SubqueryRunner
}

func (m *mockEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	return Generic(expr, row, m)
}

func (m *mockEvaluator) ResolveColumn(tableName, colName string) (*storage.ColumnInfo, error) {
	for i := range m.columns {
		if m.columns[i].Name == colName {
			return &m.columns[i], nil
		}
	}
	return nil, fmt.Errorf("column %q not found", colName)
}

func (m *mockEvaluator) ColumnList() []storage.ColumnInfo {
	return m.columns
}

func (m *mockEvaluator) GetSubqueryRunner() SubqueryRunner {
	return m.subqueryRunner
}

// newMockEval creates a mockEvaluator with given column names.
func newMockEval(colNames ...string) *mockEvaluator {
	cols := make([]storage.ColumnInfo, len(colNames))
	for i, name := range colNames {
		cols[i] = storage.ColumnInfo{Name: name, Index: i, DataType: "TEXT"}
	}
	return &mockEvaluator{columns: cols}
}

func TestGeneric_Literals(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name string
		expr ast.Expr
		want Value
	}{
		{"IntLit", &ast.IntLitExpr{Value: 42}, int64(42)},
		{"FloatLit", &ast.FloatLitExpr{Value: 3.14}, 3.14},
		{"StringLit", &ast.StringLitExpr{Value: "hello"}, "hello"},
		{"NullLit", &ast.NullLitExpr{}, nil},
		{"BoolLitTrue", &ast.BoolLitExpr{Value: true}, true},
		{"BoolLitFalse", &ast.BoolLitExpr{Value: false}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Generic(tt.expr, row, eval)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestGeneric_IdentExpr(t *testing.T) {
	eval := newMockEval("id", "name")
	row := Row{int64(1), "Alice"}

	tests := []struct {
		name    string
		expr    *ast.IdentExpr
		want    Value
		wantErr bool
	}{
		{"resolve_id", &ast.IdentExpr{Name: "id"}, int64(1), false},
		{"resolve_name", &ast.IdentExpr{Name: "name"}, "Alice", false},
		{"unknown_column", &ast.IdentExpr{Name: "unknown"}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Generic(tt.expr, row, eval)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWhere(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name    string
		expr    ast.Expr
		want    bool
		wantErr bool
	}{
		{"true", &ast.BoolLitExpr{Value: true}, true, false},
		{"false", &ast.BoolLitExpr{Value: false}, false, false},
		{"non_boolean_int", &ast.IntLitExpr{Value: 1}, false, true},
		{"non_boolean_string", &ast.StringLitExpr{Value: "yes"}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Where(tt.expr, row, eval)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNull(t *testing.T) {
	eval := newMockEval("col")

	tests := []struct {
		name string
		row  Row
		not  bool
		want bool
	}{
		{"null_is_null", Row{nil}, false, true},
		{"value_is_null", Row{int64(1)}, false, false},
		{"null_is_not_null", Row{nil}, true, false},
		{"value_is_not_null", Row{int64(1)}, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ast.IsNullExpr{
				Expr: &ast.IdentExpr{Name: "col"},
				Not:  tt.not,
			}
			got, err := IsNull(e, tt.row, eval)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIn(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name string
		expr *ast.InExpr
		want Value
	}{
		{
			"match",
			&ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 2},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}, &ast.IntLitExpr{Value: 3}},
			},
			true,
		},
		{
			"no_match",
			&ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 5},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}},
			},
			false,
		},
		{
			"not_in_match",
			&ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 2},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}},
				Not:    true,
			},
			false,
		},
		{
			"not_in_no_match",
			&ast.InExpr{
				Left:   &ast.IntLitExpr{Value: 5},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}, &ast.IntLitExpr{Value: 2}},
				Not:    true,
			},
			true,
		},
		{
			"null_left",
			&ast.InExpr{
				Left:   &ast.NullLitExpr{},
				Values: []ast.Expr{&ast.IntLitExpr{Value: 1}},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := In(tt.expr, row, eval)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBetween(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name string
		expr *ast.BetweenExpr
		want Value
	}{
		{
			"in_range",
			&ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 5},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			true,
		},
		{
			"at_lower_bound",
			&ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 1},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			true,
		},
		{
			"at_upper_bound",
			&ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 10},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			true,
		},
		{
			"out_of_range",
			&ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 15},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			false,
		},
		{
			"not_between_in_range",
			&ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 5},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
				Not:  true,
			},
			false,
		},
		{
			"not_between_out_of_range",
			&ast.BetweenExpr{
				Left: &ast.IntLitExpr{Value: 15},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
				Not:  true,
			},
			true,
		},
		{
			"null_left",
			&ast.BetweenExpr{
				Left: &ast.NullLitExpr{},
				Low:  &ast.IntLitExpr{Value: 1},
				High: &ast.IntLitExpr{Value: 10},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Between(tt.expr, row, eval)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLike(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name    string
		expr    *ast.LikeExpr
		want    Value
		wantErr bool
	}{
		{
			"exact_match",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "hello"},
				Pattern: &ast.StringLitExpr{Value: "hello"},
			},
			true, false,
		},
		{
			"percent_prefix",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "hello world"},
				Pattern: &ast.StringLitExpr{Value: "%world"},
			},
			true, false,
		},
		{
			"percent_suffix",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "hello world"},
				Pattern: &ast.StringLitExpr{Value: "hello%"},
			},
			true, false,
		},
		{
			"underscore_wildcard",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "cat"},
				Pattern: &ast.StringLitExpr{Value: "c_t"},
			},
			true, false,
		},
		{
			"no_match",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "hello"},
				Pattern: &ast.StringLitExpr{Value: "world%"},
			},
			false, false,
		},
		{
			"not_like_match",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "hello"},
				Pattern: &ast.StringLitExpr{Value: "hello"},
				Not:     true,
			},
			false, false,
		},
		{
			"not_like_no_match",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "hello"},
				Pattern: &ast.StringLitExpr{Value: "world"},
				Not:     true,
			},
			true, false,
		},
		{
			"null_left",
			&ast.LikeExpr{
				Left:    &ast.NullLitExpr{},
				Pattern: &ast.StringLitExpr{Value: "%"},
			},
			false, false,
		},
		{
			"null_pattern",
			&ast.LikeExpr{
				Left:    &ast.StringLitExpr{Value: "hello"},
				Pattern: &ast.NullLitExpr{},
			},
			false, false,
		},
		{
			"non_string_left",
			&ast.LikeExpr{
				Left:    &ast.IntLitExpr{Value: 42},
				Pattern: &ast.StringLitExpr{Value: "%"},
			},
			nil, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Like(tt.expr, row, eval)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNot(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name    string
		expr    *ast.NotExpr
		want    Value
		wantErr bool
	}{
		{
			"true_to_false",
			&ast.NotExpr{Expr: &ast.BoolLitExpr{Value: true}},
			false, false,
		},
		{
			"false_to_true",
			&ast.NotExpr{Expr: &ast.BoolLitExpr{Value: false}},
			true, false,
		},
		{
			"non_boolean_error",
			&ast.NotExpr{Expr: &ast.IntLitExpr{Value: 1}},
			nil, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Not(tt.expr, row, eval)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCase(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name string
		expr *ast.CaseExpr
		want Value
	}{
		{
			"simple_case_match",
			&ast.CaseExpr{
				Operand: &ast.IntLitExpr{Value: 2},
				Whens: []ast.CaseWhen{
					{When: &ast.IntLitExpr{Value: 1}, Then: &ast.StringLitExpr{Value: "one"}},
					{When: &ast.IntLitExpr{Value: 2}, Then: &ast.StringLitExpr{Value: "two"}},
				},
			},
			"two",
		},
		{
			"simple_case_no_match_with_else",
			&ast.CaseExpr{
				Operand: &ast.IntLitExpr{Value: 99},
				Whens: []ast.CaseWhen{
					{When: &ast.IntLitExpr{Value: 1}, Then: &ast.StringLitExpr{Value: "one"}},
				},
				Else: &ast.StringLitExpr{Value: "other"},
			},
			"other",
		},
		{
			"simple_case_no_match_no_else",
			&ast.CaseExpr{
				Operand: &ast.IntLitExpr{Value: 99},
				Whens: []ast.CaseWhen{
					{When: &ast.IntLitExpr{Value: 1}, Then: &ast.StringLitExpr{Value: "one"}},
				},
			},
			nil,
		},
		{
			"searched_case_match",
			&ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.BoolLitExpr{Value: false}, Then: &ast.StringLitExpr{Value: "no"}},
					{When: &ast.BoolLitExpr{Value: true}, Then: &ast.StringLitExpr{Value: "yes"}},
				},
			},
			"yes",
		},
		{
			"searched_case_with_else",
			&ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.BoolLitExpr{Value: false}, Then: &ast.StringLitExpr{Value: "no"}},
				},
				Else: &ast.StringLitExpr{Value: "default"},
			},
			"default",
		},
		{
			"searched_case_no_match_no_else",
			&ast.CaseExpr{
				Whens: []ast.CaseWhen{
					{When: &ast.BoolLitExpr{Value: false}, Then: &ast.StringLitExpr{Value: "no"}},
				},
			},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Case(tt.expr, row, eval)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCast(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name    string
		expr    *ast.CastExpr
		want    Value
		wantErr bool
	}{
		// INT conversions
		{
			"int_to_int",
			&ast.CastExpr{Expr: &ast.IntLitExpr{Value: 42}, TargetType: "INT"},
			int64(42), false,
		},
		{
			"float_to_int",
			&ast.CastExpr{Expr: &ast.FloatLitExpr{Value: 3.7}, TargetType: "INT"},
			int64(3), false,
		},
		{
			"string_to_int",
			&ast.CastExpr{Expr: &ast.StringLitExpr{Value: "123"}, TargetType: "INT"},
			int64(123), false,
		},
		{
			"string_to_int_error",
			&ast.CastExpr{Expr: &ast.StringLitExpr{Value: "abc"}, TargetType: "INT"},
			nil, true,
		},
		// FLOAT conversions
		{
			"float_to_float",
			&ast.CastExpr{Expr: &ast.FloatLitExpr{Value: 3.14}, TargetType: "FLOAT"},
			3.14, false,
		},
		{
			"int_to_float",
			&ast.CastExpr{Expr: &ast.IntLitExpr{Value: 5}, TargetType: "FLOAT"},
			float64(5), false,
		},
		{
			"string_to_float",
			&ast.CastExpr{Expr: &ast.StringLitExpr{Value: "2.5"}, TargetType: "FLOAT"},
			2.5, false,
		},
		{
			"string_to_float_error",
			&ast.CastExpr{Expr: &ast.StringLitExpr{Value: "xyz"}, TargetType: "FLOAT"},
			nil, true,
		},
		// TEXT conversions
		{
			"string_to_text",
			&ast.CastExpr{Expr: &ast.StringLitExpr{Value: "hello"}, TargetType: "TEXT"},
			"hello", false,
		},
		{
			"int_to_text",
			&ast.CastExpr{Expr: &ast.IntLitExpr{Value: 42}, TargetType: "TEXT"},
			"42", false,
		},
		{
			"float_to_text",
			&ast.CastExpr{Expr: &ast.FloatLitExpr{Value: 3.14}, TargetType: "TEXT"},
			"3.14", false,
		},
		// NULL handling
		{
			"null_to_int",
			&ast.CastExpr{Expr: &ast.NullLitExpr{}, TargetType: "INT"},
			nil, false,
		},
		{
			"null_to_text",
			&ast.CastExpr{Expr: &ast.NullLitExpr{}, TargetType: "TEXT"},
			nil, false,
		},
		{
			"null_to_float",
			&ast.CastExpr{Expr: &ast.NullLitExpr{}, TargetType: "FLOAT"},
			nil, false,
		},
		// Unsupported type
		{
			"unsupported_type",
			&ast.CastExpr{Expr: &ast.IntLitExpr{Value: 1}, TargetType: "BOOLEAN"},
			nil, true,
		},
		// Bool to INT should fail
		{
			"bool_to_int_error",
			&ast.CastExpr{Expr: &ast.BoolLitExpr{Value: true}, TargetType: "INT"},
			nil, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Cast(tt.expr, row, eval)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestLogical(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name    string
		expr    *ast.LogicalExpr
		want    Value
		wantErr bool
	}{
		{
			"and_true_true",
			&ast.LogicalExpr{
				Left: &ast.BoolLitExpr{Value: true}, Op: "AND",
				Right: &ast.BoolLitExpr{Value: true},
			},
			true, false,
		},
		{
			"and_true_false",
			&ast.LogicalExpr{
				Left: &ast.BoolLitExpr{Value: true}, Op: "AND",
				Right: &ast.BoolLitExpr{Value: false},
			},
			false, false,
		},
		{
			"and_short_circuit_false",
			&ast.LogicalExpr{
				Left: &ast.BoolLitExpr{Value: false}, Op: "AND",
				Right: &ast.BoolLitExpr{Value: true},
			},
			false, false,
		},
		{
			"or_true_false",
			&ast.LogicalExpr{
				Left: &ast.BoolLitExpr{Value: true}, Op: "OR",
				Right: &ast.BoolLitExpr{Value: false},
			},
			true, false,
		},
		{
			"or_false_false",
			&ast.LogicalExpr{
				Left: &ast.BoolLitExpr{Value: false}, Op: "OR",
				Right: &ast.BoolLitExpr{Value: false},
			},
			false, false,
		},
		{
			"or_false_true",
			&ast.LogicalExpr{
				Left: &ast.BoolLitExpr{Value: false}, Op: "OR",
				Right: &ast.BoolLitExpr{Value: true},
			},
			true, false,
		},
		{
			"left_non_boolean_error",
			&ast.LogicalExpr{
				Left: &ast.IntLitExpr{Value: 1}, Op: "AND",
				Right: &ast.BoolLitExpr{Value: true},
			},
			nil, true,
		},
		{
			"right_non_boolean_error",
			&ast.LogicalExpr{
				Left: &ast.BoolLitExpr{Value: true}, Op: "AND",
				Right: &ast.IntLitExpr{Value: 1},
			},
			nil, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Logical(tt.expr, row, eval)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectInnerTableNames(t *testing.T) {
	tests := []struct {
		name string
		stmt *ast.SelectStmt
		want map[string]bool
	}{
		{
			"table_name_only",
			&ast.SelectStmt{TableName: "Users"},
			map[string]bool{"users": true},
		},
		{
			"table_name_with_alias",
			&ast.SelectStmt{TableName: "Users", TableAlias: "u"},
			map[string]bool{"users": true, "u": true},
		},
		{
			"with_joins",
			&ast.SelectStmt{
				TableName:  "Users",
				TableAlias: "u",
				Joins: []ast.JoinClause{
					{TableName: "Orders", TableAlias: "o"},
					{TableName: "Products"},
				},
			},
			map[string]bool{"users": true, "u": true, "orders": true, "o": true, "products": true},
		},
		{
			"empty_table_name",
			&ast.SelectStmt{},
			map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CollectInnerTableNames(tt.stmt)
			if len(got) != len(tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
				return
			}
			for k := range tt.want {
				if !got[k] {
					t.Errorf("missing key %q in result %v", k, got)
				}
			}
		})
	}
}

func TestScalarSubquery(t *testing.T) {
	tests := []struct {
		name       string
		runner     SubqueryRunner
		want       Value
		wantErr    bool
		errContain string
	}{
		{
			"single_row",
			func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
				return &SubqueryResult{Rows: []Row{{int64(42)}}}, nil
			},
			int64(42), false, "",
		},
		{
			"no_rows_returns_nil",
			func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
				return &SubqueryResult{Rows: []Row{}}, nil
			},
			nil, false, "",
		},
		{
			"multiple_rows_error",
			func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
				return &SubqueryResult{Rows: []Row{{int64(1)}, {int64(2)}}}, nil
			},
			nil, true, "at most one row",
		},
		{
			"no_runner_error",
			nil,
			nil, true, "not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &mockEvaluator{subqueryRunner: tt.runner}
			e := &ast.ScalarExpr{Subquery: &ast.SelectStmt{TableName: "t"}}
			got, err := ScalarSubquery(e, Row{}, ev)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContain != "" {
					if !contains(err.Error(), tt.errContain) {
						t.Errorf("error %q does not contain %q", err.Error(), tt.errContain)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExists(t *testing.T) {
	tests := []struct {
		name   string
		runner SubqueryRunner
		not    bool
		want   Value
	}{
		{
			"exists_with_rows",
			func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
				return &SubqueryResult{Rows: []Row{{int64(1)}}}, nil
			},
			false, true,
		},
		{
			"exists_no_rows",
			func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
				return &SubqueryResult{Rows: []Row{}}, nil
			},
			false, false,
		},
		{
			"not_exists_with_rows",
			func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
				return &SubqueryResult{Rows: []Row{{int64(1)}}}, nil
			},
			true, false,
		},
		{
			"not_exists_no_rows",
			func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
				return &SubqueryResult{Rows: []Row{}}, nil
			},
			true, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &mockEvaluator{subqueryRunner: tt.runner}
			e := &ast.ExistsExpr{Subquery: &ast.SelectStmt{TableName: "t"}, Not: tt.not}
			got, err := Exists(e, Row{}, ev)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExists_NoRunner(t *testing.T) {
	ev := &mockEvaluator{subqueryRunner: nil}
	e := &ast.ExistsExpr{Subquery: &ast.SelectStmt{TableName: "t"}}
	_, err := Exists(e, Row{}, ev)
	if err == nil {
		t.Fatal("expected error for nil runner, got nil")
	}
}

func TestBinary(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name string
		expr *ast.BinaryExpr
		want Value
	}{
		{
			"equal_true",
			&ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 1}},
			true,
		},
		{
			"equal_false",
			&ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "=", Right: &ast.IntLitExpr{Value: 2}},
			false,
		},
		{
			"not_equal",
			&ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "!=", Right: &ast.IntLitExpr{Value: 2}},
			true,
		},
		{
			"less_than",
			&ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 1}, Op: "<", Right: &ast.IntLitExpr{Value: 2}},
			true,
		},
		{
			"greater_than",
			&ast.BinaryExpr{Left: &ast.IntLitExpr{Value: 3}, Op: ">", Right: &ast.IntLitExpr{Value: 2}},
			true,
		},
		{
			"string_equal",
			&ast.BinaryExpr{Left: &ast.StringLitExpr{Value: "a"}, Op: "=", Right: &ast.StringLitExpr{Value: "a"}},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Binary(tt.expr, row, eval)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArithmetic(t *testing.T) {
	eval := newMockEval()
	row := Row{}

	tests := []struct {
		name string
		expr *ast.ArithmeticExpr
		want Value
	}{
		{
			"addition",
			&ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 3}, Op: "+", Right: &ast.IntLitExpr{Value: 2}},
			int64(5),
		},
		{
			"subtraction",
			&ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 5}, Op: "-", Right: &ast.IntLitExpr{Value: 2}},
			int64(3),
		},
		{
			"multiplication",
			&ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 3}, Op: "*", Right: &ast.IntLitExpr{Value: 4}},
			int64(12),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Arithmetic(tt.expr, row, eval)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestGeneric_UnsupportedExpr(t *testing.T) {
	eval := newMockEval()
	// WindowExpr should return an error when evaluated via Generic
	e := &ast.WindowExpr{Name: "ROW_NUMBER"}
	_, err := Generic(e, Row{}, eval)
	if err == nil {
		t.Fatal("expected error for WindowExpr, got nil")
	}
}

func TestInSubquery(t *testing.T) {
	runner := func(sub *ast.SelectStmt, ev ExprEvaluator, row Row) (*SubqueryResult, error) {
		return &SubqueryResult{Rows: []Row{{int64(1)}, {int64(2)}, {int64(3)}}}, nil
	}
	ev := &mockEvaluator{subqueryRunner: runner}

	tests := []struct {
		name string
		expr *ast.InExpr
		want Value
	}{
		{
			"in_subquery_match",
			&ast.InExpr{
				Left:     &ast.IntLitExpr{Value: 2},
				Subquery: &ast.SelectStmt{TableName: "t"},
			},
			true,
		},
		{
			"in_subquery_no_match",
			&ast.InExpr{
				Left:     &ast.IntLitExpr{Value: 5},
				Subquery: &ast.SelectStmt{TableName: "t"},
			},
			false,
		},
		{
			"not_in_subquery_match",
			&ast.InExpr{
				Left:     &ast.IntLitExpr{Value: 2},
				Subquery: &ast.SelectStmt{TableName: "t"},
				Not:      true,
			},
			false,
		},
		{
			"not_in_subquery_no_match",
			&ast.InExpr{
				Left:     &ast.IntLitExpr{Value: 5},
				Subquery: &ast.SelectStmt{TableName: "t"},
				Not:      true,
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := In(tt.expr, Row{}, ev)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInSubquery_NoRunner(t *testing.T) {
	ev := &mockEvaluator{subqueryRunner: nil}
	e := &ast.InExpr{
		Left:     &ast.IntLitExpr{Value: 1},
		Subquery: &ast.SelectStmt{TableName: "t"},
	}
	_, err := In(e, Row{}, ev)
	if err == nil {
		t.Fatal("expected error for nil runner, got nil")
	}
}

// contains checks if s contains substr (simple helper to avoid importing strings in test).
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
