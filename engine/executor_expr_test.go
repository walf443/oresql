package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/ast"
)

func TestValidateAndCoerceValue(t *testing.T) {
	tests := []struct {
		name    string
		val     Value
		col     ColumnInfo
		want    Value
		wantErr bool
	}{
		{
			name: "INT valid",
			val:  int64(42),
			col:  ColumnInfo{Name: "id", DataType: "INT"},
			want: int64(42),
		},
		{
			name: "FLOAT valid",
			val:  float64(3.14),
			col:  ColumnInfo{Name: "score", DataType: "FLOAT"},
			want: float64(3.14),
		},
		{
			name: "TEXT valid",
			val:  "hello",
			col:  ColumnInfo{Name: "name", DataType: "TEXT"},
			want: "hello",
		},
		{
			name: "int64 to float64 coercion",
			val:  int64(10),
			col:  ColumnInfo{Name: "score", DataType: "FLOAT"},
			want: float64(10),
		},
		{
			name:    "INT type mismatch",
			val:     "hello",
			col:     ColumnInfo{Name: "id", DataType: "INT"},
			wantErr: true,
		},
		{
			name:    "FLOAT type mismatch",
			val:     "hello",
			col:     ColumnInfo{Name: "score", DataType: "FLOAT"},
			wantErr: true,
		},
		{
			name:    "TEXT type mismatch",
			val:     int64(42),
			col:     ColumnInfo{Name: "name", DataType: "TEXT"},
			wantErr: true,
		},
		{
			name:    "NULL with NOT NULL",
			val:     nil,
			col:     ColumnInfo{Name: "id", DataType: "INT", NotNull: true},
			wantErr: true,
		},
		{
			name: "NULL with nullable",
			val:  nil,
			col:  ColumnInfo{Name: "name", DataType: "TEXT"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateAndCoerceValue(tt.val, tt.col)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvalComparison(t *testing.T) {
	tests := []struct {
		name    string
		left    Value
		op      string
		right   Value
		want    bool
		wantErr bool
	}{
		// int64 comparisons
		{"int64 equal", int64(5), "=", int64(5), true, false},
		{"int64 not equal", int64(5), "!=", int64(3), true, false},
		{"int64 less", int64(3), "<", int64(5), true, false},
		{"int64 greater", int64(5), ">", int64(3), true, false},
		{"int64 less or equal", int64(5), "<=", int64(5), true, false},
		{"int64 greater or equal", int64(5), ">=", int64(5), true, false},
		{"int64 not less", int64(5), "<", int64(3), false, false},

		// float64 comparisons
		{"float64 equal", float64(3.14), "=", float64(3.14), true, false},
		{"float64 not equal", float64(3.14), "!=", float64(2.71), true, false},
		{"float64 less", float64(2.71), "<", float64(3.14), true, false},
		{"float64 greater", float64(3.14), ">", float64(2.71), true, false},

		// int/float mixed
		{"int float equal", int64(5), "=", float64(5.0), true, false},
		{"int float less", int64(3), "<", float64(3.5), true, false},
		{"int float greater", int64(4), ">", float64(3.5), true, false},

		// string comparisons
		{"string equal", "abc", "=", "abc", true, false},
		{"string not equal", "abc", "!=", "def", true, false},
		{"string less", "abc", "<", "def", true, false},
		{"string greater", "def", ">", "abc", true, false},

		// NULL comparisons
		{"NULL left", nil, "=", int64(5), false, false},
		{"NULL right", int64(5), "=", nil, false, false},
		{"NULL both", nil, "=", nil, false, false},

		// type mismatch
		{"int string mismatch", int64(5), "=", "hello", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalComparison(tt.left, tt.op, tt.right)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got, "evalComparison(%v, %q, %v)", tt.left, tt.op, tt.right)
		})
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name string
		a    Value
		b    Value
		want int
	}{
		// int64
		{"int64 equal", int64(5), int64(5), 0},
		{"int64 less", int64(3), int64(5), -1},
		{"int64 greater", int64(5), int64(3), 1},

		// float64
		{"float64 equal", float64(3.14), float64(3.14), 0},
		{"float64 less", float64(2.71), float64(3.14), -1},
		{"float64 greater", float64(3.14), float64(2.71), 1},

		// int/float mixed
		{"int float equal", int64(5), float64(5.0), 0},
		{"int float less", int64(3), float64(3.5), -1},
		{"float int greater", float64(4.5), int64(4), 1},

		// string
		{"string equal", "abc", "abc", 0},
		{"string less", "abc", "def", -1},
		{"string greater", "def", "abc", 1},

		// NULL
		{"NULL both", nil, nil, 0},
		{"NULL left sorts last", nil, int64(5), 1},
		{"NULL right sorts last", int64(5), nil, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			assert.Equal(t, tt.want, got, "compareValues(%v, %v)", tt.a, tt.b)
		})
	}
}

func TestEvalArithmetic(t *testing.T) {
	tests := []struct {
		name    string
		left    Value
		op      string
		right   Value
		want    Value
		wantErr bool
	}{
		// int + int
		{"int add", int64(3), "+", int64(4), int64(7), false},
		{"int sub", int64(10), "-", int64(3), int64(7), false},
		{"int mul", int64(3), "*", int64(4), int64(12), false},
		{"int div", int64(10), "/", int64(3), int64(3), false},

		// float + float
		{"float add", float64(1.5), "+", float64(2.5), float64(4.0), false},
		{"float sub", float64(5.0), "-", float64(2.5), float64(2.5), false},
		{"float mul", float64(2.0), "*", float64(3.5), float64(7.0), false},
		{"float div", float64(10.0), "/", float64(4.0), float64(2.5), false},

		// int + float -> float
		{"int float add", int64(3), "+", float64(1.5), float64(4.5), false},
		{"float int mul", float64(2.5), "*", int64(4), float64(10.0), false},

		// division by zero
		{"int div zero", int64(10), "/", int64(0), nil, true},
		{"float div zero", float64(10.0), "/", float64(0), nil, true},

		// NULL propagation
		{"NULL left", nil, "+", int64(5), nil, false},
		{"NULL right", int64(5), "+", nil, nil, false},
		{"NULL both", nil, "+", nil, nil, false},

		// type mismatch
		{"int string mismatch", int64(5), "+", "hello", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalArithmetic(tt.left, tt.op, tt.right)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got, "evalArithmetic(%v, %q, %v)", tt.left, tt.op, tt.right)
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name   string
		val    Value
		want   float64
		wantOK bool
	}{
		{"int64", int64(42), float64(42), true},
		{"float64", float64(3.14), float64(3.14), true},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toFloat64(tt.val)
			require.Equal(t, tt.wantOK, ok, "toFloat64(%v) ok", tt.val)
			if ok {
				assert.Equal(t, tt.want, got, "toFloat64(%v)", tt.val)
			}
		})
	}
}

func TestMatchLike(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		pattern string
		want    bool
	}{
		{"percent any", "hello", "%", true},
		{"percent prefix", "hello", "hel%", true},
		{"percent suffix", "hello", "%llo", true},
		{"percent middle", "hello", "h%o", true},
		{"percent no match", "hello", "world%", false},
		{"underscore single", "hello", "hell_", true},
		{"underscore no match", "hello", "hel_", false},
		{"exact match", "hello", "hello", true},
		{"exact no match", "hello", "world", false},
		{"empty pattern empty string", "", "", true},
		{"empty pattern nonempty string", "hello", "", false},
		{"nonempty pattern empty string", "", "hello", false},
		{"percent empty string", "", "%", true},
		{"escaped percent", "100%", "100\\%", true},
		{"escaped percent no match", "100x", "100\\%", false},
		{"escaped underscore", "a_b", "a\\_b", true},
		{"escaped underscore no match", "axb", "a\\_b", false},
		{"escaped backslash", "a\\b", "a\\\\b", true},
		{"prefix and suffix percent", "hello world", "%lo wor%", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchLike(tt.str, tt.pattern)
			assert.Equal(t, tt.want, got, "matchLike(%q, %q)", tt.str, tt.pattern)
		})
	}
}

func TestEvalLiteral(t *testing.T) {
	tests := []struct {
		name    string
		expr    ast.Expr
		want    Value
		wantErr bool
	}{
		{"IntLit", &ast.IntLitExpr{Value: 42}, int64(42), false},
		{"FloatLit", &ast.FloatLitExpr{Value: 3.14}, float64(3.14), false},
		{"StringLit", &ast.StringLitExpr{Value: "hello"}, "hello", false},
		{"NullLit", &ast.NullLitExpr{}, nil, false},
		{
			"ArithmeticExpr",
			&ast.ArithmeticExpr{
				Left:  &ast.IntLitExpr{Value: 3},
				Op:    "+",
				Right: &ast.IntLitExpr{Value: 4},
			},
			int64(7),
			false,
		},
		{
			"unsupported type",
			&ast.IdentExpr{Name: "col"},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalLiteral(tt.expr)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got, "evalLiteral()")
		})
	}
}

func TestFormatExpr(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expr
		want string
	}{
		{"IntLit", &ast.IntLitExpr{Value: 42}, "42"},
		{"FloatLit", &ast.FloatLitExpr{Value: 3.14}, "3.14"},
		{"StringLit", &ast.StringLitExpr{Value: "hello"}, "'hello'"},
		{"NullLit", &ast.NullLitExpr{}, "NULL"},
		{"IdentExpr no table", &ast.IdentExpr{Name: "col1"}, "col1"},
		{"IdentExpr with table", &ast.IdentExpr{Table: "t", Name: "col1"}, "t.col1"},
		{
			"ArithmeticExpr",
			&ast.ArithmeticExpr{
				Left:  &ast.IntLitExpr{Value: 1},
				Op:    "+",
				Right: &ast.IntLitExpr{Value: 2},
			},
			"1 + 2",
		},
		{"unsupported type", &ast.StarExpr{}, "?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatExpr(tt.expr)
			assert.Equal(t, tt.want, got, "formatExpr()")
		})
	}
}

func TestValidateTableRef(t *testing.T) {
	tests := []struct {
		name        string
		tableRef    string
		targetTable string
		wantErr     bool
	}{
		{"empty ref", "", "users", false},
		{"match", "users", "users", false},
		{"case insensitive", "Users", "users", false},
		{"mismatch", "orders", "users", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableRef(tt.tableRef, tt.targetTable)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestExtractLikePrefix(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"abc%", "abc"},
		{"a\\_b%", "a_b"},
		{"%abc", ""},
		{"_abc", ""},
		{"abc", "abc"},
		{"abc\\%def", "abc%def"},
		{"a\\\\b%", "a\\b"},
		{"", ""},
		{"\\%%", "%"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got := extractLikePrefix(tt.pattern)
			assert.Equal(t, tt.want, got, "extractLikePrefix(%q)", tt.pattern)
		})
	}
}

func TestNextPrefix(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		want   string
		wantOK bool
	}{
		{"simple", "abc", "abd", true},
		{"trailing 0xFF", "ab" + string([]byte{0xFF}), "ac", true},
		{"all 0xFF single", string([]byte{0xFF}), "", false},
		{"all 0xFF double", string([]byte{0xFF, 0xFF}), "", false},
		{"empty", "", "", false},
		{"single char", "a", "b", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := nextPrefix(tt.input)
			require.Equal(t, tt.wantOK, ok, "nextPrefix(%q) ok", tt.input)
			assert.Equal(t, tt.want, got, "nextPrefix(%q)", tt.input)
		})
	}
}
