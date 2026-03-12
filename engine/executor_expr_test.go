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
