package scalar

import (
	"errors"
	"math"
	"testing"

	"github.com/walf443/oresql/ast"
)

// ---------------------------------------------------------------------------
// IsScalar
// ---------------------------------------------------------------------------

func TestIsScalar(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect bool
	}{
		// Registry functions
		{"ABS", "ABS", true},
		{"ROUND", "ROUND", true},
		{"MOD", "MOD", true},
		{"CEIL", "CEIL", true},
		{"FLOOR", "FLOOR", true},
		{"POWER", "POWER", true},
		{"LENGTH", "LENGTH", true},
		{"UPPER", "UPPER", true},
		{"LOWER", "LOWER", true},
		{"SUBSTRING", "SUBSTRING", true},
		{"TRIM", "TRIM", true},
		{"CONCAT", "CONCAT", true},
		{"JSON_OBJECT", "JSON_OBJECT", true},
		{"JSON_ARRAY", "JSON_ARRAY", true},
		// Special cases (not in Registry but handled by switch)
		{"COALESCE", "COALESCE", true},
		{"NULLIF", "NULLIF", true},
		{"JSON_VALUE", "JSON_VALUE", true},
		{"JSON_QUERY", "JSON_QUERY", true},
		{"JSON_EXISTS", "JSON_EXISTS", true},
		// Non-scalar names
		{"COUNT is aggregate", "COUNT", false},
		{"SUM is aggregate", "SUM", false},
		{"unknown func", "FOOBAR", false},
		{"empty string", "", false},
		{"lowercase abs", "abs", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsScalar(tt.input)
			if got != tt.expect {
				t.Errorf("IsScalar(%q) = %v, want %v", tt.input, got, tt.expect)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IsValidJSON
// ---------------------------------------------------------------------------

func TestIsValidJSON(t *testing.T) {
	tests := []struct {
		name   string
		val    Value
		expect bool
	}{
		{"valid object", `{"a":1}`, true},
		{"valid array", `[1,2,3]`, true},
		{"valid string", `"hello"`, true},
		{"valid number", `42`, true},
		{"valid null", `null`, true},
		{"valid bool", `true`, true},
		{"invalid json", `{bad`, false},
		{"empty string", ``, false},
		{"plain text", `hello world`, false},
		{"[]byte always true", []byte(`anything`), true},
		{"[]byte empty", []byte{}, true},
		{"int not json", int64(42), false},
		{"float not json", float64(3.14), false},
		{"nil", nil, false},
		{"bool not json", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsValidJSON(tt.val)
			if got != tt.expect {
				t.Errorf("IsValidJSON(%v) = %v, want %v", tt.val, got, tt.expect)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// EvalArgsWith
// ---------------------------------------------------------------------------

func TestEvalArgsWith(t *testing.T) {
	t.Run("empty args", func(t *testing.T) {
		vals, err := EvalArgsWith(nil, func(e ast.Expr) (Value, error) {
			t.Fatal("should not be called")
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != 0 {
			t.Errorf("expected empty slice, got %v", vals)
		}
	})

	t.Run("multiple args evaluated in order", func(t *testing.T) {
		args := []ast.Expr{
			&ast.IntLitExpr{Value: 10},
			&ast.StringLitExpr{Value: "hello"},
		}
		evalFn := func(e ast.Expr) (Value, error) {
			switch v := e.(type) {
			case *ast.IntLitExpr:
				return v.Value, nil
			case *ast.StringLitExpr:
				return v.Value, nil
			}
			return nil, errors.New("unexpected")
		}
		vals, err := EvalArgsWith(args, evalFn)
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != 2 {
			t.Fatalf("expected 2 values, got %d", len(vals))
		}
		if vals[0] != int64(10) {
			t.Errorf("vals[0] = %v, want 10", vals[0])
		}
		if vals[1] != "hello" {
			t.Errorf("vals[1] = %v, want hello", vals[1])
		}
	})

	t.Run("error propagation", func(t *testing.T) {
		args := []ast.Expr{
			&ast.IntLitExpr{Value: 1},
			&ast.IntLitExpr{Value: 2},
		}
		callCount := 0
		evalFn := func(e ast.Expr) (Value, error) {
			callCount++
			if callCount == 2 {
				return nil, errors.New("eval error")
			}
			return int64(1), nil
		}
		_, err := EvalArgsWith(args, evalFn)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "eval error" {
			t.Errorf("error = %q, want %q", err.Error(), "eval error")
		}
	})
}

// ---------------------------------------------------------------------------
// TryCompileJSONPath
// ---------------------------------------------------------------------------

func TestTryCompileJSONPath(t *testing.T) {
	t.Run("valid string literal path", func(t *testing.T) {
		call := &ast.CallExpr{
			Name: "JSON_VALUE",
			Args: []ast.Expr{
				&ast.IdentExpr{Name: "col"},
				&ast.StringLitExpr{Value: "$.name"},
			},
		}
		p := TryCompileJSONPath(call)
		if p == nil {
			t.Error("expected compiled path, got nil")
		}
	})

	t.Run("invalid path string", func(t *testing.T) {
		call := &ast.CallExpr{
			Name: "JSON_VALUE",
			Args: []ast.Expr{
				&ast.IdentExpr{Name: "col"},
				&ast.StringLitExpr{Value: "%%%invalid"},
			},
		}
		p := TryCompileJSONPath(call)
		if p != nil {
			t.Errorf("expected nil for invalid path, got %v", p)
		}
	})

	t.Run("non-string second arg", func(t *testing.T) {
		call := &ast.CallExpr{
			Name: "JSON_VALUE",
			Args: []ast.Expr{
				&ast.IdentExpr{Name: "col"},
				&ast.IntLitExpr{Value: 42},
			},
		}
		p := TryCompileJSONPath(call)
		if p != nil {
			t.Errorf("expected nil for non-string arg, got %v", p)
		}
	})

	t.Run("fewer than 2 args", func(t *testing.T) {
		call := &ast.CallExpr{
			Name: "JSON_VALUE",
			Args: []ast.Expr{
				&ast.IdentExpr{Name: "col"},
			},
		}
		p := TryCompileJSONPath(call)
		if p != nil {
			t.Errorf("expected nil for < 2 args, got %v", p)
		}
	})
}

// ---------------------------------------------------------------------------
// String functions via Registry
// ---------------------------------------------------------------------------

func TestLength(t *testing.T) {
	fn := Registry["LENGTH"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"ascii", []Value{"hello"}, int64(5), false},
		{"empty", []Value{""}, int64(0), false},
		{"multibyte", []Value{"こんにちは"}, int64(5), false},
		{"nil returns nil", []Value{nil}, nil, false},
		{"wrong type", []Value{int64(42)}, nil, true},
		{"wrong arg count", []Value{}, nil, true},
		{"too many args", []Value{"a", "b"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("LENGTH = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestUpper(t *testing.T) {
	fn := Registry["UPPER"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"basic", []Value{"hello"}, "HELLO", false},
		{"already upper", []Value{"ABC"}, "ABC", false},
		{"nil", []Value{nil}, nil, false},
		{"wrong type", []Value{int64(1)}, nil, true},
		{"no args", []Value{}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("UPPER = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestLower(t *testing.T) {
	fn := Registry["LOWER"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"basic", []Value{"HELLO"}, "hello", false},
		{"already lower", []Value{"abc"}, "abc", false},
		{"nil", []Value{nil}, nil, false},
		{"wrong type", []Value{int64(1)}, nil, true},
		{"no args", []Value{}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("LOWER = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestSubstring(t *testing.T) {
	fn := Registry["SUBSTRING"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"2-arg from start", []Value{"hello", int64(1)}, "hello", false},
		{"2-arg from middle", []Value{"hello", int64(3)}, "llo", false},
		{"3-arg", []Value{"hello", int64(2), int64(3)}, "ell", false},
		{"3-arg length exceeds", []Value{"hello", int64(4), int64(10)}, "lo", false},
		{"pos beyond string", []Value{"hi", int64(10)}, "", false},
		{"pos zero clamps to start", []Value{"hello", int64(0)}, "hello", false},
		{"negative pos clamps", []Value{"hello", int64(-5)}, "hello", false},
		{"nil first arg", []Value{nil, int64(1)}, nil, false},
		{"nil third arg", []Value{"hello", int64(1), nil}, nil, false},
		{"multibyte", []Value{"あいうえお", int64(2), int64(3)}, "いうえ", false},
		{"too few args", []Value{"a"}, nil, true},
		{"too many args", []Value{"a", int64(1), int64(2), int64(3)}, nil, true},
		{"non-string first", []Value{int64(1), int64(1)}, nil, true},
		{"non-int second", []Value{"a", "b"}, nil, true},
		{"non-int third", []Value{"a", int64(1), "b"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("SUBSTRING = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestTrim(t *testing.T) {
	fn := Registry["TRIM"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"spaces", []Value{"  hello  "}, "hello", false},
		{"tabs and newlines", []Value{"\t\nhello\t\n"}, "hello", false},
		{"no whitespace", []Value{"hello"}, "hello", false},
		{"nil", []Value{nil}, nil, false},
		{"wrong type", []Value{int64(1)}, nil, true},
		{"no args", []Value{}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("TRIM = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestConcat(t *testing.T) {
	fn := Registry["CONCAT"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"two strings", []Value{"hello", " world"}, "hello world", false},
		{"single string", []Value{"hello"}, "hello", false},
		{"three strings", []Value{"a", "b", "c"}, "abc", false},
		{"null returns null", []Value{"hello", nil, "world"}, nil, false},
		{"first arg null", []Value{nil, "world"}, nil, false},
		{"non-string arg", []Value{"hello", int64(1)}, nil, true},
		{"no args", []Value{}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("CONCAT = %v, want %v", got, tt.expect)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Numeric functions via Registry
// ---------------------------------------------------------------------------

func TestAbs(t *testing.T) {
	fn := Registry["ABS"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"positive int", []Value{int64(5)}, int64(5), false},
		{"negative int", []Value{int64(-5)}, int64(5), false},
		{"zero int", []Value{int64(0)}, int64(0), false},
		{"positive float", []Value{float64(3.14)}, float64(3.14), false},
		{"negative float", []Value{float64(-3.14)}, float64(3.14), false},
		{"nil", []Value{nil}, nil, false},
		{"string arg", []Value{"hello"}, nil, true},
		{"no args", []Value{}, nil, true},
		{"too many args", []Value{int64(1), int64(2)}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("ABS = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestRound(t *testing.T) {
	fn := Registry["ROUND"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"int 1 arg", []Value{int64(42)}, int64(42), false},
		{"float 1 arg", []Value{float64(3.7)}, float64(4.0), false},
		{"float round down", []Value{float64(3.2)}, float64(3.0), false},
		{"float 2 arg precision", []Value{float64(3.14159), int64(2)}, float64(3.14), false},
		{"float 0 precision", []Value{float64(3.7), int64(0)}, float64(4.0), false},
		{"int negative precision", []Value{int64(1234), int64(-2)}, int64(1200), false},
		{"nil first arg", []Value{nil}, nil, false},
		{"nil second arg", []Value{float64(3.14), nil}, nil, false},
		{"non-int precision", []Value{float64(3.14), "a"}, nil, true},
		{"string arg", []Value{"hello"}, nil, true},
		{"no args", []Value{}, nil, true},
		{"too many args", []Value{float64(1), int64(1), int64(1)}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("ROUND = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestMod(t *testing.T) {
	fn := Registry["MOD"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"int mod", []Value{int64(10), int64(3)}, int64(1), false},
		{"int mod exact", []Value{int64(9), int64(3)}, int64(0), false},
		{"float mod", []Value{float64(10.5), float64(3.0)}, math.Mod(10.5, 3.0), false},
		{"mixed int float", []Value{int64(10), float64(3.0)}, math.Mod(10, 3.0), false},
		{"int div by zero", []Value{int64(10), int64(0)}, nil, true},
		{"float div by zero", []Value{float64(10.0), float64(0.0)}, nil, true},
		{"nil first", []Value{nil, int64(3)}, nil, false},
		{"nil second", []Value{int64(10), nil}, nil, false},
		{"string args", []Value{"a", "b"}, nil, true},
		{"wrong arg count", []Value{int64(1)}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("MOD = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestCeil(t *testing.T) {
	fn := Registry["CEIL"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"int passthrough", []Value{int64(5)}, int64(5), false},
		{"float up", []Value{float64(3.2)}, int64(4), false},
		{"float exact", []Value{float64(3.0)}, int64(3), false},
		{"negative float", []Value{float64(-3.7)}, int64(-3), false},
		{"nil", []Value{nil}, nil, false},
		{"string arg", []Value{"hello"}, nil, true},
		{"no args", []Value{}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("CEIL = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestFloor(t *testing.T) {
	fn := Registry["FLOOR"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"int passthrough", []Value{int64(5)}, int64(5), false},
		{"float down", []Value{float64(3.7)}, int64(3), false},
		{"float exact", []Value{float64(3.0)}, int64(3), false},
		{"negative float", []Value{float64(-3.2)}, int64(-4), false},
		{"nil", []Value{nil}, nil, false},
		{"string arg", []Value{"hello"}, nil, true},
		{"no args", []Value{}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("FLOOR = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestPower(t *testing.T) {
	fn := Registry["POWER"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"int base and exp", []Value{int64(2), int64(3)}, float64(8), false},
		{"float base", []Value{float64(2.5), int64(2)}, math.Pow(2.5, 2), false},
		{"zero exp", []Value{int64(5), int64(0)}, float64(1), false},
		{"nil first", []Value{nil, int64(2)}, nil, false},
		{"nil second", []Value{int64(2), nil}, nil, false},
		{"string args", []Value{"a", "b"}, nil, true},
		{"wrong arg count", []Value{int64(1)}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("POWER = %v, want %v", got, tt.expect)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// JSON functions via Registry
// ---------------------------------------------------------------------------

func TestJSONObject(t *testing.T) {
	fn := Registry["JSON_OBJECT"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"single pair", []Value{"key", "value"}, `{"key":"value"}`, false},
		{"two pairs", []Value{"a", int64(1), "b", int64(2)}, `{"a":1,"b":2}`, false},
		{"null value", []Value{"k", nil}, `{"k":null}`, false},
		{"bool value", []Value{"k", true}, `{"k":true}`, false},
		{"empty", []Value{}, "{}", false},
		{"odd args error", []Value{"a"}, nil, true},
		{"odd args 3", []Value{"a", int64(1), "b"}, nil, true},
		{"non-string key", []Value{int64(1), "val"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("JSON_OBJECT = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestJSONArray(t *testing.T) {
	fn := Registry["JSON_ARRAY"]
	tests := []struct {
		name   string
		args   []Value
		expect Value
		hasErr bool
	}{
		{"ints", []Value{int64(1), int64(2), int64(3)}, `[1,2,3]`, false},
		{"strings", []Value{"a", "b"}, `["a","b"]`, false},
		{"mixed types", []Value{int64(1), "hello", nil, true}, `[1,"hello",null,true]`, false},
		{"empty", []Value{}, `[]`, false},
		{"single element", []Value{float64(3.14)}, `[3.14]`, false},
		{"nested json string", []Value{`{"a":1}`}, `[{"a":1}]`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn(tt.args)
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("JSON_ARRAY = %v, want %v", got, tt.expect)
			}
		})
	}
}
