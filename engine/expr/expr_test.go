package expr

import (
	"testing"

	"github.com/walf443/oresql/ast"
)

func TestArithmetic(t *testing.T) {
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
		{"int div by zero", int64(10), "/", int64(0), nil, true},

		// float + float
		{"float add", float64(1.5), "+", float64(2.5), float64(4.0), false},
		{"float sub", float64(5.0), "-", float64(2.5), float64(2.5), false},
		{"float mul", float64(2.0), "*", float64(3.5), float64(7.0), false},
		{"float div", float64(10.0), "/", float64(4.0), float64(2.5), false},
		{"float div by zero", float64(10.0), "/", float64(0), nil, true},

		// int + float mixed
		{"int+float add", int64(3), "+", float64(1.5), float64(4.5), false},
		{"float+int add", float64(1.5), "+", int64(3), float64(4.5), false},
		{"int+float mul", int64(2), "*", float64(3.5), float64(7.0), false},

		// nil values
		{"left nil", nil, "+", int64(1), nil, false},
		{"right nil", int64(1), "+", nil, nil, false},
		{"both nil", nil, "+", nil, nil, false},

		// unknown operator
		{"unknown op int", int64(1), "^", int64(2), nil, true},
		{"unknown op float", float64(1.0), "^", float64(2.0), nil, true},

		// non-numeric operands
		{"string operands", "hello", "+", "world", nil, true},
		{"string and int", "hello", "+", int64(1), nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Arithmetic(tt.left, tt.op, tt.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("Arithmetic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Arithmetic() = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestComparison(t *testing.T) {
	tests := []struct {
		name    string
		left    Value
		op      string
		right   Value
		want    bool
		wantErr bool
	}{
		// numeric comparisons (int vs int)
		{"int eq true", int64(5), "=", int64(5), true, false},
		{"int eq false", int64(5), "=", int64(3), false, false},
		{"int lt true", int64(3), "<", int64(5), true, false},
		{"int lt false", int64(5), "<", int64(3), false, false},
		{"int gt true", int64(5), ">", int64(3), true, false},
		{"int le true", int64(5), "<=", int64(5), true, false},
		{"int ge true", int64(5), ">=", int64(5), true, false},
		{"int ne true", int64(5), "!=", int64(3), true, false},
		{"int ne false", int64(5), "!=", int64(5), false, false},

		// numeric comparisons (float vs float)
		{"float lt", float64(1.5), "<", float64(2.5), true, false},
		{"float eq", float64(2.5), "=", float64(2.5), true, false},

		// numeric comparisons (mixed int/float)
		{"int<float", int64(1), "<", float64(1.5), true, false},
		{"float<int", float64(0.5), "<", int64(1), true, false},

		// string comparisons
		{"str eq", "abc", "=", "abc", true, false},
		{"str ne", "abc", "!=", "def", true, false},
		{"str lt", "abc", "<", "def", true, false},
		{"str gt", "def", ">", "abc", true, false},

		// nil handling
		{"left nil", nil, "=", int64(1), false, false},
		{"right nil", int64(1), "=", nil, false, false},
		{"both nil", nil, "=", nil, false, false},

		// type mismatches
		{"int vs string", int64(1), "=", "hello", false, true},
		{"string vs int", "hello", "=", int64(1), false, true},
		{"bool operand", true, "=", false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Comparison(tt.left, tt.op, tt.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("Comparison() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Comparison() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		a    Value
		b    Value
		want int
	}{
		// int < int
		{"int less", int64(1), int64(2), -1},
		{"int equal", int64(5), int64(5), 0},
		{"int greater", int64(10), int64(2), 1},

		// float < float
		{"float less", float64(1.1), float64(2.2), -1},
		{"float equal", float64(3.14), float64(3.14), 0},
		{"float greater", float64(9.9), float64(1.1), 1},

		// mixed int/float
		{"int<float", int64(1), float64(1.5), -1},
		{"int=float", int64(2), float64(2.0), 0},
		{"int>float", int64(3), float64(2.5), 1},
		{"float<int", float64(0.5), int64(1), -1},
		{"float=int", float64(2.0), int64(2), 0},
		{"float>int", float64(3.5), int64(3), 1},

		// string < string
		{"str less", "abc", "def", -1},
		{"str equal", "abc", "abc", 0},
		{"str greater", "def", "abc", 1},

		// nil placement (nil sorts last)
		{"nil nil", nil, nil, 0},
		{"nil int", nil, int64(1), 1},
		{"int nil", int64(1), nil, -1},
		{"nil string", nil, "abc", 1},
		{"string nil", "abc", nil, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Compare(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("Compare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyCmpOp(t *testing.T) {
	tests := []struct {
		name    string
		cmp     int
		op      string
		want    bool
		wantErr bool
	}{
		// cmp = -1 (a < b)
		{"lt -1 =", -1, "=", false, false},
		{"lt -1 !=", -1, "!=", true, false},
		{"lt -1 <", -1, "<", true, false},
		{"lt -1 >", -1, ">", false, false},
		{"lt -1 <=", -1, "<=", true, false},
		{"lt -1 >=", -1, ">=", false, false},

		// cmp = 0 (a == b)
		{"eq 0 =", 0, "=", true, false},
		{"eq 0 !=", 0, "!=", false, false},
		{"eq 0 <", 0, "<", false, false},
		{"eq 0 >", 0, ">", false, false},
		{"eq 0 <=", 0, "<=", true, false},
		{"eq 0 >=", 0, ">=", true, false},

		// cmp = 1 (a > b)
		{"gt 1 =", 1, "=", false, false},
		{"gt 1 !=", 1, "!=", true, false},
		{"gt 1 <", 1, "<", false, false},
		{"gt 1 >", 1, ">", true, false},
		{"gt 1 <=", 1, "<=", false, false},
		{"gt 1 >=", 1, ">=", true, false},

		// unknown operator
		{"unknown op", 0, "??", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApplyCmpOp(tt.cmp, tt.op)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyCmpOp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ApplyCmpOp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name   string
		v      Value
		wantF  float64
		wantOK bool
	}{
		{"int64", int64(42), 42.0, true},
		{"float64", float64(3.14), 3.14, true},
		{"string invalid", "hello", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotF, gotOK := ToFloat64(tt.v)
			if gotOK != tt.wantOK {
				t.Errorf("ToFloat64() ok = %v, want %v", gotOK, tt.wantOK)
				return
			}
			if gotF != tt.wantF {
				t.Errorf("ToFloat64() = %v, want %v", gotF, tt.wantF)
			}
		})
	}
}

func TestLogicalOp(t *testing.T) {
	tests := []struct {
		name    string
		left    bool
		op      string
		right   bool
		want    Value
		wantErr bool
	}{
		// AND truth table
		{"AND true true", true, "AND", true, true, false},
		{"AND true false", true, "AND", false, false, false},
		{"AND false true", false, "AND", true, false, false},
		{"AND false false", false, "AND", false, false, false},

		// OR truth table
		{"OR true true", true, "OR", true, true, false},
		{"OR true false", true, "OR", false, true, false},
		{"OR false true", false, "OR", true, true, false},
		{"OR false false", false, "OR", false, false, false},

		// unknown operator
		{"unknown op", true, "XOR", false, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LogicalOp(tt.left, tt.op, tt.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LogicalOp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("LogicalOp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateTableRef(t *testing.T) {
	tests := []struct {
		name    string
		ref     string
		target  string
		wantErr bool
	}{
		{"match exact", "users", "users", false},
		{"mismatch", "orders", "users", true},
		{"empty ref", "", "users", false},
		{"case insensitive match", "Users", "users", false},
		{"case insensitive match reverse", "users", "Users", false},
		{"case insensitive mismatch", "Orders", "users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTableRef(tt.ref, tt.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTableRef() error = %v, wantErr %v", err, tt.wantErr)
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
		// % wildcard
		{"percent prefix", "hello world", "%world", true},
		{"percent suffix", "hello world", "hello%", true},
		{"percent both", "hello world", "%lo wo%", true},
		{"percent only", "anything", "%", true},
		{"percent empty str", "", "%", true},
		{"percent no match", "hello", "%xyz%", false},

		// _ wildcard
		{"underscore single", "cat", "c_t", true},
		{"underscore no match", "cart", "c_t", false},
		{"underscore multiple", "abcd", "a__d", true},

		// escape sequences
		{"escape percent", "100%", `100\%`, true},
		{"escape percent no match", "10000", `100\%`, false},
		{"escape underscore", "a_b", `a\_b`, true},
		{"escape underscore no match", "axb", `a\_b`, false},

		// exact match
		{"exact match", "hello", "hello", true},
		{"exact no match", "hello", "world", false},

		// empty
		{"empty both", "", "", true},
		{"empty pattern", "hello", "", false},
		{"empty str with pattern", "", "a", false},

		// combined
		{"combined % and _", "hello world", "h_llo%", true},
		{"combined no match", "hello world", "h_lo%", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchLike(tt.str, tt.pattern)
			if got != tt.want {
				t.Errorf("MatchLike(%q, %q) = %v, want %v", tt.str, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestMatchFullText(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		term      string
		tokenizer string
		want      bool
	}{
		// word tokenizer - exact match
		{"word exact match", "hello world", "hello", "word", true},
		{"word exact match end", "hello world", "world", "word", true},
		{"word no match", "hello world", "hell", "word", false},
		{"word boundary", "hello-world", "hello", "word", true},
		{"word boundary second", "hello-world", "world", "word", true},

		// word tokenizer - case insensitivity
		{"word case insensitive", "Hello World", "hello", "word", true},
		{"word case insensitive upper term", "hello world", "HELLO", "word", true},

		// word tokenizer - empty tokenizer defaults to word
		{"empty tokenizer defaults word", "hello world", "hello", "", true},
		{"empty tokenizer no partial", "hello world", "hell", "", false},

		// bigram tokenizer - substring
		{"bigram substring", "hello world", "llo", "bigram", true},
		{"bigram full", "hello world", "hello world", "bigram", true},
		{"bigram no match", "hello world", "xyz", "bigram", false},
		{"bigram case insensitive", "Hello World", "hello", "bigram", true},

		// edge cases
		{"empty text", "", "hello", "word", false},
		{"empty term", "hello", "", "word", false},
		{"empty term bigram", "hello", "", "bigram", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchFullText(tt.text, tt.term, tt.tokenizer)
			if got != tt.want {
				t.Errorf("MatchFullText(%q, %q, %q) = %v, want %v",
					tt.text, tt.term, tt.tokenizer, got, tt.want)
			}
		})
	}
}

func TestForEachChild(t *testing.T) {
	collect := func(e ast.Expr) []ast.Expr {
		var children []ast.Expr
		ForEachChild(e, func(child ast.Expr) {
			children = append(children, child)
		})
		return children
	}

	t.Run("BinaryExpr", func(t *testing.T) {
		left := &ast.IntLitExpr{Value: 1}
		right := &ast.IntLitExpr{Value: 2}
		expr := &ast.BinaryExpr{Left: left, Op: "=", Right: right}
		children := collect(expr)
		if len(children) != 2 {
			t.Fatalf("expected 2 children, got %d", len(children))
		}
		if children[0] != left || children[1] != right {
			t.Error("children do not match expected left and right")
		}
	})

	t.Run("LogicalExpr", func(t *testing.T) {
		left := &ast.BoolLitExpr{Value: true}
		right := &ast.BoolLitExpr{Value: false}
		expr := &ast.LogicalExpr{Left: left, Op: "AND", Right: right}
		children := collect(expr)
		if len(children) != 2 {
			t.Fatalf("expected 2 children, got %d", len(children))
		}
		if children[0] != left || children[1] != right {
			t.Error("children do not match expected left and right")
		}
	})

	t.Run("NotExpr", func(t *testing.T) {
		inner := &ast.BoolLitExpr{Value: true}
		expr := &ast.NotExpr{Expr: inner}
		children := collect(expr)
		if len(children) != 1 {
			t.Fatalf("expected 1 child, got %d", len(children))
		}
		if children[0] != inner {
			t.Error("child does not match expected inner expr")
		}
	})

	t.Run("InExpr", func(t *testing.T) {
		left := &ast.IdentExpr{Name: "id"}
		v1 := &ast.IntLitExpr{Value: 1}
		v2 := &ast.IntLitExpr{Value: 2}
		v3 := &ast.IntLitExpr{Value: 3}
		expr := &ast.InExpr{Left: left, Values: []ast.Expr{v1, v2, v3}}
		children := collect(expr)
		// 1 (left) + 3 (values) = 4
		if len(children) != 4 {
			t.Fatalf("expected 4 children, got %d", len(children))
		}
		if children[0] != left {
			t.Error("first child should be left")
		}
		if children[1] != v1 || children[2] != v2 || children[3] != v3 {
			t.Error("value children do not match")
		}
	})

	t.Run("BetweenExpr", func(t *testing.T) {
		left := &ast.IdentExpr{Name: "age"}
		low := &ast.IntLitExpr{Value: 10}
		high := &ast.IntLitExpr{Value: 20}
		expr := &ast.BetweenExpr{Left: left, Low: low, High: high}
		children := collect(expr)
		if len(children) != 3 {
			t.Fatalf("expected 3 children, got %d", len(children))
		}
		if children[0] != left || children[1] != low || children[2] != high {
			t.Error("children do not match expected left, low, high")
		}
	})

	t.Run("CallExpr", func(t *testing.T) {
		arg1 := &ast.IdentExpr{Name: "col1"}
		arg2 := &ast.IdentExpr{Name: "col2"}
		expr := &ast.CallExpr{Name: "SUM", Args: []ast.Expr{arg1, arg2}}
		children := collect(expr)
		if len(children) != 2 {
			t.Fatalf("expected 2 children, got %d", len(children))
		}
		if children[0] != arg1 || children[1] != arg2 {
			t.Error("children do not match expected args")
		}
	})

	t.Run("CaseExpr with operand and else", func(t *testing.T) {
		operand := &ast.IdentExpr{Name: "status"}
		when1 := &ast.IntLitExpr{Value: 1}
		then1 := &ast.StringLitExpr{Value: "one"}
		when2 := &ast.IntLitExpr{Value: 2}
		then2 := &ast.StringLitExpr{Value: "two"}
		elseExpr := &ast.StringLitExpr{Value: "other"}
		expr := &ast.CaseExpr{
			Operand: operand,
			Whens: []ast.CaseWhen{
				{When: when1, Then: then1},
				{When: when2, Then: then2},
			},
			Else: elseExpr,
		}
		children := collect(expr)
		// 1 (operand) + 2*2 (when/then pairs) + 1 (else) = 6
		if len(children) != 6 {
			t.Fatalf("expected 6 children, got %d", len(children))
		}
		if children[0] != operand {
			t.Error("first child should be operand")
		}
		if children[5] != elseExpr {
			t.Error("last child should be else")
		}
	})

	t.Run("leaf node IntLitExpr", func(t *testing.T) {
		expr := &ast.IntLitExpr{Value: 42}
		children := collect(expr)
		if len(children) != 0 {
			t.Errorf("expected 0 children for leaf node, got %d", len(children))
		}
	})

	t.Run("leaf node IdentExpr", func(t *testing.T) {
		expr := &ast.IdentExpr{Name: "col"}
		children := collect(expr)
		if len(children) != 0 {
			t.Errorf("expected 0 children for leaf node, got %d", len(children))
		}
	})

	t.Run("LikeExpr", func(t *testing.T) {
		left := &ast.IdentExpr{Name: "name"}
		pattern := &ast.StringLitExpr{Value: "%foo%"}
		expr := &ast.LikeExpr{Left: left, Pattern: pattern}
		children := collect(expr)
		if len(children) != 2 {
			t.Fatalf("expected 2 children, got %d", len(children))
		}
		if children[0] != left || children[1] != pattern {
			t.Error("children do not match expected left and pattern")
		}
	})
}
