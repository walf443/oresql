// Package scalar provides scalar function implementations for SQL evaluation
// with no dependency on the engine package.
package scalar

import (
	"fmt"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/json_path"
)

// Value is an alias for any, matching storage.Value.
type Value = any

// Func is a scalar function that takes pre-evaluated arguments and returns a value.
type Func func(args []Value) (Value, error)

// Registry maps uppercase function names to their implementations.
// Functions registered here have their arguments eagerly evaluated before dispatch.
var Registry = map[string]Func{
	// Numeric functions
	"ABS":   evalFuncAbs,
	"ROUND": evalFuncRound,
	"MOD":   evalFuncMod,
	"CEIL":  evalFuncCeil,
	"FLOOR": evalFuncFloor,
	"POWER": evalFuncPower,
	// String functions
	"LENGTH":    evalFuncLength,
	"UPPER":     evalFuncUpper,
	"LOWER":     evalFuncLower,
	"SUBSTRING": evalFuncSubstring,
	"TRIM":      evalFuncTrim,
	"CONCAT":    evalFuncConcat,
	// JSON functions
	"JSON_OBJECT": evalFuncJSONObject,
	"JSON_ARRAY":  evalFuncJSONArray,
}

// EvalArgsWith evaluates argument expressions using a custom evaluator function.
func EvalArgsWith(args []ast.Expr, evalFn func(ast.Expr) (Value, error)) ([]Value, error) {
	vals := make([]Value, len(args))
	for i, arg := range args {
		val, err := evalFn(arg)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

// EvalJSONPathFunc dispatches JSON path functions (JSON_VALUE, JSON_QUERY, JSON_EXISTS).
func EvalJSONPathFunc(name string, args []Value, compiled *json_path.Path) (Value, error) {
	switch name {
	case "JSON_VALUE":
		return evalFuncJSONValue(args, compiled)
	case "JSON_QUERY":
		return evalFuncJSONQuery(args, compiled)
	case "JSON_EXISTS":
		return evalFuncJSONExists(args, compiled)
	default:
		return nil, fmt.Errorf("unknown JSON path function: %s", name)
	}
}

// TryCompileJSONPath checks if the second argument of a JSON function call
// is a string literal, and if so, pre-parses the path for reuse across rows.
func TryCompileJSONPath(call *ast.CallExpr) *json_path.Path {
	if len(call.Args) >= 2 {
		if lit, ok := call.Args[1].(*ast.StringLitExpr); ok {
			if p, err := json_path.Parse(lit.Value); err == nil {
				return p
			}
		}
	}
	return nil
}

// IsValidJSON returns true if val is a string containing valid JSON.
func IsValidJSON(val Value) bool {
	switch v := val.(type) {
	case string:
		return jsonValid(v)
	case []byte:
		return true
	default:
		return false
	}
}
