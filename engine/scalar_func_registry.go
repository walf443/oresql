package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/json_path"
)

// ScalarFunc is a scalar function that takes pre-evaluated arguments and returns a value.
type ScalarFunc func(args []Value) (Value, error)

// scalarFuncRegistry maps uppercase function names to their implementations.
// Functions registered here have their arguments eagerly evaluated before dispatch.
var scalarFuncRegistry = map[string]ScalarFunc{
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

// evalArgsWith evaluates argument expressions using a custom evaluator function.
func evalArgsWith(args []ast.Expr, evalFn func(ast.Expr) (Value, error)) ([]Value, error) {
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

// evalJSONPathFunc dispatches JSON path functions (JSON_VALUE, JSON_QUERY, JSON_EXISTS).
func evalJSONPathFunc(name string, args []Value, compiled *json_path.Path) (Value, error) {
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
