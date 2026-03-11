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

// evalArgs evaluates all argument expressions and returns the resulting values.
func evalArgs(args []ast.Expr, row Row, info *TableInfo) ([]Value, error) {
	vals := make([]Value, len(args))
	for i, arg := range args {
		val, err := evalExpr(arg, row, info)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
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

// evalScalarFunc evaluates a scalar function call against a single row.
func evalScalarFunc(call *ast.CallExpr, row Row, info *TableInfo) (Value, error) {
	// Special-case functions that need lazy evaluation or extra context.
	switch call.Name {
	case "COALESCE":
		return evalFuncCoalesce(call.Args, row, info)
	case "NULLIF":
		return evalFuncNullif(call.Args, row, info)
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
		args, err := evalArgs(call.Args, row, info)
		if err != nil {
			return nil, err
		}
		compiled := tryCompileJSONPath(call)
		return evalJSONPathFunc(call.Name, args, compiled)
	}

	// Registry-based dispatch for standard scalar functions.
	if fn, ok := scalarFuncRegistry[call.Name]; ok {
		args, err := evalArgs(call.Args, row, info)
		if err != nil {
			return nil, err
		}
		return fn(args)
	}

	return nil, fmt.Errorf("aggregate function %s not allowed in this context", call.Name)
}

// evalFuncCoalesce returns the first non-NULL argument (lazy evaluation).
func evalFuncCoalesce(exprs []ast.Expr, row Row, info *TableInfo) (Value, error) {
	for _, expr := range exprs {
		val, err := evalExpr(expr, row, info)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	return nil, nil
}

// evalFuncNullif returns NULL if the two arguments are equal, otherwise the first argument.
func evalFuncNullif(exprs []ast.Expr, row Row, info *TableInfo) (Value, error) {
	if len(exprs) != 2 {
		return nil, fmt.Errorf("NULLIF requires exactly 2 arguments, got %d", len(exprs))
	}
	val1, err := evalExpr(exprs[0], row, info)
	if err != nil {
		return nil, err
	}
	val2, err := evalExpr(exprs[1], row, info)
	if err != nil {
		return nil, err
	}
	if val1 == nil || val2 == nil {
		return val1, nil
	}
	eq, err := evalComparison(val1, "=", val2)
	if err != nil {
		return val1, nil
	}
	if eq {
		return nil, nil
	}
	return val1, nil
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
