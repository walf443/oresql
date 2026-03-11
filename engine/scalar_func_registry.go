package engine

import (
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/scalar"
	"github.com/walf443/oresql/json_path"
)

// ScalarFunc is a scalar function that takes pre-evaluated arguments and returns a value.
type ScalarFunc = scalar.Func

// scalarFuncRegistry maps uppercase function names to their implementations.
var scalarFuncRegistry = scalar.Registry

// evalArgsWith evaluates argument expressions using a custom evaluator function.
func evalArgsWith(args []ast.Expr, evalFn func(ast.Expr) (Value, error)) ([]Value, error) {
	return scalar.EvalArgsWith(args, evalFn)
}

// evalJSONPathFunc dispatches JSON path functions (JSON_VALUE, JSON_QUERY, JSON_EXISTS).
func evalJSONPathFunc(name string, args []Value, compiled *json_path.Path) (Value, error) {
	return scalar.EvalJSONPathFunc(name, args, compiled)
}
