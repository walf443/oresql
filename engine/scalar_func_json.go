package engine

import (
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/scalar"
	"github.com/walf443/oresql/json_path"
)

// tryCompileJSONPath checks if the second argument of a JSON function call
// is a string literal, and if so, pre-parses the path for reuse across rows.
func tryCompileJSONPath(call *ast.CallExpr) *json_path.Path {
	return scalar.TryCompileJSONPath(call)
}

// isValidJSON returns true if val is a string containing valid JSON.
func isValidJSON(val Value) bool {
	return scalar.IsValidJSON(val)
}
