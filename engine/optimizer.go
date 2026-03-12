package engine

import (
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/optimize"
)

// optimizeStatement applies constant folding to WHERE/HAVING clauses in a statement.
func optimizeStatement(stmt ast.Statement) { optimize.Statement(stmt) }
