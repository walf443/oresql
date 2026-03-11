package engine

import (
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/optimize"
)

// optimizeStatement applies constant folding to WHERE/HAVING clauses in a statement.
func optimizeStatement(stmt ast.Statement) { optimize.Statement(stmt) }

// optimizeExpr recursively rewrites an expression tree by folding constant expressions.
func optimizeExpr(e ast.Expr) ast.Expr { return optimize.Expr(e) }
