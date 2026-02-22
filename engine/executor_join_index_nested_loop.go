package engine

import (
	"sort"
	"strings"

	"github.com/walf443/oresql/ast"
)

// equiJoinPair represents a pair of columns from two tables used in an equi-join.
// leftTable/leftCol always refer to the FROM table, rightTable/rightCol to the JOIN table.
type equiJoinPair struct {
	leftTable, leftCol   string // lowercase table name, lowercase column name
	rightTable, rightCol string
}

// joinTableInfo holds table metadata for join optimization.
type joinTableInfo struct {
	info      *TableInfo
	tableName string // lowercase
	alias     string
}

// matchesTable returns true if the given qualifier (table name or alias) matches this table.
func (jt *joinTableInfo) matchesTable(qualifier string) bool {
	if qualifier == "" {
		return false
	}
	lower := strings.ToLower(qualifier)
	return lower == jt.tableName || (jt.alias != "" && lower == strings.ToLower(jt.alias))
}

// flattenAND decomposes an AND-connected expression tree into a flat slice.
// Non-AND expressions (including OR) are returned as a single-element slice.
func flattenAND(expr ast.Expr) []ast.Expr {
	if expr == nil {
		return nil
	}
	logical, ok := expr.(*ast.LogicalExpr)
	if !ok || logical.Op != "AND" {
		return []ast.Expr{expr}
	}
	var result []ast.Expr
	result = append(result, flattenAND(logical.Left)...)
	result = append(result, flattenAND(logical.Right)...)
	return result
}

// collectTableRefs collects all table qualifiers (IdentExpr.Table) referenced in an expression.
// Only non-empty qualifiers are included. Keys are lowercased.
func collectTableRefs(expr ast.Expr) map[string]bool {
	refs := make(map[string]bool)
	collectTableRefsRecursive(expr, refs)
	return refs
}

func collectTableRefsRecursive(expr ast.Expr, refs map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if e.Table != "" {
			refs[strings.ToLower(e.Table)] = true
		}
	case *ast.BinaryExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Right, refs)
	case *ast.LogicalExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Right, refs)
	case *ast.NotExpr:
		collectTableRefsRecursive(e.Expr, refs)
	case *ast.IsNullExpr:
		collectTableRefsRecursive(e.Expr, refs)
	case *ast.InExpr:
		collectTableRefsRecursive(e.Left, refs)
		for _, v := range e.Values {
			collectTableRefsRecursive(v, refs)
		}
	case *ast.BetweenExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Low, refs)
		collectTableRefsRecursive(e.High, refs)
	case *ast.LikeExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Pattern, refs)
	case *ast.ArithmeticExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Right, refs)
	}
}

// collectUnqualifiedIdents collects all unqualified column names from an expression.
func collectUnqualifiedIdents(expr ast.Expr) []string {
	var names []string
	collectUnqualifiedIdentsRecursive(expr, &names)
	return names
}

func collectUnqualifiedIdentsRecursive(expr ast.Expr, names *[]string) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if e.Table == "" {
			*names = append(*names, strings.ToLower(e.Name))
		}
	case *ast.BinaryExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Right, names)
	case *ast.LogicalExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Right, names)
	case *ast.NotExpr:
		collectUnqualifiedIdentsRecursive(e.Expr, names)
	case *ast.IsNullExpr:
		collectUnqualifiedIdentsRecursive(e.Expr, names)
	case *ast.InExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		for _, v := range e.Values {
			collectUnqualifiedIdentsRecursive(v, names)
		}
	case *ast.BetweenExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Low, names)
		collectUnqualifiedIdentsRecursive(e.High, names)
	case *ast.LikeExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Pattern, names)
	case *ast.ArithmeticExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Right, names)
	}
}

// stripTableQualifier removes table qualifiers that match the given table name or alias.
// This is needed because evalExpr/evalWhere use validateTableRef which doesn't know about aliases.
func stripTableQualifier(expr ast.Expr, tableName, alias string) ast.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if e.Table == "" {
			return e
		}
		lower := strings.ToLower(e.Table)
		if lower == strings.ToLower(tableName) || (alias != "" && lower == strings.ToLower(alias)) {
			return &ast.IdentExpr{Name: e.Name}
		}
		return e
	case *ast.BinaryExpr:
		return &ast.BinaryExpr{
			Left:  stripTableQualifier(e.Left, tableName, alias),
			Op:    e.Op,
			Right: stripTableQualifier(e.Right, tableName, alias),
		}
	case *ast.LogicalExpr:
		return &ast.LogicalExpr{
			Left:  stripTableQualifier(e.Left, tableName, alias),
			Op:    e.Op,
			Right: stripTableQualifier(e.Right, tableName, alias),
		}
	case *ast.NotExpr:
		return &ast.NotExpr{Expr: stripTableQualifier(e.Expr, tableName, alias)}
	case *ast.IsNullExpr:
		return &ast.IsNullExpr{Expr: stripTableQualifier(e.Expr, tableName, alias), Not: e.Not}
	case *ast.InExpr:
		newVals := make([]ast.Expr, len(e.Values))
		for i, v := range e.Values {
			newVals[i] = stripTableQualifier(v, tableName, alias)
		}
		return &ast.InExpr{Left: stripTableQualifier(e.Left, tableName, alias), Values: newVals, Not: e.Not}
	case *ast.BetweenExpr:
		return &ast.BetweenExpr{
			Left: stripTableQualifier(e.Left, tableName, alias),
			Low:  stripTableQualifier(e.Low, tableName, alias),
			High: stripTableQualifier(e.High, tableName, alias),
			Not:  e.Not,
		}
	case *ast.LikeExpr:
		return &ast.LikeExpr{
			Left:    stripTableQualifier(e.Left, tableName, alias),
			Pattern: stripTableQualifier(e.Pattern, tableName, alias),
			Not:     e.Not,
		}
	case *ast.ArithmeticExpr:
		return &ast.ArithmeticExpr{
			Left:  stripTableQualifier(e.Left, tableName, alias),
			Op:    e.Op,
			Right: stripTableQualifier(e.Right, tableName, alias),
		}
	default:
		return expr
	}
}

// combineExprsAND combines a slice of expressions into a single AND-connected expression.
// Returns nil for empty slice, the single element for one, or a chain of LogicalExpr for multiple.
func combineExprsAND(exprs []ast.Expr) ast.Expr {
	if len(exprs) == 0 {
		return nil
	}
	result := exprs[0]
	for i := 1; i < len(exprs); i++ {
		result = &ast.LogicalExpr{Left: result, Op: "AND", Right: exprs[i]}
	}
	return result
}

// resolveUnqualifiedTable determines which table an unqualified column belongs to.
// Returns "from", "join", or "" (ambiguous/unknown).
func resolveUnqualifiedTable(colName string, fromTable, joinTable *joinTableInfo) string {
	lower := strings.ToLower(colName)
	inFrom := false
	inJoin := false
	for _, col := range fromTable.info.Columns {
		if strings.ToLower(col.Name) == lower {
			inFrom = true
			break
		}
	}
	for _, col := range joinTable.info.Columns {
		if strings.ToLower(col.Name) == lower {
			inJoin = true
			break
		}
	}
	if inFrom && !inJoin {
		return "from"
	}
	if inJoin && !inFrom {
		return "join"
	}
	return "" // ambiguous or not found
}

// classifyWhereConditions classifies WHERE conditions into FROM-only, JOIN-only, and cross-table.
// Conditions referencing only one table are pushed down to that table.
// Conditions referencing both tables or ambiguous columns go to cross-table.
func classifyWhereConditions(
	where ast.Expr,
	fromTable, joinTable *joinTableInfo,
) (fromConds, joinConds, crossConds []ast.Expr) {
	conds := flattenAND(where)
	for _, cond := range conds {
		refs := collectTableRefs(cond)
		unqualified := collectUnqualifiedIdents(cond)

		// Determine which tables are referenced
		refsFrom := false
		refsJoin := false
		ambiguous := false

		for ref := range refs {
			if fromTable.matchesTable(ref) {
				refsFrom = true
			} else if joinTable.matchesTable(ref) {
				refsJoin = true
			}
		}

		// Resolve unqualified columns
		for _, name := range unqualified {
			target := resolveUnqualifiedTable(name, fromTable, joinTable)
			switch target {
			case "from":
				refsFrom = true
			case "join":
				refsJoin = true
			default:
				ambiguous = true
			}
		}

		if ambiguous || (refsFrom && refsJoin) {
			crossConds = append(crossConds, cond)
		} else if refsFrom {
			fromConds = append(fromConds, cond)
		} else if refsJoin {
			joinConds = append(joinConds, cond)
		} else {
			// No table refs at all (e.g., 1=1) — treat as cross-table
			crossConds = append(crossConds, cond)
		}
	}
	return
}

// extractEquiJoinPair extracts an equi-join pair from an ON condition.
// It looks for a pattern like t1.col = t2.col where t1 is fromTable and t2 is joinTable (or reversed).
// Returns the pair and any remaining ON conditions as residual.
// The pair is normalized so leftTable=from, rightTable=join.
func extractEquiJoinPair(
	on ast.Expr,
	fromTable, joinTable *joinTableInfo,
) (*equiJoinPair, ast.Expr) {
	conds := flattenAND(on)

	var pair *equiJoinPair
	var residuals []ast.Expr

	for _, cond := range conds {
		if pair != nil {
			residuals = append(residuals, cond)
			continue
		}
		bin, ok := cond.(*ast.BinaryExpr)
		if !ok || bin.Op != "=" {
			residuals = append(residuals, cond)
			continue
		}
		leftIdent, leftOk := bin.Left.(*ast.IdentExpr)
		rightIdent, rightOk := bin.Right.(*ast.IdentExpr)
		if !leftOk || !rightOk {
			residuals = append(residuals, cond)
			continue
		}
		if leftIdent.Table == "" || rightIdent.Table == "" {
			residuals = append(residuals, cond)
			continue
		}

		leftIsFrom := fromTable.matchesTable(leftIdent.Table)
		leftIsJoin := joinTable.matchesTable(leftIdent.Table)
		rightIsFrom := fromTable.matchesTable(rightIdent.Table)
		rightIsJoin := joinTable.matchesTable(rightIdent.Table)

		if leftIsFrom && rightIsJoin {
			pair = &equiJoinPair{
				leftTable:  fromTable.tableName,
				leftCol:    strings.ToLower(leftIdent.Name),
				rightTable: joinTable.tableName,
				rightCol:   strings.ToLower(rightIdent.Name),
			}
		} else if leftIsJoin && rightIsFrom {
			pair = &equiJoinPair{
				leftTable:  fromTable.tableName,
				leftCol:    strings.ToLower(rightIdent.Name),
				rightTable: joinTable.tableName,
				rightCol:   strings.ToLower(leftIdent.Name),
			}
		} else {
			residuals = append(residuals, cond)
		}
	}

	return pair, combineExprsAND(residuals)
}

// scoreJoinOrder scores a candidate join order where outerInfo is the driving table
// and innerInfo is the inner (lookup) table.
func (e *Executor) scoreJoinOrder(
	outerInfo, innerInfo *joinTableInfo,
	outerWhere []ast.Expr,
	equiJoin *equiJoinPair,
) int {
	score := 0

	// +1 if outer table has WHERE pushdown conditions
	if len(outerWhere) > 0 {
		score++
	}

	// +2 if outer table's WHERE conditions can use an index
	if len(outerWhere) > 0 {
		combined := combineExprsAND(outerWhere)
		stripped := stripTableQualifier(combined, outerInfo.tableName, outerInfo.alias)
		if _, ok := e.tryIndexScan(stripped, outerInfo.info); ok {
			score += 2
		}
	}

	// +3 if inner table has an index on the equi-join column
	if equiJoin != nil {
		var innerCol string
		if outerInfo.tableName == equiJoin.leftTable {
			innerCol = equiJoin.rightCol
		} else {
			innerCol = equiJoin.leftCol
		}
		col, err := innerInfo.info.FindColumn(innerCol)
		if err == nil {
			idx := e.storage.LookupSingleColumnIndex(innerInfo.info.Name, col.Index)
			if idx != nil {
				score += 3
			}
		}
	}

	return score
}

// executeOptimizedTwoTableJoin executes an optimized 2-table INNER JOIN.
func (e *Executor) executeOptimizedTwoTableJoin(stmt *ast.SelectStmt) (*Result, error) {
	join := stmt.Joins[0]

	fromInfo, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	joinInfo, err := e.catalog.GetTable(join.TableName)
	if err != nil {
		return nil, err
	}

	fromTable := &joinTableInfo{
		info:      fromInfo,
		tableName: strings.ToLower(stmt.TableName),
		alias:     stmt.TableAlias,
	}
	joinTable := &joinTableInfo{
		info:      joinInfo,
		tableName: strings.ToLower(join.TableName),
		alias:     join.TableAlias,
	}

	// 1. Extract equi-join pair and residual ON conditions
	equiJoin, residualON := extractEquiJoinPair(join.On, fromTable, joinTable)

	// 2. Classify WHERE conditions
	var fromWhere, joinWhere, crossWhere []ast.Expr
	if stmt.Where != nil {
		fromWhere, joinWhere, crossWhere = classifyWhereConditions(stmt.Where, fromTable, joinTable)
	}

	// 3. Determine driving table order
	scoreFromFirst := e.scoreJoinOrder(fromTable, joinTable, fromWhere, equiJoin)
	scoreJoinFirst := e.scoreJoinOrder(joinTable, fromTable, joinWhere, equiJoin)

	var outerTbl, innerTbl *joinTableInfo
	var outerWhere, innerWhere []ast.Expr
	reversed := false

	if scoreJoinFirst > scoreFromFirst {
		outerTbl = joinTable
		innerTbl = fromTable
		outerWhere = joinWhere
		innerWhere = fromWhere
		reversed = true
	} else {
		outerTbl = fromTable
		innerTbl = joinTable
		outerWhere = fromWhere
		innerWhere = joinWhere
	}

	// 4. Scan outer (driving) table
	var outerRows []Row
	if len(outerWhere) > 0 {
		combined := combineExprsAND(outerWhere)
		stripped := stripTableQualifier(combined, outerTbl.tableName, outerTbl.alias)
		if keys, ok := e.tryIndexScan(stripped, outerTbl.info); ok {
			outerRows, err = e.storage.GetByKeys(outerTbl.info.Name, keys)
			if err != nil {
				return nil, err
			}
		} else {
			outerRows, err = e.storage.Scan(outerTbl.info.Name)
			if err != nil {
				return nil, err
			}
		}
		// Apply full WHERE filter (index may not cover all conditions)
		var filtered []Row
		for _, row := range outerRows {
			match, mErr := evalWhere(stripped, row, outerTbl.info)
			if mErr != nil {
				return nil, mErr
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		outerRows = filtered
	} else {
		outerRows, err = e.storage.Scan(outerTbl.info.Name)
		if err != nil {
			return nil, err
		}
	}

	// 5. Determine inner table lookup strategy
	var innerEquiCol string
	var outerEquiCol string
	var innerIdx *SecondaryIndex

	if equiJoin != nil {
		if outerTbl.tableName == equiJoin.leftTable {
			outerEquiCol = equiJoin.leftCol
			innerEquiCol = equiJoin.rightCol
		} else {
			outerEquiCol = equiJoin.rightCol
			innerEquiCol = equiJoin.leftCol
		}
		col, findErr := innerTbl.info.FindColumn(innerEquiCol)
		if findErr == nil {
			innerIdx = e.storage.LookupSingleColumnIndex(innerTbl.info.Name, col.Index)
		}
	}

	// Pre-filter inner rows if no index lookup
	var preFilteredInner []Row
	if innerIdx == nil {
		preFilteredInner, err = e.storage.Scan(innerTbl.info.Name)
		if err != nil {
			return nil, err
		}
		if len(innerWhere) > 0 {
			combined := combineExprsAND(innerWhere)
			stripped := stripTableQualifier(combined, innerTbl.tableName, innerTbl.alias)
			var filtered []Row
			for _, row := range preFilteredInner {
				match, mErr := evalWhere(stripped, row, innerTbl.info)
				if mErr != nil {
					return nil, mErr
				}
				if match {
					filtered = append(filtered, row)
				}
			}
			preFilteredInner = filtered
		}
	}

	// Resolve outer equi-join column index
	outerEquiColIdx := -1
	if equiJoin != nil {
		col, findErr := outerTbl.info.FindColumn(outerEquiCol)
		if findErr == nil {
			outerEquiColIdx = col.Index
		}
	}

	// Prepare inner WHERE for filtering index-looked-up rows
	var innerWhereStripped ast.Expr
	if innerIdx != nil && len(innerWhere) > 0 {
		combined := combineExprsAND(innerWhere)
		innerWhereStripped = stripTableQualifier(combined, innerTbl.tableName, innerTbl.alias)
	}

	// Build JoinContext for post-filter evaluation (always FROM + JOIN order)
	jc := newJoinContext([]struct {
		info  *TableInfo
		alias string
	}{
		{info: fromInfo, alias: stmt.TableAlias},
		{info: joinInfo, alias: join.TableAlias},
	})

	// Combine post-filter conditions: cross-table WHERE + residual ON
	var postFilterExprs []ast.Expr
	postFilterExprs = append(postFilterExprs, crossWhere...)
	if residualON != nil {
		postFilterExprs = append(postFilterExprs, residualON)
	}
	postFilter := combineExprsAND(postFilterExprs)

	// 6. Nested loop join
	var joined []Row

	for _, outerRow := range outerRows {
		var innerCandidates []Row

		if innerIdx != nil && equiJoin != nil && outerEquiColIdx >= 0 {
			// Index nested loop: look up inner table by equi-join value
			lookupVal := outerRow[outerEquiColIdx]
			if lookupVal == nil {
				// NULL = NULL is false in SQL, skip
				continue
			}
			keys := innerIdx.Lookup([]Value{lookupVal})
			if len(keys) == 0 {
				continue
			}
			innerCandidates, err = e.storage.GetByKeys(innerTbl.info.Name, keys)
			if err != nil {
				return nil, err
			}
			// Apply inner WHERE filter
			if innerWhereStripped != nil {
				var filtered []Row
				for _, row := range innerCandidates {
					match, mErr := evalWhere(innerWhereStripped, row, innerTbl.info)
					if mErr != nil {
						return nil, mErr
					}
					if match {
						filtered = append(filtered, row)
					}
				}
				innerCandidates = filtered
			}
		} else {
			innerCandidates = preFilteredInner
		}

		for _, innerRow := range innerCandidates {
			// Build merged row in FROM + JOIN order regardless of driving table
			var fromRow, joinRow Row
			if reversed {
				fromRow = innerRow
				joinRow = outerRow
			} else {
				fromRow = outerRow
				joinRow = innerRow
			}
			mergedRow := make(Row, len(fromRow)+len(joinRow))
			copy(mergedRow, fromRow)
			copy(mergedRow[len(fromRow):], joinRow)

			// Apply equi-join condition for non-index path
			if innerIdx == nil && equiJoin != nil {
				match, mErr := evalWhereJoin(join.On, mergedRow, jc)
				if mErr != nil {
					return nil, mErr
				}
				if !match {
					continue
				}
			}

			// For non-equi join, apply full ON condition
			if equiJoin == nil {
				match, mErr := evalWhereJoin(join.On, mergedRow, jc)
				if mErr != nil {
					return nil, mErr
				}
				if !match {
					continue
				}
			}

			// Apply post-filter (cross-table WHERE + residual ON)
			if postFilter != nil {
				match, mErr := evalWhereJoin(postFilter, mergedRow, jc)
				if mErr != nil {
					return nil, mErr
				}
				if !match {
					continue
				}
			}

			joined = append(joined, mergedRow)
		}
	}

	// 7. Delegate to post-join processing (ORDER BY, LIMIT, OFFSET, projection, DISTINCT)
	return e.postJoinProcess(stmt, joined, jc)
}

// postJoinProcess handles ORDER BY, OFFSET, LIMIT, projection, and DISTINCT after join.
func (e *Executor) postJoinProcess(stmt *ast.SelectStmt, rows []Row, jc *JoinContext) (*Result, error) {
	// Sort by ORDER BY
	if len(stmt.OrderBy) > 0 {
		var sortErr error
		sort.SliceStable(rows, func(i, j int) bool {
			if sortErr != nil {
				return false
			}
			for _, ob := range stmt.OrderBy {
				vi, err := evalExprJoin(ob.Expr, rows[i], jc)
				if err != nil {
					sortErr = err
					return false
				}
				vj, err := evalExprJoin(ob.Expr, rows[j], jc)
				if err != nil {
					sortErr = err
					return false
				}
				if vi == nil && vj == nil {
					continue
				}
				if vi == nil {
					return false
				}
				if vj == nil {
					return true
				}
				cmp := compareValues(vi, vj)
				if cmp == 0 {
					continue
				}
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
		if sortErr != nil {
			return nil, sortErr
		}
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		off := int(*stmt.Offset)
		if off >= len(rows) {
			rows = nil
		} else {
			rows = rows[off:]
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		lim := int(*stmt.Limit)
		if lim < len(rows) {
			rows = rows[:lim]
		}
	}

	// Resolve column names and project
	var colNames []string
	var colExprs []ast.Expr
	isStar := false

	if len(stmt.Columns) == 1 {
		if _, ok := stmt.Columns[0].(*ast.StarExpr); ok {
			isStar = true
			for _, col := range jc.MergedInfo.Columns {
				colNames = append(colNames, col.Name)
			}
		}
	}

	if !isStar {
		for _, colExpr := range stmt.Columns {
			alias := ""
			inner := colExpr
			if a, ok := colExpr.(*ast.AliasExpr); ok {
				alias = a.Alias
				inner = a.Expr
			}
			colExprs = append(colExprs, inner)
			if alias != "" {
				colNames = append(colNames, alias)
			} else if ident, ok := inner.(*ast.IdentExpr); ok {
				col, err := jc.FindColumn(ident.Table, ident.Name)
				if err != nil {
					return nil, err
				}
				colNames = append(colNames, col.Name)
			} else {
				colNames = append(colNames, formatExpr(inner))
			}
		}
	}

	// Project columns
	var resultRows []Row
	for _, row := range rows {
		if isStar {
			projected := make(Row, len(jc.MergedInfo.Columns))
			copy(projected, row)
			resultRows = append(resultRows, projected)
		} else {
			projected := make(Row, len(colExprs))
			for i, expr := range colExprs {
				val, err := evalExprJoin(expr, row, jc)
				if err != nil {
					return nil, err
				}
				projected[i] = val
			}
			resultRows = append(resultRows, projected)
		}
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = dedup(resultRows)
	}

	return &Result{Columns: colNames, Rows: resultRows}, nil
}
