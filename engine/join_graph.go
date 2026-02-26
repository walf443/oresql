package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// JoinGraphNode represents a single table in the join graph.
type JoinGraphNode struct {
	TableName  string     // lowercase real table name
	Alias      string     // alias (empty if none)
	Info       *TableInfo // schema info
	Rows       []Row      // pre-materialized rows (non-nil for FROM subquery)
	LocalWhere []ast.Expr // WHERE conditions referencing only this table
}

// effectiveName returns the alias if present, otherwise the table name.
func (n *JoinGraphNode) effectiveName() string {
	if n.Alias != "" {
		return strings.ToLower(n.Alias)
	}
	return n.TableName
}

// JoinGraphEdge represents an ON relationship between two tables.
type JoinGraphEdge struct {
	TableA, TableB string         // effective names (alias preferred)
	JoinType       string         // ast.JoinInner or ast.JoinLeft
	OnExpr         ast.Expr       // original ON expression
	EquiJoinPairs  []equiJoinPair // equi-join pairs extracted from ON
	ResidualOn     ast.Expr       // ON conditions not covered by equi-join pairs
	IndexOnA       bool           // whether A's equi-join column has an index
	IndexOnB       bool           // whether B's equi-join column has an index
}

// JoinGraph represents the complete join plan structure.
type JoinGraph struct {
	Nodes      map[string]*JoinGraphNode // effective name -> node
	Edges      map[string]*JoinGraphEdge // "tableA\x00tableB" -> edge
	Adjacency  map[string][]string       // effective name -> connected effective names
	CrossWhere []ast.Expr                // WHERE conditions spanning multiple tables
	FromTable  string                    // effective name of the FROM table
	TableOrder []string                  // original FROM + JOIN1 + JOIN2... order (effective names)
}

// edgeKey returns a canonical key for an edge between two tables.
// The key is ordered lexicographically to ensure consistency.
func edgeKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "\x00" + b
}

// extractAllEquiJoinPairs extracts all equi-join pairs from an ON condition
// between two tables identified by their joinTableInfo.
// Returns the list of pairs and any residual ON conditions.
func extractAllEquiJoinPairs(
	on ast.Expr,
	tableA, tableB *joinTableInfo,
) ([]equiJoinPair, ast.Expr) {
	conds := flattenAND(on)

	var pairs []equiJoinPair
	var residuals []ast.Expr

	for _, cond := range conds {
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

		leftIsA := tableA.matchesTable(leftIdent.Table)
		leftIsB := tableB.matchesTable(leftIdent.Table)
		rightIsA := tableA.matchesTable(rightIdent.Table)
		rightIsB := tableB.matchesTable(rightIdent.Table)

		if leftIsA && rightIsB {
			pairs = append(pairs, equiJoinPair{
				leftTable:  tableA.tableName,
				leftCol:    strings.ToLower(leftIdent.Name),
				rightTable: tableB.tableName,
				rightCol:   strings.ToLower(rightIdent.Name),
			})
		} else if leftIsB && rightIsA {
			pairs = append(pairs, equiJoinPair{
				leftTable:  tableA.tableName,
				leftCol:    strings.ToLower(rightIdent.Name),
				rightTable: tableB.tableName,
				rightCol:   strings.ToLower(leftIdent.Name),
			})
		} else {
			residuals = append(residuals, cond)
		}
	}

	return pairs, combineExprsAND(residuals)
}

// resolveUnqualifiedTableN determines which table an unqualified column belongs to
// among N tables. Returns the effective name of the table, or "" if ambiguous or not found.
func resolveUnqualifiedTableN(colName string, nodes map[string]*JoinGraphNode) string {
	lower := strings.ToLower(colName)
	var found string
	count := 0
	for effName, node := range nodes {
		for _, col := range node.Info.Columns {
			if strings.ToLower(col.Name) == lower {
				found = effName
				count++
				break
			}
		}
	}
	if count == 1 {
		return found
	}
	return "" // ambiguous or not found
}

// classifyWhereConditionsN classifies WHERE conditions for N tables.
// Each condition is assigned to a single table's LocalWhere if it references only that table,
// otherwise it goes to crossWhere.
func classifyWhereConditionsN(
	where ast.Expr,
	graph *JoinGraph,
) (localWhereMap map[string][]ast.Expr, crossWhere []ast.Expr) {
	localWhereMap = make(map[string][]ast.Expr)
	conds := flattenAND(where)

	// Build a map from table name/alias to effective name for quick lookup
	nameToEffective := make(map[string]string)
	for effName, node := range graph.Nodes {
		nameToEffective[node.TableName] = effName
		if node.Alias != "" {
			nameToEffective[strings.ToLower(node.Alias)] = effName
		}
	}

	for _, cond := range conds {
		// Conditions containing subqueries must go to crossWhere because
		// the local WHERE path (evalExpr) cannot handle subquery expressions.
		if containsSubquery(cond) {
			crossWhere = append(crossWhere, cond)
			continue
		}

		refs := collectTableRefs(cond)
		unqualified := collectUnqualifiedIdents(cond)

		referencedTables := make(map[string]bool)
		ambiguous := false

		// Resolve qualified references
		for ref := range refs {
			if effName, ok := nameToEffective[ref]; ok {
				referencedTables[effName] = true
			} else {
				ambiguous = true
			}
		}

		// Resolve unqualified references
		for _, name := range unqualified {
			target := resolveUnqualifiedTableN(name, graph.Nodes)
			if target == "" {
				ambiguous = true
			} else {
				referencedTables[target] = true
			}
		}

		if ambiguous || len(referencedTables) > 1 || len(referencedTables) == 0 {
			crossWhere = append(crossWhere, cond)
		} else {
			// Exactly one table referenced
			for effName := range referencedTables {
				localWhereMap[effName] = append(localWhereMap[effName], cond)
			}
		}
	}
	return
}

// buildJoinGraph constructs a JoinGraph from a SELECT statement with JOINs.
func (e *Executor) buildJoinGraph(stmt *ast.SelectStmt) (*JoinGraph, error) {
	graph := &JoinGraph{
		Nodes:     make(map[string]*JoinGraphNode),
		Edges:     make(map[string]*JoinGraphEdge),
		Adjacency: make(map[string][]string),
	}

	// 1. Create node for FROM table
	var fromInfo *TableInfo
	var fromRows []Row
	if stmt.FromSubquery != nil {
		var err error
		fromInfo, fromRows, err = e.materializeSubquery(stmt.FromSubquery, stmt.TableAlias)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		fromInfo, err = e.catalog.GetTable(stmt.TableName)
		if err != nil {
			return nil, err
		}
	}
	fromNode := &JoinGraphNode{
		TableName: strings.ToLower(fromInfo.Name),
		Alias:     stmt.TableAlias,
		Info:      fromInfo,
		Rows:      fromRows,
	}
	fromEffName := fromNode.effectiveName()
	graph.Nodes[fromEffName] = fromNode
	graph.FromTable = fromEffName
	graph.TableOrder = append(graph.TableOrder, fromEffName)

	// 2. Create nodes for each JOIN table
	for _, join := range stmt.Joins {
		joinInfo, err := e.catalog.GetTable(join.TableName)
		if err != nil {
			return nil, err
		}
		joinNode := &JoinGraphNode{
			TableName: strings.ToLower(join.TableName),
			Alias:     join.TableAlias,
			Info:      joinInfo,
		}
		effName := joinNode.effectiveName()
		graph.Nodes[effName] = joinNode
		graph.TableOrder = append(graph.TableOrder, effName)
	}

	// 3. Create edges from ON clauses
	// For multi-table JOINs, each ON clause refers to the JOIN table and some previously joined table.
	// We need to figure out which two tables each ON clause connects.
	for _, join := range stmt.Joins {
		joinNode := graph.Nodes[effectiveNameForJoin(join)]

		// Determine which tables the ON clause references
		var tableAName, tableBName string
		if join.On != nil {
			tableAName, tableBName = findOnTables(join.On, graph, effectiveNameForJoin(join))
		} else {
			// CROSS JOIN: no ON clause, connect to FROM table
			tableAName = graph.FromTable
			tableBName = effectiveNameForJoin(join)
		}

		if tableAName == "" || tableBName == "" {
			// Fallback: if we can't determine the pair, skip optimization for this edge
			continue
		}

		// Convert RIGHT JOIN to LEFT JOIN by swapping the two tables.
		// RIGHT JOIN preserves all rows from the right (join) table,
		// which is equivalent to LEFT JOIN with tables swapped.
		joinType := join.JoinType
		if joinType == ast.JoinRight {
			tableAName, tableBName = tableBName, tableAName
			joinType = ast.JoinLeft
		}

		nodeA := graph.Nodes[tableAName]
		nodeB := graph.Nodes[tableBName]
		_ = joinNode // the join node is either A or B

		tableAInfo := &joinTableInfo{
			info:      nodeA.Info,
			tableName: nodeA.TableName,
			alias:     nodeA.Alias,
		}
		tableBInfo := &joinTableInfo{
			info:      nodeB.Info,
			tableName: nodeB.TableName,
			alias:     nodeB.Alias,
		}

		pairs, residual := extractAllEquiJoinPairs(join.On, tableAInfo, tableBInfo)

		edge := &JoinGraphEdge{
			TableA:        tableAName,
			TableB:        tableBName,
			JoinType:      joinType,
			OnExpr:        join.On,
			EquiJoinPairs: pairs,
			ResidualOn:    residual,
		}

		// Check index availability on equi-join columns
		if len(pairs) > 0 {
			// pairs[0].leftTable/leftCol corresponds to tableA, rightTable/rightCol to tableB
			// But edge.TableA/TableB are effective names. We need to match correctly.
			// The pair's leftTable is always tableAInfo.tableName, rightTable is tableBInfo.tableName.
			// We look up index on each table's equi-join column.
			for _, pair := range pairs {
				// Check index on tableA's column
				col, findErr := nodeA.Info.FindColumn(pair.leftCol)
				if findErr == nil {
					idx := e.storage.LookupSingleColumnIndex(nodeA.Info.Name, col.Index)
					if idx != nil {
						edge.IndexOnA = true
					}
				}
				// Check index on tableB's column
				col, findErr = nodeB.Info.FindColumn(pair.rightCol)
				if findErr == nil {
					idx := e.storage.LookupSingleColumnIndex(nodeB.Info.Name, col.Index)
					if idx != nil {
						edge.IndexOnB = true
					}
				}
				break // only check first pair for index
			}
		}

		key := edgeKey(tableAName, tableBName)
		graph.Edges[key] = edge
		graph.Adjacency[tableAName] = append(graph.Adjacency[tableAName], tableBName)
		graph.Adjacency[tableBName] = append(graph.Adjacency[tableBName], tableAName)
	}

	// 4. Classify WHERE conditions
	if stmt.Where != nil {
		localWhereMap, crossWhere := classifyWhereConditionsN(stmt.Where, graph)

		// For LEFT JOIN, WHERE conditions on the inner (right) table must be
		// applied after the join (as CrossWhere) to see NULL-padded rows.
		leftJoinInnerTables := make(map[string]bool)
		for _, edge := range graph.Edges {
			if edge.JoinType == ast.JoinLeft {
				leftJoinInnerTables[edge.TableB] = true
			}
		}

		for effName, conds := range localWhereMap {
			if leftJoinInnerTables[effName] {
				crossWhere = append(crossWhere, conds...)
			} else {
				graph.Nodes[effName].LocalWhere = conds
			}
		}
		graph.CrossWhere = crossWhere
	}

	return graph, nil
}

// effectiveNameForJoin returns the effective name for a JoinClause.
func effectiveNameForJoin(join ast.JoinClause) string {
	if join.TableAlias != "" {
		return strings.ToLower(join.TableAlias)
	}
	return strings.ToLower(join.TableName)
}

// findOnTables determines which two tables an ON clause connects.
// joinEffName is the effective name of the JOIN table being added.
// Returns the effective names of the two tables.
func findOnTables(on ast.Expr, graph *JoinGraph, joinEffName string) (string, string) {
	refs := collectTableRefs(on)

	// Build a map from table name/alias to effective name
	nameToEffective := make(map[string]string)
	for effName, node := range graph.Nodes {
		nameToEffective[node.TableName] = effName
		if node.Alias != "" {
			nameToEffective[strings.ToLower(node.Alias)] = effName
		}
	}

	referencedEffNames := make(map[string]bool)
	for ref := range refs {
		if effName, ok := nameToEffective[ref]; ok {
			referencedEffNames[effName] = true
		}
	}

	// Also try unqualified columns
	unqualified := collectUnqualifiedIdents(on)
	for _, colName := range unqualified {
		target := resolveUnqualifiedTableN(colName, graph.Nodes)
		if target != "" {
			referencedEffNames[target] = true
		}
	}

	// If we found exactly 2 tables, return them with the JOIN table as second
	if len(referencedEffNames) == 2 {
		var other string
		for name := range referencedEffNames {
			if name != joinEffName {
				other = name
			}
		}
		return other, joinEffName
	}

	// Fallback: the JOIN table and the first referenced table that isn't the JOIN table
	for name := range referencedEffNames {
		if name != joinEffName {
			return name, joinEffName
		}
	}

	// Last resort: connect to FROM table
	return graph.FromTable, joinEffName
}

// tableScore is used for scoring tables during join order optimization.
type tableScore struct {
	effName     string
	score       int
	originalIdx int
}

// OptimizeJoinOrder determines the optimal join order using a greedy algorithm.
func (e *Executor) OptimizeJoinOrder(graph *JoinGraph) []string {
	n := len(graph.Nodes)
	if n <= 1 {
		return graph.TableOrder
	}

	// Step 1: Choose driving table
	originalOrder := make(map[string]int)
	for i, name := range graph.TableOrder {
		originalOrder[name] = i
	}

	// Collect tables that are the inner (right) side of a LEFT JOIN.
	// These cannot be used as driving tables.
	leftJoinInner := make(map[string]bool)
	for _, edge := range graph.Edges {
		if edge.JoinType == ast.JoinLeft {
			leftJoinInner[edge.TableB] = true
		}
	}

	var candidates []tableScore
	for effName, node := range graph.Nodes {
		// LEFT JOIN inner tables cannot be driving table
		if leftJoinInner[effName] {
			continue
		}
		score := 0
		if len(node.LocalWhere) > 0 {
			score++
			// Check if WHERE can use index
			combined := combineExprsAND(node.LocalWhere)
			stripped := stripTableQualifier(combined, node.TableName, node.Alias)
			if _, ok := e.tryIndexScan(stripped, node.Info); ok {
				score += 2
			}
		}
		candidates = append(candidates, tableScore{
			effName:     effName,
			score:       score,
			originalIdx: originalOrder[effName],
		})
	}

	// If all tables are LEFT JOIN inner tables, fall back to original order
	if len(candidates) == 0 {
		return graph.TableOrder
	}

	// Sort: highest score first, then by original order
	sortTableScores(candidates)
	drivingTable := candidates[0].effName

	// Step 2: Greedily add tables
	order := []string{drivingTable}
	joined := map[string]bool{drivingTable: true}

	for len(order) < n {
		var best *tableScore
		for effName, node := range graph.Nodes {
			if joined[effName] {
				continue
			}

			// LEFT JOIN constraint: inner table can only be added when
			// its outer table has been joined
			if leftJoinInner[effName] {
				canAdd := false
				for _, neighbor := range graph.Adjacency[effName] {
					if !joined[neighbor] {
						continue
					}
					key := edgeKey(effName, neighbor)
					edge := graph.Edges[key]
					if edge != nil && edge.JoinType == ast.JoinLeft && edge.TableB == effName {
						canAdd = true
						break
					}
				}
				if !canAdd {
					continue
				}
			}

			score := 0
			hasEdge := false

			// Check if this table has an edge to any joined table
			for _, neighbor := range graph.Adjacency[effName] {
				if joined[neighbor] {
					hasEdge = true
					key := edgeKey(effName, neighbor)
					edge := graph.Edges[key]
					if edge != nil && len(edge.EquiJoinPairs) > 0 {
						// Check if this table's equi-join column has an index
						pair := edge.EquiJoinPairs[0]
						var thisCol string
						if effName == edge.TableA {
							thisCol = pair.leftCol
						} else {
							thisCol = pair.rightCol
						}
						col, findErr := node.Info.FindColumn(thisCol)
						if findErr == nil {
							idx := e.storage.LookupSingleColumnIndex(node.Info.Name, col.Index)
							if idx != nil {
								score += 3 // Index Nested Loop possible
							}
						}
					}
					break
				}
			}

			if len(node.LocalWhere) > 0 {
				score++
				combined := combineExprsAND(node.LocalWhere)
				stripped := stripTableQualifier(combined, node.TableName, node.Alias)
				if _, ok := e.tryIndexScan(stripped, node.Info); ok {
					score += 2
				}
			}

			// Prefer tables with edges over cross joins
			if !hasEdge {
				score -= 10
			}

			ts := tableScore{
				effName:     effName,
				score:       score,
				originalIdx: originalOrder[effName],
			}
			if best == nil || ts.score > best.score || (ts.score == best.score && ts.originalIdx < best.originalIdx) {
				best = &ts
			}
		}

		if best != nil {
			order = append(order, best.effName)
			joined[best.effName] = true
		}
	}

	return order
}

// sortTableScores sorts table scores by score (descending), then by original index (ascending).
func sortTableScores(scores []tableScore) {
	for i := 1; i < len(scores); i++ {
		for j := i; j > 0; j-- {
			if scores[j].score > scores[j-1].score ||
				(scores[j].score == scores[j-1].score && scores[j].originalIdx < scores[j-1].originalIdx) {
				scores[j], scores[j-1] = scores[j-1], scores[j]
			} else {
				break
			}
		}
	}
}

// buildJoinContextFromGraph creates a JoinContext from the graph's table order.
func buildJoinContextFromGraph(graph *JoinGraph) *JoinContext {
	jcEntries := make([]struct {
		info  *TableInfo
		alias string
	}, len(graph.TableOrder))
	for i, tName := range graph.TableOrder {
		node := graph.Nodes[tName]
		jcEntries[i] = struct {
			info  *TableInfo
			alias string
		}{info: node.Info, alias: node.Alias}
	}
	return newJoinContext(jcEntries)
}

// compositeJoinPlan describes how to use a composite index for a JOIN lookup
// combined with LocalWhere conditions on the inner table.
type compositeJoinPlan struct {
	index      IndexReader
	eqVals     []Value         // equality values for columns after the equi-join column
	fullLookup bool            // all index columns covered by equality → use Lookup()
	rangeCol   *rangeCondition // range condition on the column after equality prefix (nil if none)
}

// findCompositeJoinIndex finds a composite index on the inner table whose first column
// matches the equi-join column and whose subsequent columns match LocalWhere conditions.
// Returns the best compositeJoinPlan or nil if no composite index is suitable.
func (e *Executor) findCompositeJoinIndex(
	equiJoinColIdx int,
	localWhereStripped ast.Expr,
	info *TableInfo,
) *compositeJoinPlan {
	indexes := e.storage.GetIndexes(info.Name)
	if len(indexes) == 0 {
		return nil
	}

	eqConds := extractEqualityConditions(localWhereStripped)
	rangeConds := extractRangeConditions(localWhereStripped)

	var bestPlan *compositeJoinPlan
	bestCoverage := 0 // number of index columns covered (equi-join col + matched conditions)

	for _, idx := range indexes {
		idxInfo := idx.GetInfo()
		if len(idxInfo.ColumnIdxs) < 2 {
			continue // need at least 2 columns (equi-join + one more)
		}
		if idxInfo.ColumnIdxs[0] != equiJoinColIdx {
			continue // first column must be the equi-join column
		}

		// Try to match subsequent columns with equality conditions
		var eqVals []Value
		matchedEq := 0
		for i := 1; i < len(idxInfo.ColumnIdxs); i++ {
			colName := strings.ToLower(idxInfo.ColumnNames[i])
			val, ok := eqConds[colName]
			if !ok {
				break
			}
			eqVals = append(eqVals, val)
			matchedEq++
		}

		coverage := 1 + matchedEq // equi-join col + matched equality conditions

		if matchedEq == len(idxInfo.ColumnIdxs)-1 {
			// All columns after equi-join are covered by equality → full lookup
			if coverage > bestCoverage {
				bestCoverage = coverage
				bestPlan = &compositeJoinPlan{
					index:      idx,
					eqVals:     eqVals,
					fullLookup: true,
				}
			}
			continue
		}

		// Check if the next unmatched column has a range condition
		nextColIdx := 1 + matchedEq
		if nextColIdx < len(idxInfo.ColumnIdxs) {
			nextColName := strings.ToLower(idxInfo.ColumnNames[nextColIdx])
			if rc, ok := rangeConds[nextColName]; ok && (rc.fromVal != nil || rc.toVal != nil) {
				rangeCoverage := coverage + 1
				if rangeCoverage > bestCoverage {
					bestCoverage = rangeCoverage
					bestPlan = &compositeJoinPlan{
						index:    idx,
						eqVals:   eqVals,
						rangeCol: rc,
					}
				}
				continue
			}
		}

		// Partial equality prefix only (prefix scan)
		if matchedEq > 0 && coverage > bestCoverage {
			bestCoverage = coverage
			bestPlan = &compositeJoinPlan{
				index:  idx,
				eqVals: eqVals,
			}
		}
	}

	return bestPlan
}

// executeJoinRows performs the join operation and returns the joined rows and JoinContext.
// This is the core join logic without post-processing (ORDER BY, projection, etc.).
// earlyLimit > 0 enables early termination when enough rows have been collected.
func (e *Executor) executeJoinRows(stmt *ast.SelectStmt, graph *JoinGraph, order []string, earlyLimit int) ([]Row, *JoinContext, error) {
	// Compute column offsets for the fixed-slot approach.
	// Slots are always in the original TableOrder, regardless of join execution order.
	totalCols := 0
	tableOffset := make(map[string]int)
	for _, name := range graph.TableOrder {
		tableOffset[name] = totalCols
		totalCols += len(graph.Nodes[name].Info.Columns)
	}

	// Step 1: Scan driving table with LocalWhere pushdown
	drivingName := order[0]
	drivingNode := graph.Nodes[drivingName]
	drivingOffset := tableOffset[drivingName]

	var drivingRows []Row
	var err error

	if drivingNode.Rows != nil {
		// Pre-materialized rows (FROM subquery)
		drivingRows = drivingNode.Rows
		if len(drivingNode.LocalWhere) > 0 {
			combined := combineExprsAND(drivingNode.LocalWhere)
			stripped := stripTableQualifier(combined, drivingNode.TableName, drivingNode.Alias)
			var filtered []Row
			for _, row := range drivingRows {
				match, mErr := evalWhere(stripped, row, drivingNode.Info)
				if mErr != nil {
					return nil, nil, mErr
				}
				if match {
					filtered = append(filtered, row)
				}
			}
			drivingRows = filtered
		}
	} else if len(drivingNode.LocalWhere) > 0 {
		combined := combineExprsAND(drivingNode.LocalWhere)
		stripped := stripTableQualifier(combined, drivingNode.TableName, drivingNode.Alias)

		if keys, ok := e.tryIndexScan(stripped, drivingNode.Info); ok {
			drivingRows, err = e.storage.GetByKeys(drivingNode.Info.Name, keys)
			if err != nil {
				return nil, nil, err
			}
		} else {
			drivingRows, err = e.storage.Scan(drivingNode.Info.Name)
			if err != nil {
				return nil, nil, err
			}
		}
		// Filter by WHERE
		var filtered []Row
		for _, row := range drivingRows {
			match, mErr := evalWhere(stripped, row, drivingNode.Info)
			if mErr != nil {
				return nil, nil, mErr
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		drivingRows = filtered
	} else {
		drivingRows, err = e.storage.Scan(drivingNode.Info.Name)
		if err != nil {
			return nil, nil, err
		}
	}

	// Place driving table rows into fixed-slot merged rows
	currentRows := make([]Row, len(drivingRows))
	for i, row := range drivingRows {
		merged := make(Row, totalCols)
		copy(merged[drivingOffset:], row)
		currentRows[i] = merged
	}

	// Step 2: Join each subsequent table
	joinedSet := map[string]bool{drivingName: true}

	for step := 1; step < len(order); step++ {
		nextName := order[step]
		nextNode := graph.Nodes[nextName]
		nextOffset := tableOffset[nextName]

		// Find the edge connecting nextName to an already-joined table
		var edge *JoinGraphEdge
		var partnerName string
		for _, neighbor := range graph.Adjacency[nextName] {
			if joinedSet[neighbor] {
				key := edgeKey(nextName, neighbor)
				if e := graph.Edges[key]; e != nil {
					edge = e
					partnerName = neighbor
					break
				}
			}
		}

		// Determine equi-join column info
		var nextEquiCol string
		var partnerEquiColIdx int = -1
		var nextIdx IndexReader

		if edge != nil && len(edge.EquiJoinPairs) > 0 {
			pair := edge.EquiJoinPairs[0]
			// Determine which side of the pair corresponds to which table
			if nextNode.TableName == pair.leftTable {
				nextEquiCol = pair.leftCol
				partnerNode := graph.Nodes[partnerName]
				col, findErr := partnerNode.Info.FindColumn(pair.rightCol)
				if findErr == nil {
					partnerEquiColIdx = tableOffset[partnerName] + col.Index
				}
			} else {
				nextEquiCol = pair.rightCol
				partnerNode := graph.Nodes[partnerName]
				col, findErr := partnerNode.Info.FindColumn(pair.leftCol)
				if findErr == nil {
					partnerEquiColIdx = tableOffset[partnerName] + col.Index
				}
			}

			// Check if nextTable has an index on the equi-join column
			col, findErr := nextNode.Info.FindColumn(nextEquiCol)
			if findErr == nil {
				nextIdx = e.storage.LookupSingleColumnIndex(nextNode.Info.Name, col.Index)
			}
		}

		// Try composite index (covers JOIN + LocalWhere in one B-tree scan)
		var cjPlan *compositeJoinPlan
		if nextEquiCol != "" && len(nextNode.LocalWhere) > 0 {
			col, findErr := nextNode.Info.FindColumn(nextEquiCol)
			if findErr == nil {
				combined := combineExprsAND(nextNode.LocalWhere)
				stripped := stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
				cjPlan = e.findCompositeJoinIndex(col.Index, stripped, nextNode.Info)
			}
		}
		if cjPlan != nil {
			nextIdx = cjPlan.index
		}

		// Build JoinContext for ON/WHERE evaluation on the merged row
		jc := buildJoinContextFromGraph(graph)

		// Prepare pre-filtered inner rows for non-index path
		var preFilteredInner []Row
		if nextIdx == nil {
			if nextNode.Rows != nil {
				// Pre-materialized rows (FROM subquery)
				preFilteredInner = nextNode.Rows
				if len(nextNode.LocalWhere) > 0 {
					combined := combineExprsAND(nextNode.LocalWhere)
					stripped := stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
					var filtered []Row
					for _, row := range preFilteredInner {
						match, mErr := evalWhere(stripped, row, nextNode.Info)
						if mErr != nil {
							return nil, nil, mErr
						}
						if match {
							filtered = append(filtered, row)
						}
					}
					preFilteredInner = filtered
				}
			} else if len(nextNode.LocalWhere) > 0 {
				combined := combineExprsAND(nextNode.LocalWhere)
				stripped := stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
				// Try index scan for LocalWhere conditions
				if keys, ok := e.tryIndexScan(stripped, nextNode.Info); ok {
					preFilteredInner, err = e.storage.GetByKeys(nextNode.Info.Name, keys)
				} else {
					preFilteredInner, err = e.storage.Scan(nextNode.Info.Name)
				}
				if err != nil {
					return nil, nil, err
				}
				// Apply LocalWhere filter (index may cover only part of the condition)
				var filtered []Row
				for _, row := range preFilteredInner {
					match, mErr := evalWhere(stripped, row, nextNode.Info)
					if mErr != nil {
						return nil, nil, mErr
					}
					if match {
						filtered = append(filtered, row)
					}
				}
				preFilteredInner = filtered
			} else {
				preFilteredInner, err = e.storage.Scan(nextNode.Info.Name)
				if err != nil {
					return nil, nil, err
				}
			}
		}

		// Build hash table for non-index equi-join (Hash Join)
		var hashTable *hashJoinTable
		var outerEquiColIdxs []int
		if nextIdx == nil && edge != nil && len(edge.EquiJoinPairs) > 0 {
			innerEquiColIdxs, outerColIdxs := resolveAllEquiJoinCols(
				edge, nextNode, graph, tableOffset,
			)
			if innerEquiColIdxs != nil {
				hashTable = buildHashJoinTable(preFilteredInner, innerEquiColIdxs)
				outerEquiColIdxs = outerColIdxs
			}
		}

		// Prepare inner WHERE for index-looked-up rows
		var innerWhereStripped ast.Expr
		var innerWhereKeys map[int64]struct{}
		if nextIdx != nil && cjPlan == nil && len(nextNode.LocalWhere) > 0 {
			combined := combineExprsAND(nextNode.LocalWhere)
			innerWhereStripped = stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
			// Try index scan for LocalWhere conditions (executed once before the join loop)
			if keys, ok := e.tryIndexScan(innerWhereStripped, nextNode.Info); ok {
				innerWhereKeys = make(map[int64]struct{}, len(keys))
				for _, k := range keys {
					innerWhereKeys[k] = struct{}{}
				}
			}
		}
		// For composite join plan, prepare innerWhereStripped for residual filtering
		if cjPlan != nil && len(nextNode.LocalWhere) > 0 {
			combined := combineExprsAND(nextNode.LocalWhere)
			innerWhereStripped = stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
		}

		// Nested loop join
		isLeftJoin := edge != nil && edge.JoinType == ast.JoinLeft
		cap := 64
		if earlyLimit > 0 {
			cap = earlyLimit
		}
		joined := make([]Row, 0, cap)
		earlyLimitReached := false
		for _, outerRow := range currentRows {
			if earlyLimitReached {
				break
			}
			var innerCandidates []Row
			skipInner := false

			if nextIdx != nil && partnerEquiColIdx >= 0 {
				// Index nested loop
				lookupVal := outerRow[partnerEquiColIdx]
				if lookupVal == nil {
					skipInner = true
				} else {
					var keys []int64
					if cjPlan != nil {
						// Use composite index for combined JOIN + LocalWhere lookup
						if cjPlan.fullLookup {
							vals := make([]Value, 1+len(cjPlan.eqVals))
							vals[0] = lookupVal
							copy(vals[1:], cjPlan.eqVals)
							keys = nextIdx.Lookup(vals)
						} else if cjPlan.rangeCol != nil {
							prefixVals := make([]Value, 1+len(cjPlan.eqVals))
							prefixVals[0] = lookupVal
							copy(prefixVals[1:], cjPlan.eqVals)
							rc := cjPlan.rangeCol
							keys = nextIdx.CompositeRangeScan(prefixVals, rc.fromVal, rc.fromInclusive, rc.toVal, rc.toInclusive)
						} else {
							// Prefix-only scan
							prefixVals := make([]Value, 1+len(cjPlan.eqVals))
							prefixVals[0] = lookupVal
							copy(prefixVals[1:], cjPlan.eqVals)
							keys = nextIdx.CompositeRangeScan(prefixVals, nil, false, nil, false)
						}
					} else {
						keys = nextIdx.Lookup([]Value{lookupVal})
						// Intersect with LocalWhere index keys if available
						if innerWhereKeys != nil {
							intersected := make([]int64, 0, len(keys))
							for _, k := range keys {
								if _, ok := innerWhereKeys[k]; ok {
									intersected = append(intersected, k)
								}
							}
							keys = intersected
						}
					}
					if len(keys) == 0 {
						skipInner = true
					} else {
						innerCandidates, err = e.storage.GetByKeys(nextNode.Info.Name, keys)
						if err != nil {
							return nil, nil, err
						}
						// Apply inner WHERE filter (index may cover only part of the condition)
						if innerWhereStripped != nil {
							var filtered []Row
							for _, row := range innerCandidates {
								match, mErr := evalWhere(innerWhereStripped, row, nextNode.Info)
								if mErr != nil {
									return nil, nil, mErr
								}
								if match {
									filtered = append(filtered, row)
								}
							}
							innerCandidates = filtered
						}
					}
				}
			} else {
				if hashTable != nil {
					// Hash Join probe
					probeKey, hasKey := hashJoinProbeKey(outerRow, outerEquiColIdxs)
					if !hasKey {
						skipInner = true
					} else {
						innerCandidates = hashTable.buckets[probeKey]
						if len(innerCandidates) == 0 {
							skipInner = true
						}
					}
				} else {
					// Full nested loop fallback (no equi-join condition)
					innerCandidates = preFilteredInner
				}
			}

			if skipInner {
				if isLeftJoin {
					nullPadded := make(Row, totalCols)
					copy(nullPadded, outerRow)
					// inner slots remain nil (NULL)
					joined = append(joined, nullPadded)
					if earlyLimit > 0 && len(joined) >= earlyLimit {
						earlyLimitReached = true
					}
				}
				continue
			}

			matched := false
			for _, innerRow := range innerCandidates {
				// Place inner row into the correct slot
				mergedRow := make(Row, totalCols)
				copy(mergedRow, outerRow)
				copy(mergedRow[nextOffset:], innerRow)

				// Evaluate ON condition (skipped for CROSS JOIN where OnExpr is nil)
				if edge != nil && edge.OnExpr != nil {
					if nextIdx != nil || hashTable != nil {
						// Index or hash join: equi-join already satisfied, evaluate residual only
						if edge.ResidualOn != nil {
							match, mErr := evalWhereJoin(edge.ResidualOn, mergedRow, jc)
							if mErr != nil {
								return nil, nil, mErr
							}
							if !match {
								continue
							}
						}
					} else {
						// Full nested loop: evaluate full ON condition
						match, mErr := evalWhereJoin(edge.OnExpr, mergedRow, jc)
						if mErr != nil {
							return nil, nil, mErr
						}
						if !match {
							continue
						}
					}
				}

				matched = true
				joined = append(joined, mergedRow)
				if earlyLimit > 0 && len(joined) >= earlyLimit {
					earlyLimitReached = true
					break
				}
			}

			if !matched && isLeftJoin {
				nullPadded := make(Row, totalCols)
				copy(nullPadded, outerRow)
				// inner slots remain nil (NULL)
				joined = append(joined, nullPadded)
				if earlyLimit > 0 && len(joined) >= earlyLimit {
					earlyLimitReached = true
				}
			}
		}

		currentRows = joined
		joinedSet[nextName] = true
	}

	// Step 3: Apply CrossWhere conditions (use modern evaluator for subquery support)
	jc := buildJoinContextFromGraph(graph)
	if len(graph.CrossWhere) > 0 {
		crossFilter := combineExprsAND(graph.CrossWhere)
		eval := newJoinEvaluator(e, jc)
		var filtered []Row
		for _, row := range currentRows {
			val, mErr := eval.Eval(crossFilter, row)
			if mErr != nil {
				return nil, nil, mErr
			}
			b, ok := val.(bool)
			if !ok {
				return nil, nil, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
			}
			if b {
				filtered = append(filtered, row)
			}
		}
		currentRows = filtered
	}
	return currentRows, jc, nil
}

// scanSourceJoin handles the JOIN scan path: builds graph, optimizes order, executes join rows.
// Returns joined rows and a joinEvaluator for the pipeline.
// earlyLimit > 0 enables early termination if no CrossWhere conditions exist.
func (e *Executor) scanSourceJoin(stmt *ast.SelectStmt, earlyLimit int) ([]Row, ExprEvaluator, error) {
	graph, err := e.buildJoinGraph(stmt)
	if err != nil {
		return nil, nil, err
	}
	order := e.OptimizeJoinOrder(graph)
	// Disable early limit if CrossWhere exists (post-join filtering may reduce rows)
	joinEarlyLimit := earlyLimit
	if len(graph.CrossWhere) > 0 {
		joinEarlyLimit = 0
	}
	rows, jc, err := e.executeJoinRows(stmt, graph, order, joinEarlyLimit)
	if err != nil {
		return nil, nil, err
	}
	return rows, newJoinEvaluator(e, jc), nil
}
