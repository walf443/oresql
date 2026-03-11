package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/join_graph"
)

// Type aliases for backward compatibility.
type JoinGraphNode = join_graph.Node
type JoinGraphEdge = join_graph.Edge
type JoinGraph = join_graph.Graph
type equiJoinPair = join_graph.EquiJoinPair
type joinTableInfo = join_graph.TableInfo

// edgeKey delegates to join_graph.EdgeKey.
func edgeKey(a, b string) string { return join_graph.EdgeKey(a, b) }

// effectiveNameForJoin delegates to join_graph.EffectiveNameForJoin.
func effectiveNameForJoin(join ast.JoinClause) string { return join_graph.EffectiveNameForJoin(join) }

// resolveUnqualifiedTableN delegates to join_graph.ResolveUnqualifiedTableN.
func resolveUnqualifiedTableN(colName string, nodes map[string]*JoinGraphNode) string {
	return join_graph.ResolveUnqualifiedTableN(colName, nodes)
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

		leftIsA := tableA.MatchesTable(leftIdent.Table)
		leftIsB := tableB.MatchesTable(leftIdent.Table)
		rightIsA := tableA.MatchesTable(rightIdent.Table)
		rightIsB := tableB.MatchesTable(rightIdent.Table)

		if leftIsA && rightIsB {
			pairs = append(pairs, equiJoinPair{
				LeftTable:  tableA.EffectiveName,
				LeftCol:    strings.ToLower(leftIdent.Name),
				RightTable: tableB.EffectiveName,
				RightCol:   strings.ToLower(rightIdent.Name),
			})
		} else if leftIsB && rightIsA {
			pairs = append(pairs, equiJoinPair{
				LeftTable:  tableA.EffectiveName,
				LeftCol:    strings.ToLower(rightIdent.Name),
				RightTable: tableB.EffectiveName,
				RightCol:   strings.ToLower(leftIdent.Name),
			})
		} else {
			residuals = append(residuals, cond)
		}
	}

	return pairs, combineExprsAND(residuals)
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
		UsingCols: make(map[string][]string),
	}

	// 1. Create node for FROM table
	var fromInfo *TableInfo
	var fromRows []Row
	var fromStorage StorageEngine
	if stmt.FromSubquery != nil {
		var err error
		fromInfo, fromRows, err = e.materializeSubquery(stmt.FromSubquery, stmt.TableAlias)
		if err != nil {
			return nil, err
		}
		fromStorage = e.db.storage
	} else if cteInfo, cteRows, ok := e.lookupCTE(stmt.TableName); ok {
		fromInfo = cteInfo
		fromRows = cteRows
		fromStorage = e.db.storage
	} else {
		db, fromInfoResolved, err := e.resolveTable(stmt.DatabaseName, stmt.TableName)
		if err != nil {
			return nil, err
		}
		fromInfo = fromInfoResolved
		fromStorage = db.storage
	}
	fromNode := &JoinGraphNode{
		TableName: strings.ToLower(fromInfo.Name),
		Alias:     stmt.TableAlias,
		Info:      fromInfo,
		Storage:   fromStorage,
		Rows:      fromRows,
	}
	fromEffName := fromNode.EffectiveName()
	graph.Nodes[fromEffName] = fromNode
	graph.FromTable = fromEffName
	graph.TableOrder = append(graph.TableOrder, fromEffName)

	// 2. Create nodes for each JOIN table
	for _, join := range stmt.Joins {
		var joinStorage StorageEngine
		var joinInfo *TableInfo
		var joinRows []Row
		if cteInfo, cteRows, ok := e.lookupCTE(join.TableName); ok {
			joinInfo = cteInfo
			joinRows = cteRows
			joinStorage = e.db.storage
		} else {
			db, resolved, err := e.resolveTable(join.DatabaseName, join.TableName)
			if err != nil {
				return nil, err
			}
			joinInfo = resolved
			joinStorage = db.storage
		}
		joinNode := &JoinGraphNode{
			TableName: strings.ToLower(join.TableName),
			Alias:     join.TableAlias,
			Info:      joinInfo,
			Storage:   joinStorage,
			Rows:      joinRows,
		}
		effName := joinNode.EffectiveName()
		graph.Nodes[effName] = joinNode
		graph.TableOrder = append(graph.TableOrder, effName)
		if len(join.Using) > 0 {
			graph.UsingCols[effName] = join.Using
		}
	}

	// 3. Create edges from ON clauses
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
			continue
		}

		// Convert RIGHT JOIN to LEFT JOIN by swapping the two tables.
		joinType := join.JoinType
		if joinType == ast.JoinRight {
			tableAName, tableBName = tableBName, tableAName
			joinType = ast.JoinLeft
		}

		nodeA := graph.Nodes[tableAName]
		nodeB := graph.Nodes[tableBName]
		_ = joinNode

		tableAInfo := &joinTableInfo{
			Info:          nodeA.Info,
			TableName:     nodeA.TableName,
			Alias:         nodeA.Alias,
			EffectiveName: tableAName,
		}
		tableBInfo := &joinTableInfo{
			Info:          nodeB.Info,
			TableName:     nodeB.TableName,
			Alias:         nodeB.Alias,
			EffectiveName: tableBName,
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
			for _, pair := range pairs {
				col, findErr := nodeA.Info.FindColumn(pair.LeftCol)
				if findErr == nil {
					idx := nodeA.Storage.LookupSingleColumnIndex(nodeA.Info.Name, col.Index)
					if idx != nil {
						edge.IndexOnA = true
					}
				}
				col, findErr = nodeB.Info.FindColumn(pair.RightCol)
				if findErr == nil {
					idx := nodeB.Storage.LookupSingleColumnIndex(nodeB.Info.Name, col.Index)
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

// findOnTables determines which two tables an ON clause connects.
func findOnTables(on ast.Expr, graph *JoinGraph, joinEffName string) (string, string) {
	refs := collectTableRefs(on)

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

	unqualified := collectUnqualifiedIdents(on)
	for _, colName := range unqualified {
		target := resolveUnqualifiedTableN(colName, graph.Nodes)
		if target != "" {
			referencedEffNames[target] = true
		}
	}

	if len(referencedEffNames) == 2 {
		var other string
		for name := range referencedEffNames {
			if name != joinEffName {
				other = name
			}
		}
		return other, joinEffName
	}

	for name := range referencedEffNames {
		if name != joinEffName {
			return name, joinEffName
		}
	}

	return graph.FromTable, joinEffName
}
