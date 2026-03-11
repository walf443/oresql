package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
)

// JoinGraphNode represents a single table in the join graph.
type JoinGraphNode struct {
	TableName  string     // lowercase real table name
	Alias      string     // alias (empty if none)
	Info       *TableInfo // schema info
	DB         *Database  // database this table belongs to (nil for subqueries)
	Rows       []Row      // pre-materialized rows (non-nil for FROM subquery)
	LocalWhere []ast.Expr // WHERE conditions referencing only this table
}

// storageEngine returns the storage engine for this node.
// Falls back to the provided default for subquery nodes.
func (n *JoinGraphNode) storageEngine(defaultDB *Database) StorageEngine {
	if n.DB != nil {
		return n.DB.storage
	}
	return defaultDB.storage
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
	UsingCols  map[string][]string       // effective name -> USING column names (for SELECT * dedup)
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
				leftTable:  tableA.effectiveName,
				leftCol:    strings.ToLower(leftIdent.Name),
				rightTable: tableB.effectiveName,
				rightCol:   strings.ToLower(rightIdent.Name),
			})
		} else if leftIsB && rightIsA {
			pairs = append(pairs, equiJoinPair{
				leftTable:  tableA.effectiveName,
				leftCol:    strings.ToLower(rightIdent.Name),
				rightTable: tableB.effectiveName,
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
		UsingCols: make(map[string][]string),
	}

	// 1. Create node for FROM table
	var fromInfo *TableInfo
	var fromRows []Row
	var fromDB *Database
	if stmt.FromSubquery != nil {
		var err error
		fromInfo, fromRows, err = e.materializeSubquery(stmt.FromSubquery, stmt.TableAlias)
		if err != nil {
			return nil, err
		}
	} else if cteInfo, cteRows, ok := e.lookupCTE(stmt.TableName); ok {
		fromInfo = cteInfo
		fromRows = cteRows
	} else {
		db, fromInfoResolved, err := e.resolveTable(stmt.DatabaseName, stmt.TableName)
		if err != nil {
			return nil, err
		}
		fromInfo = fromInfoResolved
		fromDB = db
	}
	fromNode := &JoinGraphNode{
		TableName: strings.ToLower(fromInfo.Name),
		Alias:     stmt.TableAlias,
		Info:      fromInfo,
		DB:        fromDB,
		Rows:      fromRows,
	}
	fromEffName := fromNode.effectiveName()
	graph.Nodes[fromEffName] = fromNode
	graph.FromTable = fromEffName
	graph.TableOrder = append(graph.TableOrder, fromEffName)

	// 2. Create nodes for each JOIN table
	for _, join := range stmt.Joins {
		var joinDB *Database
		var joinInfo *TableInfo
		var joinRows []Row
		if cteInfo, cteRows, ok := e.lookupCTE(join.TableName); ok {
			joinInfo = cteInfo
			joinRows = cteRows
		} else {
			db, resolved, err := e.resolveTable(join.DatabaseName, join.TableName)
			if err != nil {
				return nil, err
			}
			joinInfo = resolved
			joinDB = db
		}
		joinNode := &JoinGraphNode{
			TableName: strings.ToLower(join.TableName),
			Alias:     join.TableAlias,
			Info:      joinInfo,
			DB:        joinDB,
			Rows:      joinRows,
		}
		effName := joinNode.effectiveName()
		graph.Nodes[effName] = joinNode
		graph.TableOrder = append(graph.TableOrder, effName)
		if len(join.Using) > 0 {
			graph.UsingCols[effName] = join.Using
		}
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
			info:          nodeA.Info,
			tableName:     nodeA.TableName,
			alias:         nodeA.Alias,
			effectiveName: tableAName,
		}
		tableBInfo := &joinTableInfo{
			info:          nodeB.Info,
			tableName:     nodeB.TableName,
			alias:         nodeB.Alias,
			effectiveName: tableBName,
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
					idx := nodeA.storageEngine(e.db).LookupSingleColumnIndex(nodeA.Info.Name, col.Index)
					if idx != nil {
						edge.IndexOnA = true
					}
				}
				// Check index on tableB's column
				col, findErr = nodeB.Info.FindColumn(pair.rightCol)
				if findErr == nil {
					idx := nodeB.storageEngine(e.db).LookupSingleColumnIndex(nodeB.Info.Name, col.Index)
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
