package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/eval"
	"github.com/walf443/oresql/engine/join_graph"
	"github.com/walf443/oresql/storage"
)

// filterRows applies a WHERE expression to rows, returning only matching ones.
func filterRows(rows []Row, where ast.Expr, ev ExprEvaluator) ([]Row, error) {
	var filtered []Row
	for _, row := range rows {
		match, err := eval.Where(where, row, ev)
		if err != nil {
			return nil, err
		}
		if match {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

// scanNodeRows scans rows from a JoinGraphNode, applying LocalWhere filtering.
// Handles pre-materialized rows (subquery/CTE), index scan, and full table scan.
func (e *Executor) scanNodeRows(node *JoinGraphNode) ([]Row, error) {
	eval := newTableEvaluator(makeSubqueryRunner(e), node.Info)

	if node.Rows != nil {
		// Pre-materialized rows (FROM subquery / CTE)
		if len(node.LocalWhere) > 0 {
			combined := combineExprsAND(node.LocalWhere)
			stripped := stripTableQualifier(combined, node.TableName, node.Alias)
			return filterRows(node.Rows, stripped, eval)
		}
		return node.Rows, nil
	}

	st := node.Storage
	var rows []Row
	var err error

	if len(node.LocalWhere) > 0 {
		combined := combineExprsAND(node.LocalWhere)
		stripped := stripTableQualifier(combined, node.TableName, node.Alias)
		if keys, ok := e.tryIndexScan(stripped, node.Info); ok {
			rows, err = st.GetByKeys(node.Info.Name, keys)
		} else {
			rows, err = st.Scan(node.Info.Name)
		}
		if err != nil {
			return nil, err
		}
		return filterRows(rows, stripped, eval)
	}

	return st.Scan(node.Info.Name)
}

// lookupIndexKeys computes the row keys for an index-based join lookup.
// Handles composite index (full lookup, range scan, prefix scan) and simple index with optional intersection.
func lookupIndexKeys(nextIdx storage.IndexReader, lookupVal Value, cjPlan *compositeJoinPlan, innerWhereKeys map[int64]struct{}) []int64 {
	if cjPlan != nil {
		prefixVals := make([]Value, 1+len(cjPlan.eqVals))
		prefixVals[0] = lookupVal
		copy(prefixVals[1:], cjPlan.eqVals)
		if cjPlan.fullLookup {
			return nextIdx.Lookup(prefixVals)
		}
		if cjPlan.rangeCol != nil {
			rc := cjPlan.rangeCol
			return nextIdx.CompositeRangeScan(prefixVals, rc.fromVal, rc.fromInclusive, rc.toVal, rc.toInclusive)
		}
		return nextIdx.CompositeRangeScan(prefixVals, nil, false, nil, false)
	}

	keys := nextIdx.Lookup([]Value{lookupVal})
	if innerWhereKeys != nil {
		intersected := make([]int64, 0, len(keys))
		for _, k := range keys {
			if _, ok := innerWhereKeys[k]; ok {
				intersected = append(intersected, k)
			}
		}
		return intersected
	}
	return keys
}

// joinStepState holds the prepared state for executing one join step.
type joinStepState struct {
	nextNode          *JoinGraphNode
	nextOffset        int
	edge              *JoinGraphEdge
	nextIdx           storage.IndexReader
	cjPlan            *compositeJoinPlan
	partnerEquiColIdx int
	joinEval          ExprEvaluator
	nextEval          ExprEvaluator
	preFilteredInner  []Row
	hashTable         *hashJoinTable
	outerEquiColIdxs  []int
	innerWhereExpr    ast.Expr
	innerWhereKeys    map[int64]struct{}
	isLeftJoin        bool
}

// findJoinEdge finds the edge connecting nextName to an already-joined table.
// Returns the edge and the partner's effective name.
func findJoinEdge(graph *JoinGraph, nextName string, joinedSet map[string]bool) (*JoinGraphEdge, string) {
	for _, neighbor := range graph.Adjacency[nextName] {
		if joinedSet[neighbor] {
			key := join_graph.EdgeKey(nextName, neighbor)
			if edge := graph.Edges[key]; edge != nil {
				return edge, neighbor
			}
		}
	}
	return nil, ""
}

// resolveEquiJoinIndex resolves equi-join column info and finds the best index for lookup.
// Sets s.partnerEquiColIdx, s.nextIdx, and s.cjPlan.
func (e *Executor) resolveEquiJoinIndex(
	s *joinStepState, graph *JoinGraph, nextName, partnerName string, tableOffset map[string]int,
) {
	if s.edge == nil || len(s.edge.EquiJoinPairs) == 0 {
		return
	}

	pair := s.edge.EquiJoinPairs[0]
	var nextEquiCol string
	if nextName == pair.LeftTable {
		nextEquiCol = pair.LeftCol
		partnerNode := graph.Nodes[partnerName]
		if col, err := partnerNode.Info.FindColumn(pair.RightCol); err == nil {
			s.partnerEquiColIdx = tableOffset[partnerName] + col.Index
		}
	} else {
		nextEquiCol = pair.RightCol
		partnerNode := graph.Nodes[partnerName]
		if col, err := partnerNode.Info.FindColumn(pair.LeftCol); err == nil {
			s.partnerEquiColIdx = tableOffset[partnerName] + col.Index
		}
	}

	nextNode := s.nextNode
	nextStorage := nextNode.Storage
	if col, err := nextNode.Info.FindColumn(nextEquiCol); err == nil {
		s.nextIdx = nextStorage.LookupSingleColumnIndex(nextNode.Info.Name, col.Index)
	}

	// Try composite index (covers JOIN + LocalWhere in one B-tree scan)
	if nextEquiCol != "" && len(nextNode.LocalWhere) > 0 {
		if col, err := nextNode.Info.FindColumn(nextEquiCol); err == nil {
			combined := combineExprsAND(nextNode.LocalWhere)
			stripped := stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
			s.cjPlan = e.findCompositeJoinIndex(col.Index, stripped, nextNode.Info, nextStorage)
		}
	}
	if s.cjPlan != nil {
		s.nextIdx = s.cjPlan.index
	}
}

// prepareInnerScan prepares inner rows for the join step.
// For non-index path: pre-filters rows and optionally builds a hash table.
// For index path: prepares inner WHERE expression and key set.
func (e *Executor) prepareInnerScan(
	s *joinStepState, graph *JoinGraph, tableOffset map[string]int,
) error {
	nextNode := s.nextNode
	if s.nextIdx == nil {
		var err error
		s.preFilteredInner, err = e.scanNodeRows(nextNode)
		if err != nil {
			return err
		}
		if s.edge != nil && len(s.edge.EquiJoinPairs) > 0 {
			innerEquiColIdxs, outerColIdxs := resolveAllEquiJoinCols(
				s.edge, nextNode, graph, tableOffset,
			)
			if innerEquiColIdxs != nil {
				s.hashTable = buildHashJoinTable(s.preFilteredInner, innerEquiColIdxs)
				s.outerEquiColIdxs = outerColIdxs
			}
		}
		return nil
	}

	// Index path: prepare inner WHERE
	if len(nextNode.LocalWhere) > 0 {
		combined := combineExprsAND(nextNode.LocalWhere)
		s.innerWhereExpr = stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
		if s.cjPlan == nil {
			if keys, ok := e.tryIndexScan(s.innerWhereExpr, nextNode.Info); ok {
				s.innerWhereKeys = make(map[int64]struct{}, len(keys))
				for _, k := range keys {
					s.innerWhereKeys[k] = struct{}{}
				}
			}
		}
	}
	return nil
}

// prepareJoinStep resolves the index strategy, pre-filters inner rows, and builds
// hash tables for one join step. Returns the prepared state.
func (e *Executor) prepareJoinStep(
	graph *JoinGraph, nextName string, joinedSet map[string]bool,
	tableOffset map[string]int,
) (*joinStepState, error) {
	nextNode := graph.Nodes[nextName]
	s := &joinStepState{
		nextNode:          nextNode,
		nextOffset:        tableOffset[nextName],
		partnerEquiColIdx: -1,
		nextEval:          newTableEvaluator(makeSubqueryRunner(e), nextNode.Info),
	}

	edge, partnerName := findJoinEdge(graph, nextName, joinedSet)
	s.edge = edge

	e.resolveEquiJoinIndex(s, graph, nextName, partnerName, tableOffset)

	jc := buildJoinContextFromGraph(graph)
	s.joinEval = newJoinEvaluator(makeSubqueryRunner(e), jc)

	if err := e.prepareInnerScan(s, graph, tableOffset); err != nil {
		return nil, err
	}

	s.isLeftJoin = s.edge != nil && s.edge.JoinType == ast.JoinLeft
	return s, nil
}

// findInnerCandidates resolves the inner row candidates for a single outer row.
// Returns the candidates and whether the inner side should be skipped (no match possible).
func (e *Executor) findInnerCandidates(s *joinStepState, outerRow Row) ([]Row, bool, error) {
	if s.nextIdx != nil && s.partnerEquiColIdx >= 0 {
		lookupVal := outerRow[s.partnerEquiColIdx]
		if lookupVal == nil {
			return nil, true, nil
		}
		keys := lookupIndexKeys(s.nextIdx, lookupVal, s.cjPlan, s.innerWhereKeys)
		if len(keys) == 0 {
			return nil, true, nil
		}
		candidates, err := s.nextNode.Storage.GetByKeys(s.nextNode.Info.Name, keys)
		if err != nil {
			return nil, false, err
		}
		if s.innerWhereExpr != nil {
			candidates, err = filterRows(candidates, s.innerWhereExpr, s.nextEval)
			if err != nil {
				return nil, false, err
			}
		}
		return candidates, false, nil
	}

	if s.hashTable != nil {
		probeKey, hasKey := hashJoinProbeKey(outerRow, s.outerEquiColIdxs)
		if !hasKey {
			return nil, true, nil
		}
		candidates := s.hashTable.buckets[probeKey]
		if len(candidates) == 0 {
			return nil, true, nil
		}
		return candidates, false, nil
	}

	return s.preFilteredInner, false, nil
}

// evalOnCondition evaluates the ON condition for a merged row.
// Returns true if the row passes the ON condition (or no ON condition exists).
func evalOnCondition(s *joinStepState, mergedRow Row) (bool, error) {
	if s.edge == nil || s.edge.OnExpr == nil {
		return true, nil
	}
	if s.nextIdx != nil || s.hashTable != nil {
		// Index or hash join: equi-join already satisfied, evaluate residual only
		if s.edge.ResidualOn == nil {
			return true, nil
		}
		return eval.Where(s.edge.ResidualOn, mergedRow, s.joinEval)
	}
	// Full nested loop: evaluate full ON condition
	return eval.Where(s.edge.OnExpr, mergedRow, s.joinEval)
}

// executeJoinStep performs the nested-loop join for one step, returning the joined rows.
func (e *Executor) executeJoinStep(s *joinStepState, currentRows []Row, totalCols, earlyLimit int) ([]Row, error) {
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

		innerCandidates, skipInner, err := e.findInnerCandidates(s, outerRow)
		if err != nil {
			return nil, err
		}

		if skipInner {
			if s.isLeftJoin {
				nullPadded := make(Row, totalCols)
				copy(nullPadded, outerRow)
				joined = append(joined, nullPadded)
				if earlyLimit > 0 && len(joined) >= earlyLimit {
					earlyLimitReached = true
				}
			}
			continue
		}

		matched := false
		for _, innerRow := range innerCandidates {
			mergedRow := make(Row, totalCols)
			copy(mergedRow, outerRow)
			copy(mergedRow[s.nextOffset:], innerRow)

			pass, mErr := evalOnCondition(s, mergedRow)
			if mErr != nil {
				return nil, mErr
			}
			if !pass {
				continue
			}

			matched = true
			joined = append(joined, mergedRow)
			if earlyLimit > 0 && len(joined) >= earlyLimit {
				earlyLimitReached = true
				break
			}
		}

		if !matched && s.isLeftJoin {
			nullPadded := make(Row, totalCols)
			copy(nullPadded, outerRow)
			joined = append(joined, nullPadded)
			if earlyLimit > 0 && len(joined) >= earlyLimit {
				earlyLimitReached = true
			}
		}
	}

	return joined, nil
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

	drivingRows, err := e.scanNodeRows(drivingNode)
	if err != nil {
		return nil, nil, err
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

		s, prepErr := e.prepareJoinStep(graph, nextName, joinedSet, tableOffset)
		if prepErr != nil {
			return nil, nil, prepErr
		}

		joined, joinErr := e.executeJoinStep(s, currentRows, totalCols, earlyLimit)
		if joinErr != nil {
			return nil, nil, joinErr
		}
		currentRows = joined
		joinedSet[nextName] = true
	}

	// Step 3: Apply CrossWhere conditions (use modern evaluator for subquery support)
	jc := buildJoinContextFromGraph(graph)
	if len(graph.CrossWhere) > 0 {
		crossFilter := combineExprsAND(graph.CrossWhere)
		eval := newJoinEvaluator(makeSubqueryRunner(e), jc)
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
	return rows, newJoinEvaluator(makeSubqueryRunner(e), jc), nil
}
