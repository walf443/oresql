package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
)

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

	drivingEval := newTableEvaluator(e, drivingNode.Info)
	if drivingNode.Rows != nil {
		// Pre-materialized rows (FROM subquery)
		drivingRows = drivingNode.Rows
		if len(drivingNode.LocalWhere) > 0 {
			combined := combineExprsAND(drivingNode.LocalWhere)
			stripped := stripTableQualifier(combined, drivingNode.TableName, drivingNode.Alias)
			var filtered []Row
			for _, row := range drivingRows {
				match, mErr := evalWhereWith(stripped, row, drivingEval)
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

		drivingStorage := drivingNode.storageEngine(e.db)
		if keys, ok := e.tryIndexScan(stripped, drivingNode.Info); ok {
			drivingRows, err = drivingStorage.GetByKeys(drivingNode.Info.Name, keys)
			if err != nil {
				return nil, nil, err
			}
		} else {
			drivingRows, err = drivingStorage.Scan(drivingNode.Info.Name)
			if err != nil {
				return nil, nil, err
			}
		}
		// Filter by WHERE
		var filtered []Row
		for _, row := range drivingRows {
			match, mErr := evalWhereWith(stripped, row, drivingEval)
			if mErr != nil {
				return nil, nil, mErr
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		drivingRows = filtered
	} else {
		drivingRows, err = drivingNode.storageEngine(e.db).Scan(drivingNode.Info.Name)
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
			if nextName == pair.leftTable {
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
			nextStorage := nextNode.storageEngine(e.db)
			col, findErr := nextNode.Info.FindColumn(nextEquiCol)
			if findErr == nil {
				nextIdx = nextStorage.LookupSingleColumnIndex(nextNode.Info.Name, col.Index)
			}
		}

		// Try composite index (covers JOIN + LocalWhere in one B-tree scan)
		var cjPlan *compositeJoinPlan
		if nextEquiCol != "" && len(nextNode.LocalWhere) > 0 {
			col, findErr := nextNode.Info.FindColumn(nextEquiCol)
			if findErr == nil {
				combined := combineExprsAND(nextNode.LocalWhere)
				stripped := stripTableQualifier(combined, nextNode.TableName, nextNode.Alias)
				cjPlan = e.findCompositeJoinIndex(col.Index, stripped, nextNode.Info, nextNode.storageEngine(e.db))
			}
		}
		if cjPlan != nil {
			nextIdx = cjPlan.index
		}

		// Build JoinContext for ON/WHERE evaluation on the merged row
		jc := buildJoinContextFromGraph(graph)
		joinEval := newJoinEvaluator(e, jc)

		// Prepare pre-filtered inner rows for non-index path
		nextEval := newTableEvaluator(e, nextNode.Info)
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
						match, mErr := evalWhereWith(stripped, row, nextEval)
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
				nextSt := nextNode.storageEngine(e.db)
				if keys, ok := e.tryIndexScan(stripped, nextNode.Info); ok {
					preFilteredInner, err = nextSt.GetByKeys(nextNode.Info.Name, keys)
				} else {
					preFilteredInner, err = nextSt.Scan(nextNode.Info.Name)
				}
				if err != nil {
					return nil, nil, err
				}
				// Apply LocalWhere filter (index may cover only part of the condition)
				var filtered []Row
				for _, row := range preFilteredInner {
					match, mErr := evalWhereWith(stripped, row, nextEval)
					if mErr != nil {
						return nil, nil, mErr
					}
					if match {
						filtered = append(filtered, row)
					}
				}
				preFilteredInner = filtered
			} else {
				preFilteredInner, err = nextNode.storageEngine(e.db).Scan(nextNode.Info.Name)
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
						innerCandidates, err = nextNode.storageEngine(e.db).GetByKeys(nextNode.Info.Name, keys)
						if err != nil {
							return nil, nil, err
						}
						// Apply inner WHERE filter (index may cover only part of the condition)
						if innerWhereStripped != nil {
							var filtered []Row
							for _, row := range innerCandidates {
								match, mErr := evalWhereWith(innerWhereStripped, row, nextEval)
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
							match, mErr := evalWhereWith(edge.ResidualOn, mergedRow, joinEval)
							if mErr != nil {
								return nil, nil, mErr
							}
							if !match {
								continue
							}
						}
					} else {
						// Full nested loop: evaluate full ON condition
						match, mErr := evalWhereWith(edge.OnExpr, mergedRow, joinEval)
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
