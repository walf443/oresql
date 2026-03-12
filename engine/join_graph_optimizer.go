package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/join_graph"
)

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

	originalOrder := make(map[string]int)
	for i, name := range graph.TableOrder {
		originalOrder[name] = i
	}

	leftJoinInner := collectLeftJoinInnerTables(graph)

	// Step 1: Choose driving table
	candidates := e.scoreDrivingCandidates(graph, leftJoinInner, originalOrder)
	if len(candidates) == 0 {
		return graph.TableOrder
	}
	sortTableScores(candidates)
	drivingTable := candidates[0].effName

	// Step 2: Greedily add tables
	order := []string{drivingTable}
	joined := map[string]bool{drivingTable: true}

	for len(order) < n {
		best := e.findBestNextTable(graph, joined, leftJoinInner, originalOrder)
		if best != nil {
			order = append(order, best.effName)
			joined[best.effName] = true
		}
	}

	return order
}

// collectLeftJoinInnerTables returns a set of tables that are the inner (right) side of a LEFT JOIN.
func collectLeftJoinInnerTables(graph *JoinGraph) map[string]bool {
	result := make(map[string]bool)
	for _, edge := range graph.Edges {
		if edge.JoinType == ast.JoinLeft {
			result[edge.TableB] = true
		}
	}
	return result
}

// scoreLocalWhere returns a score bonus for a node's LocalWhere conditions.
// +1 for having WHERE, +2 more if an index can be used.
func (e *Executor) scoreLocalWhere(node *JoinGraphNode) int {
	if len(node.LocalWhere) == 0 {
		return 0
	}
	score := 1
	combined := combineExprsAND(node.LocalWhere)
	stripped := stripTableQualifier(combined, node.TableName, node.Alias)
	if _, ok := e.tryIndexScan(stripped, node.Info); ok {
		score += 2
	}
	return score
}

// scoreDrivingCandidates scores each table as a potential driving table.
// LEFT JOIN inner tables are excluded.
func (e *Executor) scoreDrivingCandidates(graph *JoinGraph, leftJoinInner map[string]bool, originalOrder map[string]int) []tableScore {
	var candidates []tableScore
	for effName, node := range graph.Nodes {
		if leftJoinInner[effName] {
			continue
		}
		candidates = append(candidates, tableScore{
			effName:     effName,
			score:       e.scoreLocalWhere(node),
			originalIdx: originalOrder[effName],
		})
	}
	return candidates
}

// canAddLeftJoinInner checks if a LEFT JOIN inner table can be added
// (its outer table must already be joined).
func canAddLeftJoinInner(effName string, graph *JoinGraph, joined map[string]bool) bool {
	for _, neighbor := range graph.Adjacency[effName] {
		if !joined[neighbor] {
			continue
		}
		key := join_graph.EdgeKey(effName, neighbor)
		edge := graph.Edges[key]
		if edge != nil && edge.JoinType == ast.JoinLeft && edge.TableB == effName {
			return true
		}
	}
	return false
}

// scoreJoinEdge returns a score based on the join edge to an already-joined table.
// Returns (score, hasEdge).
func (e *Executor) scoreJoinEdge(effName string, node *JoinGraphNode, graph *JoinGraph, joined map[string]bool) (int, bool) {
	for _, neighbor := range graph.Adjacency[effName] {
		if !joined[neighbor] {
			continue
		}
		key := join_graph.EdgeKey(effName, neighbor)
		edge := graph.Edges[key]
		if edge != nil && len(edge.EquiJoinPairs) > 0 {
			pair := edge.EquiJoinPairs[0]
			thisCol := pair.RightCol
			if effName == edge.TableA {
				thisCol = pair.LeftCol
			}
			col, findErr := node.Info.FindColumn(thisCol)
			if findErr == nil {
				idx := node.Storage.LookupSingleColumnIndex(node.Info.Name, col.Index)
				if idx != nil {
					return 3, true // Index Nested Loop possible
				}
			}
		}
		return 0, true
	}
	return 0, false
}

// findBestNextTable finds the best table to add next in the greedy join ordering.
func (e *Executor) findBestNextTable(graph *JoinGraph, joined, leftJoinInner map[string]bool, originalOrder map[string]int) *tableScore {
	var best *tableScore
	for effName, node := range graph.Nodes {
		if joined[effName] {
			continue
		}
		if leftJoinInner[effName] && !canAddLeftJoinInner(effName, graph, joined) {
			continue
		}

		edgeScore, hasEdge := e.scoreJoinEdge(effName, node, graph, joined)
		score := edgeScore + e.scoreLocalWhere(node)
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
	return best
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
	return newJoinContext(jcEntries, graph.UsingCols)
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
	st StorageEngine,
) *compositeJoinPlan {
	indexes := st.GetIndexes(info.Name)
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
