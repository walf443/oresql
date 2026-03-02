package engine

import "strings"

// hashJoinTable is a hash table built from inner table rows for equi-join.
type hashJoinTable struct {
	buckets map[string][]Row
}

// buildHashJoinTable constructs a hash table from inner rows keyed by equi-join columns.
// Rows with any NULL value in the join columns are excluded (NULL != NULL in SQL).
func buildHashJoinTable(rows []Row, colIdxs []int) *hashJoinTable {
	ht := &hashJoinTable{
		buckets: make(map[string][]Row, len(rows)),
	}
	for _, row := range rows {
		key, ok := hashJoinEncodeKey(row, colIdxs)
		if !ok {
			continue // skip rows with NULL in join columns
		}
		ht.buckets[key] = append(ht.buckets[key], row)
	}
	return ht
}

// hashJoinEncodeKey encodes column values from a row into a hash table key string.
// Returns ("", false) if any value is NULL.
func hashJoinEncodeKey(row Row, colIdxs []int) (string, bool) {
	for _, idx := range colIdxs {
		if row[idx] == nil {
			return "", false
		}
	}
	var buf strings.Builder
	for _, idx := range colIdxs {
		encodeValue(&buf, row[idx])
	}
	return buf.String(), true
}

// hashJoinProbeKey encodes values from a merged outer row for hash table lookup.
// Returns ("", false) if any value is NULL.
func hashJoinProbeKey(row Row, colIdxs []int) (string, bool) {
	return hashJoinEncodeKey(row, colIdxs)
}

// resolveAllEquiJoinCols resolves all equi-join pair column indexes for hash join.
// Returns (innerColIdxs, outerColIdxs) where each index is relative to the inner/outer row.
// innerColIdxs are column indexes within the inner table's raw rows.
// outerColIdxs are column indexes within the merged outer row (using tableOffset).
// Returns (nil, nil) if any column cannot be resolved.
func resolveAllEquiJoinCols(
	edge *JoinGraphEdge,
	nextNode *JoinGraphNode,
	graph *JoinGraph,
	tableOffset map[string]int,
) (innerColIdxs []int, outerColIdxs []int) {
	nextEffName := nextNode.effectiveName()
	for _, pair := range edge.EquiJoinPairs {
		var innerCol, outerCol string
		var partnerName string

		if nextEffName == pair.leftTable {
			innerCol = pair.leftCol
			outerCol = pair.rightCol
			partnerName = pair.rightTable
		} else {
			innerCol = pair.rightCol
			outerCol = pair.leftCol
			partnerName = pair.leftTable
		}

		if partnerName == "" {
			return nil, nil
		}

		innerColInfo, err := nextNode.Info.FindColumn(innerCol)
		if err != nil {
			return nil, nil
		}

		partnerNode := graph.Nodes[partnerName]
		outerColInfo, err := partnerNode.Info.FindColumn(outerCol)
		if err != nil {
			return nil, nil
		}

		innerColIdxs = append(innerColIdxs, innerColInfo.Index)
		outerColIdxs = append(outerColIdxs, tableOffset[partnerName]+outerColInfo.Index)
	}
	return innerColIdxs, outerColIdxs
}
