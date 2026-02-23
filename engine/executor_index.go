package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
)

// tryPrimaryKeyLookup attempts to use the primary key BTree for direct lookup.
// When the table has a single INT PK and the WHERE clause contains an equality
// condition on it, we can do an O(log n) lookup instead of a full table scan.
func (e *Executor) tryPrimaryKeyLookup(where ast.Expr, info *TableInfo) ([]int64, bool) {
	if info.PrimaryKeyCol < 0 {
		return nil, false
	}
	if where == nil {
		return nil, false
	}
	eqConds := extractEqualityConditions(where)
	if len(eqConds) == 0 {
		return nil, false
	}
	pkColName := strings.ToLower(info.Columns[info.PrimaryKeyCol].Name)
	val, ok := eqConds[pkColName]
	if !ok {
		return nil, false
	}
	pkVal, ok := val.(int64)
	if !ok {
		return nil, false
	}
	return []int64{pkVal}, true
}

// tryIndexLookup attempts to use an index for equality conditions in WHERE.
// Returns BTree keys matching the index lookup.
func (e *Executor) tryIndexLookup(where ast.Expr, info *TableInfo) ([]int64, bool) {
	if where == nil {
		return nil, false
	}
	eqConds := extractEqualityConditions(where)
	if len(eqConds) == 0 {
		return nil, false
	}

	// Try all indexes on this table, pick one where all columns have equality conditions
	indexes := e.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		vals := make([]Value, len(idx.Info.ColumnNames))
		allFound := true
		for i, colName := range idx.Info.ColumnNames {
			val, ok := eqConds[strings.ToLower(colName)]
			if !ok {
				allFound = false
				break
			}
			vals[i] = val
		}
		if !allFound {
			continue
		}
		keys := idx.Lookup(vals)
		if keys == nil {
			return []int64{}, true
		}
		return keys, true
	}
	return nil, false
}

// extractEqualityConditions extracts all column = literal conditions from a WHERE expression.
// For AND chains, it collects all equality conditions. Returns map[lowercase_col_name]Value.
func extractEqualityConditions(expr ast.Expr) map[string]Value {
	result := make(map[string]Value)
	collectEqualities(expr, result)
	return result
}

func collectEqualities(expr ast.Expr, result map[string]Value) {
	switch e := expr.(type) {
	case *ast.BinaryExpr:
		if e.Op != "=" {
			return
		}
		ident, ok := e.Left.(*ast.IdentExpr)
		if !ok {
			return
		}
		var val Value
		switch lit := e.Right.(type) {
		case *ast.IntLitExpr:
			val = lit.Value
		case *ast.FloatLitExpr:
			val = lit.Value
		case *ast.StringLitExpr:
			val = lit.Value
		default:
			return
		}
		result[strings.ToLower(ident.Name)] = val
	case *ast.IsNullExpr:
		if e.Not {
			return
		}
		ident, ok := e.Expr.(*ast.IdentExpr)
		if !ok {
			return
		}
		result[strings.ToLower(ident.Name)] = nil
	case *ast.LogicalExpr:
		if e.Op == "AND" {
			collectEqualities(e.Left, result)
			collectEqualities(e.Right, result)
		}
	}
}

// extractInConditions extracts all column IN (literal, ...) conditions from a WHERE expression.
// For AND chains, it collects all IN conditions. NOT IN is skipped.
// Returns map[lowercase_col_name][]Value.
func extractInConditions(expr ast.Expr) map[string][]Value {
	result := make(map[string][]Value)
	collectInConditions(expr, result)
	return result
}

func collectInConditions(expr ast.Expr, result map[string][]Value) {
	switch e := expr.(type) {
	case *ast.InExpr:
		if e.Not {
			return
		}
		ident, ok := e.Left.(*ast.IdentExpr)
		if !ok {
			return
		}
		var vals []Value
		for _, valExpr := range e.Values {
			switch lit := valExpr.(type) {
			case *ast.IntLitExpr:
				vals = append(vals, lit.Value)
			case *ast.FloatLitExpr:
				vals = append(vals, lit.Value)
			case *ast.StringLitExpr:
				vals = append(vals, lit.Value)
			default:
				return // non-literal value, skip this IN condition
			}
		}
		result[strings.ToLower(ident.Name)] = vals
	case *ast.LogicalExpr:
		if e.Op == "AND" {
			collectInConditions(e.Left, result)
			collectInConditions(e.Right, result)
		}
	}
}

// dedupKeys removes duplicate int64 keys, preserving order.
func dedupKeys(keys []int64) []int64 {
	seen := make(map[int64]bool)
	var result []int64
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			result = append(result, k)
		}
	}
	return result
}

// tryIndexInLookup attempts to use an index for IN conditions in WHERE.
// Returns BTree keys matching the index lookup.
func (e *Executor) tryIndexInLookup(where ast.Expr, info *TableInfo) ([]int64, bool) {
	if where == nil {
		return nil, false
	}
	inConds := extractInConditions(where)
	if len(inConds) == 0 {
		return nil, false
	}

	// 1. Try single-column indexes
	for colName, vals := range inConds {
		col, err := info.FindColumn(colName)
		if err != nil {
			continue
		}
		idx := e.storage.LookupSingleColumnIndex(info.Name, col.Index)
		if idx == nil {
			continue
		}
		var keys []int64
		for _, val := range vals {
			keys = append(keys, idx.Lookup([]Value{val})...)
		}
		keys = dedupKeys(keys)
		if len(keys) == 0 {
			return []int64{}, true
		}
		return keys, true
	}

	// 2. Try composite indexes: prefix equality + last column IN
	eqConds := extractEqualityConditions(where)
	indexes := e.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		if len(idx.Info.ColumnNames) < 2 {
			continue
		}
		prefixLen := len(idx.Info.ColumnNames) - 1
		lastCol := strings.ToLower(idx.Info.ColumnNames[prefixLen])
		inVals, hasIn := inConds[lastCol]
		if !hasIn {
			continue
		}
		// Check if first N-1 columns have equality conditions
		prefixVals := make([]Value, 0, prefixLen)
		allPrefixFound := true
		for i := 0; i < prefixLen; i++ {
			val, ok := eqConds[strings.ToLower(idx.Info.ColumnNames[i])]
			if !ok {
				allPrefixFound = false
				break
			}
			prefixVals = append(prefixVals, val)
		}
		if !allPrefixFound {
			continue
		}
		var keys []int64
		for _, v := range inVals {
			lookupVals := make([]Value, len(prefixVals)+1)
			copy(lookupVals, prefixVals)
			lookupVals[prefixLen] = v
			keys = append(keys, idx.Lookup(lookupVals)...)
		}
		keys = dedupKeys(keys)
		if len(keys) == 0 {
			return []int64{}, true
		}
		return keys, true
	}

	return nil, false
}

// rangeCondition represents a range condition on a single column.
type rangeCondition struct {
	colName       string
	fromVal       *Value
	fromInclusive bool
	toVal         *Value
	toInclusive   bool
}

// extractRangeConditions extracts range conditions from a WHERE expression.
// It collects >, >=, <, <= comparisons and BETWEEN expressions, merging conditions
// on the same column.
func extractRangeConditions(expr ast.Expr) map[string]*rangeCondition {
	result := make(map[string]*rangeCondition)
	collectRangeConditions(expr, result)
	return result
}

func collectRangeConditions(expr ast.Expr, result map[string]*rangeCondition) {
	switch e := expr.(type) {
	case *ast.BinaryExpr:
		// Check for col > val, col >= val, col < val, col <= val
		ident, ok := e.Left.(*ast.IdentExpr)
		if !ok {
			return
		}
		var val Value
		switch lit := e.Right.(type) {
		case *ast.IntLitExpr:
			val = lit.Value
		case *ast.FloatLitExpr:
			val = lit.Value
		case *ast.StringLitExpr:
			val = lit.Value
		default:
			return
		}

		colName := strings.ToLower(ident.Name)
		rc, exists := result[colName]
		if !exists {
			rc = &rangeCondition{colName: colName}
			result[colName] = rc
		}

		switch e.Op {
		case ">":
			rc.fromVal = &val
			rc.fromInclusive = false
		case ">=":
			rc.fromVal = &val
			rc.fromInclusive = true
		case "<":
			rc.toVal = &val
			rc.toInclusive = false
		case "<=":
			rc.toVal = &val
			rc.toInclusive = true
		}

	case *ast.BetweenExpr:
		if e.Not {
			return
		}
		ident, ok := e.Left.(*ast.IdentExpr)
		if !ok {
			return
		}
		var lowVal, highVal Value
		switch lit := e.Low.(type) {
		case *ast.IntLitExpr:
			lowVal = lit.Value
		case *ast.FloatLitExpr:
			lowVal = lit.Value
		case *ast.StringLitExpr:
			lowVal = lit.Value
		default:
			return
		}
		switch lit := e.High.(type) {
		case *ast.IntLitExpr:
			highVal = lit.Value
		case *ast.FloatLitExpr:
			highVal = lit.Value
		case *ast.StringLitExpr:
			highVal = lit.Value
		default:
			return
		}

		colName := strings.ToLower(ident.Name)
		result[colName] = &rangeCondition{
			colName:       colName,
			fromVal:       &lowVal,
			fromInclusive: true,
			toVal:         &highVal,
			toInclusive:   true,
		}

	case *ast.LikeExpr:
		if e.Not {
			return
		}
		ident, ok := e.Left.(*ast.IdentExpr)
		if !ok {
			return
		}
		patLit, ok := e.Pattern.(*ast.StringLitExpr)
		if !ok {
			return
		}
		prefix := extractLikePrefix(patLit.Value)
		if prefix == "" {
			return
		}
		colName := strings.ToLower(ident.Name)
		var fromVal Value = prefix
		rc := &rangeCondition{
			colName:       colName,
			fromVal:       &fromVal,
			fromInclusive: true,
		}
		if next, ok := nextPrefix(prefix); ok {
			var toVal Value = next
			rc.toVal = &toVal
			rc.toInclusive = false
		}
		result[colName] = rc

	case *ast.LogicalExpr:
		if e.Op == "AND" {
			collectRangeConditions(e.Left, result)
			collectRangeConditions(e.Right, result)
		}
	}
}

// tryIndexRangeScan attempts to use an index for range conditions in WHERE.
// Returns BTree keys matching the index range scan.
func (e *Executor) tryIndexRangeScan(where ast.Expr, info *TableInfo) ([]int64, bool) {
	if where == nil {
		return nil, false
	}

	rangeConds := extractRangeConditions(where)
	if len(rangeConds) == 0 {
		return nil, false
	}

	// Try each range condition to find a matching single-column index
	for _, rc := range rangeConds {
		if rc.fromVal == nil && rc.toVal == nil {
			continue
		}
		col, err := info.FindColumn(rc.colName)
		if err != nil {
			continue
		}
		idx := e.storage.LookupSingleColumnIndex(info.Name, col.Index)
		if idx == nil {
			continue
		}

		keys := idx.RangeScan(rc.fromVal, rc.fromInclusive, rc.toVal, rc.toInclusive)
		if keys == nil {
			return []int64{}, true
		}
		return keys, true
	}

	// Try composite indexes: prefix equality + next column range
	eqConds := extractEqualityConditions(where)
	indexes := e.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		if len(idx.Info.ColumnNames) < 2 {
			continue
		}
		// Try from longest prefix to shortest, pick the most selective match
		for prefixLen := len(idx.Info.ColumnNames) - 1; prefixLen >= 1; prefixLen-- {
			prefixVals := make([]Value, 0, prefixLen)
			allPrefixFound := true
			for i := 0; i < prefixLen; i++ {
				val, ok := eqConds[strings.ToLower(idx.Info.ColumnNames[i])]
				if !ok {
					allPrefixFound = false
					break
				}
				prefixVals = append(prefixVals, val)
			}
			if !allPrefixFound {
				continue
			}
			// Check if the next column has a range condition
			rangeCol := strings.ToLower(idx.Info.ColumnNames[prefixLen])
			rc, ok := rangeConds[rangeCol]
			if !ok || (rc.fromVal == nil && rc.toVal == nil) {
				continue
			}
			keys := idx.CompositeRangeScan(prefixVals, rc.fromVal, rc.fromInclusive, rc.toVal, rc.toInclusive)
			if keys == nil {
				return []int64{}, true
			}
			return keys, true
		}
	}

	return nil, false
}

// tryIndexScan attempts to use an index for the WHERE clause.
// Tries PK direct lookup, then equality lookup, then IN lookup, then range scan.
// Returns BTree keys and whether an index was used.
func (e *Executor) tryIndexScan(where ast.Expr, info *TableInfo) ([]int64, bool) {
	if keys, ok := e.tryPrimaryKeyLookup(where, info); ok {
		return keys, true
	}
	if keys, ok := e.tryIndexLookup(where, info); ok {
		return keys, true
	}
	if keys, ok := e.tryIndexInLookup(where, info); ok {
		return keys, true
	}
	if keys, ok := e.tryIndexRangeScan(where, info); ok {
		return keys, true
	}
	return nil, false
}
