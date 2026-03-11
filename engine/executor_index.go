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
	indexes := e.db.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		idxInfo := idx.GetInfo()
		vals := make([]Value, len(idxInfo.ColumnNames))
		allFound := true
		for i, colName := range idxInfo.ColumnNames {
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
	result := make([]int64, 0, len(keys))
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
		idx := e.db.storage.LookupSingleColumnIndex(info.Name, col.Index)
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
	indexes := e.db.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		idxInfo := idx.GetInfo()
		if len(idxInfo.ColumnNames) < 2 {
			continue
		}
		prefixLen := len(idxInfo.ColumnNames) - 1
		lastCol := strings.ToLower(idxInfo.ColumnNames[prefixLen])
		inVals, hasIn := inConds[lastCol]
		if !hasIn {
			continue
		}
		// Check if first N-1 columns have equality conditions
		prefixVals := make([]Value, 0, prefixLen)
		allPrefixFound := true
		for i := 0; i < prefixLen; i++ {
			val, ok := eqConds[strings.ToLower(idxInfo.ColumnNames[i])]
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
		idx := e.db.storage.LookupSingleColumnIndex(info.Name, col.Index)
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
	indexes := e.db.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		idxInfo := idx.GetInfo()
		if len(idxInfo.ColumnNames) < 2 {
			continue
		}
		// Try from longest prefix to shortest, pick the most selective match
		for prefixLen := len(idxInfo.ColumnNames) - 1; prefixLen >= 1; prefixLen-- {
			prefixVals := make([]Value, 0, prefixLen)
			allPrefixFound := true
			for i := 0; i < prefixLen; i++ {
				val, ok := eqConds[strings.ToLower(idxInfo.ColumnNames[i])]
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
			rangeCol := strings.ToLower(idxInfo.ColumnNames[prefixLen])
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

// indexOrderResult describes how an index can satisfy ORDER BY.
type indexOrderResult struct {
	index     IndexReader // nil if PK ordering
	reverse   bool        // true for DESC (first ORDER BY column direction)
	usePK     bool        // true if ORDER BY on PK column
	fullOrder bool        // true: no sort needed, false: only first column ordered
	// WHERE range conditions combinable with this index
	fromVal       *Value
	fromInclusive bool
	toVal         *Value
	toInclusive   bool
}

// tryIndexOrder checks if ORDER BY can be satisfied (fully or partially) by an index or PK.
// Returns nil if no index provides ordering for the first ORDER BY column.
func (e *Executor) tryIndexOrder(
	orderBy []ast.OrderByClause, where ast.Expr, info *TableInfo,
	hasLimit bool,
) *indexOrderResult {
	if len(orderBy) == 0 {
		return nil
	}

	// First ORDER BY expression must be a simple column reference
	ident, ok := orderBy[0].Expr.(*ast.IdentExpr)
	if !ok {
		return nil
	}
	colName := strings.ToLower(ident.Name)

	col, err := info.FindColumn(colName)
	if err != nil {
		return nil
	}

	reverse := orderBy[0].Desc
	fullOrder := len(orderBy) == 1

	// Extract range conditions from WHERE for potential combination
	var rangeConds map[string]*rangeCondition
	if where != nil {
		rangeConds = extractRangeConditions(where)
	}

	// Check PK column
	if info.PrimaryKeyCol >= 0 && col.Index == info.PrimaryKeyCol {
		result := &indexOrderResult{
			usePK:     true,
			reverse:   reverse,
			fullOrder: fullOrder,
		}
		return result
	}

	// Check secondary indexes
	idx := e.db.storage.LookupSingleColumnIndex(info.Name, col.Index)
	if idx == nil {
		return nil
	}

	// Nullable column + LIMIT + ASC: fall back to avoid NULL ordering issues.
	// For DESC (reverse scan), NULLs (encoded as 0x00, smallest key) naturally
	// end up at the end of results, matching SQL's "NULL sorts last" semantics.
	// For ASC, NULLs would appear first, which is incorrect for SQL.
	if !col.NotNull && !col.PrimaryKey && hasLimit && !reverse {
		return nil
	}

	result := &indexOrderResult{
		index:     idx,
		reverse:   reverse,
		fullOrder: fullOrder,
	}

	// Try to combine with WHERE range conditions on the same column
	if rc, ok := rangeConds[colName]; ok {
		result.fromVal = rc.fromVal
		result.fromInclusive = rc.fromInclusive
		result.toVal = rc.toVal
		result.toInclusive = rc.toInclusive
	}

	return result
}

// indexScanParams describes parameters for a streaming index scan via OrderedRangeScan.
// Used for single-column index equality/range lookups with LIMIT optimization.
type indexScanParams struct {
	index         IndexReader
	fromVal       *Value
	fromInclusive bool
	toVal         *Value
	toInclusive   bool
}

// tryIndexScanParams attempts to extract streaming index scan parameters from WHERE.
// Only handles single-column indexes with equality or range conditions.
// Returns nil, false for PK, IN, composite indexes (those fall through to batch path).
func (e *Executor) tryIndexScanParams(where ast.Expr, info *TableInfo) (*indexScanParams, bool) {
	if where == nil {
		return nil, false
	}

	// Try equality conditions on single-column indexes
	eqConds := extractEqualityConditions(where)
	for colName, val := range eqConds {
		// Skip PK columns (at most 1 row, no benefit from streaming)
		if info.PrimaryKeyCol >= 0 {
			pkColName := strings.ToLower(info.Columns[info.PrimaryKeyCol].Name)
			if colName == pkColName {
				continue
			}
		}
		col, err := info.FindColumn(colName)
		if err != nil {
			continue
		}
		idx := e.db.storage.LookupSingleColumnIndex(info.Name, col.Index)
		if idx == nil {
			continue
		}
		v := val
		return &indexScanParams{
			index:         idx,
			fromVal:       &v,
			fromInclusive: true,
			toVal:         &v,
			toInclusive:   true,
		}, true
	}

	// Try range conditions on single-column indexes
	rangeConds := extractRangeConditions(where)
	for _, rc := range rangeConds {
		if rc.fromVal == nil && rc.toVal == nil {
			continue
		}
		col, err := info.FindColumn(rc.colName)
		if err != nil {
			continue
		}
		idx := e.db.storage.LookupSingleColumnIndex(info.Name, col.Index)
		if idx == nil {
			continue
		}
		return &indexScanParams{
			index:         idx,
			fromVal:       rc.fromVal,
			fromInclusive: rc.fromInclusive,
			toVal:         rc.toVal,
			toInclusive:   rc.toInclusive,
		}, true
	}

	return nil, false
}

// flattenOrBranches recursively flattens an OR tree into a slice of non-OR expressions.
// Returns nil if the expression is not an OR logical expression.
func flattenOrBranches(expr ast.Expr) []ast.Expr {
	logical, ok := expr.(*ast.LogicalExpr)
	if !ok || logical.Op != "OR" {
		return nil
	}
	var branches []ast.Expr
	// Flatten left
	if left := flattenOrBranches(logical.Left); left != nil {
		branches = append(branches, left...)
	} else {
		branches = append(branches, logical.Left)
	}
	// Flatten right
	if right := flattenOrBranches(logical.Right); right != nil {
		branches = append(branches, right...)
	} else {
		branches = append(branches, logical.Right)
	}
	return branches
}

// tryIndexMergeUnion attempts to use multiple indexes for OR conditions.
// Each OR branch is delegated to tryIndexScan. If all branches use an index,
// the results are merged with deduplication. If any branch cannot use an index,
// falls back to full scan.
func (e *Executor) tryIndexMergeUnion(where ast.Expr, info *TableInfo) ([]int64, bool) {
	branches := flattenOrBranches(where)
	if branches == nil {
		return nil, false
	}
	var allKeys []int64
	for _, branch := range branches {
		keys, ok := e.tryIndexScan(branch, info)
		if !ok {
			return nil, false
		}
		allKeys = append(allKeys, keys...)
	}
	return dedupKeys(allKeys), true
}

// tryIndexScan attempts to use an index for the WHERE clause.
// Tries PK direct lookup, then equality lookup, then IN lookup, then range scan,
// then index merge union for OR conditions.
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
	if keys, ok := e.tryIndexMergeUnion(where, info); ok {
		return keys, true
	}
	return nil, false
}

// --- Covering index support ---

// collectNeededColumns collects all column indexes referenced by the query
// (SELECT columns, WHERE, ORDER BY). Returns a set of column indexes.
func collectNeededColumns(columns []ast.Expr, where ast.Expr, orderBy []ast.OrderByClause, info *TableInfo) map[int]bool {
	needed := make(map[int]bool)
	for _, col := range columns {
		collectColumnRefs(col, info, needed)
	}
	if where != nil {
		collectColumnRefs(where, info, needed)
	}
	for _, ob := range orderBy {
		collectColumnRefs(ob.Expr, info, needed)
	}
	return needed
}

// collectColumnRefs recursively walks an AST expression and records all column
// indexes it references into the needed map.
func collectColumnRefs(expr ast.Expr, info *TableInfo, needed map[int]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *ast.IdentExpr:
		col, err := info.FindColumn(e.Name)
		if err == nil {
			needed[col.Index] = true
		}
	case *ast.StarExpr:
		for i := range info.Columns {
			needed[i] = true
		}
	case *ast.CallExpr:
		// Aggregate functions with * (e.g. COUNT(*)) don't need column data
		for _, arg := range e.Args {
			if _, isStar := arg.(*ast.StarExpr); !isStar {
				collectColumnRefs(arg, info, needed)
			}
		}
	default:
		forEachChildExpr(expr, func(child ast.Expr) {
			collectColumnRefs(child, info, needed)
		})
	}
}

// isIndexCovering returns true if all needed columns are covered by the index columns + PK.
func isIndexCovering(idx IndexReader, neededCols map[int]bool, pkColIdx int) bool {
	idxInfo := idx.GetInfo()
	covered := make(map[int]bool)
	for _, colIdx := range idxInfo.ColumnIdxs {
		covered[colIdx] = true
	}
	if pkColIdx >= 0 {
		covered[pkColIdx] = true
	}
	for col := range neededCols {
		if !covered[col] {
			return false
		}
	}
	return true
}

// tryIndexLookupCovering attempts to use a covering index for equality conditions.
// Returns covering rows directly without PK lookup.
func (e *Executor) tryIndexLookupCovering(where ast.Expr, info *TableInfo, neededCols map[int]bool) ([]Row, bool) {
	if where == nil {
		return nil, false
	}
	eqConds := extractEqualityConditions(where)
	if len(eqConds) == 0 {
		return nil, false
	}

	indexes := e.db.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		idxInfo := idx.GetInfo()
		vals := make([]Value, len(idxInfo.ColumnNames))
		allFound := true
		for i, colName := range idxInfo.ColumnNames {
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
		if !isIndexCovering(idx, neededCols, info.PrimaryKeyCol) {
			continue
		}
		cir, ok := idx.(CoveringIndexReader)
		if !ok {
			continue
		}
		rows := cir.LookupCovering(vals, len(info.Columns), info.PrimaryKeyCol)
		if rows == nil {
			return []Row{}, true
		}
		return rows, true
	}
	return nil, false
}

// isPKOnlyCovering returns true if neededCols contains only the PK column (or is empty).
// An empty neededCols means the query doesn't need any column data (e.g. COUNT(*)).
func isPKOnlyCovering(neededCols map[int]bool, pkColIdx int) bool {
	if pkColIdx < 0 {
		return false
	}
	for col := range neededCols {
		if col != pkColIdx {
			return false
		}
	}
	return true
}

// buildPKOnlyRow constructs a Row with only the PK column populated.
// Other columns are left as nil.
func buildPKOnlyRow(key int64, numCols int, pkColIdx int) Row {
	row := make(Row, numCols)
	row[pkColIdx] = key
	return row
}
