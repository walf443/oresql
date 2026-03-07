package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
)

// SelectPlanType enumerates the top-level execution strategies for SELECT.
type SelectPlanType int

const (
	PlanNoTable           SelectPlanType = iota // SELECT without FROM
	PlanSubquery                                // FROM subquery
	PlanIndexOrderScan                          // ORDER BY satisfied by index
	PlanGroupByIndex                            // GROUP BY on indexed column
	PlanCountStar                               // COUNT(*) via RowCount
	PlanMinMax                                  // MIN/MAX via index edge lookup
	PlanStreamingIndex                          // WHERE + LIMIT via index streaming
	PlanStreamingBatch                          // WHERE + LIMIT via batch index keys
	PlanStreamingFullScan                       // WHERE/DISTINCT + LIMIT via full scan streaming
	PlanBatchIndex                              // WHERE via index scan (batch)
	PlanFullScan                                // Full table scan
)

// WhereIndexType describes how the WHERE clause uses an index.
type WhereIndexType int

const (
	WhereNoIndex     WhereIndexType = iota // No index used for WHERE
	WherePKLookup                          // Primary key equality lookup
	WhereIndexLookup                       // Secondary index equality lookup
	WhereIndexIn                           // Index IN lookup
	WhereRangeScan                         // Index range scan
	WhereIndexMerge                        // Index merge union (OR conditions)
)

// SelectPlan describes the execution plan for a SELECT statement.
type SelectPlan struct {
	Type         SelectPlanType
	TableName    string
	DatabaseName string

	// Resolved database and table info (nil when not resolved)
	db   *Database
	info *TableInfo

	// Possible keys (all available indexes)
	PossibleKeys []string

	// WHERE index usage
	WhereIndex     WhereIndexType
	WhereIndexName string

	// ORDER BY index usage
	IndexOrder *indexOrderResult

	// Streaming index scan parameters (for PlanStreamingIndex)
	streamingParams *indexScanParams

	// Batch index keys (for PlanStreamingBatch / PlanBatchIndex)
	batchKeys []int64

	// Extras describing the plan
	Extras []string

	// JOIN sub-plans
	JoinPlans []JoinPlan
}

// JoinPlan describes the execution plan for a single JOIN.
type JoinPlan struct {
	JoinType     string
	TableName    string
	PossibleKeys []string
	AccessType   string // "full scan", "ref", etc.
	KeyUsed      string
	Extras       []string
}

// AccessType returns a human-readable access type string for the plan.
func (p *SelectPlan) AccessType() string {
	switch p.Type {
	case PlanNoTable:
		return "no table"
	case PlanSubquery:
		return "subquery"
	case PlanIndexOrderScan:
		return "index scan"
	case PlanGroupByIndex:
		return "index scan"
	case PlanCountStar:
		return "row count"
	case PlanMinMax:
		return "index"
	case PlanStreamingIndex, PlanStreamingBatch, PlanBatchIndex:
		return p.whereAccessType()
	case PlanStreamingFullScan, PlanFullScan:
		if p.WhereIndex != WhereNoIndex {
			return p.whereAccessType()
		}
		return "full scan"
	}
	return "full scan"
}

// whereAccessType returns the access type string based on WhereIndex.
func (p *SelectPlan) whereAccessType() string {
	switch p.WhereIndex {
	case WherePKLookup:
		return "const"
	case WhereIndexLookup:
		return "ref"
	case WhereIndexIn:
		return "range"
	case WhereRangeScan:
		return "range"
	case WhereIndexMerge:
		return "index merge"
	default:
		return "full scan"
	}
}

// KeyUsed returns the name of the index used.
func (p *SelectPlan) KeyUsed() string {
	if p.IndexOrder != nil {
		if p.IndexOrder.usePK {
			return "PRIMARY"
		}
		if p.IndexOrder.index != nil {
			return p.IndexOrder.index.GetInfo().Name
		}
	}
	if p.WhereIndex == WherePKLookup {
		return "PRIMARY"
	}
	return p.WhereIndexName
}

// planSelect builds a SelectPlan describing how the SELECT will be executed.
// This is the single source of truth for both EXPLAIN and executeSelect.
// It determines only the execution strategy and related fields (Type, db, info,
// IndexOrder, streamingParams, batchKeys, WhereIndex). Display metadata
// (PossibleKeys, Extras, JoinPlans) is added separately by enrichPlanForExplain.
func (e *Executor) planSelect(stmt *ast.SelectStmt) *SelectPlan {
	plan := &SelectPlan{
		TableName:    stmt.TableName,
		DatabaseName: stmt.DatabaseName,
	}

	// SELECT without FROM
	if stmt.TableName == "" && stmt.FromSubquery == nil {
		plan.Type = PlanNoTable
		return plan
	}

	// FROM subquery
	if stmt.FromSubquery != nil {
		plan.Type = PlanSubquery
		return plan
	}

	// Try to resolve table
	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		plan.Type = PlanFullScan
		return plan
	}
	info, err := db.catalog.GetTable(stmt.TableName)
	if err != nil {
		plan.Type = PlanFullScan
		return plan
	}
	plan.db = db
	plan.info = info

	// 1. Try ORDER BY index optimization
	if len(stmt.OrderBy) > 0 && len(stmt.Joins) == 0 && stmt.TableAlias == "" &&
		stmt.FromSubquery == nil &&
		len(stmt.GroupBy) == 0 && !hasAggregate(stmt.Columns) && !stmt.Distinct &&
		!hasWindowFunction(stmt.Columns) {
		if ior := e.tryIndexOrder(stmt.OrderBy, stmt.Where, info, stmt.Limit != nil); ior != nil {
			plan.Type = PlanIndexOrderScan
			plan.IndexOrder = ior
			return plan
		}
	}

	// 2. Try GROUP BY index optimization
	if len(stmt.GroupBy) == 1 && len(stmt.Joins) == 0 && stmt.FromSubquery == nil &&
		stmt.TableAlias == "" && stmt.Having == nil && !stmt.Distinct &&
		!hasWindowFunction(stmt.Columns) && !containsSubquery(stmt.Where) {
		if gbIdent, ok := stmt.GroupBy[0].(*ast.IdentExpr); ok {
			col, err := info.FindColumn(gbIdent.Name)
			if err == nil {
				isPK := col.Index == info.PrimaryKeyCol
				idx := db.storage.LookupSingleColumnIndex(info.Name, col.Index)
				if isPK || idx != nil {
					plan.Type = PlanGroupByIndex
					if isPK {
						plan.WhereIndexName = "PRIMARY"
					} else {
						plan.WhereIndexName = idx.GetInfo().Name
					}
					return plan
				}
			}
		}
	}

	// 3. Try COUNT(*) optimization
	if e.isCountStarOptimizable(stmt) {
		plan.Type = PlanCountStar
		return plan
	}

	// 4. Try MIN/MAX optimization
	if e.isMinMaxOptimizable(stmt, info) {
		plan.Type = PlanMinMax
		return plan
	}

	// Determine WHERE index usage
	e.planWhereIndex(plan, stmt.Where, info)

	// 5. Check streaming path eligibility
	canEarlyLimit := stmt.Limit != nil &&
		len(stmt.OrderBy) == 0 &&
		len(stmt.GroupBy) == 0 &&
		!hasAggregate(stmt.Columns) &&
		!hasWindowFunction(stmt.Columns)

	if canEarlyLimit && (stmt.Distinct || stmt.Where != nil) &&
		stmt.FromSubquery == nil && len(stmt.Joins) == 0 && stmt.TableAlias == "" &&
		!containsSubquery(stmt.Where) && !columnsContainSubquery(stmt.Columns) {
		if _, _, cteOk := e.lookupCTE(stmt.TableName); !cteOk {
			if stmt.Where != nil {
				if params, ok := e.tryIndexScanParams(stmt.Where, info); ok {
					plan.Type = PlanStreamingIndex
					plan.streamingParams = params
					// indexScanParams may represent equality or range; set WhereIndex from params
					if plan.WhereIndex == WhereNoIndex {
						plan.WhereIndex = WhereRangeScan
						if params.fromVal != nil && params.toVal != nil &&
							*params.fromVal == *params.toVal {
							plan.WhereIndex = WhereIndexLookup
						}
						plan.WhereIndexName = findIndexNameForScanParams(params, info, db)
					}
					return plan
				}
			}
			if keys, indexUsed := e.tryIndexScan(stmt.Where, info); indexUsed {
				plan.Type = PlanStreamingBatch
				plan.batchKeys = keys
			} else {
				plan.Type = PlanStreamingFullScan
			}
			return plan
		}
	}

	// 6. Batch path
	if plan.WhereIndex != WhereNoIndex {
		plan.Type = PlanBatchIndex
	} else {
		plan.Type = PlanFullScan
	}

	return plan
}

// enrichPlanForExplain populates display-only metadata on an existing SelectPlan.
// This includes PossibleKeys, Extras, JoinPlans, WhereIndexName, and covering
// index detection. It must be called only for EXPLAIN output, after planSelect.
func (e *Executor) enrichPlanForExplain(plan *SelectPlan, stmt *ast.SelectStmt) {
	if plan.Type == PlanSubquery {
		plan.Extras = append(plan.Extras, "FROM subquery")
		return
	}

	if plan.info == nil || plan.db == nil {
		return
	}

	plan.PossibleKeys = collectPossibleKeys(plan.info, plan.db)

	// Enrich WhereIndexName if not already set
	if plan.WhereIndex != WhereNoIndex && plan.WhereIndexName == "" && plan.WhereIndex != WherePKLookup {
		switch plan.WhereIndex {
		case WhereIndexLookup, WhereIndexIn:
			plan.WhereIndexName = findUsedIndexName(stmt.Where, plan.info, plan.db)
		case WhereRangeScan:
			plan.WhereIndexName = findUsedRangeIndexName(stmt.Where, plan.info, plan.db)
		case WhereIndexMerge:
			plan.Extras = append(plan.Extras, "Using union")
		}
	}

	switch plan.Type {
	case PlanIndexOrderScan:
		if plan.IndexOrder != nil {
			dir := "ASC"
			if plan.IndexOrder.reverse {
				dir = "DESC"
			}
			if plan.IndexOrder.fullOrder {
				plan.Extras = append(plan.Extras, "Using index for ORDER BY ("+dir+")")
			} else {
				plan.Extras = append(plan.Extras, "Using index for partial ORDER BY ("+dir+")")
			}
		}
		e.planCoveringIndex(plan, stmt)
	case PlanGroupByIndex:
		plan.Extras = append(plan.Extras, "Using index for GROUP BY")
	case PlanCountStar:
		plan.Extras = append(plan.Extras, "Using row count optimization")
	case PlanMinMax:
		plan.Extras = append(plan.Extras, "Using index for MIN/MAX")
	case PlanStreamingIndex, PlanStreamingBatch, PlanStreamingFullScan:
		e.planCoveringIndex(plan, stmt)
	case PlanBatchIndex, PlanFullScan:
		e.planCoveringIndex(plan, stmt)
	}

	e.addCommonExtras(plan, stmt)
	e.addJoinPlans(plan, stmt)
}

// planWhereIndex determines which index (if any) can be used for the WHERE clause.
// It also stores the batch keys in the plan for use during execution.
func (e *Executor) planWhereIndex(plan *SelectPlan, where ast.Expr, info *TableInfo) {
	if where == nil || plan.db == nil {
		return
	}
	if keys, ok := e.tryPrimaryKeyLookup(where, info); ok && keys != nil {
		plan.WhereIndex = WherePKLookup
		plan.batchKeys = keys
		return
	}
	if keys, ok := e.tryIndexLookup(where, info); ok {
		plan.WhereIndex = WhereIndexLookup
		plan.batchKeys = keys
		return
	}
	if keys, ok := e.tryIndexInLookup(where, info); ok {
		plan.WhereIndex = WhereIndexIn
		plan.batchKeys = keys
		return
	}
	if keys, ok := e.tryIndexRangeScan(where, info); ok {
		plan.WhereIndex = WhereRangeScan
		plan.batchKeys = keys
		return
	}
	if keys, ok := e.tryIndexMergeUnion(where, info); ok {
		plan.WhereIndex = WhereIndexMerge
		plan.batchKeys = keys
		return
	}
}

// planCoveringIndex checks if the query can be satisfied entirely from an index
// (without reading the base table rows) and adds "Using covering index" to Extras.
func (e *Executor) planCoveringIndex(plan *SelectPlan, stmt *ast.SelectStmt) {
	if plan.info == nil || plan.db == nil {
		return
	}
	// JOINs are not handled for covering index detection
	if len(stmt.Joins) > 0 {
		return
	}

	neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, plan.info)

	// PK-only covering (e.g., SELECT id FROM t ORDER BY id)
	if isPKOnlyCovering(neededCols, plan.info.PrimaryKeyCol) {
		plan.Extras = append(plan.Extras, "Using covering index")
		return
	}

	// Check index used for ORDER BY scan
	if plan.IndexOrder != nil && plan.IndexOrder.index != nil {
		if isIndexCovering(plan.IndexOrder.index, neededCols, plan.info.PrimaryKeyCol) {
			plan.Extras = append(plan.Extras, "Using covering index")
			return
		}
	}

	// Check index used for WHERE lookup
	if plan.WhereIndex != WhereNoIndex && plan.WhereIndexName != "" {
		indexes := plan.db.storage.GetIndexes(plan.info.Name)
		for _, idx := range indexes {
			if idx.GetInfo().Name == plan.WhereIndexName {
				if isIndexCovering(idx, neededCols, plan.info.PrimaryKeyCol) {
					plan.Extras = append(plan.Extras, "Using covering index")
					return
				}
				break
			}
		}
	}

	// Check if any index used for equality covering (tryIndexLookupCovering path)
	if plan.WhereIndex == WhereIndexLookup || plan.WhereIndex == WhereIndexIn {
		indexes := plan.db.storage.GetIndexes(plan.info.Name)
		for _, idx := range indexes {
			if isIndexCovering(idx, neededCols, plan.info.PrimaryKeyCol) {
				// Verify this index matches the WHERE conditions
				eqConds := extractEqualityConditions(stmt.Where)
				idxInfo := idx.GetInfo()
				allFound := true
				for _, colName := range idxInfo.ColumnNames {
					if _, ok := eqConds[strings.ToLower(colName)]; !ok {
						allFound = false
						break
					}
				}
				if allFound {
					plan.Extras = append(plan.Extras, "Using covering index")
					return
				}
			}
		}
	}
}

// addCommonExtras appends common extra information based on statement features.
func (e *Executor) addCommonExtras(plan *SelectPlan, stmt *ast.SelectStmt) {
	if stmt.Where != nil && plan.Type == PlanFullScan {
		plan.Extras = append(plan.Extras, "Using where")
	}
	if len(stmt.GroupBy) > 0 {
		plan.Extras = append(plan.Extras, "Using group by")
	}
	if stmt.Having != nil {
		plan.Extras = append(plan.Extras, "Using having")
	}
	if len(stmt.OrderBy) > 0 {
		hasOrderExtra := false
		for _, ex := range plan.Extras {
			if strings.Contains(ex, "ORDER BY") {
				hasOrderExtra = true
				break
			}
		}
		if !hasOrderExtra {
			plan.Extras = append(plan.Extras, "Using filesort")
		}
	}
	if stmt.Distinct {
		plan.Extras = append(plan.Extras, "Using distinct")
	}
	if stmt.Limit != nil {
		switch plan.Type {
		case PlanStreamingIndex, PlanStreamingBatch, PlanStreamingFullScan, PlanIndexOrderScan:
			plan.Extras = append(plan.Extras, "Using streaming limit")
		default:
			plan.Extras = append(plan.Extras, "Using limit")
		}
	}
}

// addJoinPlans builds JoinPlan entries for each JOIN clause.
func (e *Executor) addJoinPlans(plan *SelectPlan, stmt *ast.SelectStmt) {
	if len(stmt.Joins) == 0 {
		return
	}

	// Build joinTableInfo for the driving (FROM) table
	fromInfo := &joinTableInfo{
		tableName:     strings.ToLower(stmt.TableName),
		alias:         strings.ToLower(stmt.TableAlias),
		effectiveName: strings.ToLower(stmt.TableName),
	}
	if fromInfo.alias != "" {
		fromInfo.effectiveName = fromInfo.alias
	}
	if plan.info != nil {
		fromInfo.info = plan.info
	}

	// Track all joined tables so far for multi-table join equi-pair extraction
	knownTables := []*joinTableInfo{fromInfo}

	for _, join := range stmt.Joins {
		jp := JoinPlan{
			JoinType:   join.JoinType,
			TableName:  join.TableName,
			AccessType: "full scan",
		}

		joinDB, err := e.resolveDatabase(join.DatabaseName)
		if err != nil {
			plan.JoinPlans = append(plan.JoinPlans, jp)
			continue
		}
		joinInfo, err := joinDB.catalog.GetTable(join.TableName)
		if err != nil {
			plan.JoinPlans = append(plan.JoinPlans, jp)
			continue
		}

		jp.PossibleKeys = collectPossibleKeys(joinInfo, joinDB)

		if join.On != nil {
			// Build joinTableInfo for this JOIN table
			joinTI := &joinTableInfo{
				info:          joinInfo,
				tableName:     strings.ToLower(join.TableName),
				alias:         strings.ToLower(join.TableAlias),
				effectiveName: strings.ToLower(join.TableName),
			}
			if joinTI.alias != "" {
				joinTI.effectiveName = joinTI.alias
			}

			// Try to find equi-join pairs by checking against all known tables
			found := false
			for _, knownTI := range knownTables {
				pairs, _ := extractAllEquiJoinPairs(join.On, knownTI, joinTI)
				if len(pairs) > 0 {
					// Check if the join table has an index on the equi-join column
					joinCol := pairs[0].rightCol
					e.resolveJoinAccessType(&jp, joinCol, joinInfo, joinDB, join, stmt)
					found = true
					break
				}
			}

			if !found {
				// Try col=literal fallback (e.g., ON t.col = 1)
				if _, ok := e.tryIndexLookup(join.On, joinInfo); ok {
					jp.AccessType = "ref"
					jp.KeyUsed = findUsedIndexName(join.On, joinInfo, joinDB)
				}
			}
		}

		if join.On != nil {
			jp.Extras = append(jp.Extras, "Using join condition")
		}
		if len(join.Using) > 0 {
			jp.Extras = append(jp.Extras, "Using USING("+strings.Join(join.Using, ", ")+")")

			// Check if USING columns have indexes
			for _, usingCol := range join.Using {
				col, findErr := joinInfo.FindColumn(usingCol)
				if findErr != nil {
					continue
				}
				if col.Index == joinInfo.PrimaryKeyCol {
					jp.AccessType = "ref"
					jp.KeyUsed = "PRIMARY"
					break
				}
				idx := joinDB.storage.LookupSingleColumnIndex(joinInfo.Name, col.Index)
				if idx != nil {
					jp.AccessType = "ref"
					jp.KeyUsed = idx.GetInfo().Name
					break
				}
			}
		}

		plan.JoinPlans = append(plan.JoinPlans, jp)

		// Add this table to known tables for subsequent joins
		joinTI := &joinTableInfo{
			info:          joinInfo,
			tableName:     strings.ToLower(join.TableName),
			alias:         strings.ToLower(join.TableAlias),
			effectiveName: strings.ToLower(join.TableName),
		}
		if joinTI.alias != "" {
			joinTI.effectiveName = joinTI.alias
		}
		knownTables = append(knownTables, joinTI)
	}
}

// resolveJoinAccessType determines the access type for a join table based on
// the equi-join column, considering single-column indexes, PK, and composite
// indexes that combine the join column with pushed-down WHERE conditions.
func (e *Executor) resolveJoinAccessType(
	jp *JoinPlan, joinCol string, joinInfo *TableInfo, joinDB *Database,
	join ast.JoinClause, stmt *ast.SelectStmt,
) {
	col, err := joinInfo.FindColumn(joinCol)
	if err != nil {
		return
	}

	// Check PK
	if col.Index == joinInfo.PrimaryKeyCol {
		jp.AccessType = "ref"
		jp.KeyUsed = "PRIMARY"
		return
	}

	// Check single-column index
	idx := joinDB.storage.LookupSingleColumnIndex(joinInfo.Name, col.Index)
	if idx != nil {
		jp.AccessType = "ref"
		jp.KeyUsed = idx.GetInfo().Name
	}

	// Try composite index covering join column + WHERE conditions
	if stmt.Where != nil {
		localWhere := extractLocalWhere(stmt.Where, join.TableName, join.TableAlias, joinInfo)
		if localWhere != nil {
			cjPlan := e.findCompositeJoinIndex(col.Index, localWhere, joinInfo, joinDB.storage)
			if cjPlan != nil {
				jp.AccessType = "ref"
				jp.KeyUsed = cjPlan.index.GetInfo().Name
			}
		}
	}
}

// extractLocalWhere extracts WHERE conditions that reference only the given table.
func extractLocalWhere(where ast.Expr, tableName, tableAlias string, info *TableInfo) ast.Expr {
	conds := flattenAND(where)
	var local []ast.Expr
	lowerName := strings.ToLower(tableName)
	lowerAlias := strings.ToLower(tableAlias)
	for _, cond := range conds {
		refs := collectTableRefs(cond)
		if len(refs) == 0 {
			continue
		}
		allLocal := true
		for ref := range refs {
			if ref != lowerName && (lowerAlias == "" || ref != lowerAlias) {
				allLocal = false
				break
			}
		}
		if allLocal {
			stripped := stripTableQualifier(cond, tableName, tableAlias)
			local = append(local, stripped)
		}
	}
	return combineExprsAND(local)
}

// isCountStarOptimizable checks whether COUNT(*) row-count optimization applies.
func (e *Executor) isCountStarOptimizable(stmt *ast.SelectStmt) bool {
	if len(stmt.GroupBy) > 0 || len(stmt.Joins) > 0 || stmt.FromSubquery != nil ||
		stmt.TableAlias != "" || stmt.Where != nil || stmt.Having != nil || stmt.Distinct {
		return false
	}
	for _, colExpr := range stmt.Columns {
		expr := colExpr
		if ae, ok := expr.(*ast.AliasExpr); ok {
			expr = ae.Expr
		}
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return false
		}
		if strings.ToUpper(call.Name) != "COUNT" {
			return false
		}
		if call.Distinct {
			return false
		}
		if len(call.Args) != 1 {
			return false
		}
		// COUNT(*) or COUNT(literal) — both use RowCount
		switch call.Args[0].(type) {
		case *ast.StarExpr, *ast.IntLitExpr, *ast.FloatLitExpr, *ast.StringLitExpr:
			// OK
		default:
			return false
		}
	}
	return true
}

// isMinMaxOptimizable checks whether MIN/MAX index optimization applies.
func (e *Executor) isMinMaxOptimizable(stmt *ast.SelectStmt, info *TableInfo) bool {
	if len(stmt.GroupBy) > 0 || len(stmt.Joins) > 0 || stmt.FromSubquery != nil ||
		stmt.TableAlias != "" || stmt.Where != nil || stmt.Having != nil || stmt.Distinct {
		return false
	}
	for _, colExpr := range stmt.Columns {
		expr := colExpr
		if ae, ok := expr.(*ast.AliasExpr); ok {
			expr = ae.Expr
		}
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return false
		}
		fn := strings.ToUpper(call.Name)
		if fn != "MIN" && fn != "MAX" {
			return false
		}
		if len(call.Args) != 1 {
			return false
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return false
		}
		col, err := info.FindColumn(ident.Name)
		if err != nil {
			return false
		}
		isPK := col.Index == info.PrimaryKeyCol
		idx := e.db.storage.LookupSingleColumnIndex(info.Name, col.Index)
		if !isPK && idx == nil {
			return false
		}
	}
	return true
}

// collectPossibleKeys returns the names of all available indexes (including PRIMARY).
func collectPossibleKeys(info *TableInfo, db *Database) []string {
	var keys []string
	if info.PrimaryKeyCol >= 0 {
		keys = append(keys, "PRIMARY")
	}
	indexes := db.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		keys = append(keys, idx.GetInfo().Name)
	}
	return keys
}

// findIndexNameForScanParams finds the index name used for streaming scan parameters.
func findIndexNameForScanParams(params *indexScanParams, info *TableInfo, db *Database) string {
	if params.index != nil {
		return params.index.GetInfo().Name
	}
	return ""
}

// computeEarlyLimit calculates the early limit value (LIMIT + OFFSET).
func computeEarlyLimit(stmt *ast.SelectStmt) int {
	if stmt.Limit == nil {
		return 0
	}
	limit := int(*stmt.Limit)
	if stmt.Offset != nil {
		limit += int(*stmt.Offset)
	}
	return limit
}
