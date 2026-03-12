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
	WhereGinMatch                          // GIN full-text match
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
	case WhereGinMatch:
		return "fulltext"
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
	if stmt.TableName == "" && stmt.FromSubquery == nil && stmt.JSONTable == nil {
		plan.Type = PlanNoTable
		return plan
	}

	// FROM subquery or JSON_TABLE
	if stmt.FromSubquery != nil || stmt.JSONTable != nil {
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
	if e.planOrderByIndex(plan, stmt, info) {
		return plan
	}

	// 2. Try GROUP BY index optimization
	if e.planGroupByIndex(plan, stmt, info, db) {
		return plan
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
	if e.planStreamingPath(plan, stmt, info) {
		return plan
	}

	// 6. Batch path
	if plan.WhereIndex != WhereNoIndex {
		plan.Type = PlanBatchIndex
	} else {
		plan.Type = PlanFullScan
	}

	return plan
}

// planOrderByIndex tries to use an index for ORDER BY. Returns true if a plan was set.
func (e *Executor) planOrderByIndex(plan *SelectPlan, stmt *ast.SelectStmt, info *TableInfo) bool {
	if len(stmt.OrderBy) == 0 || len(stmt.Joins) > 0 || stmt.TableAlias != "" ||
		stmt.FromSubquery != nil ||
		len(stmt.GroupBy) > 0 || hasAggregate(stmt.Columns) || stmt.Distinct ||
		hasWindowFunction(stmt.Columns) {
		return false
	}
	ior := e.tryIndexOrder(stmt.OrderBy, stmt.Where, info, stmt.Limit != nil)
	if ior == nil {
		return false
	}
	plan.Type = PlanIndexOrderScan
	plan.IndexOrder = ior
	return true
}

// planGroupByIndex tries to use an index for GROUP BY. Returns true if a plan was set.
func (e *Executor) planGroupByIndex(plan *SelectPlan, stmt *ast.SelectStmt, info *TableInfo, db *Database) bool {
	if len(stmt.GroupBy) != 1 || len(stmt.Joins) > 0 || stmt.FromSubquery != nil ||
		stmt.TableAlias != "" || stmt.Having != nil || stmt.Distinct ||
		hasWindowFunction(stmt.Columns) || containsSubquery(stmt.Where) {
		return false
	}
	gbIdent, ok := stmt.GroupBy[0].(*ast.IdentExpr)
	if !ok {
		return false
	}
	col, err := info.FindColumn(gbIdent.Name)
	if err != nil {
		return false
	}
	isPK := col.Index == info.PrimaryKeyCol
	idx := db.storage.LookupSingleColumnIndex(info.Name, col.Index)
	if !isPK && idx == nil {
		return false
	}
	plan.Type = PlanGroupByIndex
	if isPK {
		plan.WhereIndexName = "PRIMARY"
	} else {
		plan.WhereIndexName = idx.GetInfo().Name
	}
	return true
}

// canStreamEarlyLimit checks whether the query shape allows streaming with early LIMIT.
func canStreamEarlyLimit(stmt *ast.SelectStmt) bool {
	return stmt.Limit != nil &&
		len(stmt.OrderBy) == 0 &&
		len(stmt.GroupBy) == 0 &&
		!hasAggregate(stmt.Columns) &&
		!hasWindowFunction(stmt.Columns) &&
		(stmt.Distinct || stmt.Where != nil) &&
		stmt.FromSubquery == nil &&
		len(stmt.Joins) == 0 &&
		stmt.TableAlias == "" &&
		!containsSubquery(stmt.Where) &&
		!columnsContainSubquery(stmt.Columns)
}

// planStreamingPath tries to set a streaming plan type. Returns true if a plan was set.
func (e *Executor) planStreamingPath(plan *SelectPlan, stmt *ast.SelectStmt, info *TableInfo) bool {
	if !canStreamEarlyLimit(stmt) {
		return false
	}
	if _, _, cteOk := e.lookupCTE(stmt.TableName); cteOk {
		return false
	}

	// Try streaming index scan
	if stmt.Where != nil {
		if params, ok := e.tryIndexScanParams(stmt.Where, info); ok {
			plan.Type = PlanStreamingIndex
			plan.streamingParams = params
			if plan.WhereIndex == WhereNoIndex {
				plan.WhereIndex = WhereRangeScan
				if params.fromVal != nil && params.toVal != nil &&
					*params.fromVal == *params.toVal {
					plan.WhereIndex = WhereIndexLookup
				}
				plan.WhereIndexName = findIndexNameForScanParams(params, info, plan.db)
			}
			return true
		}
	}

	// Try streaming batch or full scan
	if plan.batchKeys != nil {
		plan.Type = PlanStreamingBatch
	} else if keys, indexUsed := e.tryIndexScan(stmt.Where, info); indexUsed {
		plan.Type = PlanStreamingBatch
		plan.batchKeys = keys
	} else {
		plan.Type = PlanStreamingFullScan
	}
	return true
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
	if rb, indexName, ok := e.tryGinBitmapLookup(where, info); ok {
		plan.WhereIndex = WhereGinMatch
		plan.WhereIndexName = indexName
		plan.batchKeys = rb.ToInt64Slice()
		return
	}
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
