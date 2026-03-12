package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
)

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
		case WhereIndexLookup:
			plan.WhereIndexName = findUsedIndexName(stmt.Where, plan.info, plan.db)
		case WhereIndexIn:
			plan.WhereIndexName = findUsedIndexName(stmt.Where, plan.info, plan.db)
			if plan.WhereIndexName == "" {
				plan.WhereIndexName = findUsedInIndexName(stmt.Where, plan.info, plan.db)
			}
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
		TableName:     strings.ToLower(stmt.TableName),
		Alias:         strings.ToLower(stmt.TableAlias),
		EffectiveName: strings.ToLower(stmt.TableName),
	}
	if fromInfo.Alias != "" {
		fromInfo.EffectiveName = fromInfo.Alias
	}
	if plan.info != nil {
		fromInfo.Info = plan.info
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
				Info:          joinInfo,
				TableName:     strings.ToLower(join.TableName),
				Alias:         strings.ToLower(join.TableAlias),
				EffectiveName: strings.ToLower(join.TableName),
			}
			if joinTI.Alias != "" {
				joinTI.EffectiveName = joinTI.Alias
			}

			// Try to find equi-join pairs by checking against all known tables
			found := false
			for _, knownTI := range knownTables {
				pairs, _ := extractAllEquiJoinPairs(join.On, knownTI, joinTI)
				if len(pairs) > 0 {
					// Check if the join table has an index on the equi-join column
					joinCol := pairs[0].RightCol
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
			Info:          joinInfo,
			TableName:     strings.ToLower(join.TableName),
			Alias:         strings.ToLower(join.TableAlias),
			EffectiveName: strings.ToLower(join.TableName),
		}
		if joinTI.Alias != "" {
			joinTI.EffectiveName = joinTI.Alias
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
