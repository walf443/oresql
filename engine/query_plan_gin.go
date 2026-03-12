package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/jsonb"
	"github.com/walf443/oresql/storage"
)

// tryGinBitmapLookup checks if the WHERE clause can use a GIN index and returns
// a RoaringBitmap of matching row keys. Using RoaringBitmap enables efficient
// And/Or operations without intermediate []int64 conversion.
func (e *Executor) tryGinBitmapLookup(where ast.Expr, info *TableInfo) (*storage.RoaringBitmap, string, bool) {
	switch expr := where.(type) {
	case *ast.LogicalExpr:
		return e.tryGinLogical(expr, info)
	case *ast.MatchExpr:
		return e.tryGinMatch(expr, info)
	case *ast.LikeExpr:
		if !expr.Not {
			return e.tryGinLike(expr, info)
		}
	case *ast.BinaryExpr:
		if expr.Op == "=" {
			return e.tryGinEquality(expr, info)
		}
	case *ast.InExpr:
		if !expr.Not {
			return e.tryGinIn(expr, info)
		}
	}
	return nil, "", false
}

// tryGinLogical handles AND/OR by combining child bitmap results.
func (e *Executor) tryGinLogical(expr *ast.LogicalExpr, info *TableInfo) (*storage.RoaringBitmap, string, bool) {
	leftBM, leftIdx, leftOk := e.tryGinBitmapLookup(expr.Left, info)
	rightBM, rightIdx, rightOk := e.tryGinBitmapLookup(expr.Right, info)

	indexName := leftIdx
	if indexName == "" {
		indexName = rightIdx
	}

	switch expr.Op {
	case "AND":
		if leftOk && rightOk {
			return leftBM.And(rightBM), indexName, true
		}
		if leftOk {
			return leftBM, leftIdx, true
		}
		if rightOk {
			return rightBM, rightIdx, true
		}
	case "OR":
		if leftOk && rightOk {
			return leftBM.Or(rightBM), indexName, true
		}
	}
	return nil, "", false
}

// tryGinMatch handles the @@ full-text match operator.
func (e *Executor) tryGinMatch(expr *ast.MatchExpr, info *TableInfo) (*storage.RoaringBitmap, string, bool) {
	ident, ok := expr.Expr.(*ast.IdentExpr)
	if !ok {
		return nil, "", false
	}
	col, err := info.FindColumn(ident.Name)
	if err != nil {
		return nil, "", false
	}
	ginIdx := e.db.storage.LookupGinIndex(info.Name, col.Index)
	if ginIdx == nil {
		return nil, "", false
	}
	expr.Tokenizer = ginIdx.GetInfo().Tokenizer
	rb := ginIdx.MatchTokenBitmap(expr.Pattern)
	if rb == nil {
		rb = storage.NewRoaringBitmap()
	}
	return rb, ginIdx.GetInfo().Name, true
}

// tryGinLike handles LIKE patterns with bigram or word tokenizer GIN indexes.
func (e *Executor) tryGinLike(expr *ast.LikeExpr, info *TableInfo) (*storage.RoaringBitmap, string, bool) {
	ident, ok := expr.Left.(*ast.IdentExpr)
	if !ok {
		return nil, "", false
	}
	patLit, ok := expr.Pattern.(*ast.StringLitExpr)
	if !ok {
		return nil, "", false
	}
	col, err := info.FindColumn(ident.Name)
	if err != nil {
		return nil, "", false
	}
	ginIdx := e.db.storage.LookupGinIndex(info.Name, col.Index)
	if ginIdx == nil {
		return nil, "", false
	}

	pat := patLit.Value
	if ginIdx.GetInfo().Tokenizer == "bigram" {
		longest := longestLiteralSegment(pat)
		if len([]rune(longest)) < 2 {
			return nil, "", false
		}
		rb := ginIdx.MatchTokenBitmap(longest)
		if rb == nil {
			rb = storage.NewRoaringBitmap()
		}
		return rb, ginIdx.GetInfo().Name, true
	}

	// Word tokenizer: only 'word%' prefix pattern
	if len(pat) < 2 || pat[0] == '%' || pat[len(pat)-1] != '%' {
		return nil, "", false
	}
	prefix := pat[:len(pat)-1]
	if strings.ContainsAny(prefix, "%_") {
		return nil, "", false
	}
	keys := ginIdx.MatchPrefix(prefix)
	return storage.RoaringFromInt64Slice(keys), ginIdx.GetInfo().Name, true
}

// tryGinEquality handles col = 'value' with bigram GIN or jsonb_path_ops GIN index.
func (e *Executor) tryGinEquality(binExpr *ast.BinaryExpr, info *TableInfo) (*storage.RoaringBitmap, string, bool) {
	// Try jsonb_path_ops first
	if rb, idxName, ok := e.tryGinJsonbPathOps(binExpr, info); ok {
		return rb, idxName, true
	}

	// Try bigram GIN on string equality
	var ident *ast.IdentExpr
	var val string
	if id, ok := binExpr.Left.(*ast.IdentExpr); ok {
		if s, ok := binExpr.Right.(*ast.StringLitExpr); ok {
			ident, val = id, s.Value
		}
	} else if id, ok := binExpr.Right.(*ast.IdentExpr); ok {
		if s, ok := binExpr.Left.(*ast.StringLitExpr); ok {
			ident, val = id, s.Value
		}
	}
	if ident == nil || val == "" {
		return nil, "", false
	}
	col, err := info.FindColumn(ident.Name)
	if err != nil {
		return nil, "", false
	}
	ginIdx := e.db.storage.LookupGinIndex(info.Name, col.Index)
	if ginIdx == nil || ginIdx.GetInfo().Tokenizer != "bigram" {
		return nil, "", false
	}
	if len([]rune(val)) < 2 {
		return nil, "", false
	}
	rb := ginIdx.MatchTokenBitmap(val)
	if rb == nil {
		rb = storage.NewRoaringBitmap()
	}
	return rb, ginIdx.GetInfo().Name, true
}

// tryGinIn handles col IN ('a', 'b', ...) with bigram GIN or jsonb_path_ops GIN index.
func (e *Executor) tryGinIn(inExpr *ast.InExpr, info *TableInfo) (*storage.RoaringBitmap, string, bool) {
	// Try jsonb_path_ops first
	if rb, idxName, ok := e.tryGinJsonbPathOpsIN(inExpr, info); ok {
		return rb, idxName, true
	}

	// Try bigram GIN
	ident, ok := inExpr.Left.(*ast.IdentExpr)
	if !ok {
		return nil, "", false
	}
	col, err := info.FindColumn(ident.Name)
	if err != nil {
		return nil, "", false
	}
	ginIdx := e.db.storage.LookupGinIndex(info.Name, col.Index)
	if ginIdx == nil || ginIdx.GetInfo().Tokenizer != "bigram" {
		return nil, "", false
	}
	result := storage.NewRoaringBitmap()
	for _, valExpr := range inExpr.Values {
		strLit, ok := valExpr.(*ast.StringLitExpr)
		if !ok {
			return nil, "", false
		}
		if len([]rune(strLit.Value)) < 2 {
			return nil, "", false
		}
		rb := ginIdx.MatchTokenBitmap(strLit.Value)
		if rb != nil {
			result = result.Or(rb)
		}
	}
	return result, ginIdx.GetInfo().Name, true
}

// tryGinJsonbPathOps checks if a binary expression matches the pattern
// JSON_VALUE(col, '$.path') = 'value' and uses a jsonb_path_ops GIN index.
func (e *Executor) tryGinJsonbPathOps(binExpr *ast.BinaryExpr, info *storage.TableInfo) (*storage.RoaringBitmap, string, bool) {
	var call *ast.CallExpr
	var litValue string
	var litInt int64
	var isInt bool

	// Match JSON_VALUE(col, path) = literal or literal = JSON_VALUE(col, path)
	if c, ok := binExpr.Left.(*ast.CallExpr); ok && strings.ToUpper(c.Name) == "JSON_VALUE" {
		call = c
		switch v := binExpr.Right.(type) {
		case *ast.StringLitExpr:
			litValue = v.Value
		case *ast.IntLitExpr:
			litInt = v.Value
			isInt = true
		default:
			return nil, "", false
		}
	} else if c, ok := binExpr.Right.(*ast.CallExpr); ok && strings.ToUpper(c.Name) == "JSON_VALUE" {
		call = c
		switch v := binExpr.Left.(type) {
		case *ast.StringLitExpr:
			litValue = v.Value
		case *ast.IntLitExpr:
			litInt = v.Value
			isInt = true
		default:
			return nil, "", false
		}
	} else {
		return nil, "", false
	}

	// JSON_VALUE requires exactly 2 args: column and path
	if len(call.Args) != 2 {
		return nil, "", false
	}
	ident, ok := call.Args[0].(*ast.IdentExpr)
	if !ok {
		return nil, "", false
	}
	pathLit, ok := call.Args[1].(*ast.StringLitExpr)
	if !ok {
		return nil, "", false
	}

	col, err := info.FindColumn(ident.Name)
	if err != nil {
		return nil, "", false
	}
	ginIdx := e.db.storage.LookupGinIndex(info.Name, col.Index)
	if ginIdx == nil || ginIdx.GetInfo().Tokenizer != "jsonb_path_ops" {
		return nil, "", false
	}

	// Build a minimal JSON document for the query pattern, then tokenize it
	// to get the hash token. E.g., path "$.status" + value "active"
	// → {"status": "active"} → tokenize → hash token
	token, err := jsonbPathOpsToken(pathLit.Value, litValue, litInt, isInt)
	if err != nil {
		return nil, "", false
	}

	rb := ginIdx.MatchTokenBitmap(token)
	if rb == nil {
		rb = storage.NewRoaringBitmap()
	}
	return rb, ginIdx.GetInfo().Name, true
}

// tryGinJsonbPathOpsIN checks if an IN expression matches the pattern
// JSON_VALUE(col, '$.path') IN ('val1', 'val2', ...) and uses a jsonb_path_ops GIN index.
func (e *Executor) tryGinJsonbPathOpsIN(inExpr *ast.InExpr, info *storage.TableInfo) (*storage.RoaringBitmap, string, bool) {
	call, ok := inExpr.Left.(*ast.CallExpr)
	if !ok || strings.ToUpper(call.Name) != "JSON_VALUE" {
		return nil, "", false
	}
	if len(call.Args) != 2 {
		return nil, "", false
	}
	ident, ok := call.Args[0].(*ast.IdentExpr)
	if !ok {
		return nil, "", false
	}
	pathLit, ok := call.Args[1].(*ast.StringLitExpr)
	if !ok {
		return nil, "", false
	}

	col, err := info.FindColumn(ident.Name)
	if err != nil {
		return nil, "", false
	}
	ginIdx := e.db.storage.LookupGinIndex(info.Name, col.Index)
	if ginIdx == nil || ginIdx.GetInfo().Tokenizer != "jsonb_path_ops" {
		return nil, "", false
	}

	result := storage.NewRoaringBitmap()
	for _, valExpr := range inExpr.Values {
		var token string
		switch v := valExpr.(type) {
		case *ast.StringLitExpr:
			token, err = jsonbPathOpsToken(pathLit.Value, v.Value, 0, false)
		case *ast.IntLitExpr:
			token, err = jsonbPathOpsToken(pathLit.Value, "", v.Value, true)
		default:
			return nil, "", false
		}
		if err != nil || token == "" {
			return nil, "", false
		}
		rb := ginIdx.MatchTokenBitmap(token)
		if rb != nil {
			result = result.Or(rb)
		}
	}
	return result, ginIdx.GetInfo().Name, true
}

// jsonbPathOpsToken builds a JSONB document from a JSON path and value,
// then tokenizes it to produce the GIN lookup token.
func jsonbPathOpsToken(path string, strVal string, intVal int64, isInt bool) (string, error) {
	// Parse path: "$", "$.key", "$.key1.key2", etc.
	pathStr := strings.TrimPrefix(path, "$")
	pathStr = strings.TrimPrefix(pathStr, ".")
	if pathStr == "" {
		return "", nil
	}
	keys := strings.Split(pathStr, ".")

	// Build nested JSON object from inside out
	var val any
	if isInt {
		val = intVal
	} else {
		val = strVal
	}

	obj := make(map[string]any)
	current := obj
	for i, key := range keys {
		if i == len(keys)-1 {
			current[key] = val
		} else {
			child := make(map[string]any)
			current[key] = child
			current = child
		}
	}

	b, err := jsonb.Encode(obj)
	if err != nil {
		return "", err
	}
	tokens, err := jsonb.PathOpsTokenize(b)
	if err != nil {
		return "", err
	}
	if len(tokens) != 1 {
		return "", nil
	}
	return tokens[0], nil
}

// longestLiteralSegment extracts the longest contiguous run of non-wildcard
// characters from a LIKE pattern. Wildcards are '%' and '_'.
func longestLiteralSegment(pattern string) string {
	best := ""
	current := []rune{}
	for _, r := range pattern {
		if r == '%' || r == '_' {
			if len(current) > len([]rune(best)) {
				best = string(current)
			}
			current = current[:0]
		} else {
			current = append(current, r)
		}
	}
	if len(current) > len([]rune(best)) {
		best = string(current)
	}
	return best
}
