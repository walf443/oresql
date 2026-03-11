package scalar

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/walf443/oresql/json_path"
	"github.com/walf443/oresql/jsonb"
)

// evalFuncJSONObject builds a JSON object from alternating key-value arguments.
// Usage: JSON_OBJECT('key1', val1, 'key2', val2, ...)
func evalFuncJSONObject(args []Value) (Value, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("JSON_OBJECT requires an even number of arguments (key-value pairs), got %d", len(args))
	}
	var buf strings.Builder
	buf.WriteByte('{')
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			return nil, fmt.Errorf("JSON_OBJECT key must be a string, got %T", args[i])
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		buf.Write(keyJSON)
		buf.WriteByte(':')
		valJSON, err := valueToJSON(args[i+1])
		if err != nil {
			return nil, err
		}
		buf.Write(valJSON)
	}
	buf.WriteByte('}')
	return buf.String(), nil
}

// evalFuncJSONArray builds a JSON array from arguments.
// Usage: JSON_ARRAY(val1, val2, ...)
func evalFuncJSONArray(args []Value) (Value, error) {
	var buf strings.Builder
	buf.WriteByte('[')
	for i, arg := range args {
		if i > 0 {
			buf.WriteByte(',')
		}
		valJSON, err := valueToJSON(arg)
		if err != nil {
			return nil, err
		}
		buf.Write(valJSON)
	}
	buf.WriteByte(']')
	return buf.String(), nil
}

// evalFuncJSONValue extracts a scalar value from a JSON string using a path expression.
// Usage: JSON_VALUE(json_text, path)
// Returns NULL if path points to a non-scalar (object/array) or if the path doesn't exist.
func evalFuncJSONValue(args []Value, compiledPath *json_path.Path) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_VALUE requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	pathStr, ok := args[1].(string)
	if compiledPath == nil && !ok {
		return nil, fmt.Errorf("JSON_VALUE second argument (path) must be a string, got %T", args[1])
	}

	// JSONB optimization: traverse binary directly without decode→JSON→parse round-trip
	if b, isBinary := args[0].([]byte); isBinary {
		result, err := jsonbTraverse("JSON_VALUE", b, pathStr, compiledPath)
		if err != nil {
			return nil, err
		}
		return jsonValueResult(result)
	}

	jsonStr, err := jsonStringFromValue("JSON_VALUE", args[0])
	if err != nil {
		return nil, err
	}

	result, err := parseJSONAndTraverse("JSON_VALUE", jsonStr, pathStr, compiledPath)
	if err != nil {
		return nil, err
	}
	return jsonValueResult(result)
}

// evalFuncJSONQuery extracts a JSON object or array from a JSON string using a path expression.
// Usage: JSON_QUERY(json_text, path)
// Returns NULL if the path points to a scalar value or doesn't exist.
func evalFuncJSONQuery(args []Value, compiledPath *json_path.Path) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_QUERY requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	pathStr, ok := args[1].(string)
	if compiledPath == nil && !ok {
		return nil, fmt.Errorf("JSON_QUERY second argument (path) must be a string, got %T", args[1])
	}

	// JSONB optimization
	if b, isBinary := args[0].([]byte); isBinary {
		result, err := jsonbTraverse("JSON_QUERY", b, pathStr, compiledPath)
		if err != nil {
			return nil, err
		}
		return jsonQueryResult(result)
	}

	jsonStr, err := jsonStringFromValue("JSON_QUERY", args[0])
	if err != nil {
		return nil, err
	}

	result, err := parseJSONAndTraverse("JSON_QUERY", jsonStr, pathStr, compiledPath)
	if err != nil {
		return nil, err
	}
	return jsonQueryResult(result)
}

// evalFuncJSONExists checks whether a path exists in a JSON string.
// Usage: JSON_EXISTS(json_text, path)
// Returns TRUE if the path exists (including JSON null values), FALSE otherwise.
func evalFuncJSONExists(args []Value, compiledPath *json_path.Path) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_EXISTS requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	pathStr, ok := args[1].(string)
	if compiledPath == nil && !ok {
		return nil, fmt.Errorf("JSON_EXISTS second argument (path) must be a string, got %T", args[1])
	}

	// JSONB optimization: use ExistsPath directly
	if b, isBinary := args[0].([]byte); isBinary {
		if compiledPath != nil {
			return jsonb.ExistsPath(b, compiledPath), nil
		}
		p, err := json_path.Parse(pathStr)
		if err != nil {
			return nil, err
		}
		return jsonb.ExistsPath(b, p), nil
	}

	jsonStr, err := jsonStringFromValue("JSON_EXISTS", args[0])
	if err != nil {
		return nil, err
	}

	var raw any
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return nil, fmt.Errorf("JSON_EXISTS: invalid JSON: %w", err)
	}

	if compiledPath != nil {
		return compiledPath.Exists(raw), nil
	}

	p, err := json_path.Parse(pathStr)
	if err != nil {
		return nil, err
	}
	return p.Exists(raw), nil
}

// jsonbTraverse traverses JSONB binary data using a path expression directly,
// avoiding the decode→JSON→parse round-trip.
func jsonbTraverse(funcName string, b []byte, pathStr string, compiledPath *json_path.Path) (any, error) {
	if compiledPath != nil {
		val, found, err := jsonb.QueryPath(b, compiledPath)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", funcName, err)
		}
		if !found {
			return nil, nil
		}
		return val, nil
	}
	p, err := json_path.Parse(pathStr)
	if err != nil {
		return nil, err
	}
	val, found, err := jsonb.QueryPath(b, p)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", funcName, err)
	}
	if !found {
		return nil, nil
	}
	return val, nil
}

// jsonValueResult converts a traversal result to JSON_VALUE semantics:
// scalars are returned as strings (per SQL standard), objects/arrays return NULL.
func jsonValueResult(result any) (Value, error) {
	if result == nil {
		return nil, nil
	}
	switch v := result.(type) {
	case map[string]any, []any:
		return nil, nil
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v)), nil
		}
		return fmt.Sprintf("%v", v), nil
	case int64:
		return fmt.Sprintf("%d", v), nil
	case string:
		return v, nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	default:
		return nil, fmt.Errorf("JSON_VALUE: unexpected type %T", result)
	}
}

// jsonQueryResult converts a traversal result to JSON_QUERY semantics:
// objects/arrays are serialized to JSON string, scalars return NULL.
func jsonQueryResult(result any) (Value, error) {
	if result == nil {
		return nil, nil
	}
	switch result.(type) {
	case map[string]any, []any:
		b, err := json.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("JSON_QUERY: failed to serialize result: %w", err)
		}
		return string(b), nil
	default:
		return nil, nil
	}
}

// parseJSONAndTraverse parses JSON text, traverses it with a path, and returns the result.
func parseJSONAndTraverse(funcName string, jsonStr string, pathStr string, compiledPath *json_path.Path) (any, error) {
	var raw any
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return nil, fmt.Errorf("%s: invalid JSON: %w", funcName, err)
	}

	if compiledPath != nil {
		return compiledPath.Execute(raw), nil
	}

	result, err := json_path.Traverse(raw, pathStr)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// jsonStringFromValue extracts a JSON string from a value.
// Accepts string (JSON text) or []byte (JSONB/msgpack) values.
func jsonStringFromValue(funcName string, val Value) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		s, err := jsonb.ToJSON(v)
		if err != nil {
			return "", fmt.Errorf("%s: %w", funcName, err)
		}
		return s, nil
	default:
		return "", fmt.Errorf("%s first argument must be a string, got %T", funcName, val)
	}
}

// valueToJSON converts a Go value to its JSON representation.
func valueToJSON(val Value) ([]byte, error) {
	if val == nil {
		return []byte("null"), nil
	}
	switch v := val.(type) {
	case string:
		if json.Valid([]byte(v)) {
			return []byte(v), nil
		}
		return json.Marshal(v)
	case []byte:
		// JSONB: decode msgpack to JSON
		s, err := jsonb.ToJSON(v)
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	case int64:
		return json.Marshal(v)
	case float64:
		return json.Marshal(v)
	case bool:
		return json.Marshal(v)
	default:
		return nil, fmt.Errorf("unsupported value type for JSON: %T", val)
	}
}

// jsonValid checks if a string is valid JSON.
func jsonValid(s string) bool {
	return json.Valid([]byte(s))
}
