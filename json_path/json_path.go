// Package json_path provides JSON path traversal for parsed JSON values.
//
// Supported path syntax:
//   - "$"     — root element
//   - ".key"  — object member access
//   - "[n]"   — array index access (0-based)
//
// Examples:
//   - "$"            — returns the root value
//   - "$.name"       — returns the "name" member of the root object
//   - "$.items[0]"   — returns the first element of the "items" array
//   - "$[1].id"      — returns the "id" member of the second array element
//   - "$.a.b.c"      — nested object member access
package json_path

import (
	"fmt"
	"strconv"
	"strings"
)

// Traverse navigates a parsed JSON value using a path expression.
// The val parameter should be the result of json.Unmarshal into interface{}.
// Returns nil if the path doesn't match (missing key, out-of-bounds index,
// or type mismatch such as member access on a non-object).
func Traverse(val interface{}, path string) (interface{}, error) {
	if len(path) == 0 || path[0] != '$' {
		return nil, fmt.Errorf("JSON path must start with '$', got %q", path)
	}
	path = path[1:] // skip '$'
	current := val

	for len(path) > 0 {
		if path[0] == '.' {
			// Object member access: .key
			path = path[1:] // skip '.'
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, nil // not an object, path doesn't match
			}
			// Read key name (until next '.', '[', or end)
			end := 0
			for end < len(path) && path[end] != '.' && path[end] != '[' {
				end++
			}
			key := path[:end]
			path = path[end:]
			v, exists := obj[key]
			if !exists {
				return nil, nil
			}
			current = v
		} else if path[0] == '[' {
			// Array index access: [n]
			closeBracket := strings.IndexByte(path, ']')
			if closeBracket < 0 {
				return nil, fmt.Errorf("JSON path: missing ']' in %q", path)
			}
			indexStr := path[1:closeBracket]
			path = path[closeBracket+1:]
			idx, err := strconv.Atoi(indexStr)
			if err != nil {
				return nil, fmt.Errorf("JSON path: invalid array index %q", indexStr)
			}
			arr, ok := current.([]interface{})
			if !ok {
				return nil, nil // not an array
			}
			if idx < 0 || idx >= len(arr) {
				return nil, nil // out of bounds
			}
			current = arr[idx]
		} else {
			return nil, fmt.Errorf("JSON path: unexpected character %q in path", string(path[0]))
		}
	}

	return current, nil
}
