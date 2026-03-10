// Package json_path provides JSON path parsing and traversal for parsed JSON values.
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
//
// Use Parse to compile a path expression once, then Execute to apply it
// to multiple JSON values efficiently:
//
//	p, err := json_path.Parse("$.name")
//	if err != nil { ... }
//	val1 := p.Execute(jsonObj1)
//	val2 := p.Execute(jsonObj2)
package json_path

import (
	"fmt"
	"strconv"
	"strings"
)

// StepKind represents the type of a path step.
type StepKind int

const (
	// StepMember represents object member access (.key).
	StepMember StepKind = iota
	// StepIndex represents array index access ([n]).
	StepIndex
)

// Step represents a single navigation step in a JSON path.
type Step struct {
	Kind  StepKind
	Key   string // used when Kind == StepMember
	Index int    // used when Kind == StepIndex
}

// Path represents a parsed JSON path expression as a sequence of steps.
type Path struct {
	Steps []Step
}

// Parse compiles a JSON path expression into a Path.
// The path must start with '$'. Parse validates the syntax and returns
// an error if the path is malformed.
func Parse(path string) (*Path, error) {
	if len(path) == 0 || path[0] != '$' {
		return nil, fmt.Errorf("JSON path must start with '$', got %q", path)
	}
	path = path[1:] // skip '$'

	var steps []Step
	for len(path) > 0 {
		if path[0] == '.' {
			path = path[1:] // skip '.'
			end := 0
			for end < len(path) && path[end] != '.' && path[end] != '[' {
				end++
			}
			if end == 0 {
				return nil, fmt.Errorf("JSON path: empty key after '.'")
			}
			steps = append(steps, Step{Kind: StepMember, Key: path[:end]})
			path = path[end:]
		} else if path[0] == '[' {
			closeBracket := strings.IndexByte(path, ']')
			if closeBracket < 0 {
				return nil, fmt.Errorf("JSON path: missing ']' in %q", path)
			}
			indexStr := path[1:closeBracket]
			idx, err := strconv.Atoi(indexStr)
			if err != nil {
				return nil, fmt.Errorf("JSON path: invalid array index %q", indexStr)
			}
			steps = append(steps, Step{Kind: StepIndex, Index: idx})
			path = path[closeBracket+1:]
		} else {
			return nil, fmt.Errorf("JSON path: unexpected character %q in path", string(path[0]))
		}
	}

	return &Path{Steps: steps}, nil
}

// Execute applies the parsed path to a JSON value and returns the result.
// The val parameter should be the result of json.Unmarshal into any.
// Returns nil if the path doesn't match (missing key, out-of-bounds index,
// or type mismatch such as member access on a non-object).
func (p *Path) Execute(val any) any {
	current := val
	for _, step := range p.Steps {
		if current == nil {
			return nil
		}
		switch step.Kind {
		case StepMember:
			obj, ok := current.(map[string]any)
			if !ok {
				return nil
			}
			v, exists := obj[step.Key]
			if !exists {
				return nil
			}
			current = v
		case StepIndex:
			arr, ok := current.([]any)
			if !ok {
				return nil
			}
			if step.Index < 0 || step.Index >= len(arr) {
				return nil
			}
			current = arr[step.Index]
		}
	}
	return current
}

// Traverse is a convenience function that parses the path and executes it
// in a single call. For repeated use of the same path, prefer Parse + Execute.
func Traverse(val any, path string) (any, error) {
	p, err := Parse(path)
	if err != nil {
		return nil, err
	}
	return p.Execute(val), nil
}
