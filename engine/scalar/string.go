package scalar

import (
	"fmt"
	"strings"
)

func evalFuncLength(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LENGTH requires string argument, got %T", args[0])
	}
	return int64(len([]rune(s))), nil
}

func evalFuncUpper(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("UPPER requires string argument, got %T", args[0])
	}
	return strings.ToUpper(s), nil
}

func evalFuncLower(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LOWER requires string argument, got %T", args[0])
	}
	return strings.ToLower(s), nil
}

func evalFuncSubstring(args []Value) (Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTRING requires 2 or 3 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING requires string as first argument, got %T", args[0])
	}
	pos, ok := args[1].(int64)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING requires integer as second argument, got %T", args[1])
	}
	runes := []rune(s)
	// 1-indexed to 0-indexed
	start := int(pos) - 1
	if start < 0 {
		start = 0
	}
	if start >= len(runes) {
		return "", nil
	}
	if len(args) == 3 {
		if args[2] == nil {
			return nil, nil
		}
		length, ok := args[2].(int64)
		if !ok {
			return nil, fmt.Errorf("SUBSTRING requires integer as third argument, got %T", args[2])
		}
		end := start + int(length)
		if end > len(runes) {
			end = len(runes)
		}
		return string(runes[start:end]), nil
	}
	return string(runes[start:]), nil
}

func evalFuncTrim(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TRIM requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("TRIM requires string argument, got %T", args[0])
	}
	return strings.TrimSpace(s), nil
}

func evalFuncConcat(args []Value) (Value, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("CONCAT requires at least 1 argument, got %d", len(args))
	}
	var b strings.Builder
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
		s, ok := arg.(string)
		if !ok {
			return nil, fmt.Errorf("CONCAT requires string arguments, got %T", arg)
		}
		b.WriteString(s)
	}
	return b.String(), nil
}
