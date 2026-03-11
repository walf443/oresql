package engine

import (
	"fmt"
	"math"
)

func evalFuncAbs(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ABS requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		if v < 0 {
			return -v, nil
		}
		return v, nil
	case float64:
		return math.Abs(v), nil
	default:
		return nil, fmt.Errorf("ABS requires numeric argument, got %T", args[0])
	}
}

func evalFuncRound(args []Value) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ROUND requires 1 or 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	d := int64(0)
	if len(args) == 2 {
		if args[1] == nil {
			return nil, nil
		}
		switch v := args[1].(type) {
		case int64:
			d = v
		default:
			return nil, fmt.Errorf("ROUND precision must be integer, got %T", args[1])
		}
	}
	switch v := args[0].(type) {
	case int64:
		if d >= 0 {
			return v, nil
		}
		shift := math.Pow(10, float64(-d))
		return int64(math.Round(float64(v)/shift) * shift), nil
	case float64:
		shift := math.Pow(10, float64(d))
		return math.Round(v*shift) / shift, nil
	default:
		return nil, fmt.Errorf("ROUND requires numeric argument, got %T", args[0])
	}
}

func evalFuncMod(args []Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MOD requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	if a, ok := args[0].(int64); ok {
		if b, ok := args[1].(int64); ok {
			if b == 0 {
				return nil, fmt.Errorf("MOD division by zero")
			}
			return a % b, nil
		}
	}
	af, aok := toFloat64(args[0])
	bf, bok := toFloat64(args[1])
	if aok && bok {
		if bf == 0 {
			return nil, fmt.Errorf("MOD division by zero")
		}
		return math.Mod(af, bf), nil
	}
	return nil, fmt.Errorf("MOD requires numeric arguments, got %T and %T", args[0], args[1])
}

func evalFuncCeil(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CEIL requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		return v, nil
	case float64:
		return int64(math.Ceil(v)), nil
	default:
		return nil, fmt.Errorf("CEIL requires numeric argument, got %T", args[0])
	}
}

func evalFuncFloor(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FLOOR requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		return v, nil
	case float64:
		return int64(math.Floor(v)), nil
	default:
		return nil, fmt.Errorf("FLOOR requires numeric argument, got %T", args[0])
	}
}

func evalFuncPower(args []Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("POWER requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	xf, xok := toFloat64(args[0])
	yf, yok := toFloat64(args[1])
	if !xok || !yok {
		return nil, fmt.Errorf("POWER requires numeric arguments, got %T and %T", args[0], args[1])
	}
	return math.Pow(xf, yf), nil
}
