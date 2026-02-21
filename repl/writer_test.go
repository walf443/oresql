package repl

import "testing"

func TestFormatValueFloat(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{float64(20), "20.0"},
		{float64(3.14), "3.14"},
		{float64(16.666666666666668), "16.666666666666668"},
		{float64(0), "0.0"},
		{int64(42), "42"},
		{"hello", "hello"},
		{nil, "<nil>"},
	}
	for _, tt := range tests {
		got := formatValue(tt.input)
		if got != tt.expected {
			t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
