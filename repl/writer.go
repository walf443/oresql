package repl

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/walf443/oresql/engine"
)

// Writer wraps an io.Writer to provide formatted output for the REPL.
type Writer struct {
	out io.Writer
}

// NewWriter creates a new Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{out: w}
}

// PrintResult writes a query result to the output.
// For SELECT results it prints column headers and rows;
// for other statements it prints the status message.
func (w *Writer) PrintResult(r *engine.Result) {
	if r.Message != "" {
		fmt.Fprintln(w.out, r.Message)
		return
	}

	// Print column headers
	fmt.Fprintln(w.out, strings.Join(r.Columns, "\t"))
	fmt.Fprintln(w.out, strings.Repeat("-", len(strings.Join(r.Columns, "\t"))+8))

	// Print rows
	for _, row := range r.Rows {
		vals := make([]string, len(row))
		for i, v := range row {
			vals[i] = formatValue(v)
		}
		fmt.Fprintln(w.out, strings.Join(vals, "\t"))
	}
	fmt.Fprintf(w.out, "(%d rows)\n", len(r.Rows))
}

// formatValue formats a single value for display.
// float64 values always include a decimal point (e.g. 20.0 instead of 20).
func formatValue(v interface{}) string {
	switch fv := v.(type) {
	case float64:
		s := strconv.FormatFloat(fv, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	default:
		return fmt.Sprintf("%v", v)
	}
}

// PrintError writes an error message prefixed with "Error: ".
func (w *Writer) PrintError(msg string) {
	fmt.Fprintf(w.out, "Error: %s\n", msg)
}

// Println writes a line of text to the output.
func (w *Writer) Println(msg string) {
	fmt.Fprintln(w.out, msg)
}
