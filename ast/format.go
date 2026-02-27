package ast

import (
	"fmt"
	"strings"
)

// FormatSQL converts an Expr back to a SQL string representation.
// Binary operators (Arithmetic, Binary, Logical) are wrapped in parentheses to avoid ambiguity.
func FormatSQL(expr Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *IntLitExpr:
		return fmt.Sprintf("%d", e.Value)
	case *FloatLitExpr:
		return fmt.Sprintf("%g", e.Value)
	case *StringLitExpr:
		return "'" + e.Value + "'"
	case *NullLitExpr:
		return "NULL"
	case *BoolLitExpr:
		if e.Value {
			return "TRUE"
		}
		return "FALSE"
	case *IdentExpr:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *StarExpr:
		return "*"
	case *AliasExpr:
		return FormatSQL(e.Expr) + " AS " + e.Alias
	case *ArithmeticExpr:
		return "(" + FormatSQL(e.Left) + " " + e.Op + " " + FormatSQL(e.Right) + ")"
	case *BinaryExpr:
		return "(" + FormatSQL(e.Left) + " " + e.Op + " " + FormatSQL(e.Right) + ")"
	case *LogicalExpr:
		return "(" + FormatSQL(e.Left) + " " + e.Op + " " + FormatSQL(e.Right) + ")"
	case *NotExpr:
		return "NOT " + FormatSQL(e.Expr)
	case *IsNullExpr:
		if e.Not {
			return FormatSQL(e.Expr) + " IS NOT NULL"
		}
		return FormatSQL(e.Expr) + " IS NULL"
	case *InExpr:
		var buf strings.Builder
		buf.WriteString(FormatSQL(e.Left))
		if e.Not {
			buf.WriteString(" NOT IN ")
		} else {
			buf.WriteString(" IN ")
		}
		if e.Subquery != nil {
			buf.WriteString("(SELECT ...)")
		} else {
			buf.WriteString("(")
			for i, v := range e.Values {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(FormatSQL(v))
			}
			buf.WriteString(")")
		}
		return buf.String()
	case *BetweenExpr:
		var buf strings.Builder
		buf.WriteString(FormatSQL(e.Left))
		if e.Not {
			buf.WriteString(" NOT BETWEEN ")
		} else {
			buf.WriteString(" BETWEEN ")
		}
		buf.WriteString(FormatSQL(e.Low))
		buf.WriteString(" AND ")
		buf.WriteString(FormatSQL(e.High))
		return buf.String()
	case *LikeExpr:
		var buf strings.Builder
		buf.WriteString(FormatSQL(e.Left))
		if e.Not {
			buf.WriteString(" NOT LIKE ")
		} else {
			buf.WriteString(" LIKE ")
		}
		buf.WriteString(FormatSQL(e.Pattern))
		return buf.String()
	case *CallExpr:
		var buf strings.Builder
		buf.WriteString(e.Name)
		buf.WriteString("(")
		for i, arg := range e.Args {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(FormatSQL(arg))
		}
		buf.WriteString(")")
		return buf.String()
	case *CastExpr:
		return "CAST(" + FormatSQL(e.Expr) + " AS " + e.TargetType + ")"
	case *CaseExpr:
		var buf strings.Builder
		buf.WriteString("CASE")
		if e.Operand != nil {
			buf.WriteString(" ")
			buf.WriteString(FormatSQL(e.Operand))
		}
		for _, w := range e.Whens {
			buf.WriteString(" WHEN ")
			buf.WriteString(FormatSQL(w.When))
			buf.WriteString(" THEN ")
			buf.WriteString(FormatSQL(w.Then))
		}
		if e.Else != nil {
			buf.WriteString(" ELSE ")
			buf.WriteString(FormatSQL(e.Else))
		}
		buf.WriteString(" END")
		return buf.String()
	case *ExistsExpr:
		if e.Not {
			return "NOT EXISTS (SELECT ...)"
		}
		return "EXISTS (SELECT ...)"
	case *WindowExpr:
		var buf strings.Builder
		buf.WriteString(e.Name)
		buf.WriteString("(")
		for i, arg := range e.Args {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(FormatSQL(arg))
		}
		buf.WriteString(") OVER ")
		if e.WindowName != "" {
			buf.WriteString(e.WindowName)
		} else {
			buf.WriteString("(")
			parts := []string{}
			if len(e.PartitionBy) > 0 {
				pb := "PARTITION BY "
				for i, p := range e.PartitionBy {
					if i > 0 {
						pb += ", "
					}
					pb += FormatSQL(p)
				}
				parts = append(parts, pb)
			}
			if len(e.OrderBy) > 0 {
				ob := "ORDER BY "
				for i, o := range e.OrderBy {
					if i > 0 {
						ob += ", "
					}
					ob += FormatSQL(o.Expr)
					if o.Desc {
						ob += " DESC"
					}
				}
				parts = append(parts, ob)
			}
			buf.WriteString(strings.Join(parts, " "))
			buf.WriteString(")")
		}
		return buf.String()
	case *ScalarExpr:
		return "(SELECT ...)"
	default:
		return "?"
	}
}
