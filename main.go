package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/walf443/oresql/engine"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

func main() {
	exec := engine.NewExecutor()
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("oresql> Welcome to oresql. Type SQL statements or 'exit' to quit.")

	for {
		fmt.Print("oresql> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "exit") || strings.EqualFold(line, "quit") {
			fmt.Println("Bye!")
			break
		}

		l := lexer.New(line)
		p := parser.New(l)
		stmt, err := p.Parse()
		if err != nil {
			fmt.Printf("Parse error: %s\n", err)
			continue
		}

		result, err := exec.Execute(stmt)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			continue
		}

		printResult(result)
	}
}

func printResult(r *engine.Result) {
	if r.Message != "" {
		fmt.Println(r.Message)
		return
	}

	// Print column headers
	fmt.Println(strings.Join(r.Columns, "\t"))
	fmt.Println(strings.Repeat("-", len(strings.Join(r.Columns, "\t"))+8))

	// Print rows
	for _, row := range r.Rows {
		vals := make([]string, len(row))
		for i, v := range row {
			vals[i] = fmt.Sprintf("%v", v)
		}
		fmt.Println(strings.Join(vals, "\t"))
	}
	fmt.Printf("(%d rows)\n", len(r.Rows))
}
