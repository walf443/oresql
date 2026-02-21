package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/walf443/oresql/engine"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

func main() {
	historyFile := filepath.Join(os.TempDir(), ".oresql_history")
	if home, err := os.UserHomeDir(); err == nil {
		historyFile = filepath.Join(home, ".oresql_history")
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:       "oresql> ",
		HistoryFile:  historyFile,
		HistoryLimit: 1000,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize readline: %s\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	exec := engine.NewExecutor()

	fmt.Println("Welcome to oresql. Type SQL statements or 'exit' to quit.")

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			continue
		}
		if err == io.EOF {
			break
		}
		line = strings.TrimSpace(line)
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
