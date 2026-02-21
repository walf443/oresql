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
	"github.com/walf443/oresql/repl"
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
	writer := repl.NewWriter(rl.Stdout())

	writer.Println("Welcome to oresql. Type SQL statements or 'exit' to quit.")

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
			writer.Println("Bye!")
			break
		}

		l := lexer.New(line)
		p := parser.New(l)
		stmt, err := p.Parse()
		if err != nil {
			writer.Println(fmt.Sprintf("Parse error: %s", err))
			continue
		}

		result, err := exec.Execute(stmt)
		if err != nil {
			writer.PrintError(err.Error())
			continue
		}

		writer.PrintResult(result)
	}
}
