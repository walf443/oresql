package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/walf443/oresql/engine"
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

	var opts []engine.Option
	if len(os.Args) > 1 {
		walPath := os.Args[1]
		wal, err := engine.NewWAL(walPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open WAL: %s\n", err)
			os.Exit(1)
		}
		defer wal.Close()
		opts = append(opts, engine.WithWAL(wal))
	}

	exec := engine.NewExecutor(opts...)

	if err := exec.ReplayWAL(); err != nil {
		fmt.Fprintf(os.Stderr, "WAL replay failed: %s\n", err)
		os.Exit(1)
	}

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

		result, err := exec.ExecuteSQL(line)
		if err != nil {
			writer.PrintError(err.Error())
			continue
		}

		writer.PrintResult(result)
	}
}
