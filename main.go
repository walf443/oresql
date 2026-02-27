package main

import (
	"flag"
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
	dataDir := flag.String("data-dir", "", "directory for persistent storage (omit for in-memory)")
	walPath := flag.String("wal", "", "path to write-ahead log file")
	flag.Parse()

	// Backward compatibility: first positional arg is WAL path
	if *walPath == "" && flag.NArg() > 0 {
		*walPath = flag.Arg(0)
	}

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

	var execOpts []engine.Option
	if *walPath != "" {
		wal, err := engine.NewWAL(*walPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open WAL: %s\n", err)
			os.Exit(1)
		}
		defer wal.Close()
		execOpts = append(execOpts, engine.WithWAL(wal))
	}

	var dbOpts []engine.DatabaseOption
	if *dataDir != "" {
		dbOpts = append(dbOpts, engine.WithDataDir(*dataDir))
	}

	db := engine.NewDatabase("default", dbOpts...)
	exec := engine.NewExecutor(db, execOpts...)

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
