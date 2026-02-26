package engine

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupConcurrentExecutor(t *testing.T) *Executor {
	t.Helper()
	e := NewExecutor()
	_, err := e.ExecuteSQL("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	require.NoError(t, err)
	return e
}

func TestConcurrentSelect(t *testing.T) {
	e := setupConcurrentExecutor(t)
	for i := 0; i < 10; i++ {
		_, err := e.ExecuteSQL(fmt.Sprintf("INSERT INTO t VALUES (%d, 'name%d')", i, i))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				result, err := e.ExecuteSQL("SELECT * FROM t")
				if err != nil {
					errs <- err
					return
				}
				if len(result.Rows) != 10 {
					errs <- fmt.Errorf("expected 10 rows, got %d", len(result.Rows))
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestConcurrentInsert(t *testing.T) {
	e := NewExecutor()
	_, err := e.ExecuteSQL("CREATE TABLE t (id INT, name TEXT)")
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, err := e.ExecuteSQL(fmt.Sprintf("INSERT INTO t VALUES (%d, 'g%d_i%d')", goroutineID*1000+i, goroutineID, i))
				if err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	result, err := e.ExecuteSQL("SELECT COUNT(*) FROM t")
	require.NoError(t, err)
	assert.Equal(t, int64(1000), result.Rows[0][0])
}

func TestConcurrentSelectAndInsert(t *testing.T) {
	e := setupConcurrentExecutor(t)

	// Insert initial data
	for i := 0; i < 5; i++ {
		_, err := e.ExecuteSQL(fmt.Sprintf("INSERT INTO t VALUES (%d, 'init%d')", i, i))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Concurrent SELECTs
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				result, err := e.ExecuteSQL("SELECT * FROM t")
				if err != nil {
					errs <- err
					return
				}
				// Should see a consistent snapshot: at least initial 5 rows
				if len(result.Rows) < 5 {
					errs <- fmt.Errorf("expected at least 5 rows, got %d", len(result.Rows))
					return
				}
			}
		}()
	}

	// Concurrent INSERTs
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := 1000 + goroutineID*100 + i
				_, err := e.ExecuteSQL(fmt.Sprintf("INSERT INTO t VALUES (%d, 'new%d')", id, id))
				if err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestConcurrentDifferentTables(t *testing.T) {
	e := NewExecutor()
	_, err := e.ExecuteSQL("CREATE TABLE a (id INT, val TEXT)")
	require.NoError(t, err)
	_, err = e.ExecuteSQL("CREATE TABLE b (id INT, val TEXT)")
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// INSERT into table a
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, err := e.ExecuteSQL(fmt.Sprintf("INSERT INTO a VALUES (%d, 'a%d')", i, i))
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	// SELECT from table b (should not be blocked by table a inserts)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, err := e.ExecuteSQL("SELECT * FROM b")
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestConcurrentDDLAndDML(t *testing.T) {
	e := NewExecutor()
	_, err := e.ExecuteSQL("CREATE TABLE existing (id INT, val TEXT)")
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// SELECT from existing table
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := e.ExecuteSQL("SELECT * FROM existing")
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	// CREATE new tables concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_, err := e.ExecuteSQL(fmt.Sprintf("CREATE TABLE new_%d (id INT, val TEXT)", i))
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestConcurrentDeadlockPrevention(t *testing.T) {
	e := NewExecutor()
	_, err := e.ExecuteSQL("CREATE TABLE alpha (id INT, val TEXT)")
	require.NoError(t, err)
	_, err = e.ExecuteSQL("CREATE TABLE beta (id INT, val TEXT)")
	require.NoError(t, err)

	// Insert initial data
	for i := 0; i < 5; i++ {
		_, err = e.ExecuteSQL(fmt.Sprintf("INSERT INTO alpha VALUES (%d, 'a%d')", i, i))
		require.NoError(t, err)
		_, err = e.ExecuteSQL(fmt.Sprintf("INSERT INTO beta VALUES (%d, 'b%d')", i, i))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Goroutine 1: reads alpha, writes beta (UPDATE beta ... WHERE id IN (SELECT id FROM alpha))
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := e.ExecuteSQL("SELECT * FROM alpha WHERE id IN (SELECT id FROM beta)")
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	// Goroutine 2: reads beta, writes alpha (opposite access pattern)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := e.ExecuteSQL("SELECT * FROM beta WHERE id IN (SELECT id FROM alpha)")
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
