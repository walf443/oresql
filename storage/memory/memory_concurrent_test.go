package memory

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/walf443/oresql/storage"
)

func TestWithTableLocksConcurrentReads(t *testing.T) {
	s := NewMemoryStorage()
	s.CreateTable(&storage.TableInfo{Name: "t", PrimaryKeyCol: -1})

	var wg sync.WaitGroup
	started := make(chan struct{})
	count := 10

	// All goroutines should be able to acquire read locks concurrently
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.WithTableLocks([]storage.TableLock{{TableName: "t", Mode: storage.TableLockRead}}, false, func() error {
				started <- struct{}{}
				// Wait a bit to ensure concurrent read access
				return nil
			})
			assert.NoError(t, err)
		}()
	}

	// Drain started signals
	go func() {
		for i := 0; i < count; i++ {
			<-started
		}
	}()

	wg.Wait()
}

func TestWithTableLocksWriteBlocksRead(t *testing.T) {
	s := NewMemoryStorage()
	s.CreateTable(&storage.TableInfo{Name: "t", PrimaryKeyCol: -1})

	var mu sync.Mutex
	var order []string

	var wg sync.WaitGroup
	writeStarted := make(chan struct{})
	writeDone := make(chan struct{})

	// Goroutine 1: acquire write lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.WithTableLocks([]storage.TableLock{{TableName: "t", Mode: storage.TableLockWrite}}, false, func() error {
			close(writeStarted)
			mu.Lock()
			order = append(order, "write")
			mu.Unlock()
			<-writeDone
			return nil
		})
		assert.NoError(t, err)
	}()

	// Wait for write lock to be held
	<-writeStarted

	// Goroutine 2: try to acquire read lock (should block until write is released)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.WithTableLocks([]storage.TableLock{{TableName: "t", Mode: storage.TableLockRead}}, false, func() error {
			mu.Lock()
			order = append(order, "read")
			mu.Unlock()
			return nil
		})
		assert.NoError(t, err)
	}()

	// Release write lock
	close(writeDone)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"write", "read"}, order)
}

func TestConcurrentInsertAndScan(t *testing.T) {
	s := NewMemoryStorage()
	s.CreateTable(&storage.TableInfo{Name: "t", PrimaryKeyCol: -1})

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Concurrent Inserts
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				err := s.Insert("t", storage.Row{int64(goroutineID*1000 + i), "hello"})
				if err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}

	// Concurrent Scans
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, err := s.Scan("t")
				if err != nil {
					errs <- err
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

	// Verify total row count
	count, err := s.RowCount("t")
	assert.NoError(t, err)
	assert.Equal(t, 500, count)
}

func TestConcurrentForEachRowNoDeadlock(t *testing.T) {
	s := NewMemoryStorage()
	s.CreateTable(&storage.TableInfo{Name: "t", PrimaryKeyCol: -1})

	// Insert some rows
	for i := 0; i < 10; i++ {
		err := s.Insert("t", storage.Row{int64(i), "val"})
		assert.NoError(t, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	// ForEachRow with callback that re-reads the same table (simulating subquery)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				err := s.ForEachRow("t", false, func(key int64, row storage.Row) bool {
					// Simulate subquery: scan the same table inside callback
					_, scanErr := s.Scan("t")
					if scanErr != nil {
						return false
					}
					return true
				}, 0)
				if err != nil {
					errs <- err
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

func TestWithTableLocksSortOrder(t *testing.T) {
	s := NewMemoryStorage()
	s.CreateTable(&storage.TableInfo{Name: "alpha", PrimaryKeyCol: -1})
	s.CreateTable(&storage.TableInfo{Name: "beta", PrimaryKeyCol: -1})

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Two goroutines accessing the same two tables in different order
	// Both should work without deadlock because locks are sorted alphabetically
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := s.WithTableLocks([]storage.TableLock{
				{TableName: "beta", Mode: storage.TableLockWrite},
				{TableName: "alpha", Mode: storage.TableLockWrite},
			}, false, func() error {
				return nil
			})
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := s.WithTableLocks([]storage.TableLock{
				{TableName: "alpha", Mode: storage.TableLockRead},
				{TableName: "beta", Mode: storage.TableLockRead},
			}, false, func() error {
				return nil
			})
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
