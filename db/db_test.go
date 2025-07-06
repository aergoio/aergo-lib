/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */
package db

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	tmpDbTestKey1    = "tempkey1"
	tmpDbTestKey2    = "tempkey2"
	tmpDbTestStrVal1 = "val1"
	tmpDbTestStrVal2 = "val2"
	tmpDbTestIntVal1 = 1
	tmpDbTestIntVal2 = 2
)

func createTmpDB(key ImplType) (dir string, db DB) {
	dir, err := ioutil.TempDir("", string(key))
	if err != nil {
		log.Fatal(err)
	}

	db = NewDB(key, dir)

	return
}

func setInitData(db DB) {
	tx := db.NewTx()

	tx.Set([]byte("1"), []byte("1"))
	tx.Set([]byte("2"), []byte("2"))
	tx.Set([]byte("3"), []byte("3"))
	tx.Set([]byte("4"), []byte("4"))
	tx.Set([]byte("5"), []byte("5"))
	tx.Set([]byte("6"), []byte("6"))
	tx.Set([]byte("7"), []byte("7"))

	tx.Commit()
}

func TestGetSetDeleteExist(t *testing.T) {
	// for each db implementation
	for key := range dbImpls {
		dir, db := createTmpDB(key)

		// initial value of empty key must be empty byte
		assert.Empty(t, db.Get([]byte(tmpDbTestKey1)), db.Type())
		assert.False(t, db.Exist([]byte(tmpDbTestKey1)), db.Type())

		// set value
		db.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal1))

		// check value set
		assert.Equal(t, tmpDbTestStrVal1, string(db.Get([]byte(tmpDbTestKey1))), db.Type())
		assert.True(t, db.Exist([]byte(tmpDbTestKey1)), db.Type())

		// delete value
		db.Delete([]byte(tmpDbTestKey1))

		// value must be erased
		assert.Empty(t, db.Get([]byte(tmpDbTestKey1)), db.Type())
		assert.False(t, db.Exist([]byte(tmpDbTestKey1)), db.Type())

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestTransactionSet(t *testing.T) {

	for key := range dbImpls {
		dir, db := createTmpDB(key)

		// create a new writable tx
		tx := db.NewTx()

		// set the value in the tx
		tx.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal1))
		// the value will not visible at a db
		assert.Empty(t, db.Get([]byte(tmpDbTestKey1)), db.Type())

		tx.Commit()

		// after commit, the value visible from the db
		assert.Equal(t, tmpDbTestStrVal1, string(db.Get([]byte(tmpDbTestKey1))), db.Type())

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestTransactionDiscard(t *testing.T) {

	for key := range dbImpls {
		dir, db := createTmpDB(key)

		// create a new writable tx
		tx := db.NewTx()
		// kv_log does not support concurrent transactions on the same thread
		if key != "kv_log" {
			// discard test
			tx = db.NewTx()
		}
		// set the value in the tx
		tx.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal2))

		// discard tx
		tx.Discard()

		assert.Panics(t, func() { tx.Commit() }, "commit after discard is not allowed")

		// after discard, the value must be reset at the db
		assert.False(t, db.Exist([]byte(tmpDbTestKey1)), db.Type())

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestTransactionDiscardAfterCommit(t *testing.T) {

	for key := range dbImpls {
		dir, db := createTmpDB(key)

		// create a new writable tx
		tx := db.NewTx()
		// kv_log does not support concurrent transactions on the same thread
		if key != "kv_log" {
			// discard test
			tx = db.NewTx()
		}
		// set the value in the tx
		tx.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal2))

		// commit tx
		tx.Commit()

		// it should be no harm to discard tx after commit
		assert.NotPanics(t, func() { tx.Discard() }, "discard after commit is not allowed (DB %s)", key)

		// after discard, the value must be reset at the db
		assert.True(t, db.Exist([]byte(tmpDbTestKey1)), db.Type())

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestConcurrentTransaction(t *testing.T) {

	for key := range dbImpls {
		dir, db := createTmpDB(key)

		var wg sync.WaitGroup
		startCh := make(chan struct{})
		wg.Add(2)

		go func() {
			defer wg.Done()
			<-startCh // Wait for the signal to continue
			tx := db.NewTx()
			tx.Set([]byte(tmpDbTestKey2), []byte(tmpDbTestStrVal2))
			tx.Commit()
		}()

		go func() {
			defer wg.Done()
			<-startCh // Wait for the signal to continue
			tx := db.NewTx()
			tx.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal1))
			tx.Commit()
		}()

		// Signal both goroutines to continue
		close(startCh)

		// Wait for both goroutines to complete
		wg.Wait()

		assert.True(t, db.Exist([]byte(tmpDbTestKey1)), db.Type())
		assert.True(t, db.Exist([]byte(tmpDbTestKey2)), db.Type())
		assert.Equal(t, []byte(tmpDbTestStrVal1), db.Get([]byte(tmpDbTestKey1)), db.Type())
		assert.Equal(t, []byte(tmpDbTestStrVal2), db.Get([]byte(tmpDbTestKey2)), db.Type())
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestTransactionCocurrentCommits(t *testing.T) {
	// Note: BadgerDB implements "Last-Commit-Wins" concurrency control semantics.
	// When two transactions modify the same data concurrently, the transaction that
	// commits last will overwrite changes made by the earlier transaction without
	// raising conflicts or errors.
	for key := range dbImpls {
		// kv_log does not support concurrent transactions on the same thread
		if key == "kv_log" {
			continue
		}

		dir, db := createTmpDB(key)

		// create a new writable tx
		tx := db.NewTx()
		// discard test
		tx2 := db.NewTx()
		// set the value with different key
		tx.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal1))
		tx2.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal2))

		// commit tx
		tx.Commit()
		tx2.Commit()

		assert.True(t, db.Exist([]byte(tmpDbTestKey1)), db.Type())
		assert.Equal(t, []byte(tmpDbTestStrVal2), db.Get([]byte(tmpDbTestKey1)), db.Type())
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestTransactionDelete(t *testing.T) {

	for key := range dbImpls {
		dir, db := createTmpDB(key)

		// create a new writable tx
		tx := db.NewTx()

		// set the value in the tx
		tx.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal1))

		// delete the value in the tx
		tx.Delete([]byte(tmpDbTestKey1))

		tx.Commit()

		// after commit, check the value from the db
		assert.Equal(t, "", string(db.Get([]byte(tmpDbTestKey1))), db.Type())

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestTransactionCommitTwice(t *testing.T) {
	for key := range dbImpls {
		dir, db := createTmpDB(key)

		// create a new writable tx
		tx := db.NewTx()

		tx.Set([]byte(tmpDbTestKey1), []byte(tmpDbTestStrVal1))

		// a first commit will success
		tx.Commit()

		// a second commit will cause panic
		assert.Panics(t, func() { tx.Commit() }, "commit after commit is not allowed. (DBImpl %s)", key)

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestBulk(t *testing.T) {

	for key := range dbImpls {
		dir, db := createTmpDB(key)

		// create a new Bulk instance
		bulk := db.NewBulk()

		// set the huge number of value in the bulk
		for i := 0; i < 1000000; i++ {
			bulk.Set([]byte(fmt.Sprintf("key%d", i)),
				[]byte(tmpDbTestStrVal1))
		}

		bulk.Flush()

		// after commit, the value visible from the db

		for i := 0; i < 1000000; i++ {
			assert.Equal(t, tmpDbTestStrVal1, string(db.Get([]byte(fmt.Sprintf("key%d", i)))), db.Type())
		}

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestIter(t *testing.T) {

	for key := range dbImpls {
		dir, db := createTmpDB(key)

		setInitData(db)

		i := 1

		for iter := db.Iterator(nil, nil); iter.Valid(); iter.Next() {
			assert.EqualValues(t, strconv.Itoa(i), string(iter.Key()))
			i++
		}

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestRangeIter(t *testing.T) {

	for dbType := range dbImpls {
		dir, db := createTmpDB(dbType)

		setInitData(db)

		// test iteration 2 -> 5 (should return 2, 3, 4)
		expectedKeys := []string{"2", "3", "4"}
		expectedValues := []string{"2", "3", "4"}
		idx := 0
		for iter := db.Iterator([]byte("2"), []byte("5")); iter.Valid(); iter.Next() {
			require.Less(t, idx, len(expectedKeys), "Iterator returned more pairs than expected: %s", dbType)
			assert.EqualValues(t, expectedKeys[idx], string(iter.Key()), dbType)
			assert.EqualValues(t, expectedValues[idx], string(iter.Value()), dbType)
			idx++
		}
		assert.EqualValues(t, len(expectedKeys), idx, "Iterator did not return the expected number of pairs: %s", dbType)

		// nil same as []byte("0")
		// test iteration 0 -> 5 (should return 1, 2, 3, 4)
		expectedKeys = []string{"1", "2", "3", "4"}
		expectedValues = []string{"1", "2", "3", "4"}
		idx = 0
		for iter := db.Iterator(nil, []byte("5")); iter.Valid(); iter.Next() {
			require.Less(t, idx, len(expectedKeys), "Iterator returned more pairs than expected: %s", dbType)
			assert.EqualValues(t, expectedKeys[idx], string(iter.Key()), dbType)
			assert.EqualValues(t, expectedValues[idx], string(iter.Value()), dbType)
			idx++
		}
		assert.EqualValues(t, len(expectedKeys), idx, "Iterator did not return the expected number of pairs: %s", dbType)

		db.Close()
		os.RemoveAll(dir)
	}
}

func TestReverseIter(t *testing.T) {

	for dbType := range dbImpls {
		dir, db := createTmpDB(dbType)

		setInitData(db)

		// test reverse iteration 5 <- 2 (should return 5, 4, 3)
		expectedKeys := []string{"5", "4", "3"}
		expectedValues := []string{"5", "4", "3"}
		idx := 0
		for iter := db.Iterator([]byte("5"), []byte("2")); iter.Valid(); iter.Next() {
			require.Less(t, idx, len(expectedKeys), "Iterator returned more pairs than expected: %s", dbType)
			assert.EqualValues(t, expectedKeys[idx], string(iter.Key()), dbType)
			assert.EqualValues(t, expectedValues[idx], string(iter.Value()), dbType)
			idx++
		}
		assert.EqualValues(t, len(expectedKeys), idx, "Iterator did not return the expected number of pairs: %s", dbType)

		// nil same as []byte("0")
		// test reverse iteration 5 -> 0 (should return 5, 4, 3, 2, 1)
		expectedKeys = []string{"5", "4", "3", "2", "1"}
		expectedValues = []string{"5", "4", "3", "2", "1"}
		idx = 0
		for iter := db.Iterator([]byte("5"), nil); iter.Valid(); iter.Next() {
			require.Less(t, idx, len(expectedKeys), "Iterator returned more pairs than expected: %s", dbType)
			assert.EqualValues(t, expectedKeys[idx], string(iter.Key()), dbType)
			assert.EqualValues(t, expectedValues[idx], string(iter.Value()), dbType)
			idx++
		}
		assert.EqualValues(t, len(expectedKeys), idx, "Iterator did not return the expected number of pairs: %s", dbType)

		db.Close()
		os.RemoveAll(dir)
	}
}
