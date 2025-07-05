/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/aergoio/kv_log"
)

// This function is always called first
func init() {
	dbConstructor := func(dir string, opts ...Option) (DB, error) {
		return newKVLogDB(dir, opts...)
	}
	registerDBConstructor(KvLogImpl, dbConstructor)
}

func newKVLogDB(dir string, opts ...Option) (DB, error) {
	dbPath := filepath.Join(dir, "data.db")

	// Default options
	options := kv_log.Options{"ReadOnly": false}

	// Process options
	for _, opt := range opts {
		switch opt.Name {
		case "ReadOnly":
			if readOnly, ok := opt.Value.(bool); ok {
				options["ReadOnly"] = readOnly
			}
		// Add other kv_log specific options here as needed
		}
	}

	// Open kv_log database
	db, err := kv_log.Open(dbPath, options)
	if err != nil {
		return nil, err
	}

	database := &kvLogDB{
		db:   db,
		path: dbPath,
	}

	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*kvLogDB)(nil)

type kvLogDB struct {
	lock sync.RWMutex
	db   *kv_log.DB
	path string
}

func (db *kvLogDB) Type() string {
	return "kv_log"
}

func (db *kvLogDB) Path() string {
	return db.path
}

func (db *kvLogDB) Set(key, value []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := db.db.Set(key, value)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *kvLogDB) Delete(key []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	err := db.db.Delete(key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *kvLogDB) Get(key []byte) []byte {
	db.lock.RLock()
	defer db.lock.RUnlock()

	key = convNilToBytes(key)

	value, err := db.db.Get(key)
	if err != nil {
		// If key doesn't exist, return empty byte array
		if err.Error() == "key not found" {
			return []byte{}
		}
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	return value
}

func (db *kvLogDB) Exist(key []byte) bool {
	db.lock.RLock()
	defer db.lock.RUnlock()

	key = convNilToBytes(key)

	// kv_log doesn't have a Has/Exist method, so we use Get and check for errors
	_, err := db.db.Get(key)
	// If there's no error, the key exists
	return err == nil
}

func (db *kvLogDB) Close() {
	db.lock.Lock()
	defer db.lock.Unlock()

	if err := db.db.Close(); err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *kvLogDB) IoCtl(ioCtlType string) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// kv_log doesn't have a Sync method, so we just ignore the IoCtl call
}

func (db *kvLogDB) NewTx() Transaction {
	tx, err := db.db.Begin()
	if err != nil {
		panic(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	return &kvLogTransaction{
		tx:          tx,
		isDiscarded: false,
		isCommitted: false,
	}
}

func (db *kvLogDB) NewBulk() Bulk {
	tx, err := db.db.Begin()
	if err != nil {
		panic(fmt.Sprintf("Failed to begin bulk transaction: %v", err))
	}
	return &kvLogBulk{
		tx:          tx,
		isDiscarded: false,
		isCommitted: false,
	}
}

//=========================================================
// Transaction Implementation
//=========================================================

type kvLogTransaction struct {
	tx          *kv_log.Transaction
	isDiscarded bool
	isCommitted bool
}

func (transaction *kvLogTransaction) Set(key, value []byte) {
	if transaction.isDiscarded {
		panic("Transaction has been discarded")
	}
	if transaction.isCommitted {
		panic("Transaction has been committed")
	}

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := transaction.tx.Set(key, value)
	if err != nil {
		panic(fmt.Sprintf("Transaction Set Error: %v", err))
	}
}

func (transaction *kvLogTransaction) Delete(key []byte) {
	if transaction.isDiscarded {
		panic("Transaction has been discarded")
	}
	if transaction.isCommitted {
		panic("Transaction has been committed")
	}

	key = convNilToBytes(key)

	err := transaction.tx.Delete(key)
	if err != nil {
		panic(fmt.Sprintf("Transaction Delete Error: %v", err))
	}
}

func (transaction *kvLogTransaction) Get(key []byte) []byte {
	if transaction.isDiscarded {
		panic("Transaction has been discarded")
	}
	if transaction.isCommitted {
		panic("Transaction has been committed")
	}

	key = convNilToBytes(key)

	value, err := transaction.tx.Get(key)
	if err != nil {
		// If key doesn't exist, return empty byte array
		if err.Error() == "key not found" {
			return []byte{}
		}
		panic(fmt.Sprintf("Transaction Get Error: %v", err))
	}
	return value
}

func (transaction *kvLogTransaction) Commit() {
	if transaction.isDiscarded {
		panic("Commit after discard tx is not allowed")
	} else if transaction.isCommitted {
		panic("Commit occurs two times")
	}

	err := transaction.tx.Commit()
	if err != nil {
		panic(fmt.Sprintf("Transaction Commit Error: %v", err))
	}

	transaction.isCommitted = true
}

func (transaction *kvLogTransaction) Discard() {
	if transaction.isCommitted {
		return // Already committed, nothing to discard
	}

	if !transaction.isDiscarded {
		err := transaction.tx.Rollback()
		if err != nil {
			panic(fmt.Sprintf("Transaction Rollback Error: %v", err))
		}
		transaction.isDiscarded = true
	}
}

//=========================================================
// Bulk Implementation
//=========================================================

type kvLogBulk struct {
	tx          *kv_log.Transaction
	isDiscarded bool
	isCommitted bool
}

func (bulk *kvLogBulk) Set(key, value []byte) {
	if bulk.isDiscarded {
		panic("Bulk operation has been discarded")
	}
	if bulk.isCommitted {
		panic("Bulk operation has been committed")
	}

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := bulk.tx.Set(key, value)
	if err != nil {
		panic(fmt.Sprintf("Bulk Set Error: %v", err))
	}
}

func (bulk *kvLogBulk) Delete(key []byte) {
	if bulk.isDiscarded {
		panic("Bulk operation has been discarded")
	}
	if bulk.isCommitted {
		panic("Bulk operation has been committed")
	}

	key = convNilToBytes(key)

	err := bulk.tx.Delete(key)
	if err != nil {
		panic(fmt.Sprintf("Bulk Delete Error: %v", err))
	}
}

func (bulk *kvLogBulk) Flush() {
	if bulk.isDiscarded {
		panic("Flush after discard bulk is not allowed")
	} else if bulk.isCommitted {
		panic("Flush occurs two times")
	}

	err := bulk.tx.Commit()
	if err != nil {
		panic(fmt.Sprintf("Bulk Flush Error: %v", err))
	}

	bulk.isCommitted = true
}

func (bulk *kvLogBulk) Discard() {
	if bulk.isCommitted {
		return // Already committed, nothing to discard
	}

	if !bulk.isDiscarded {
		err := bulk.tx.Rollback()
		if err != nil {
			panic(fmt.Sprintf("Bulk Rollback Error: %v", err))
		}
		bulk.isDiscarded = true
	}
}

func (bulk *kvLogBulk) DiscardLast() {
	if bulk.isCommitted {
		return // Already committed, nothing to discard
	}

	if !bulk.isDiscarded {
		err := bulk.tx.Rollback()
		if err != nil {
			panic(fmt.Sprintf("Bulk Rollback Error: %v", err))
		}
		bulk.isDiscarded = true
	}
}

//=========================================================
// Iterator Implementation
//=========================================================

type kvLogIterator struct {
	iter      *kv_log.Iterator
	isInvalid bool
}

func (db *kvLogDB) Iterator(start, end []byte) Iterator {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// kv_log doesn't support sorted iteration with range,
	// so we just create an iterator that iterates over all keys
	iter := db.db.NewIterator(nil, nil)

	return &kvLogIterator{
		iter:      iter,
		isInvalid: false,
	}
}

func (iter *kvLogIterator) Next() {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	iter.iter.Next()
}

func (iter *kvLogIterator) Valid() bool {
	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	return iter.iter.Valid()
}

func (iter *kvLogIterator) Key() (key []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	return iter.iter.Key()
}

func (iter *kvLogIterator) Value() (value []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	return iter.iter.Value()
}

func (iter *kvLogIterator) Close() {
	iter.isInvalid = true
	iter.iter.Close()
}
