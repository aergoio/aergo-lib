/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"fmt"
	"path/filepath"

	"github.com/aergoio/hashtabledb"
)

// This function is always called first
func init() {
	dbConstructor := func(dir string, opts ...Option) (DB, error) {
		return newHashTableDB(dir, opts...)
	}
	registerDBConstructor(HashTableImpl, dbConstructor)
}

func newHashTableDB(dir string, opts ...Option) (DB, error) {
	dbPath := filepath.Join(dir, "data.db")

	// Default options
	options := hashtabledb.Options{
		"ReadOnly": false,
		//"CacheSize": 1024 * 1024 * 1024, // 1GB
		//"FastRollback": false,
	}

	// Process options
	for _, opt := range opts {
		switch opt.Name {
		case "ReadOnly":
			if readOnly, ok := opt.Value.(bool); ok {
				options["ReadOnly"] = readOnly
			}
		// Add other hashtabledb specific options here as needed
		}
	}

	// Open hashtabledb database
	db, err := hashtabledb.Open(dbPath, options)
	if err != nil {
		return nil, err
	}

	db.SetOption("AddExternalKey", []byte("dpos.LibStatus"))
	db.SetOption("AddExternalKey", []byte("chain.latest"))

	database := &hashTableDB{
		db:   db,
		path: dbPath,
	}

	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*hashTableDB)(nil)

type hashTableDB struct {
	db   *hashtabledb.DB
	path string
}

func (db *hashTableDB) Type() string {
	return "hashtabledb"
}

func (db *hashTableDB) Path() string {
	return db.path
}

func (db *hashTableDB) Set(key, value []byte) {
	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := db.db.Set(key, value)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *hashTableDB) Delete(key []byte) {
	key = convNilToBytes(key)

	err := db.db.Delete(key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *hashTableDB) Get(key []byte) []byte {
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

func (db *hashTableDB) Exist(key []byte) bool {
	key = convNilToBytes(key)

	// hashtabledb doesn't have a Has/Exist method, so we use Get and check for errors
	_, err := db.db.Get(key)
	// If there's no error, the key exists
	return err == nil
}

func (db *hashTableDB) Close() {
	if err := db.db.Close(); err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *hashTableDB) IoCtl(ioCtlType string) {
	// hashtabledb doesn't have a Sync method, so we just ignore the IoCtl call
}

func (db *hashTableDB) NewTx() Transaction {
	tx, err := db.db.Begin()
	if err != nil {
		panic(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	return &hashTableTransaction{
		tx:          tx,
		isDiscarded: false,
		isCommitted: false,
	}
}

func (db *hashTableDB) NewBulk() Bulk {
	tx, err := db.db.Begin()
	if err != nil {
		panic(fmt.Sprintf("Failed to begin bulk transaction: %v", err))
	}
	return &hashTableBulk{
		tx:          tx,
		isDiscarded: false,
		isCommitted: false,
	}
}

//=========================================================
// Transaction Implementation
//=========================================================

type hashTableTransaction struct {
	tx          *hashtabledb.Transaction
	isDiscarded bool
	isCommitted bool
}

func (transaction *hashTableTransaction) Set(key, value []byte) {
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

func (transaction *hashTableTransaction) Delete(key []byte) {
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

func (transaction *hashTableTransaction) Get(key []byte) []byte {
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

func (transaction *hashTableTransaction) Commit() {
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

func (transaction *hashTableTransaction) Discard() {
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

type hashTableBulk struct {
	tx          *hashtabledb.Transaction
	isDiscarded bool
	isCommitted bool
}

func (bulk *hashTableBulk) Set(key, value []byte) {
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

func (bulk *hashTableBulk) Delete(key []byte) {
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

func (bulk *hashTableBulk) Flush() {
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

func (bulk *hashTableBulk) Discard() {
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

func (bulk *hashTableBulk) DiscardLast() {
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

type hashTableIterator struct {
	iter      *hashtabledb.Iterator
	isInvalid bool
}

func (db *hashTableDB) Iterator(start, end []byte) Iterator {
	iter := db.db.NewIterator()

	return &hashTableIterator{
		iter:      iter,
		isInvalid: false,
	}
}

func (iter *hashTableIterator) Next() {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	iter.iter.Next()
}

func (iter *hashTableIterator) Valid() bool {
	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	return iter.iter.Valid()
}

func (iter *hashTableIterator) Key() (key []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	return iter.iter.Key()
}

func (iter *hashTableIterator) Value() (value []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	return iter.iter.Value()
}

func (iter *hashTableIterator) Close() {
	iter.isInvalid = true
	iter.iter.Close()
}
