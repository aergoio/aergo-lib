/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"encoding/gob"
	"os"
	"path"
	"sync"
)

// This function is always called first
func init() {
	dbConstructor := func(dir string) (DB, error) {
		return newDelayedDeletionDB(dir)
	}
	registerDBConstructor(DelayedDeletionImpl, dbConstructor)
}

func newDelayedDeletionDB(dir string) (DB, error) {
	var db DB
	var deletions []map[string]bool

	// open a badgerdb
	db, err := newBadgerDB(dir)
	if err != nil {
		return nil, err
	}

	// load the list of deletions from the file
	filePath := path.Join(dir, "deletions")
	file, err := os.Open(filePath)
	if err == nil {
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(&deletions)
		if err != nil {
			return nil, err
		}
	}
	file.Close()
	// delete the file
	os.Remove(filePath)

	if deletions == nil {
		deletions = make([]map[string]bool, 0)
	}

	database := &deldeldb{
		db:        db,
		dir:       filePath,
		deletions: deletions,
	}

	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*deldeldb)(nil)

// this file implements a database that delays the deletion of keys.
// it keeps the list of keys that have been deleted on the last 32 transactions
// and only deletes the keys of the oldest transaction on a commit, when the
// list of transactions has 32 elements.
// it is a wrapper around an underlying database where most calls are passed
// through and only the delete, set and commit functions are modified to process
// data before passing it to the underlying database.
// when a key is deleted, it is added to the list of deletions of the last
// transaction.
// when a key is set, it is removed from the list of deletions of all transactions.
// when a commit is called, the list of deletions of the oldest transaction is
// passed to the underlying database to be deleted.
// the list of deletions of the oldest transaction is then removed from the list
// of deletions.

// define the number of transactions to delay the deletion of keys
const MAX_TRANSACTIONS = 32

type deldeldb struct {
	lock      sync.Mutex
	db        DB
	deletions []map[string]bool
	dir       string
}

func (db *deldeldb) Type() string {
	return "deldeldb"
}

func (db *deldeldb) Path() string {
	return db.dir
}

// add a new group of deletions
func (db *deldeldb) add_transaction() {
	db.deletions = append(db.deletions, make(map[string]bool))
}

// this function does not lock the mutex
func (db *deldeldb) process_set(key []byte) {

	key = convNilToBytes(key)

	// remove the key from the list of deletions of all versions
	for _, deletions := range db.deletions {
		delete(deletions, string(key))
	}

}

// this function does not lock the mutex
func (db *deldeldb) process_delete(key []byte) {

	key = convNilToBytes(key)

	// add the key to the list of deletions of the last transaction
	db.deletions[len(db.deletions)-1][string(key)] = true

}

// this function does not lock the mutex
func (db *deldeldb) get(key []byte) []byte {

	key = convNilToBytes(key)

	// retrieve the value of the key from the underlying database
	return db.db.Get(key)

}

func (db *deldeldb) Set(key, value []byte) {
	db.lock.Lock()
	db.add_transaction()
	db.process_set(key)
	// set the key-value pair in the underlying database
	db.db.Set(key, value)
	db.lock.Unlock()
}

func (db *deldeldb) Delete(key []byte) {
	db.lock.Lock()
	db.add_transaction()
	db.process_delete(key)
	db.lock.Unlock()
}

func (db *deldeldb) Get(key []byte) []byte {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.get(key)
}

func (db *deldeldb) Exist(key []byte) bool {
	db.lock.Lock()
	defer db.lock.Unlock()

	return db.db.Exist(key)
}

func (db *deldeldb) save() {

	// save the list of deletions to a file
	filePath := path.Join(db.dir, "deletions")
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(db.deletions)
	}
	file.Close()

}

func (db *deldeldb) Close() {
	db.lock.Lock()
	db.save()
	db.lock.Unlock()
}

func (db *deldeldb) IoCtl(ioCtlType string) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if ioCtlType == "reset-deletions" {
		db.deletions = make([]map[string]bool, 0)
	} else if ioCtlType == "save" {
		db.save()
	}
}

func (db *deldeldb) NewTx() Transaction {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.add_transaction()

	// start a new transaction on the underlying database
	// and return a new transaction that wraps it
	return &deldelTransaction{
		db:        db,
		tx:        db.db.NewTx(),
		isDiscard: false,
		isCommit:  false,
	}
}

func (db *deldeldb) NewBulk() Bulk {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.add_transaction()

	// start a new bulk on the underlying database
	// and return a new bulk that wraps it
	return &deldelBulk{
		db:        db,
		bulk:      db.db.NewBulk(),
		isDiscard: false,
		isCommit:  false,
	}
}

//=========================================================
// Transaction Implementation
//=========================================================

type deldelTransaction struct {
	txLock    sync.Mutex
	db        *deldeldb
	tx        Transaction
	isDiscard bool
	isCommit  bool
}

func (transaction *deldelTransaction) Set(key, value []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	// remove the key from the list of deletions of all versions
	transaction.db.lock.Lock()
	transaction.db.process_set(key)
	transaction.db.lock.Unlock()

	// set the key-value pair in the underlying database
	transaction.tx.Set(key, value)
}

func (transaction *deldelTransaction) Delete(key []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	key = convNilToBytes(key)

	// add the key to the list of deletions of the last transaction
	transaction.db.lock.Lock()
	transaction.db.process_delete(key)
	transaction.db.lock.Unlock()
}

func (transaction *deldelTransaction) Commit() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	if transaction.isDiscard {
		panic("Commit after dicard tx is not allowed")
	} else if transaction.isCommit {
		panic("Commit occures two times")
	}

	db := transaction.db

	db.lock.Lock()
	defer db.lock.Unlock()

	// check if there are 32 transactions
	if len(db.deletions) > MAX_TRANSACTIONS {
		// process the list of deletions of the oldest transaction
		for key, _ := range db.deletions[0] {
			// delete the key from the underlying database
			transaction.tx.Delete([]byte(key))
		}
		// remove the list of deletions of the oldest transaction
		db.deletions = db.deletions[1:]
	}

	// commit the transaction on the underlying database
	transaction.tx.Commit()

	transaction.isCommit = true
}

func (transaction *deldelTransaction) Discard() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	// if the transaction was not committed, then discard the last list of deletions
	if !transaction.isCommit {
		transaction.db.lock.Lock()
		transaction.db.deletions = transaction.db.deletions[:len(transaction.db.deletions)-1]
		transaction.db.lock.Unlock()
	}

	// discard the transaction on the underlying database
	transaction.tx.Discard()

	transaction.isDiscard = true
}

//=========================================================
// Bulk Implementation
//=========================================================

type deldelBulk struct {
	txLock    sync.Mutex
	db        *deldeldb
	bulk      Bulk
	isDiscard bool
	isCommit  bool
}

func (bulk *deldelBulk) Set(key, value []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	// remove the key from the list of deletions of all versions
	bulk.db.lock.Lock()
	bulk.db.process_set(key)
	bulk.db.lock.Unlock()

	// set the key-value pair in the underlying database
	bulk.bulk.Set(key, value)
}

func (bulk *deldelBulk) Delete(key []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	key = convNilToBytes(key)

	// add the key to the list of deletions of the last transaction
	bulk.db.lock.Lock()
	bulk.db.process_delete(key)
	bulk.db.lock.Unlock()
}

func (bulk *deldelBulk) Flush() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	if bulk.isDiscard {
		panic("Commit after dicard tx is not allowed")
	} else if bulk.isCommit {
		panic("Commit occures two times")
	}

	db := bulk.db

	db.lock.Lock()
	defer db.lock.Unlock()

	// check if there are 32 transactions
	if len(db.deletions) > MAX_TRANSACTIONS {
		// process the list of deletions of the oldest transaction
		for key, _ := range db.deletions[0] {
			// delete the key from the underlying database
			bulk.bulk.Delete([]byte(key))
		}
		// remove the list of deletions of the oldest transaction
		db.deletions = db.deletions[1:]
	}

	// commit the transaction on the underlying database
	bulk.bulk.Flush()

	bulk.isCommit = true
}

func (bulk *deldelBulk) DiscardLast() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	// if the transaction was not committed, then discard the last list of deletions
	if !bulk.isCommit {
		bulk.db.lock.Lock()
		bulk.db.deletions = bulk.db.deletions[:len(bulk.db.deletions)-1]
		bulk.db.lock.Unlock()
	}

	// discard the transaction on the underlying database
	bulk.bulk.DiscardLast()

	bulk.isDiscard = true
}

//=========================================================
// Iterator Implementation
//=========================================================

func (db *deldeldb) Iterator(start, end []byte) Iterator {
	db.lock.Lock()
	defer db.lock.Unlock()

	// just call the underlying database
	return db.db.Iterator(start, end)
}
