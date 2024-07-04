/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"bytes"
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

	if len(database.deletions) == 0 {
		database.add_version()
	}

	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*deldeldb)(nil)

// this file implements a database that delays the deletion of keys.
// it keeps the list of keys that have been deleted on the last 300 transactions
// and only deletes the keys of the oldest transaction on a commit, when the
// list of transactions has 300 elements.
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
const MAX_TRANSACTIONS = 300

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
func (db *deldeldb) add_version() {
	db.deletions = append(db.deletions, make(map[string]bool))
}

// commonSet handles the common logic for setting a key-value pair
func (db *deldeldb) commonSet(key, value []byte, autoCommit bool, getFunc func([]byte) []byte, setFunc func([]byte, []byte)) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if autoCommit {
		//db.add_version()
	}

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	// remove the key from the list of deletions of all versions
	skey := string(key)
	for _, deletions := range db.deletions {
		delete(deletions, skey)
	}

	// retrieve the current value
	currentValue := getFunc(key)

	// the first byte is the reference counter
	var referenceCounter uint8

	// check if the new value is different from the current value
	if len(currentValue) == 0 || !bytes.Equal(currentValue[1:], value) {
		// set the reference counter to 1
		referenceCounter = 1
		// set the key-value pair in the underlying database
		value = append([]byte{referenceCounter}, value...)
		setFunc(key, value)
		// if the previous stored value is a hash
		if len(currentValue) == 33 {
			// delete the value associated with the hash  [hash(value) -> value]
			db.queueDelete(currentValue[1:])
		}
	} else {
		// increase the reference counter
		currentValue[0]++
		setFunc(key, currentValue)
	}
}

//commonDelete handles the common logic for deleting a key-value pair
func (db *deldeldb) commonDelete(key []byte, autoCommit bool) {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	if autoCommit {
		//db.add_version()
	}

	db.queueDelete(key)
}

func (db *deldeldb) queueDelete(key []byte) {
	// add the key to the list of deletions of the last transaction
	db.deletions[len(db.deletions)-1][string(key)] = true
}

func (db *deldeldb) processDeletions(getFunc func([]byte) []byte, setFunc func([]byte, []byte), deleteFunc func([]byte)) {

	// check if there are more transactions than the maximum
	if len(db.deletions) > MAX_TRANSACTIONS {
		// process the list of deletions of the oldest transaction
		for key, _ := range db.deletions[0] {
			// delete the key from the underlying database
			db.processDelete([]byte(key), getFunc, setFunc, deleteFunc)
		}
		// remove the list of deletions of the oldest transaction
		db.deletions = db.deletions[1:]
	}

}

func (db *deldeldb) processDelete(key []byte, getFunc func([]byte) []byte, setFunc func([]byte, []byte), deleteFunc func([]byte)) {

	// retrieve the current value
	currentValue := getFunc(key)

	// the first byte is the reference counter
	var referenceCounter uint8
	if len(currentValue) > 0 {
		referenceCounter = currentValue[0]
	} else {
		referenceCounter = 0
	}

	// decrease the reference counter
	if referenceCounter > 0 {
		referenceCounter--
	}

	// check if the reference counter is 0
	if referenceCounter == 0 {
		// delete the key-value pair in the underlying database
		deleteFunc(key)
	} else {
		// update the reference counter
		currentValue[0] = referenceCounter
		setFunc(key, currentValue)
	}
}

//commonGet handles the common logic for getting a key-value pair
func (db *deldeldb) commonGet(key []byte, getFunc func([]byte) []byte) []byte {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	// retrieve the value of the key from the underlying database
	value := getFunc(key)

	// remove the reference counter
	if len(value) > 0 {
		value = value[1:]
	}
	return value
}

func (db *deldeldb) Set(key, value []byte) {
	db.commonSet(key, value, true, db.db.Get, db.db.Set)
}

func (db *deldeldb) Delete(key []byte) {
	db.commonDelete(key, true)
}

func (db *deldeldb) Get(key []byte) []byte {
	return db.commonGet(key, db.db.Get)
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

	switch ioCtlType {
	case "add-version":
		db.add_version()
	case "reset-deletions":
		db.deletions = make([]map[string]bool, 0)
		db.add_version()
	case "save":
		db.save()
	}
}

func (db *deldeldb) NewTx() Transaction {
	db.lock.Lock()
	defer db.lock.Unlock()

	//db.add_version()

	// start a new transaction on the underlying database
	// and return a new transaction that wraps it
	return &deldelTransaction{
		db:          db,
		tx:          db.db.NewTx(),
		isDiscarded: false,
		isCommitted: false,
	}
}

func (db *deldeldb) NewBulk() Bulk {
	db.lock.Lock()
	defer db.lock.Unlock()

	//db.add_version()

	// start a new transaction on the underlying database
	// and return a new bulk that wraps it
	return &deldelBulk{
		db:          db,
		tx:          db.db.NewTx(),
		isDiscarded: false,
		isCommitted: false,
	}
}

//=========================================================
// Transaction Implementation
//=========================================================

type deldelTransaction struct {
	txLock      sync.Mutex
	db          *deldeldb
	tx          Transaction
	isDiscarded bool
	isCommitted bool
}

func (transaction *deldelTransaction) Set(key, value []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	transaction.db.commonSet(key, value, false, transaction.tx.Get, transaction.tx.Set)
}

func (transaction *deldelTransaction) Delete(key []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	transaction.db.commonDelete(key, false)
}

func (transaction *deldelTransaction) Get(key []byte) []byte {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	return transaction.db.commonGet(key, transaction.tx.Get)
}

func (transaction *deldelTransaction) Commit() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	if transaction.isDiscarded {
		panic("Commit after dicard tx is not allowed")
	} else if transaction.isCommitted {
		panic("Commit occures two times")
	}

	db := transaction.db

	db.lock.Lock()
	defer db.lock.Unlock()

	// process the delayed deletions
	db.processDeletions(transaction.tx.Get, transaction.tx.Set, transaction.tx.Delete)

	// commit the transaction on the underlying database
	transaction.tx.Commit()

	transaction.isCommitted = true
}

func (transaction *deldelTransaction) Discard() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	if transaction.isDiscarded {
		return
	}

	// if the transaction was not committed, then discard the last list of deletions
	if !transaction.isCommitted {
		transaction.db.lock.Lock()
		transaction.db.deletions = transaction.db.deletions[:len(transaction.db.deletions)-1]
		transaction.db.lock.Unlock()
	}

	// discard the transaction on the underlying database
	transaction.tx.Discard()

	transaction.isDiscarded = true
}

//=========================================================
// Bulk Implementation
//=========================================================

type deldelBulk struct {
	txLock      sync.Mutex
	db          *deldeldb
	tx          Transaction
	isDiscarded bool
	isCommitted bool
}

func (bulk *deldelBulk) Set(key, value []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	bulk.db.commonSet(key, value, false, bulk.tx.Get, bulk.tx.Set)
}

func (bulk *deldelBulk) Delete(key []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	bulk.db.commonDelete(key, false)
}

func (bulk *deldelBulk) Flush() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	if bulk.isDiscarded {
		panic("Commit after dicard tx is not allowed")
	} else if bulk.isCommitted {
		panic("Commit occures two times")
	}

	db := bulk.db

	db.lock.Lock()
	defer db.lock.Unlock()

	// process the delayed deletions
	db.processDeletions(bulk.tx.Get, bulk.tx.Set, bulk.tx.Delete)

	// commit the transaction on the underlying database
	bulk.tx.Commit()

	bulk.isCommitted = true
}

func (bulk *deldelBulk) Discard() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	// if the transaction was not committed, then discard the last list of deletions
	if !bulk.isCommitted {
		bulk.db.lock.Lock()
		bulk.db.deletions = bulk.db.deletions[:len(bulk.db.deletions)-1]
		bulk.db.lock.Unlock()
	}

	// discard the transaction on the underlying database
	bulk.tx.Discard()

	bulk.isDiscarded = true
}

//=========================================================
// Iterator Implementation
//=========================================================

// the iterator is returning the value with the reference counter
// but as it is not being used, it is left as is

func (db *deldeldb) Iterator(start, end []byte) Iterator {
	db.lock.Lock()
	defer db.lock.Unlock()

	// just call the underlying database
	return db.db.Iterator(start, end)
}
