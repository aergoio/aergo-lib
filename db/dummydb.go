/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"os"
	"path"
	"sort"
	"sync"
)

// This function is always called first
func init() {
	dbConstructor := func(dir string) (DB, error) {
		return newDummyDB(dir)
	}
	registerDBConstructor(DummyImpl, dbConstructor)
}

func newDummyDB(dir string) (DB, error) {
	var db []map[string][]byte

	filePath := path.Join(dir, "database")

	file, err := os.Open(filePath)
	if err == nil {
		decoder := gob.NewDecoder(file) //
		err = decoder.Decode(&db)

		if err != nil {
			return nil, err
		}
	}
	file.Close()

	if db == nil {
		db = make([]map[string][]byte, 0)
	}

	database := &dummydb{
		db:  db,
		dir: filePath,
	}

	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*dummydb)(nil)

// this defines a slice of maps. each map has this format: map[string][]byte
// the slice is used to simulate a database with multiple versions
// the first element in the slice is the newest version
// the last element in the slice is the oldest version

type dummydb struct {
	lock sync.Mutex
	db   []map[string][]byte
	dir  string
	versions_since_save int
}

func (db *dummydb) Type() string {
	return "dummydb"
}

func (db *dummydb) Path() string {
	return db.dir
}

// add a new version to the database
func (db *dummydb) add_version() {
	// save the database to a file every 1000 versions
	if db.versions_since_save >= 1000 {
		db.save()
	}
	db.versions_since_save++
	// add a new version to the database
	db.db = append([]map[string][]byte{make(map[string][]byte)}, db.db...)
	// check if the database has more than 512 versions. if it does,
	// remove the version just before the oldest one.
	// eg: keeps 0-510, removes 511, keeps 512
	// this is used to keep the genesis block in the database
	if len(db.db) > 512 {
		oldest := db.db[len(db.db)-1]
		db.db = append(db.db[:len(db.db)-2], oldest)
	}
}

// this function does not lock the mutex
func (db *dummydb) set(key, value []byte) {

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	// add the key-value pair to the last version
	db.db[0][string(key)] = value

}

// this function does not lock the mutex
func (db *dummydb) delete(key []byte) {

	key = convNilToBytes(key)

	// remove the key-value pair from all versions
	for _, kv := range db.db {
		delete(kv, string(key))
	}

}

// this function does not lock the mutex
func (db *dummydb) get(key []byte) []byte {

	key = convNilToBytes(key)

	// iterate over the database from the latest version to the oldest
	// and return the value of the key if it exists
	for _, kv := range db.db {
		if value := kv[string(key)]; value != nil {
			return value
		}
	}

	// if the key does not exist, return nil
	return nil
}

func (db *dummydb) Set(key, value []byte) {
	db.lock.Lock()
	db.add_version()
	db.set(key, value)
	db.lock.Unlock()
}

func (db *dummydb) Delete(key []byte) {
	db.lock.Lock()
	db.add_version()
	db.delete(key)
	db.lock.Unlock()
}

func (db *dummydb) Get(key []byte) []byte {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.get(key)
}

func (db *dummydb) Exist(key []byte) bool {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	// check if the key exists. if it does, return true
	for _, kv := range db.db {
		if kv[string(key)] != nil {
			return true
		}
	}

	// if the key does not exist, return false
	return false
}

func (db *dummydb) save() {

	file, err := os.OpenFile(db.dir, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(db.db)
	}
	file.Close()

	db.versions_since_save = 0

}

func (db *dummydb) Close() {
	db.lock.Lock()
	db.save()
	db.lock.Unlock()
}

func (db *dummydb) NewTx() Transaction {

	return &dummyTransaction{
		db:        db,
		opList:    list.New(),
		isDiscard: false,
		isCommit:  false,
	}
}

func (db *dummydb) NewBulk() Bulk {

	return &dummyBulk{
		db:        db,
		opList:    list.New(),
		isDiscard: false,
		isCommit:  false,
	}
}

//=========================================================
// Transaction Implementation
//=========================================================

type dummyTransaction struct {
	txLock    sync.Mutex
	db        *dummydb
	opList    *list.List
	isDiscard bool
	isCommit  bool
}

func (transaction *dummyTransaction) Set(key, value []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	transaction.opList.PushBack(&txOp{true, key, value})
}

func (transaction *dummyTransaction) Delete(key []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	key = convNilToBytes(key)

	transaction.opList.PushBack(&txOp{false, key, nil})
}

func (transaction *dummyTransaction) Commit() {
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

	db.add_version()

	for e := transaction.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.set(op.key, op.value)
		} else {
			db.delete(op.key)
		}
	}

	transaction.isCommit = true
}

func (transaction *dummyTransaction) Discard() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	transaction.isDiscard = true
}

//=========================================================
// Bulk Implementation
//=========================================================

type dummyBulk struct {
	txLock    sync.Mutex
	db        *dummydb
	opList    *list.List
	isDiscard bool
	isCommit  bool
}

func (bulk *dummyBulk) Set(key, value []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	bulk.opList.PushBack(&txOp{true, key, value})
}

func (bulk *dummyBulk) Delete(key []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	key = convNilToBytes(key)

	bulk.opList.PushBack(&txOp{false, key, nil})
}

func (bulk *dummyBulk) Flush() {
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

	db.add_version()

	for e := bulk.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.set(op.key, op.value)
		} else {
			db.delete(op.key)
		}
	}

	bulk.isCommit = true
}

func (bulk *dummyBulk) DiscardLast() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	bulk.isDiscard = true
}

//=========================================================
// Iterator Implementation
//=========================================================

type dummyIterator struct {
	start     []byte
	end       []byte
	reverse   bool
	keys      []string
	isInvalid bool
	cursor    int
	db        *dummydb
}

func (db *dummydb) Iterator(start, end []byte) Iterator {
	db.lock.Lock()
	defer db.lock.Unlock()

	var reverse bool

	// if end is bigger than start, then reverse order
	if bytes.Compare(start, end) == 1 {
		reverse = true
	} else {
		reverse = false
	}

	// create a list of unique keys using a map as a set
	set := make(map[string]bool)

	// iterate over all versions
	for _, kv := range db.db {
		// iterate over all keys in a version
		for key := range kv {
			// check if the key is already in the set
			if !set[key] {
				// check if the key is in the range
				if isKeyInRange([]byte(key), start, end, reverse) {
					set[key] = true
				}
			}
		}
	}

	// create a list of keys
	var keys sort.StringSlice
	for key := range set {
		keys = append(keys, key)
	}

	// sort the keys
	if reverse {
		sort.Sort(sort.Reverse(keys))
	} else {
		sort.Strings(keys)
	}

	return &dummyIterator{
		start:     start,
		end:       end,
		reverse:   reverse,
		isInvalid: false,
		keys:      keys,
		cursor:    0,
		db:        db,
	}
}

func (iter *dummyIterator) Next() {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	iter.cursor++
}

func (iter *dummyIterator) Valid() bool {
	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	return 0 <= iter.cursor && iter.cursor < len(iter.keys)
}

func (iter *dummyIterator) Key() (key []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	return []byte(iter.keys[iter.cursor])
}

func (iter *dummyIterator) Value() (value []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	key := []byte(iter.keys[iter.cursor])

	return iter.db.Get(key)
}
