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

type kv struct {
	key   string
	value []byte
}

// This function is always called first
func init() {
	dbConstructor := func(dir string) (DB, error) {
		return newDummyDB(dir)
	}
	registorDBConstructor(DummyImpl, dbConstructor)
}

func newDummyDB(dir string) (DB, error) {
	var db []kv

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
		db = make([]kv, 0)
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

type dummydb struct {
	lock sync.Mutex
	db   []kv
	dir  string
}

func (db *dummydb) Type() string {
	return "dummydb"
}

func (db *dummydb) Set(key, value []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	// check if the key already exists. if it does, remove it from the slice
	for i, kv := range db.db {
		if kv.key == string(key) {
			db.db = append(db.db[:i], db.db[i+1:]...)
			break
		}
	}
	// now add the new key-value pair to the slice
	db.db = append(db.db, kv{key: string(key), value: value})
	// if the slice is longer than 64, remove the first element
	if len(db.db) > 64 {
		db.db = db.db[1:]
	}

}

func (db *dummydb) Delete(key []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	// check if the key already exists. if it does, remove it from the slice
	for i, kv := range db.db {
		if kv.key == string(key) {
			db.db = append(db.db[:i], db.db[i+1:]...)
			break
		}
	}
}

func (db *dummydb) Get(key []byte) []byte {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	// check if the key exists. if it does, return the value
	for _, kv := range db.db {
		if kv.key == string(key) {
			return kv.value
		}
	}

	// if the key does not exist, return nil
	return nil
}

func (db *dummydb) Exist(key []byte) bool {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	// check if the key exists. if it does, return true
	for _, kv := range db.db {
		if kv.key == string(key) {
			return true
		}
	}

	// if the key does not exist, return false
	return false
}

func (db *dummydb) Close() {
	db.lock.Lock()
	defer db.lock.Unlock()

	file, err := os.OpenFile(db.dir, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(db.db)
	}
	file.Close()
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

	for e := transaction.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.Set(op.key, op.value)
		} else {
			db.Delete(op.key)
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

	for e := bulk.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.Set(op.key, op.value)
		} else {
			db.Delete(op.key)
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

	// if end is bigger then start, then reverse order
	if bytes.Compare(start, end) == 1 {
		reverse = true
	} else {
		reverse = false
	}

	var keys sort.StringSlice

	for _, kv := range db.db {
		if isKeyInRange([]byte(kv.key), start, end, reverse) {
			keys = append(keys, kv.key)
		}
	}
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
