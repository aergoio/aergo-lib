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
	"path/filepath"
	"sort"
	"strings"
	"strconv"
	"fmt"
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

	// list all the db files
	files, err := filepath.Glob(dir + "/db-*")
	if err != nil {
		logger.Error().Msg("dummydb - error getting files: " + err.Error())
		panic(err)
	}
	// if there is at least one file
	if len(files) > 0 {
		// sort files numerically
		sort.Slice(files, func(i, j int) bool {
			numI, _ := strconv.ParseUint(strings.TrimPrefix(path.Base(files[i]), "db-"), 10, 64)
			numJ, _ := strconv.ParseUint(strings.TrimPrefix(path.Base(files[j]), "db-"), 10, 64)
			return numI < numJ
		})
		// try to read the last file. if it fails, read the next one
		for pos := len(files) - 1; pos >= 0; pos-- {
			file, err := os.Open(files[pos])
			if err == nil {
				decoder := gob.NewDecoder(file)
				err = decoder.Decode(&db)
			}
			file.Close()
			// if there is any error
			if err != nil {
				// delete this file
				os.Remove(files[pos])
				// remove the file from the slice
				files = files[:pos]
				// try to read the next file
				continue
			}
			// if there is no error, break the loop
			break
		}
		// if there are more than 3 files, remove the oldest ones
		for len(files) > 3 {
			os.Remove(files[0])
			files = files[1:]
		}
	}

	// get the version number from the last file
	var version uint64
	if len(files) > 0 {
		fileName := path.Base(files[len(files)-1])
		version, err = strconv.ParseUint(strings.TrimPrefix(fileName, "db-"), 10, 64)
		if err != nil {
			logger.Error().Msg("dummydb - error getting version: " + err.Error())
			panic(err)
		}
	}

	if db == nil {
		db = make([]map[string][]byte, 0)
	}

	database := &dummydb{
		db:      db,
		dir:     dir,
		files:   files,
		version: version,
	}

	if len(db) == 0 {
		database.add_version()
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
	lock    sync.Mutex
	db      []map[string][]byte
	dir     string
	files   []string
	version uint64
}

func (db *dummydb) Type() string {
	return "dummydb"
}

func (db *dummydb) Path() string {
	return db.dir
}

// add a new version to the database
func (db *dummydb) add_version() {
	logger.Debug().Msg("dummydb add_version")
	// save the database to a file every 256 versions
	if db.version%256 == 0 && db.version != 0 {
		db.save()
	}
	db.version++
	// add a new version to the database
	db.db = append(db.db, make(map[string][]byte))
	// check if the database has more than 512 versions. if it does,
	// remove the version at position 1, keeping the genesis block at position 0
	if len(db.db) == 513 {
		// Remove the block at position 1
		db.db = append(db.db[:1], db.db[2:]...)
	} else if len(db.db) > 513 {
		// Remove excess blocks, keeping only the first (genesis) and the last 511 blocks
		start := len(db.db) - 511
		db.db = append(db.db[:1], db.db[start:]...)
	}
}

// this function does not lock the mutex
func (db *dummydb) set(key, value []byte) {

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	// add the key-value pair to the last version
	version := len(db.db) - 1
	db.db[version][string(key)] = value

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

	// iterate over the database from the newest version to the oldest
	// and return the value of the key if it exists
	for i := len(db.db) - 1; i >= 0; i-- {
		kv := db.db[i]
		if value := kv[string(key)]; value != nil {
			return value
		}
	}

	// if the key does not exist, return nil
	return nil
}

func (db *dummydb) Set(key, value []byte) {
	db.lock.Lock()
	//db.add_version()
	db.set(key, value)
	db.lock.Unlock()
}

func (db *dummydb) Delete(key []byte) {
	db.lock.Lock()
	//db.add_version()
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

	// use the version number in the file name
	fileName := fmt.Sprintf("%s/db-%d", db.dir, db.version)
	logger.Info().Msg("dummydb - saving to file: " + fileName)

	// save the database to a file
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		encoder := gob.NewEncoder(file)
		err = encoder.Encode(db.db)
		file.Close()
	}
	if err != nil {
		logger.Error().Msg("dummydb - error saving to file: " + err.Error())
		return
	}

	// check if it is already on the list
	for _, file := range db.files {
		if file == fileName {
			return
		}
	}

	// add it to the list of db files
	db.files = append(db.files, fileName)

	// keep only the last 3 files
	if len(db.files) > 3 {
		os.Remove(db.files[0])
		db.files = db.files[1:]
	}

}

func (db *dummydb) Close() {
	db.lock.Lock()
	db.save()
	db.lock.Unlock()
}

func (db *dummydb) IoCtl(ioCtlType string) {
	db.lock.Lock()
	defer db.lock.Unlock()

	switch ioCtlType {
	case "new-version":
		db.add_version()
	case "save":
		db.save()
	default:
		panic("unknown ioctl type: " + ioCtlType)
	}
}

func (db *dummydb) NewTx() Transaction {

	return &dummyTransaction{
		db:          db,
		opList:      list.New(),
		isDiscarded: false,
		isCommitted: false,
	}
}

func (db *dummydb) NewBulk() Bulk {

	return &dummyBulk{
		db:          db,
		opList:      list.New(),
		isDiscarded: false,
		isCommitted: false,
	}
}

//=========================================================
// Transaction Implementation
//=========================================================

type dummyTransaction struct {
	txLock      sync.Mutex
	db          *dummydb
	opList      *list.List
	isDiscarded bool
	isCommitted bool
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

func (transaction *dummyTransaction) Get(key []byte) []byte {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	// first check if the key is present on the tx
	for e := transaction.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if bytes.Equal(op.key, key) {
			return op.value
		}
	}

	// if the key is not present on the tx, return the value from the db
	return transaction.db.Get(key)
}

func (transaction *dummyTransaction) Commit() {
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

	//db.add_version()

	for e := transaction.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.set(op.key, op.value)
		} else {
			db.delete(op.key)
		}
	}

	transaction.isCommitted = true
}

func (transaction *dummyTransaction) Discard() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	transaction.isDiscarded = true
}

//=========================================================
// Bulk Implementation
//=========================================================

type dummyBulk struct {
	txLock      sync.Mutex
	db          *dummydb
	opList      *list.List
	isDiscarded bool
	isCommitted bool
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

	if bulk.isDiscarded {
		panic("Commit after dicard tx is not allowed")
	} else if bulk.isCommitted {
		panic("Commit occures two times")
	}

	db := bulk.db

	db.lock.Lock()
	defer db.lock.Unlock()

	//db.add_version()

	for e := bulk.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.set(op.key, op.value)
		} else {
			db.delete(op.key)
		}
	}

	bulk.isCommitted = true
}

func (bulk *dummyBulk) Discard() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	bulk.isDiscarded = true
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

func (iter *dummyIterator) Close() {
	iter.isInvalid = true
}
