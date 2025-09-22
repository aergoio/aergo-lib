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
	"time"
)

// This function is always called first
func init() {
	dbConstructor := func(dir string, opts ...Option) (DB, error) {
		return newDummyDB(dir, opts...)
	}
	registerDBConstructor(DummyImpl, dbConstructor)
}

func newDummyDB(dir string, opts ...Option) (DB, error) {
	var data *dummydbData

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
				data = &dummydbData{}
				err = decoder.Decode(data)
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

	// Initialize circular buffer
	var buffer []map[string][]byte
	var head, size int

	if data == nil {
		// New database: initialize with genesis
		buffer = make([]map[string][]byte, 512)
		for i := range buffer {
			buffer[i] = make(map[string][]byte)
		}
		head = 0 // head points to newest (genesis at position 0)
		size = 0
	} else {
		// Loaded from file: use the saved data
		buffer = data.Versions
		head = data.Head
		size = data.Size
	}

	database := &dummydb{
		db:        buffer,
		head:      head,
		size:      size,
		dir:       dir,
		files:     files,
		version:   version,
		dirty:     false,
		saveTimer: time.NewTicker(15 * time.Second),
		stopChan:  make(chan struct{}),
	}

	// Start the periodic save goroutine
	go func() {
		for {
			select {
			case <-database.saveTimer.C:
				database.lock.Lock()
				if database.dirty {
					database.save()
				}
				database.lock.Unlock()
			case <-database.stopChan:
				return
			}
		}
	}()

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

// dummydbData represents the data that gets saved/loaded
type dummydbData struct {
	Versions []map[string][]byte // the version data
	Head     int                 // index of newest version (1-511)
	Size     int                 // current number of versions (1-512)
}

type dummydb struct {
	lock      sync.Mutex
	db        []map[string][]byte // now fixed size 512, index 0 is genesis
	head      int                 // index of newest version (1-511)
	size      int                 // current number of versions (1-512)
	dir       string
	files     []string
	version   uint64
	dirty     bool
	saveTimer *time.Ticker
	stopChan  chan struct{}
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

	// If the database is empty, store on the genesis block position
	if db.size == 0 {
		db.head = 0
	} else {
		// Move head to next position on the circular buffer
		db.head++
		if db.head >= 512 {
			db.head = 1
		}
	}

	// Initialize/clear the slot on the circular buffer
	db.db[db.head] = make(map[string][]byte)

	// If not at max capacity, increase size
	if db.size < 512 {
		db.size++
	}
}

// this function does not lock the mutex
func (db *dummydb) set(key, value []byte) {

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	// add the key-value pair to the newest/last version
	db.db[db.head][string(key)] = value

	db.dirty = true
}

// this function does not lock the mutex
func (db *dummydb) delete(key []byte) {

	key = convNilToBytes(key)

	// remove the key-value pair from all versions
	for _, kv := range db.db {
		delete(kv, string(key))
	}

	db.dirty = true
}

// this function does not lock the mutex
func (db *dummydb) get(key []byte) []byte {

	key = convNilToBytes(key)

	// iterate over the database from the newest version to the oldest
	// Start from newest (at head)
	current := db.head

	// Iterate through all versions in circular buffer (newest to oldest)
	for i := 0; i < db.size-1; i++ { // -1 because genesis is checked separately
		if value := db.db[current][string(key)]; value != nil {
			return value
		}
		current--
		if current <= 0 {
			current = 511
		}
	}

	// Always check genesis last (it's the oldest)
	if value := db.db[0][string(key)]; value != nil {
		return value
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
	// Check if the database is dirty; if not, abort
	if !db.dirty {
		return
	}

	// Increment version for this save
	db.version++

	// use the version number in the file name
	fileName := fmt.Sprintf("%s/db-%d", db.dir, db.version)
	logger.Info().Msg("dummydb - saving to file: " + fileName)

	// save the database to a file
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		data := dummydbData{
			Versions: db.db,
			Head:     db.head,
			Size:     db.size,
		}
		encoder := gob.NewEncoder(file)
		err = encoder.Encode(data)
		file.Close()
	}
	if err != nil {
		logger.Error().Msg("dummydb - error saving to file: " + err.Error())
		return
	}

	// add it to the list of db files
	db.files = append(db.files, fileName)

	// keep only the last 3 files
	if len(db.files) > 3 {
		os.Remove(db.files[0])
		db.files = db.files[1:]
	}

	// Reset the dirty flag after successful save
	db.dirty = false

}

func (db *dummydb) Close() {
	// Stop the periodic save timer and signal goroutine to stop
	db.saveTimer.Stop()
	close(db.stopChan)

	db.lock.Lock()
	db.save()
	db.lock.Unlock()
}

func (db *dummydb) IoCtl(ioCtlType string) {
	db.lock.Lock()
	defer db.lock.Unlock()

	switch ioCtlType {
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

	db.add_version()

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

	db.add_version()

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

func (bulk *dummyBulk) DiscardLast() {
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
