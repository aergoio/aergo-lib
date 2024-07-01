/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aergoio/aergo-lib/log"
	"github.com/dgraph-io/badger/v3"
)

const (
	badgerDbDiscardRatio   = 0.5 // run gc when 50% of samples can be collected
	badgerDbGcInterval     = 10 * time.Minute
	badgerDbGcSize         = 1 << 20 // 1 MB
	badgerValueLogFileSize = 1 << 26
)

// This function is always called first
func init() {
	dbConstructor := func(dir string) (DB, error) {
		return newBadgerDB(dir)
	}
	registerDBConstructor(BadgerImpl, dbConstructor)
}

func (db *badgerDB) runBadgerGC() {
	ticker := time.NewTicker(1 * time.Minute)

	lastGcT := time.Now()
	_, lastDbVlogSize := db.db.Size()
	for {
		select {
		case <-ticker.C:
			// check current db size
			currentDblsmSize, currentDbVlogSize := db.db.Size()

			// exceed badgerDbGcInterval time or badgerDbGcSize is increase slowly (it means resource is free)
			if time.Now().Sub(lastGcT) > badgerDbGcInterval || lastDbVlogSize+badgerDbGcSize > currentDbVlogSize {
				startGcT := time.Now()
				logger.Debug().Str("name", db.name).Int64("lsmSize", currentDblsmSize).Int64("vlogSize", currentDbVlogSize).Msg("Start to GC at badger")
				err := db.db.RunValueLogGC(badgerDbDiscardRatio)
				if err != nil {
					if err == badger.ErrNoRewrite {
						logger.Debug().Str("name", db.name).Str("msg", err.Error()).Msg("Nothing to GC at badger")
					} else {
						logger.Error().Str("name", db.name).Err(err).Msg("Fail to GC at badger")
					}
					lastDbVlogSize = currentDbVlogSize
				} else {
					afterGcDblsmSize, afterGcDbVlogSize := db.db.Size()

					logger.Debug().Str("name", db.name).Int64("lsmSize", afterGcDblsmSize).Int64("vlogSize", afterGcDbVlogSize).
						Dur("takenTime", time.Now().Sub(startGcT)).Msg("Finish to GC at badger")
					lastDbVlogSize = afterGcDbVlogSize
				}
				lastGcT = time.Now()
			}

		case <-db.ctx.Done():
			return
		}
	}
}

// newBadgerDB create a DB instance that uses badger db and implements DB interface.
// An input parameter, dir, is a root directory to store db files.
func newBadgerDB(dir string) (DB, error) {
	// set option file
	opts := badger.DefaultOptions(dir)

	// TODO : options tuning.
	// Quick fix to prevent RAM usage from going to the roof when adding 10Million new keys during tests
	// *** BadgerDB v3 no longer supports FileIO option. To fix build, the related lines are commented out. ***
	// opts.ValueLogLoadingMode = options.FileIO
	// opts.TableLoadingMode = options.FileIO
	opts.ValueThreshold = 1024 // store values, whose size is smaller than 1k, to a lsm tree -> to invoke flushing memtable

	// to reduce size of value log file for low throughtput of cloud; 1GB -> 64 MB
	// Time to read or write 1GB file in cloud (normal disk, not high provisioned) takes almost 20 seconds for GC
	opts.ValueLogFileSize = badgerValueLogFileSize

	//opts.MaxTableSize = 1 << 20 // 2 ^ 20 = 1048576, max mempool size invokes updating vlog header for gc

	// set aergo-lib logger instead of default badger stderr logger
	opts.Logger = logger

	// open badger db
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	database := &badgerDB{
		db:         db,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		name:       dir,
	}

	go database.runBadgerGC()

	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*badgerDB)(nil)

type badgerDB struct {
	db         *badger.DB
	ctx        context.Context
	cancelFunc context.CancelFunc
	name       string
}

// Type function returns a database type name
func (db *badgerDB) Type() string {
	return "badgerdb"
}

func (db *badgerDB) Path() string {
	return db.name
}

func (db *badgerDB) Set(key, value []byte) {
	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})

	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *badgerDB) Delete(key []byte) {
	key = convNilToBytes(key)

	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *badgerDB) Get(key []byte) []byte {
	key = convNilToBytes(key)

	var val []byte
	err := db.db.View(func(txn *badger.Txn) error {

		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		getVal, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		val = getVal

		return nil
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return []byte{}
		}
		panic(fmt.Sprintf("Database Error: %v", err))
	}

	return val
}

func (db *badgerDB) Exist(key []byte) bool {
	key = convNilToBytes(key)

	var isExist bool

	err := db.db.View(func(txn *badger.Txn) error {

		_, err := txn.Get(key)
		if err != nil {
			return err
		}

		isExist = true

		return nil
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return false
		}
	}

	return isExist
}

func (db *badgerDB) Close() {

	db.cancelFunc() // wait until gc goroutine is finished
	err := db.db.Close()
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *badgerDB) NewTx() Transaction {
	badgerTx := db.db.NewTransaction(true)

	retTransaction := &badgerTransaction{
		db:      db,
		tx:      badgerTx,
		createT: time.Now(),
	}

	return retTransaction
}

func (db *badgerDB) NewBulk() Bulk {
	badgerWriteBatch := db.db.NewWriteBatch()

	retBulk := &badgerBulk{
		db:      db,
		bulk:    badgerWriteBatch,
		createT: time.Now(),
	}

	return retBulk
}

//=========================================================
// Transaction Implementation
//=========================================================

type badgerTransaction struct {
	db        *badgerDB
	tx        *badger.Txn
	createT   time.Time
	setCount  uint
	delCount  uint
	keySize   uint64
	valueSize uint64
}

func (transaction *badgerTransaction) Set(key, value []byte) {
	// TODO Updating trie nodes may require many updates but ErrTxnTooBig is not handled
	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := transaction.tx.Set(key, value)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}

	transaction.setCount++
	transaction.keySize += uint64(len(key))
	transaction.valueSize += uint64(len(value))
}

func (transaction *badgerTransaction) Delete(key []byte) {
	// TODO Reverting trie may require many updates but ErrTxnTooBig is not handled
	key = convNilToBytes(key)

	err := transaction.tx.Delete(key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}

	transaction.delCount++
}

func (transaction *badgerTransaction) Commit() {
	writeStartT := time.Now()
	err := transaction.tx.Commit()
	writeEndT := time.Now()

	if writeEndT.Sub(writeStartT) > time.Millisecond*100 {
		// write warn log when write tx take too long time (100ms)
		logger.Warn().Str("name", transaction.db.name).Str("callstack1", log.SkipCaller(2)).Str("callstack2", log.SkipCaller(3)).
			Dur("prepareTime", writeStartT.Sub(transaction.createT)).
			Dur("takenTime", writeEndT.Sub(writeStartT)).
			Uint("delCount", transaction.delCount).Uint("setCount", transaction.setCount).
			Uint64("setKeySize", transaction.keySize).Uint64("setValueSize", transaction.valueSize).
			Msg("commit takes long time")
	}

	if err != nil {
		//TODO if there is conflict during commit, this panic will occurs
		panic(err)
	}
}

func (transaction *badgerTransaction) Discard() {
	transaction.tx.Discard()
}

//=========================================================
// Bulk Implementation
//=========================================================
type badgerBulk struct {
	db        *badgerDB
	bulk      *badger.WriteBatch
	createT   time.Time
	setCount  uint
	delCount  uint
	keySize   uint64
	valueSize uint64
}

func (bulk *badgerBulk) Set(key, value []byte) {
	// TODO Updating trie nodes may require many updates but ErrTxnTooBig is not handled
	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := bulk.bulk.Set(key, value)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}

	bulk.setCount++
	bulk.keySize += uint64(len(key))
	bulk.valueSize += uint64(len(value))
}

func (bulk *badgerBulk) Delete(key []byte) {
	// TODO Reverting trie may require many updates but ErrTxnTooBig is not handled
	key = convNilToBytes(key)

	err := bulk.bulk.Delete(key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}

	bulk.delCount++
}

func (bulk *badgerBulk) Flush() {
	writeStartT := time.Now()
	err := bulk.bulk.Flush()
	writeEndT := time.Now()

	if writeEndT.Sub(writeStartT) > time.Millisecond*100 || writeEndT.Sub(bulk.createT) > time.Millisecond*500 {
		// write warn log when write bulk tx take too long time (100ms or 500ms total)
		logger.Warn().Str("name", bulk.db.name).Str("callstack1", log.SkipCaller(2)).Str("callstack2", log.SkipCaller(3)).
			Dur("prepareAndCommitTime", writeStartT.Sub(bulk.createT)).
			Uint("delCount", bulk.delCount).Uint("setCount", bulk.setCount).
			Uint64("setKeySize", bulk.keySize).Uint64("setValueSize", bulk.valueSize).
			Dur("flushTime", writeEndT.Sub(writeStartT)).Msg("flush takes long time")
	}

	if err != nil {
		//TODO if there is conflict during commit, this panic will occurs
		panic(err)
	}
}

func (bulk *badgerBulk) DiscardLast() {
	bulk.bulk.Cancel()
}

//=========================================================
// Iterator Implementation
//=========================================================

type badgerIterator struct {
	start   []byte
	end     []byte
	reverse bool
	iter    *badger.Iterator
}

func (db *badgerDB) Iterator(start, end []byte) Iterator {
	badgerTx := db.db.NewTransaction(true)

	var reverse bool

	// if end is bigger than start, then reverse order
	if bytes.Compare(start, end) == 1 {
		reverse = true
	} else {
		reverse = false
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false
	opt.Reverse = reverse

	badgerIter := badgerTx.NewIterator(opt)

	badgerIter.Seek(start)

	retIter := &badgerIterator{
		start:   start,
		end:     end,
		reverse: reverse,
		iter:    badgerIter,
	}
	return retIter
}

func (iter *badgerIterator) Next() {
	if iter.Valid() {
		iter.iter.Next()
	} else {
		panic("Iterator is Invalid")
	}
}

func (iter *badgerIterator) Valid() bool {

	if !iter.iter.Valid() {
		return false
	}

	if iter.end != nil {
		if iter.reverse == false {
			if bytes.Compare(iter.end, iter.iter.Item().Key()) <= 0 {
				return false
			}
		} else {
			if bytes.Compare(iter.iter.Item().Key(), iter.end) <= 0 {
				return false
			}
		}
	}

	return true
}

func (iter *badgerIterator) Key() (key []byte) {
	return iter.iter.Item().Key()
}

func (iter *badgerIterator) Value() (value []byte) {
	retVal, err := iter.iter.Item().ValueCopy(nil)

	if err != nil {
		//FIXME: test and handle errs
		panic(err)
	}

	return retVal
}
