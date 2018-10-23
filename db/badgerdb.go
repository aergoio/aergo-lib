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

	"github.com/sunpuyo/badger"
	"github.com/sunpuyo/badger/options"
)

const (
	badgerDbDiscardRatio = 0.1 // run gc when 10% of samples can be collected
	badgerDbGcInterval   = 1 * time.Minute
	badgerDbGcSize       = 1 << 20 // 1 MB
)

// This function is always called first
func init() {
	dbConstructor := func(dir string) (DB, error) {
		return NewBadgerDB(dir)
	}
	registorDBConstructor(BadgerImpl, dbConstructor)
}

func (db *badgerDB) runBadgerGC() {
	ticker := time.NewTicker(10 * time.Second)

	lastGcT := time.Now()
	_, lastDbVlogSize := db.db.Size()
	for {
		select {
		case <-ticker.C:
			// check current db size
			_, currentDbVlogSize := db.db.Size()

			// exceed badgerDbGcInterval time or badgerDbGcSize is increase slowly (it means resource is free)
			if time.Now().Sub(lastGcT) > badgerDbGcInterval || lastDbVlogSize+badgerDbGcSize > currentDbVlogSize {
				startGcT := time.Now()
				err := db.db.RunValueLogGC(badgerDbDiscardRatio)
				if err != nil {
					if err == badger.ErrNoRewrite {
						logger.Debug().Err(err).Msg("Nothing to GC at badger")
					} else {
						logger.Error().Err(err).Msg("Fail to GC at badger")
					}
				} else {
					lastGcT = time.Now()
					_, afterGcDbVlogSize := db.db.Size()

					logger.Debug().Dur("takenTime", time.Now().Sub(startGcT)).Int64("gcBytes", currentDbVlogSize-afterGcDbVlogSize).Msg("Run GC at badger")
				}
			}

		case <-db.ctx.Done():
			return
		}
	}
}

// NewBadgerDB create a DB instance that uses badger db and implements DB interface.
// An input parameter, dir, is a root directory to store db files.
func NewBadgerDB(dir string) (DB, error) {
	// set option file
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	// TODO : options tuning.
	// Quick fix to prevent RAM usage from going to the roof when adding 10Million new keys during tests
	opts.ValueLogLoadingMode = options.FileIO
	opts.TableLoadingMode = options.LoadToRAM
	opts.ValueThreshold = 1024 // store values, whose size is smaller than 1k, to a lsm tree -> to invoke flushing memtable

	//opts.MaxTableSize = 1 << 20 // 2 ^ 20 = 1048576, max mempool size invokes updating vlog header for gc

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
}

// Type function returns a database type name
func (db *badgerDB) Type() string {
	return "badgerdb"
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
		db: db,
		tx: badgerTx,
	}

	return retTransaction
}

//=========================================================
// Transaction Implementation
//=========================================================

type badgerTransaction struct {
	db *badgerDB
	tx *badger.Txn
}

/*
func (transaction *badgerTransaction) Get(key []byte) []byte {
	key = convNilToBytes(key)

	getVal, err := transaction.tx.Get(key)

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return []byte{}
		}
		panic(fmt.Sprintf("Database Error: %v", err))
	}

	val, err := getVal.Value()
	if err != nil {
		//TODO handle retry error??
		panic(fmt.Sprintf("Database Error: %v", err))
	}

	return val
}
*/
func (transaction *badgerTransaction) Set(key, value []byte) {
	// TODO Updating trie nodes may require many updates but ErrTxnTooBig is not handled
	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := transaction.tx.Set(key, value)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (transaction *badgerTransaction) Delete(key []byte) {
	// TODO Reverting trie may require many updates but ErrTxnTooBig is not handled
	key = convNilToBytes(key)

	err := transaction.tx.Delete(key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (transaction *badgerTransaction) Commit() {
	err := transaction.tx.Commit()

	if err != nil {
		//TODO if there is conflict during commit, this panic will occurs
		panic(err)
	}
}

func (transaction *badgerTransaction) Discard() {
	transaction.tx.Discard()
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

	// if end is bigger then start, then reverse order
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
