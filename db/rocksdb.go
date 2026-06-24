/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/linxGnu/grocksdb"
)

// Singleton ReadOptions instance for reuse
var (
	readOptsOnce sync.Once
	readOpts     *grocksdb.ReadOptions
)

func init() {
	dbConstructor := func(dir string, opts ...Opt) (DB, error) {
		return newRocksDB(dir, opts...)
	}
	registerDBConstructor(RocksImpl, dbConstructor)
}

func newRocksDB(dir string, opts ...Opt) (DB, error) {
	dbPath := filepath.Join(dir, "data.db")

	options := grocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetCompression(grocksdb.NoCompression)

	cache := grocksdb.NewLRUCache(1024 * 1024 * 1024) // 1GB
	blockOpts := grocksdb.NewDefaultBlockBasedTableOptions()
	blockOpts.SetBlockCache(cache)
	options.SetBlockBasedTableFactory(blockOpts)

	options.SetMaxOpenFiles(500)
	options.SetWriteBufferSize(256 * 1024 * 1024)
	options.SetMaxWriteBufferNumber(6)
	options.SetMinWriteBufferNumberToMerge(2)
	options.SetOptimizeFiltersForHits(true)
	options.SetLevelCompactionDynamicLevelBytes(true)

	for _, opt := range opts {
		switch opt.Name {
		case "BlockCacheSize":
			if cacheSize, ok := opt.Value.(uint64); ok {
				cache := grocksdb.NewLRUCache(cacheSize)
				blockOpts := grocksdb.NewDefaultBlockBasedTableOptions()
				blockOpts.SetBlockCache(cache)
				blockOpts.SetCacheIndexAndFilterBlocks(true)
				blockOpts.SetPinL0FilterAndIndexBlocksInCache(true)
				options.SetBlockBasedTableFactory(blockOpts)
			}
		case "WriteBufferSize":
			if bufferSize, ok := opt.Value.(uint64); ok {
				options.SetWriteBufferSize(bufferSize)
			}
		case "MaxOpenFiles":
			if maxFiles, ok := opt.Value.(int); ok {
				options.SetMaxOpenFiles(maxFiles)
			}
		case "Compression":
			if compression, ok := opt.Value.(string); ok {
				switch compression {
				case "none":
					options.SetCompression(grocksdb.NoCompression)
				case "snappy":
					options.SetCompression(grocksdb.SnappyCompression)
				case "lz4":
					options.SetCompression(grocksdb.LZ4Compression)
				case "lz4hc":
					options.SetCompression(grocksdb.LZ4HCCompression)
				}
			}
		case "UseDirectReads":
			if useDirectReads, ok := opt.Value.(bool); ok {
				options.SetUseDirectReads(useDirectReads)
			}
		}
	}

	db, err := grocksdb.OpenDb(options, dbPath)
	if err != nil {
		options.Destroy()
		return nil, err
	}

	return &rocksDB{
		db:      db,
		options: options,
	}, nil
}

var _ DB = (*rocksDB)(nil)

type rocksDB struct {
	db      *grocksdb.DB
	options *grocksdb.Options
}

func (db *rocksDB) Type() string {
	return "rocksdb"
}

func (db *rocksDB) Set(key, value []byte) {
	key = convNilToBytes(key)
	value = convNilToBytes(value)

	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false)
	defer writeOpts.Destroy()

	if err := db.db.Put(writeOpts, key, value); err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *rocksDB) Delete(key []byte) {
	key = convNilToBytes(key)

	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false)
	defer writeOpts.Destroy()

	if err := db.db.Delete(writeOpts, key); err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func getReadOptions() *grocksdb.ReadOptions {
	readOptsOnce.Do(func() {
		readOpts = grocksdb.NewDefaultReadOptions()
		readOpts.SetFillCache(true)
		readOpts.SetVerifyChecksums(false)
	})
	return readOpts
}

func (db *rocksDB) Get(key []byte) []byte {
	key = convNilToBytes(key)

	value, err := db.db.Get(getReadOptions(), key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	defer value.Free()

	if value.Data() == nil {
		return []byte{}
	}

	result := make([]byte, value.Size())
	copy(result, value.Data())
	return result
}

func (db *rocksDB) Exist(key []byte) bool {
	key = convNilToBytes(key)

	value, err := db.db.Get(getReadOptions(), key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	defer value.Free()

	return value.Data() != nil
}

func (db *rocksDB) Close() {
	db.db.Close()
	db.options.Destroy()
}

// SetCompactionEvent is a no-op: rocksdb compaction events are not exposed yet.
func (db *rocksDB) SetCompactionEvent(event CompactionEventHandler) {}

func (db *rocksDB) IoCtl(ioCtlType string) {}

func (db *rocksDB) NewTx() Transaction {
	return &rocksTransaction{
		db:        db,
		batch:     grocksdb.NewWriteBatch(),
		isDiscard: false,
		isCommit:  false,
	}
}

func (db *rocksDB) NewBulk() Bulk {
	return &rocksBulk{
		db:        db,
		batch:     grocksdb.NewWriteBatch(),
		isDiscard: false,
		isCommit:  false,
	}
}

type rocksTransaction struct {
	db        *rocksDB
	batch     *grocksdb.WriteBatch
	isDiscard bool
	isCommit  bool
}

func (transaction *rocksTransaction) Set(key, value []byte) {
	key = convNilToBytes(key)
	value = convNilToBytes(value)
	transaction.batch.Put(key, value)
}

func (transaction *rocksTransaction) Delete(key []byte) {
	key = convNilToBytes(key)
	transaction.batch.Delete(key)
}

func (transaction *rocksTransaction) Commit() {
	if transaction.isDiscard {
		panic("Commit after discard tx is not allowed")
	} else if transaction.isCommit {
		panic("Commit occurs two times")
	}

	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false)
	defer writeOpts.Destroy()

	if err := transaction.db.db.Write(writeOpts, transaction.batch); err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	transaction.isCommit = true
}

func (transaction *rocksTransaction) Discard() {
	transaction.batch.Destroy()
	transaction.isDiscard = true
}

type rocksBulk struct {
	db        *rocksDB
	batch     *grocksdb.WriteBatch
	isDiscard bool
	isCommit  bool
}

func (bulk *rocksBulk) Set(key, value []byte) {
	key = convNilToBytes(key)
	value = convNilToBytes(value)
	bulk.batch.Put(key, value)
}

func (bulk *rocksBulk) Delete(key []byte) {
	key = convNilToBytes(key)
	bulk.batch.Delete(key)
}

func (bulk *rocksBulk) Flush() {
	if bulk.isDiscard {
		panic("Commit after discard tx is not allowed")
	} else if bulk.isCommit {
		panic("Commit occurs two times")
	}

	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false)
	defer writeOpts.Destroy()

	if err := bulk.db.db.Write(writeOpts, bulk.batch); err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	bulk.isCommit = true
}

func (bulk *rocksBulk) DiscardLast() {
	bulk.batch.Destroy()
	bulk.isDiscard = true
}

type rocksIterator struct {
	iter      *grocksdb.Iterator
	start     []byte
	end       []byte
	reverse   bool
	isInvalid bool
}

func (db *rocksDB) Iterator(start, end []byte) Iterator {
	var reverse bool

	if bytes.Compare(start, end) == 1 {
		reverse = true
	} else {
		reverse = false
	}

	iter := db.db.NewIterator(getReadOptions())

	if reverse {
		if start == nil {
			iter.SeekToLast()
		} else {
			iter.Seek(start)
			if iter.Valid() {
				soakey := iter.Key()
				if bytes.Compare(start, soakey.Data()) < 0 {
					iter.Prev()
				}
				soakey.Free()
			} else {
				iter.SeekToLast()
			}
		}
	} else if start == nil {
		iter.SeekToFirst()
	} else {
		iter.Seek(start)
	}

	return &rocksIterator{
		iter:      iter,
		start:     start,
		end:       end,
		reverse:   reverse,
		isInvalid: false,
	}
}

func (iter *rocksIterator) Next() {
	if iter.Valid() {
		if iter.reverse {
			iter.iter.Prev()
		} else {
			iter.iter.Next()
		}
	} else {
		panic("Iterator is Invalid")
	}
}

func (iter *rocksIterator) Valid() bool {
	if iter.isInvalid {
		return false
	}

	if err := iter.iter.Err(); err != nil {
		panic(err)
	}

	if !iter.iter.Valid() {
		iter.isInvalid = true
		return false
	}

	end := iter.end
	key := iter.iter.Key()
	defer key.Free()

	if iter.reverse {
		if end != nil && bytes.Compare(key.Data(), end) <= 0 {
			iter.isInvalid = true
			return false
		}
	} else if end != nil && bytes.Compare(end, key.Data()) <= 0 {
		iter.isInvalid = true
		return false
	}

	return true
}

func (iter *rocksIterator) Key() []byte {
	if !iter.Valid() {
		panic("Iterator is invalid")
	} else if err := iter.iter.Err(); err != nil {
		panic(err)
	}

	originalKey := iter.iter.Key()
	defer originalKey.Free()

	key := make([]byte, originalKey.Size())
	copy(key, originalKey.Data())
	return key
}

func (iter *rocksIterator) Value() []byte {
	if !iter.Valid() {
		panic("Iterator is invalid")
	} else if err := iter.iter.Err(); err != nil {
		panic(err)
	}

	originalValue := iter.iter.Value()
	defer originalValue.Free()

	value := make([]byte, originalValue.Size())
	copy(value, originalValue.Data())
	return value
}
