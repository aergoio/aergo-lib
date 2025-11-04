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

// This function is always called first
func init() {
	dbConstructor := func(dir string, opts ...Option) (DB, error) {
		return newRocksDB(dir, opts...)
	}
	registerDBConstructor(RocksImpl, dbConstructor)
}

func newRocksDB(dir string, opts ...Option) (DB, error) {
	dbPath := filepath.Join(dir, "data.db")

	// Create default options
	options := grocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)

	// Optimize for read performance
	options.SetCompression(grocksdb.NoCompression)

	// Set larger cache size for better cold read performance
	cache := grocksdb.NewLRUCache(1024 * 1024 * 1024) // 1GB
	blockOpts := grocksdb.NewDefaultBlockBasedTableOptions()
	blockOpts.SetBlockCache(cache)
	//blockOpts.SetCacheIndexAndFilterBlocks(true)
	//blockOpts.SetPinL0FilterAndIndexBlocksInCache(true)
	//blockOpts.SetCacheIndexAndFilterBlocksWithHighPriority(true)
	//blockOpts.SetPinTopLevelIndexAndFilter(true)
	options.SetBlockBasedTableFactory(blockOpts)

	// Optimize for cold reads - more conservative settings
	options.SetMaxOpenFiles(500)
	// options.SetUseDirectReads(true) // Disabled for better cold read performance

	// Write performance optimizations
	options.SetWriteBufferSize(256 * 1024 * 1024) // 256MB write buffer (up from 64MB default)
	options.SetMaxWriteBufferNumber(6)            // Allow more memtables to reduce write stalls
	options.SetMinWriteBufferNumberToMerge(2)     // Merge when we have 2+ memtables

	// Additional optimizations for cold reads
	options.SetOptimizeFiltersForHits(true)
	options.SetLevelCompactionDynamicLevelBytes(true)

	// Process options
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

	database := &rocksDB{
		db:      db,
		options: options,
	}
	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
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

	err := db.db.Put(writeOpts, key, value)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *rocksDB) Delete(key []byte) {
	key = convNilToBytes(key)

	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false)
	defer writeOpts.Destroy()

	err := db.db.Delete(writeOpts, key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

// getReadOptions returns the singleton ReadOptions instance
func getReadOptions() *grocksdb.ReadOptions {
	readOptsOnce.Do(func() {
		readOpts = grocksdb.NewDefaultReadOptions()
		readOpts.SetFillCache(true) // Enable block cache filling
		readOpts.SetVerifyChecksums(false) // Skip checksum verification for better performance
	})
	return readOpts
}

func (db *rocksDB) Get(key []byte) []byte {
	key = convNilToBytes(key)

	// Use singleton ReadOptions
	readOpts := getReadOptions()
	
	value, err := db.db.Get(readOpts, key)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	defer value.Free()

	if value.Data() == nil {
		return []byte{}
	}

	// Make a copy since RocksDB slice will be freed
	result := make([]byte, value.Size())
	copy(result, value.Data())
	return result
}

func (db *rocksDB) Exist(key []byte) bool {
	key = convNilToBytes(key)

	// Use singleton ReadOptions
	readOpts := getReadOptions()

	value, err := db.db.Get(readOpts, key)
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

func (db *rocksDB) IoCtl(ioCtlType string) {
	// No implemented command yet
}

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

//=========================================================
// Transaction Implementation
//=========================================================

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

	err := transaction.db.db.Write(writeOpts, transaction.batch)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	transaction.isCommit = true
}

func (transaction *rocksTransaction) Discard() {
	transaction.batch.Destroy()
	transaction.isDiscard = true
}

//=========================================================
// Bulk Implementation
//=========================================================

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

	err := bulk.db.db.Write(writeOpts, bulk.batch)
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	bulk.isCommit = true
}

func (bulk *rocksBulk) DiscardLast() {
	bulk.batch.Destroy()
	bulk.isDiscard = true
}

//=========================================================
// Iterator Implementation
//=========================================================

type rocksIterator struct {
	iter      *grocksdb.Iterator
	readOpts  *grocksdb.ReadOptions // Store readOpts for cleanup
	start     []byte
	end       []byte
	reverse   bool
	isInvalid bool
}

func (db *rocksDB) Iterator(start, end []byte) Iterator {
	var reverse bool

	// if end is bigger then start, then reverse order
	if bytes.Compare(start, end) == 1 {
		reverse = true
	} else {
		reverse = false
	}

	// Use singleton ReadOptions
	readOpts := getReadOptions()
	iter := db.db.NewIterator(readOpts)

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
	} else {
		if start == nil {
			iter.SeekToFirst()
		} else {
			iter.Seek(start)
		}
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
	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	if err := iter.iter.Err(); err != nil {
		panic(err)
	}

	// If source is invalid, invalid.
	if !iter.iter.Valid() {
		iter.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var end = iter.end
	key := iter.iter.Key()
	defer key.Free()

	if iter.reverse {
		if end != nil && bytes.Compare(key.Data(), end) <= 0 {
			iter.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key.Data()) <= 0 {
			iter.isInvalid = true
			return false
		}
	}

	// Valid
	return true
}

func (iter *rocksIterator) Key() (key []byte) {
	if !iter.Valid() {
		panic("Iterator is invalid")
	} else if err := iter.iter.Err(); err != nil {
		panic(err)
	}

	originalKey := iter.iter.Key()
	defer originalKey.Free()

	key = make([]byte, originalKey.Size())
	copy(key, originalKey.Data())

	return key
}

func (iter *rocksIterator) Value() (value []byte) {
	if !iter.Valid() {
		panic("Iterator is invalid")
	} else if err := iter.iter.Err(); err != nil {
		panic(err)
	}

	originalValue := iter.iter.Value()
	defer originalValue.Free()

	value = make([]byte, originalValue.Size())
	copy(value, originalValue.Data())

	return value
}
