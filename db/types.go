/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

// ImplType represents implementators of a DB interface
type ImplType string

const (
	// BadgerImpl represents a name of DB interface implementation using badgerdb
	BadgerImpl ImplType = "badgerdb"

	// LevelImpl represents a name of DB interface implementation using leveldb
	LevelImpl ImplType = "leveldb"

	// MemoryImpl represents a name of DB interface implementation in memory
	MemoryImpl ImplType = "memorydb"

	// DummyImpl represents a name of DB interface implementation in memory
	// that only keeps the first block + the last 512 blocks
	DummyImpl ImplType = "dummydb"
)

type dbConstructor func(dir string) (DB, error)

// DB is an general interface to access at storage data
type DB interface {
	Type() string
	Path() string
	Set(key, value []byte)
	Delete(key []byte)
	Get(key []byte) []byte
	Exist(key []byte) bool
	Iterator(start, end []byte) Iterator
	NewTx() Transaction
	NewBulk() Bulk
	Close()
	//Print()
	//Stats() map[string]string
}

// Transaction is used to batch multiple operations
type Transaction interface {
	//	Get(key []byte) []byte
	Set(key, value []byte)
	Delete(key []byte)
	Commit()
	Discard()
}

// Bulk is used to batch multiple transactions
// This will internally commit transactions when reach maximum tx size
type Bulk interface {
	Set(key, value []byte)
	Delete(key []byte)
	Flush()
	DiscardLast()
}

// Iterator is used to navigate specific key ranges
type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
}
