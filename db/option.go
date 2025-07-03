package db

import (
	"fmt"
	"time"
)

const (
	// OptCompactionController enables manual compaction. value type boolean
	OptCompactionController = "compactionController"
	// value type int
	OptCompactionControllerPort = "compactionControllerPort"
	// OptCompactionEventHandler set event handler. value type CompactionEventHandler
	OptCompactionEventHandler = "compactionEventHandler"

	// OptEnableConfigure enables fine-tuning feature of DB. value type boolean
	OptEnableConfigure = "enableConfigure"
)

// Option refers to an option item applied to DB initialization. The detailed option types vary depending on
// the DB implementation, and it is the caller's responsibility to set the correct options.
type Option struct {
	Name  string
	Value interface{}
}

func (o Option) String() string {
	return fmt.Sprintf("%s=%v", o.Name, o.Value)
}

// WithControlCompaction return opts for enabling/disabling the control compaction feature of DB
func WithControlCompaction(enable bool, port int) []Option {
	return []Option{{
		Name:  OptCompactionController,
		Value: enable,
	}, {
		Name:  OptCompactionControllerPort,
		Value: port,
	}}
}

type CompactionEvent struct {
	Level       int
	NextLevel   int
	LastLevel   int
	NumSplits   int
	TopTables   []uint64
	BotTables   []uint64
	CompactorID int
	Adjusted    float64
	Score       float64
	Timestamp   time.Time
	Reason      string
	Parallelism int
	Start       bool
}

type CompactionEventHandler func(event CompactionEvent)
