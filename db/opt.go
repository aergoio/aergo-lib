package db

import "fmt"

const (
	OptCompactionController     = "compactionController"
	OptCompactionControllerPort = "compactionControllerPort"
)

// Opt refers to an option item applied to DB initialization. The detailed option types vary depending on
// the DB implementation, and it is the caller's responsibility to set the correct options.
type Opt struct {
	Name  string
	Value interface{}
}

func (o Opt) String() string {
	return fmt.Sprintf("%s=%v", o.Name, o.Value)
}

// WithControlCompaction return opts for enabling/disabling control compaction feature of DB
func WithControlCompaction(enable bool, port int) []Opt {
	return []Opt{{
		Name:  OptCompactionController,
		Value: enable,
	}, {
		Name:  OptCompactionControllerPort,
		Value: port,
	}}

}
