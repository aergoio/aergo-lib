package db

import (
	"github.com/aergoio/aergo-lib/log"
	"github.com/dgraph-io/badger/v3"
)

type badgerExtendedLog struct {
	*log.Logger
}

func newBadgerExtendedLog(logger *log.Logger) *badgerExtendedLog {
	if logger == nil {
		panic("base logger of raft is nil")
	}

	return &badgerExtendedLog{logger.WithSkipFrameCount(3)}
}

var _ badger.Logger = (*badgerExtendedLog)(nil)

func (l *badgerExtendedLog) Errorf(f string, v ...interface{}) {
	l.Error().Msgf(f, v...)
}

func (l *badgerExtendedLog) Warningf(f string, v ...interface{}) {
	l.Warn().Msgf(f, v...)
}

func (l *badgerExtendedLog) Infof(f string, v ...interface{}) {
	// reduce info to debug level because infos at badgerdb are too detail
	l.Debug().Msgf(f, v...) // INFO -> DEBUG
}

func (l *badgerExtendedLog) Debugf(f string, v ...interface{}) {
	l.Debug().Msgf(f, v...)
}
