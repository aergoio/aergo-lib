/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package log

import (
	"runtime"
	"strconv"
)

// LazyEval can be used to evaluate an argument under a correct log level.
type LazyEval func() string

func (l LazyEval) String() string {
	return l()
}

// DoLazyEval returns LazyEval. Unnecessary evalution can be prevented by using
// "%v" format string,
func DoLazyEval(c func() string) LazyEval {
	return LazyEval(c)
}

func SkipCaller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "?"
	}
	return file + ":" + strconv.Itoa(line)
}
