//go:build purego || appengine || js

// This file contains the safe implementation of nanotime using time.Now().

package puddleg

import (
	"time"
)

func nanotime() int64 {
	return time.Now().UnixNano()
}
