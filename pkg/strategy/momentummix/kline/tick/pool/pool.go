package pool

import (
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/tick"
	"sync"
)

var WindowBasePool = sync.Pool{
	New: func() interface{} {
		return tick.NewWindowBase()
	},
}

var SecPool = sync.Pool{
	New: func() interface{} {
		return tick.NewSecond(nil)
	},
}

var MinPool = sync.Pool{
	New: func() interface{} {
		return tick.NewMinute(nil)
	},
}

var HourPool = sync.Pool{
	New: func() interface{} {
		return tick.NewHour(nil)
	},
}
