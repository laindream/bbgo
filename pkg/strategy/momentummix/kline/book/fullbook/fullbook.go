package fullbook

import (
	"context"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/exchange/binance"
	"github.com/c9s/bbgo/pkg/types"
	"sync"
	"time"
)

type WatchDog struct {
	Symbol         string
	UpdateInterval time.Duration
	MaxBookCount   int
	bookSnapshots  []*types.SliceOrderBook
	session        *bbgo.ExchangeSession
	mu             *sync.Mutex
}

func NewWatchDog(symbol string, session *bbgo.ExchangeSession) *WatchDog {
	wd := &WatchDog{
		Symbol:         symbol,
		UpdateInterval: time.Minute * 5,
		MaxBookCount:   100,
		bookSnapshots:  make([]*types.SliceOrderBook, 0),
		session:        session,
	}
	go wd.Run()
	return wd
}

func (w *WatchDog) GetBookSnapshots() []*types.SliceOrderBook {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bookSnapshots
}

func (w *WatchDog) GetLatestBookSnapshot() *types.SliceOrderBook {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.bookSnapshots) == 0 {
		return nil
	}
	return w.bookSnapshots[len(w.bookSnapshots)-1]
}

func (w *WatchDog) Run() {
	time.Sleep(time.Minute)
	for {
		w.mu.Lock()
		if len(w.bookSnapshots) > w.MaxBookCount {
			w.bookSnapshots = w.bookSnapshots[1:]
		}
		w.mu.Unlock()
		ctx := context.Background()
		snapshot, _, err := w.session.Exchange.(*binance.Exchange).QueryDepth(ctx, w.Symbol)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		w.mu.Lock()
		w.bookSnapshots = append(w.bookSnapshots, &snapshot)
		w.mu.Unlock()

		time.Sleep(w.UpdateInterval)
	}
}
