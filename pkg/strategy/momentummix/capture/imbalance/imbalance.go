package imbalance

import (
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/tick"
	"github.com/c9s/bbgo/pkg/types"
	"sync"
	"time"
)

type CaptureConfig struct {
	MinIntervalSeconds                       int64 `json:"minIntervalSeconds" toml:"minIntervalSeconds" yaml:"minIntervalSeconds"`
	MaxIntervalSeconds                       int64 `json:"maxIntervalSeconds" toml:"maxIntervalSeconds" yaml:"maxIntervalSeconds"`
	MinContinuousIntervalMilliSeconds        int64 `json:"minContinuousIntervalMilliSeconds" toml:"minContinuousIntervalMilliSeconds" yaml:"minContinuousIntervalMilliSeconds"`
	SellBuyQuantityImbalanceMinRatio         int64 `json:"sellBuyImbalanceMinRatio" toml:"sellBuyImbalanceMinRatio" yaml:"sellBuyImbalanceMinRatio"`
	SellBuyBookTopQuantityImbalanceMinRatio  int64 `json:"sellBuyBookTopQuantityImbalanceMinRatio" toml:"sellBuyBookTopQuantityImbalanceMinRatio" yaml:"sellBuyBookTopQuantityImbalanceMinRatio"`
	SellBuyBookDeepQuantityImbalanceMinRatio int64 `json:"sellBuyBookDeepQuantityImbalanceMinRatio" toml:"sellBuyBookDeepQuantityImbalanceMinRatio" yaml:"sellBuyBookDeepQuantityImbalanceMinRatio"`
	MinBookDepth                             int64 `json:"minBookDepth" toml:"minBookDepth" yaml:"minBookDepth"`
	MaxBookDepth                             int64 `json:"maxBookDepth" toml:"maxBookDepth" yaml:"maxBookDepth"`
}

type Capture struct {
	CaptureConfig
	MinInterval           time.Duration
	MaxInterval           time.Duration
	MinContinuousInterval time.Duration

	EventDuration    time.Duration
	EventTriggerTime time.Time

	mu        sync.Mutex
	aggKline  *aggtrade.Kline
	tickKline *tick.Kline

	Symbol   string
	Exchange types.ExchangeName

	orderExecutor bbgo.OrderExecutor

	AggTradeDelay time.Duration
	BookDelay     time.Duration
	TickDelay     time.Duration

	NearDeepQuantityRateRatioWindow *NearDeepQuantityRateRatioWindow
}

func NewCapture(config CaptureConfig, symbol string, exchange types.ExchangeName, aggKline *aggtrade.Kline, tickKline *tick.Kline) *Capture {
	return &Capture{
		MinInterval:                     time.Duration(config.MinIntervalSeconds) * time.Second,
		MaxInterval:                     time.Duration(config.MaxIntervalSeconds) * time.Second,
		MinContinuousInterval:           time.Duration(config.MinContinuousIntervalMilliSeconds) * time.Millisecond,
		aggKline:                        aggKline,
		tickKline:                       tickKline,
		Symbol:                          symbol,
		Exchange:                        exchange,
		CaptureConfig:                   config,
		NearDeepQuantityRateRatioWindow: NewNearDeepQuantityRateRatioWindow(30),
	}
}

func (c *Capture) EmitOrderBookChange(bookDiff types.SliceOrderBook) {

}

func (c *Capture) EmitTick(ticker types.BookTicker) {

}
