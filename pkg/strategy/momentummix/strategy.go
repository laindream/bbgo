package momentummix

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/types"
	"time"
)

const ID = "momentum_mix"

func init() {
	// Register our struct type to BBGO
	// Note that you don't need to field the fields.
	// BBGO uses reflect to parse your type information.
	bbgo.RegisterStrategy(ID, &Strategy{})
}

type Strategy struct {
	Symbol   string         `json:"symbol"`
	Interval types.Interval `json:"interval"`
}

func (s *Strategy) ID() string {
	return ID
}

//BookChannel        = Channel("book")
//KLineChannel       = Channel("kline")
//BookTickerChannel  = Channel("bookTicker")
//MarketTradeChannel = Channel("trade")
//AggTradeChannel    = Channel("aggTrade")
//ForceOrderChannel  = Channel("forceOrder")

func (s *Strategy) Subscribe(session *bbgo.ExchangeSession) {
	//session.Subscribe(types.KLineChannel, s.Symbol, types.SubscribeOptions{Interval: s.Interval})
	//session.Subscribe(types.BookTickerChannel, s.Symbol, types.SubscribeOptions{})
	//session.Subscribe(types.AggTradeChannel, s.Symbol, types.SubscribeOptions{})
	//session.Subscribe(types.MarketTradeChannel, s.Symbol, types.SubscribeOptions{})
	//session.Subscribe(types.ForceOrderChannel, s.Symbol, types.SubscribeOptions{})

	session.Subscribe(types.BookChannel, s.Symbol, types.SubscribeOptions{
		Depth: types.DepthLevelFull,
		Speed: types.SpeedHigh,
	})
}

func (s *Strategy) Run(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) error {
	//session.MarketDataStream.OnKLineClosed(func(kline types.KLine) {
	//	if kline.Symbol != s.Symbol || kline.Interval != s.Interval {
	//		return
	//	}
	//	fmt.Println(kline)
	//})
	//session.MarketDataStream.OnKLineClosed(types.KLineWith("BTCUSDT", types.Interval1m, func(kline types.KLine) {
	//	// 在这里处理你的 kline
	//}))
	//session.MarketDataStream.OnMarketTrade(func(trade types.Trade) {
	//	fmt.Println(trade)
	//})
	session.MarketDataStream.OnKLine(func(kline types.KLine) {
		//printData(kline)
	})
	session.MarketDataStream.OnBookTickerUpdate(func(bookTicker types.BookTicker) {
		//printData(bookTicker)
	})
	session.MarketDataStream.OnAggTrade(func(trade types.Trade) {
		//printData(trade)
	})
	session.MarketDataStream.OnMarketTrade(func(trade types.Trade) {
		//printData(trade)
	})
	session.MarketDataStream.OnForceOrder(func(info types.LiquidationInfo) {
		//printData(info)
	})

	session.MarketDataStream.OnBookUpdate(func(book types.SliceOrderBook) {
		printData(book)
	})
	return nil
}

func printData(o interface{}) {
	oJsonStr, _ := json.Marshal(o)
	var dataTime time.Time
	switch o.(type) {
	case types.KLine:
		dataTime = time.Time(o.(types.KLine).StartTime)
	case types.SliceOrderBook:
		dataTime = o.(types.SliceOrderBook).Time
	case types.Trade:
		dataTime = time.Time(o.(types.Trade).Time)
	case types.LiquidationInfo:
		dataTime = time.Time(o.(types.LiquidationInfo).TradeTime)
	}
	var timeDiff time.Duration
	if !dataTime.IsZero() {
		timeDiff = time.Now().Sub(dataTime)
	}

	fmt.Printf("[%s](%s) %s\n", time.Now().Format("2006-01-02 15:04:05.00000"), timeDiff, oJsonStr)
}
