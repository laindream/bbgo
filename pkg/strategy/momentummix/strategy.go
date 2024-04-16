package momentummix

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/config"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/tick"
	"github.com/c9s/bbgo/pkg/types"
	"sort"
	"time"
)

const ID = "momentum_mix"

func init() {
	// Register our struct type to BBGO
	// Note that you don't need to field the fields.
	// BBGO uses reflect to parse your type information.
	bbgo.RegisterStrategy(ID, &Strategy{})
}

var DefaultSearchDepth = 10

type Strategy struct {
	Symbols              []string         `json:"symbols"`
	Interval             types.Interval   `json:"interval"`
	EventIntervalSeconds int              `json:"eventIntervalSeconds"`
	InfluxDB             *config.InfluxDB `json:"influxDB"`
	FeeRates             map[string]types.ExchangeFee
	SearchDepth          map[string]int
	LastTriggerTime      map[string]time.Time
	AggKline             map[string]*aggtrade.Kline
	TickKline            map[string]*tick.Kline
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

	//session.Subscribe(types.BookChannel, s.Symbol, types.SubscribeOptions{
	//	Depth: types.DepthLevelFull,
	//	Speed: types.SpeedHigh,
	//})

	for _, symbol := range s.Symbols {
		session.Subscribe(types.AggTradeChannel, symbol, types.SubscribeOptions{})
		session.Subscribe(types.BookTickerChannel, symbol, types.SubscribeOptions{})
		session.Subscribe(types.BookChannel, symbol, types.SubscribeOptions{
			Depth: types.DepthLevelFull,
			Speed: types.SpeedHigh,
		})
	}
}

func (s *Strategy) Run(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) error {
	if err := s.Init(ctx, session); err != nil {
		return err
	}
	session.MarketDataStream.OnKLine(func(kline types.KLine) {
		//printData(kline)
	})
	isTickKlinePrintMap := make(map[string]bool)
	session.MarketDataStream.OnBookTickerUpdate(func(bookTicker types.BookTicker) {
		s.TickKline[bookTicker.Symbol].AppendTick(&bookTicker)
		printKey := fmt.Sprintf("%s-%s", bookTicker.Symbol, bookTicker.TransactionTime.Format("2006-01-02 15:04"))
		if bookTicker.TransactionTime.Minute()%5 == 0 && !isTickKlinePrintMap[printKey] {
			var totalCount int
			for _, kline := range s.TickKline {
				totalCount += kline.TickCount
			}
			isTickKlinePrintMap[printKey] = true
			tickers, window := s.TickKline[bookTicker.Symbol].Get(bookTicker.TransactionTime.Truncate(time.Second).Add(-time.Minute*5), bookTicker.TransactionTime.Truncate(time.Second))
			log.Infof("[%s][%s][%s]5M|%s|ALL Count: %d|%d|%d, Window: %+v", "TickKline", bookTicker.Symbol,
				bookTicker.TransactionTime.Format("2006-01-02 15:04:05.00000"),
				bookTicker.Symbol,
				len(tickers), s.TickKline[bookTicker.Symbol].TickCount, totalCount,
				window)

		}
		//s.printData(session, bookTicker)
	})
	isAggKlinePrintMap := make(map[string]bool)
	session.MarketDataStream.OnAggTrade(func(trade types.Trade) {
		s.AggKline[trade.Symbol].AppendTrade(&trade)
		printKey := fmt.Sprintf("%s-%s", trade.Symbol, time.Time(trade.Time).Format("2006-01-02 15:04"))
		//time.Time(trade.Time).Minute()%5 == 0 &&
		if !isAggKlinePrintMap[printKey] {
			var totalCount int
			for _, kline := range s.AggKline {
				totalCount += kline.TradeCount
			}
			isAggKlinePrintMap[printKey] = true
			trades, window := s.AggKline[trade.Symbol].Get(time.Time(trade.Time).Truncate(time.Second).Add(-time.Minute*2), time.Time(trade.Time).Truncate(time.Second).Add(-time.Minute*1))
			log.Infof("[%s][%s][%s]5M|%s|ALL Count: %d|%d|%d, Window: %+v", "AggKline", trade.Symbol,
				time.Time(trade.Time).Format("2006-01-02 15:04:05.00000"),
				trade.Symbol,
				len(trades), s.AggKline[trade.Symbol].TradeCount, totalCount,
				window)
		}
	})
	session.MarketDataStream.OnMarketTrade(func(trade types.Trade) {
		//printData(trade)
	})
	session.MarketDataStream.OnForceOrder(func(info types.LiquidationInfo) {
		//printData(info)
	})

	session.MarketDataStream.OnBookUpdate(func(book types.SliceOrderBook) {
		s.printFilteredOrderBook(session, book)
	})

	session.MarketDataStream.OnBookSnapshot(func(book types.SliceOrderBook) {
		//printData(session, book)
	})
	return nil
}

func (s *Strategy) GetSearchDepth(symbol string) int {
	if depth, ok := s.SearchDepth[symbol]; ok {
		return depth
	}
	return DefaultSearchDepth
}

func (s *Strategy) SetSearchDepth(symbol string, depth int) {
	s.SearchDepth[symbol] = depth
}

func (s *Strategy) GetLastTriggerTime(symbol string) time.Time {
	if t, ok := s.LastTriggerTime[symbol]; ok {
		return t
	}
	return time.Time{}
}

func (s *Strategy) SetLastTriggerTime(symbol string, t time.Time) {
	s.LastTriggerTime[symbol] = t
}

func (s *Strategy) printFilteredOrderBook(session *bbgo.ExchangeSession, o types.SliceOrderBook) {
	dataTime := o.TransactionTime
	var timeDiff time.Duration
	if !dataTime.IsZero() {
		timeDiff = time.Now().Sub(dataTime)
	}
	order, ok := session.OrderBook(o.Symbol)
	if ok {
		depth := s.GetSearchDepth(o.Symbol)
		bestAsk, _ := order.BestAsk()
		bestBid, _ := order.BestBid()
		ibd, imbalanceOrder, _ := s.CalculateOrderImbalanceRatioWithDepth(depth, order)
		ibp, _, _ := s.CalculateOrderImbalanceRatioWithPricePercentage(0.5, order)
		if !(ibd > 20 || ibd < 0.05) {
			return
		}
		diff := time.Now().Sub(s.GetLastTriggerTime(o.Symbol))
		if !s.GetLastTriggerTime(o.Symbol).IsZero() && diff < time.Duration(s.EventIntervalSeconds)*time.Second {
			s.SetSearchDepth(o.Symbol, depth+1)
			log.Infof("[%s][%s][%d][%s](%s) %s,diff:%s", "OrderBook", o.Symbol, depth, time.Now().Format("2006-01-02 15:04:05.00000"), timeDiff, "Increase Depth", diff)
		}
		s.SetLastTriggerTime(o.Symbol, time.Now())

		l1 := fmt.Sprintf("[%s][%s][%d][%s](%s)", "OrderBook", o.Symbol, depth, time.Now().Format("2006-01-02 15:04:05.00000"), timeDiff)
		l2 := fmt.Sprintf("[IBD:%.2f:%s]", ibd, imbalanceOrder)
		l3 := fmt.Sprintf("[IBP:%.2f]", ibp)
		l4 := fmt.Sprintf("[A:%d|B:%d]", len(order.SideBook(types.SideTypeSell)), len(order.SideBook(types.SideTypeBuy)))
		l5 := fmt.Sprintf("[BA:%s|BB:%s]", bestAsk, bestBid)
		l6 := fmt.Sprintf("[-A:%s|-B:%s]", s.topVolumes(order.SideBook(types.SideTypeSell), 5, 5), s.topVolumes(order.SideBook(types.SideTypeBuy), 5, 5))
		log.Infof("%s%s%s%s%s%s", l1, l2, l3, l4, l5, l6)
	}
}

func (s *Strategy) printData(session *bbgo.ExchangeSession, o interface{}) {
	oJsonStr, _ := json.Marshal(o)
	var dataTime time.Time
	switch o.(type) {
	case types.KLine:
		dataTime = time.Time(o.(types.KLine).StartTime)
		var timeDiff time.Duration
		if !dataTime.IsZero() {
			timeDiff = time.Now().Sub(dataTime)
		}
		log.Infof("[%s](%s) %s", time.Now().Format("2006-01-02 15:04:05.00000"), timeDiff, oJsonStr)
	case types.BookTicker:
		dataTime = o.(types.BookTicker).TransactionTime
		var timeDiff time.Duration
		if !dataTime.IsZero() {
			timeDiff = time.Now().Sub(dataTime)
		}
		log.Infof("[%s][%s](%s) %s", "BookTicker", time.Now().Format("2006-01-02 15:04:05.00000"), timeDiff, oJsonStr)
	case types.SliceOrderBook:
		dataTime = o.(types.SliceOrderBook).TransactionTime
		var timeDiff time.Duration
		if !dataTime.IsZero() {
			timeDiff = time.Now().Sub(dataTime)
		}
		log.Infof("[%s][%s](%s)", "OrderBook", time.Now().Format("2006-01-02 15:04:05.00000"), timeDiff)

		log.Infof("[UA:%d|UB:%d]", len(o.(types.SliceOrderBook).Asks), len(o.(types.SliceOrderBook).Bids))
		order, ok := session.OrderBook(s.Symbols[0])
		if ok {
			bestAsk, _ := order.BestAsk()
			bestBid, _ := order.BestBid()
			ibd, _, _ := s.CalculateOrderImbalanceRatioWithDepth(10, order)
			ibp, _, _ := s.CalculateOrderImbalanceRatioWithPricePercentage(0.5, order)
			log.Infof("[IBD:%.2f]", ibd)
			log.Infof("[IBP:%.2f]", ibp)
			log.Infof("[A:%d|B:%d]", len(order.SideBook(types.SideTypeSell)), len(order.SideBook(types.SideTypeBuy)))
			log.Infof("[BA:%s|BB:%s]", bestAsk, bestBid)
			log.Infof("[-A:%s|-B:%s]", s.topVolumes(order.SideBook(types.SideTypeSell), 5, 5), s.topVolumes(order.SideBook(types.SideTypeBuy), 5, 5))
		}
	case types.Trade:
		dataTime = time.Time(o.(types.Trade).Time)
		var timeDiff time.Duration
		if !dataTime.IsZero() {
			timeDiff = time.Now().Sub(dataTime)
		}
		log.Infof("[%s](%s) %s", time.Now().Format("2006-01-02 15:04:05.00000"), timeDiff, oJsonStr)
	case types.LiquidationInfo:
		dataTime = time.Time(o.(types.LiquidationInfo).TradeTime)
	}
}

func (s *Strategy) topVolumes(pvs types.PriceVolumeSlice, priceRangePercentage float64, count int) types.PriceVolumeSlice {
	var result types.PriceVolumeSlice
	currentPrice := pvs[0].Price
	priceRange := currentPrice.Mul(fixedpoint.NewFromFloat(priceRangePercentage / 100))
	for _, pv := range pvs {
		if pv.Price >= currentPrice-priceRange && pv.Price <= currentPrice+priceRange {
			result = append(result, pv)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Volume > result[j].Volume
	})
	if len(result) > count {
		result = result[:count]
	}
	return result
}

type OrderImbalanceEvent struct {
	AskAgg  types.PriceVolume
	BestAsk types.PriceVolume
	BidAgg  types.PriceVolume
	BestBid types.PriceVolume
}

func (o *OrderImbalanceEvent) String() string {
	return fmt.Sprintf("AskAgg:%s BestAsk:%s BidAgg:%s BestBid:%s", o.AskAgg, o.BestAsk, o.BidAgg, o.BestBid)
}

// return A/B
func (s *Strategy) CalculateOrderImbalanceRatioWithDepth(depth int, orderBook *types.StreamOrderBook) (float64, *OrderImbalanceEvent, error) {
	asks := orderBook.SideBook(types.SideTypeSell)
	bids := orderBook.SideBook(types.SideTypeBuy)

	if len(asks) == 0 || len(bids) == 0 {
		return 0, nil, fmt.Errorf("no enough data")
	}

	var askVolume, bidVolume fixedpoint.Value
	var askEndPrice, bidEndPrice fixedpoint.Value
	for i := 0; i < depth; i++ {
		askVolume += asks[i].Volume
		bidVolume += bids[i].Volume
		askEndPrice = asks[i].Price
		bidEndPrice = bids[i].Price
	}

	return askVolume.Float64() / bidVolume.Float64(),
		&OrderImbalanceEvent{
			AskAgg:  types.PriceVolume{Price: askEndPrice, Volume: askVolume},
			BestAsk: types.PriceVolume{Price: asks[0].Price, Volume: asks[0].Volume},
			BidAgg:  types.PriceVolume{Price: bidEndPrice, Volume: bidVolume},
			BestBid: types.PriceVolume{Price: bids[0].Price, Volume: bids[0].Volume},
		}, nil
}

func (s *Strategy) CalculateOrderImbalanceRatioWithPricePercentage(priceRangePercentage float64, orderBook *types.StreamOrderBook) (float64, *OrderImbalanceEvent, error) {
	asks := orderBook.SideBook(types.SideTypeSell)
	bids := orderBook.SideBook(types.SideTypeBuy)

	if len(asks) == 0 || len(bids) == 0 {
		return 0, nil, fmt.Errorf("no enough data")
	}

	var askVolume, bidVolume fixedpoint.Value
	var askEndPrice, bidEndPrice fixedpoint.Value
	currentPrice := asks[0].Price
	priceRange := currentPrice.Mul(fixedpoint.NewFromFloat(priceRangePercentage / 100))
	for _, pv := range asks {
		if pv.Price >= currentPrice-priceRange && pv.Price <= currentPrice+priceRange {
			askVolume += pv.Volume
			askEndPrice = pv.Price
		}
	}

	for _, pv := range bids {
		if pv.Price >= currentPrice-priceRange && pv.Price <= currentPrice+priceRange {
			bidVolume += pv.Volume
			bidEndPrice = pv.Price
		}
	}

	return askVolume.Float64() / bidVolume.Float64(),
		&OrderImbalanceEvent{
			AskAgg:  types.PriceVolume{Price: askEndPrice, Volume: askVolume},
			BestAsk: types.PriceVolume{Price: asks[0].Price, Volume: asks[0].Volume},
			BidAgg:  types.PriceVolume{Price: bidEndPrice, Volume: bidVolume},
			BestBid: types.PriceVolume{Price: bids[0].Price, Volume: bids[0].Volume},
		}, nil
}
