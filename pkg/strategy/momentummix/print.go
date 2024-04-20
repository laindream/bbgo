package momentummix

import (
	"fmt"
	"github.com/c9s/bbgo/pkg/types"
	"time"
)

var isAggKlinePrintMap = make(map[string]bool)

func (s *Strategy) PrintAggTrade(trade types.Trade) {
	printKey := fmt.Sprintf("%s-%s", trade.Symbol, time.Time(trade.Time).Format("2006-01-02 15:04"))
	if !isAggKlinePrintMap[printKey] {
		var totalCount int
		for _, kline := range s.AggKline {
			totalCount += kline.TradeCount
		}
		isAggKlinePrintMap[printKey] = true
		from := time.Time(trade.Time).Truncate(time.Second).Add(-(7*time.Minute + 23*time.Second + 567*time.Millisecond))
		to := time.Time(trade.Time).Truncate(time.Second).Add(-(2*time.Minute + 56*time.Second + 872*time.Millisecond))
		trades, window := s.AggKline[trade.Symbol].Get(from, to)
		log.Infof("[%s][%s][%s]5M|%s|ALL Count: %d|%d|%d, Window: %+v", "AggKline", trade.Symbol,
			time.Time(trade.Time).Format("2006-01-02 15:04:05.00000"),
			trade.Symbol,
			len(trades), s.AggKline[trade.Symbol].TradeCount, totalCount,
			window)
		window = s.AggKline[trade.Symbol].GetWindow(from, to)
		log.Infof("window: %+v", window)
		persist, w, err := s.AggKline[trade.Symbol].GetPersist(time.Time(trade.Time).Truncate(time.Second).
			Add(-time.Minute), time.Time(trade.Time).Truncate(time.Second))
		if err != nil {
			log.Errorf("failed to get persist trades: %v", err)
		}
		log.Infof("Persist Trades: %d, Window: %+v", len(persist), w)
	}
}

var isTickKlinePrintMap = make(map[string]bool)

func (s *Strategy) PrintBookTicker(bookTicker types.BookTicker) {
	printKey := fmt.Sprintf("%s-%s", bookTicker.Symbol, bookTicker.TransactionTime.Format("2006-01-02 15:04"))
	if !isTickKlinePrintMap[printKey] {
		var totalCount int
		for _, kline := range s.TickKline {
			totalCount += kline.TickCount
		}
		isTickKlinePrintMap[printKey] = true
		tickers, window := s.TickKline[bookTicker.Symbol].Get(bookTicker.TransactionTime.Truncate(time.Second).
			Add(-time.Minute), bookTicker.TransactionTime.Truncate(time.Second))
		log.Infof("[%s][%s][%s]5M|%s|ALL Count: %d|%d|%d, Window: %+v", "TickKline", bookTicker.Symbol,
			bookTicker.TransactionTime.Format("2006-01-02 15:04:05.00000"),
			bookTicker.Symbol,
			len(tickers), s.TickKline[bookTicker.Symbol].TickCount, totalCount,
			window)
		persist, w, err := s.TickKline[bookTicker.Symbol].GetPersist(bookTicker.TransactionTime.Truncate(time.Second).
			Add(-time.Minute), bookTicker.TransactionTime.Truncate(time.Second))
		if err != nil {
			log.Errorf("failed to get persist ticks: %v", err)
		}
		log.Infof("Persist Ticks: %d, Window: %+v", len(persist), w)
	}
}
