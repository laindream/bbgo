package trigger

import (
	"github.com/c9s/bbgo/pkg/strategy/momentummix/capture/imbalance"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/history"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/aggtrade"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/sirupsen/logrus"
	"time"
)

type BookTickerTriggerFn func(bookTicker types.BookTicker)

type QuoteQuantityExceedTrigger struct {
	Symbol                                 string
	AdaptTriggerNearQuoteQuantityRateRatio float64
	AdaptKeepNearQuoteQuantityRateRatio    float64
	FirstTriggerTime                       *time.Time
	FinalTriggerTime                       *time.Time
	LastTriggerTime                        *time.Time

	Capture  *imbalance.Capture
	History  *history.MarketHistory
	AggKline *aggtrade.Kline

	MinWindowTradeCount int
	MaxWindowTradeCount int

	MinTriggerNearDuration time.Duration
	TriggerNearDuration    time.Duration
	MaxTriggerNearDuration time.Duration

	MinKeepNearDuration time.Duration
	KeepNearDuration    time.Duration
	MaxKeepNearDuration time.Duration

	MinKeepDuration time.Duration
	MaxKeepDuration time.Duration

	MinTriggerInterval time.Duration
	MaxTriggerInterval time.Duration

	OnTrigger     BookTickerTriggerFn
	OnUnTrigger   BookTickerTriggerFn
	OnKeepTrigger BookTickerTriggerFn
	Logger        *logrus.Logger
}

func (q *QuoteQuantityExceedTrigger) IsTrigger() bool {
	return q.FirstTriggerTime != nil
}

func (q *QuoteQuantityExceedTrigger) Trigger(bookTicker types.BookTicker) {
	if q.LastTriggerTime != nil && bookTicker.TransactionTime.Sub(*q.LastTriggerTime) > q.MaxTriggerInterval {
		q.AdaptTriggerNearQuoteQuantityRateRatio = q.AdaptTriggerNearQuoteQuantityRateRatio * 0.75
	}
	if q.LastTriggerTime != nil && bookTicker.TransactionTime.Sub(*q.LastTriggerTime) < q.MinTriggerInterval {
		q.AdaptTriggerNearQuoteQuantityRateRatio = q.AdaptTriggerNearQuoteQuantityRateRatio * 2
	}
	if q.FirstTriggerTime == nil {
		q.FirstTriggerTime = &bookTicker.TransactionTime
	}
	q.FinalTriggerTime = &bookTicker.TransactionTime
	q.OnTrigger(bookTicker)
}

func (q *QuoteQuantityExceedTrigger) TriggerDuration() time.Duration {
	if q.FirstTriggerTime == nil {
		return 0
	}
	return q.FinalTriggerTime.Sub(*q.FirstTriggerTime)
}

func (q *QuoteQuantityExceedTrigger) KeepTrigger(bookTicker types.BookTicker) {
	q.FinalTriggerTime = &bookTicker.TransactionTime
	q.OnKeepTrigger(bookTicker)
}

func (q *QuoteQuantityExceedTrigger) UnTrigger(bookTicker types.BookTicker) {
	if bookTicker.TransactionTime.Sub(*q.FirstTriggerTime) > q.MaxKeepDuration {
		q.AdaptKeepNearQuoteQuantityRateRatio = q.AdaptKeepNearQuoteQuantityRateRatio * 2
	}
	if bookTicker.TransactionTime.Sub(*q.FirstTriggerTime) < q.MinKeepDuration {
		q.AdaptKeepNearQuoteQuantityRateRatio = q.AdaptKeepNearQuoteQuantityRateRatio * 0.75
	}
	q.FirstTriggerTime = nil
	q.FinalTriggerTime = nil
	q.LastTriggerTime = &bookTicker.TransactionTime
	q.OnUnTrigger(bookTicker)
}

func (q *QuoteQuantityExceedTrigger) BookTickerPush(bookTicker types.BookTicker) {
	stat := q.History.GetStat()
	historyQuoteQuantityRate := stat.GetQuoteQuantityRate()
	if historyQuoteQuantityRate == 0 {
		return
	}
	if q.IsTrigger() {
		keepNearWindow := q.GetKeepNearWindow(bookTicker)
		keepNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionAll, q.KeepNearDuration)
		if keepNearQuoteQuantityRate == 0 {
			return
		}
		keepBuyNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionBuy, q.KeepNearDuration)
		keepSellNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionSell, q.KeepNearDuration)
		keepRatio := keepNearQuoteQuantityRate / historyQuoteQuantityRate
		if keepRatio > q.AdaptKeepNearQuoteQuantityRateRatio {
			q.KeepTrigger(bookTicker)
		} else {
			if bookTicker.TransactionTime.Sub(*q.FinalTriggerTime) > q.KeepNearDuration {
				q.Logger.Infof("[UnTrigger][%s][%s][TND:%s][KND:%s][%f][%f]ratio: %f, buy: %f, sell: %f, tick: %+v",
					q.Symbol,
					q.TriggerDuration(),
					q.TriggerNearDuration,
					q.KeepNearDuration,
					q.AdaptTriggerNearQuoteQuantityRateRatio,
					q.AdaptKeepNearQuoteQuantityRateRatio,
					keepRatio,
					keepBuyNearQuoteQuantityRate,
					keepSellNearQuoteQuantityRate,
					bookTicker)
				q.UnTrigger(bookTicker)
			}
		}
	} else {
		triggerNearWindow := q.GetTriggerNearWindow(bookTicker)
		triggerNearQuoteQuantityRate := triggerNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionAll, q.TriggerNearDuration)
		if triggerNearQuoteQuantityRate == 0 {
			return
		}
		triggerBuyNearQuoteQuantityRate := triggerNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionBuy, q.TriggerNearDuration)
		triggerSellNearQuoteQuantityRate := triggerNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionSell, q.TriggerNearDuration)
		triggerRatio := triggerNearQuoteQuantityRate / historyQuoteQuantityRate
		if triggerRatio > q.AdaptTriggerNearQuoteQuantityRateRatio {
			q.Trigger(bookTicker)
			q.Logger.Infof("[Trigger][%s][TND:%s][KND:%s][%f][%f]ratio: %f, buy: %f, sell: %f, tick: %+v",
				q.Symbol,
				q.TriggerNearDuration,
				q.KeepNearDuration,
				q.AdaptTriggerNearQuoteQuantityRateRatio,
				q.AdaptKeepNearQuoteQuantityRateRatio,
				triggerRatio,
				triggerBuyNearQuoteQuantityRate,
				triggerSellNearQuoteQuantityRate,
				bookTicker)
		}
	}
}

var maxSearchCount = 8

func (q *QuoteQuantityExceedTrigger) GetTriggerNearWindow(bookTicker types.BookTicker) *aggtrade.WindowBase {
	triggerNearWindow := q.AggKline.GetWindow(bookTicker.TransactionTime.Add(-q.TriggerNearDuration), bookTicker.TransactionTime)
	searchCount := 0
	for searchCount < maxSearchCount &&
		(triggerNearWindow.GetTradeCount() <= q.MinWindowTradeCount ||
			triggerNearWindow.GetTradeCount() > q.MaxWindowTradeCount) {
		searchCount++
		if triggerNearWindow.GetTradeCount() <= q.MinWindowTradeCount {
			upperTriggerNearDuration := q.TriggerNearDuration * 5 / 4
			if upperTriggerNearDuration > q.MaxTriggerNearDuration {
				break
			}
			q.TriggerNearDuration = upperTriggerNearDuration
		}
		if triggerNearWindow.GetTradeCount() > q.MaxWindowTradeCount {
			lowerTriggerNearDuration := q.TriggerNearDuration * 9 / 10
			if lowerTriggerNearDuration < q.MinTriggerNearDuration {
				break
			}
			q.TriggerNearDuration = lowerTriggerNearDuration
		}
		triggerNearWindow = q.AggKline.GetWindow(bookTicker.TransactionTime.Add(-q.TriggerNearDuration), bookTicker.TransactionTime)
	}
	return triggerNearWindow
}

func (q *QuoteQuantityExceedTrigger) GetKeepNearWindow(bookTicker types.BookTicker) *aggtrade.WindowBase {
	keepNearWindow := q.AggKline.GetWindow(bookTicker.TransactionTime.Add(-q.KeepNearDuration), bookTicker.TransactionTime)
	searchCount := 0
	for searchCount < maxSearchCount &&
		(keepNearWindow.GetTradeCount() <= q.MinWindowTradeCount ||
			keepNearWindow.GetTradeCount() > q.MaxWindowTradeCount) {
		searchCount++
		if keepNearWindow.GetTradeCount() <= q.MinWindowTradeCount {
			upperKeepNearDuration := q.KeepNearDuration * 5 / 4
			if upperKeepNearDuration > q.MaxKeepNearDuration {
				break
			}
			q.KeepNearDuration = upperKeepNearDuration
		}
		if keepNearWindow.GetTradeCount() > q.MaxWindowTradeCount {
			lowerKeepNearDuration := q.KeepNearDuration * 9 / 10
			if lowerKeepNearDuration < q.MinKeepNearDuration {
				break
			}
			q.KeepNearDuration = lowerKeepNearDuration
		}
		keepNearWindow = q.AggKline.GetWindow(bookTicker.TransactionTime.Add(-q.KeepNearDuration), bookTicker.TransactionTime)
	}
	return keepNearWindow
}
