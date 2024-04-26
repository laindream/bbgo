package trigger

import (
	"github.com/c9s/bbgo/pkg/strategy/momentummix/capture/imbalance"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/history"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/tick"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/sirupsen/logrus"
	"time"
)

type BookTickerTriggerFn func(bookTicker types.BookTicker)

var ActionBuy = "buy"
var ActionSell = "sell"

type QuoteQuantityExceedTrigger struct {
	Symbol                                 string
	AdaptTriggerNearQuoteQuantityRateRatio float64
	AdaptKeepNearQuoteQuantityRateRatio    float64
	FirstTriggerTime                       *time.Time
	FirstTriggerTicker                     *types.BookTicker
	OngoingHighTicker                      *types.BookTicker
	OngoingLowTicker                       *types.BookTicker
	FinalTriggerTime                       *time.Time
	FinalTriggerTicker                     *types.BookTicker
	Action                                 string
	LastTriggerTime                        *time.Time
	OngoingStopLossRate                    float64
	OngoingProfitThresholdRate             float64

	Capture   *imbalance.Capture
	History   *history.MarketHistory
	TickKline *tick.Kline
	AggKline  *aggtrade.Kline

	MinTriggerWindowTradeCount int
	MaxTriggerWindowTradeCount int
	MinKeepWindowTradeCount    int
	MaxKeepWindowTradeCount    int

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

	Leverage float64

	MinImbalanceThresholdTriggerRate float64
	ImbalanceThresholdTriggerRate    float64
	MaxImbalanceThresholdTriggerRate float64
	ImbalanceThresholdKeepRate       float64

	MaxStopLossRate float64
	MinStopLossRate float64
	StopLossRate    float64

	MaxNearPriceFluctuationRate     float64
	MinNearPriceFluctuationRate     float64
	NearPriceFluctuationRate        float64
	MaxNearPriceFluctuationDuration time.Duration
	MinNearPriceFluctuationDuration time.Duration
	NearPriceFluctuationDuration    time.Duration

	MaxFarPriceFluctuationRate     float64
	MinFarPriceFluctuationRate     float64
	FarPriceFluctuationRate        float64
	MaxFarPriceFluctuationDuration time.Duration
	MinFarPriceFluctuationDuration time.Duration
	FarPriceFluctuationDuration    time.Duration

	MinProfitThresholdRate float64
	MaxProfitThresholdRate float64
	ProfitThresholdRate    float64

	MinHoldDuration time.Duration
	MaxHoldDuration time.Duration

	MinTakeProfitRate float64
	MaxTakeProfitRate float64
	TakeProfitRate    float64

	OnTrigger     BookTickerTriggerFn
	OnUnTrigger   BookTickerTriggerFn
	OnKeepTrigger BookTickerTriggerFn
	Logger        *logrus.Logger

	WinRate           float64
	TotalProfitRate   float64
	TotalTriggerCount int
	WinCount          int
}

func (q *QuoteQuantityExceedTrigger) IsTrigger() bool {
	return q.FirstTriggerTime != nil
}

func (q *QuoteQuantityExceedTrigger) Trigger(bookTicker types.BookTicker) {
	q.TotalTriggerCount++
	if q.LastTriggerTime != nil && bookTicker.TransactionTime.Sub(*q.LastTriggerTime) > q.MaxTriggerInterval {
		q.AdaptTriggerNearQuoteQuantityRateRatio = q.AdaptTriggerNearQuoteQuantityRateRatio * 0.75
		q.ReduceTriggerPrice()
		q.ReduceImbalanceThresholdTriggerRate()
	}
	if q.LastTriggerTime != nil && bookTicker.TransactionTime.Sub(*q.LastTriggerTime) < q.MinTriggerInterval {
		q.AdaptTriggerNearQuoteQuantityRateRatio = q.AdaptTriggerNearQuoteQuantityRateRatio * 1.2
		q.ScaleImbalanceThresholdTriggerRate()
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
		q.AdaptKeepNearQuoteQuantityRateRatio = q.AdaptKeepNearQuoteQuantityRateRatio * 1.2
	}
	if bookTicker.TransactionTime.Sub(*q.FirstTriggerTime) < q.MinKeepDuration {
		q.AdaptKeepNearQuoteQuantityRateRatio = q.AdaptKeepNearQuoteQuantityRateRatio * 0.75
	}
	q.FirstTriggerTime = nil
	q.FinalTriggerTime = nil
	q.LastTriggerTime = &bookTicker.TransactionTime
	q.FirstTriggerTicker = nil
	q.FinalTriggerTicker = nil
	q.OngoingHighTicker = nil
	q.OngoingLowTicker = nil
	q.OnUnTrigger(bookTicker)
}

func (q *QuoteQuantityExceedTrigger) RecordTicker(bookTicker types.BookTicker) {
	if q.FirstTriggerTicker == nil {
		q.FirstTriggerTicker = &bookTicker
	}
	q.FinalTriggerTicker = &bookTicker
	if q.OngoingHighTicker == nil {
		q.OngoingHighTicker = &bookTicker
	}
	if q.OngoingLowTicker == nil {
		q.OngoingLowTicker = &bookTicker
	}
	if bookTicker.Buy.Float64() > q.OngoingHighTicker.Buy.Float64() {
		q.OngoingHighTicker = &bookTicker
	}
	if bookTicker.Sell.Float64() < q.OngoingLowTicker.Sell.Float64() {
		q.OngoingLowTicker = &bookTicker
	}
}

func (q *QuoteQuantityExceedTrigger) BookTickerPush(bookTicker types.BookTicker) {
	stat := q.History.GetStat()
	historyQuoteQuantityRate := stat.GetQuoteQuantityRate()
	if historyQuoteQuantityRate == 0 {
		return
	}

	nearBeforeTime := bookTicker.TransactionTime.Add(-q.NearPriceFluctuationDuration)
	nearBeforeTicker, _ := q.TickKline.GetFirstAndEnd(nearBeforeTime, bookTicker.TransactionTime)
	if nearBeforeTicker == nil {
		return
	}
	nearBeforeBuyPriceFluctuationRate := (bookTicker.Buy.Float64() - nearBeforeTicker.Buy.Float64()) / nearBeforeTicker.Buy.Float64()
	nearBeforeSellPriceFluctuationRate := (bookTicker.Sell.Float64() - nearBeforeTicker.Sell.Float64()) / nearBeforeTicker.Sell.Float64()
	farBeforeTime := bookTicker.TransactionTime.Add(-q.FarPriceFluctuationDuration)
	farBeforeTicker, _ := q.TickKline.GetFirstAndEnd(farBeforeTime, bookTicker.TransactionTime)
	if farBeforeTicker == nil {
		return
	}
	farBeforeBuyPriceFluctuationRate := (bookTicker.Buy.Float64() - farBeforeTicker.Buy.Float64()) / farBeforeTicker.Buy.Float64()
	farBeforeSellPriceFluctuationRate := (bookTicker.Sell.Float64() - farBeforeTicker.Sell.Float64()) / farBeforeTicker.Sell.Float64()

	if q.IsTrigger() {
		keepNearWindow := q.GetKeepNearWindow(bookTicker)
		keepNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionAll, q.KeepNearDuration)
		if keepNearQuoteQuantityRate == 0 {
			return
		}
		keepBuyNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionBuy, q.KeepNearDuration)
		keepSellNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionSell, q.KeepNearDuration)
		keepRatio := keepNearQuoteQuantityRate / historyQuoteQuantityRate
		q.RecordTicker(bookTicker)
		isKeep := true
		unTriggerReason := ""
		if q.Action == ActionBuy {
			buyPriceFluctuationRate := (bookTicker.Buy.Float64() - q.FirstTriggerTicker.Buy.Float64()) / q.FirstTriggerTicker.Buy.Float64()
			if -buyPriceFluctuationRate > q.StopLossRate {
				isKeep = false
				unTriggerReason = "stop_loss"
			}
			highProfit := q.OngoingHighTicker.Sell.Float64() - q.FirstTriggerTicker.Sell.Float64()
			highProfitRate := highProfit / q.FirstTriggerTicker.Sell.Float64()
			if highProfit > 0 && highProfitRate > q.ProfitThresholdRate {
				sellTakeProfitRate := (q.OngoingHighTicker.Sell.Float64() - bookTicker.Sell.Float64()) / highProfit
				if sellTakeProfitRate > q.TakeProfitRate {
					isKeep = false
					unTriggerReason = "take_profit"
					q.WinCount++
				}
			}

		}
		if q.Action == ActionSell {
			sellPriceFluctuationRate := (bookTicker.Sell.Float64() - q.FirstTriggerTicker.Sell.Float64()) / q.FirstTriggerTicker.Sell.Float64()
			if sellPriceFluctuationRate > q.StopLossRate {
				isKeep = false
				unTriggerReason = "stop_loss"
			}
			highProfit := q.FirstTriggerTicker.Buy.Float64() - q.OngoingLowTicker.Buy.Float64()
			highProfitRate := highProfit / q.FirstTriggerTicker.Buy.Float64()
			if highProfit > 0 && highProfitRate > q.ProfitThresholdRate {
				buyTakeProfitRate := (bookTicker.Buy.Float64() - q.OngoingLowTicker.Buy.Float64()) / highProfit
				if buyTakeProfitRate > q.TakeProfitRate {
					isKeep = false
					unTriggerReason = "take_profit"
					q.WinCount++
				}
			}
		}
		if isKeep {
			q.KeepTrigger(bookTicker)
		} else {
			var profitRate float64
			if q.Action == ActionBuy {
				profitRate = (bookTicker.Sell.Float64() - q.FirstTriggerTicker.Sell.Float64()) / q.FirstTriggerTicker.Sell.Float64()
			}
			if q.Action == ActionSell {
				profitRate = (q.FirstTriggerTicker.Buy.Float64() - bookTicker.Buy.Float64()) / q.FirstTriggerTicker.Buy.Float64()
			}
			q.TotalProfitRate += profitRate
			q.WinRate = float64(q.WinCount) / float64(q.TotalTriggerCount)
			if unTriggerReason == "stop_loss" && q.WinRate < 0.6 {
				q.ScaleTriggerPrice()
			}
			q.Logger.Infof("[UnTrigger][%s][%s][%s][%s][TND:%s][KND:%s][%f][%f][PF:%f][TC:%d][WR:%f][TP:%f]ratio: %f, buy: %f, sell: %f, tick: %+v",
				q.Symbol,
				q.Action,
				unTriggerReason,
				q.TriggerDuration(),
				q.TriggerNearDuration,
				q.KeepNearDuration,
				q.AdaptTriggerNearQuoteQuantityRateRatio,
				q.AdaptKeepNearQuoteQuantityRateRatio,
				profitRate,
				q.TotalTriggerCount,
				q.WinRate,
				q.TotalProfitRate,
				keepRatio,
				keepBuyNearQuoteQuantityRate,
				keepSellNearQuoteQuantityRate,
				bookTicker)
			q.UnTrigger(bookTicker)
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
		var imbalanceTriggerRate float64
		isPriceFluctuationTriggerCheckPass := false

		if triggerBuyNearQuoteQuantityRate > triggerSellNearQuoteQuantityRate {
			q.Action = ActionBuy
			imbalanceTriggerRate = triggerBuyNearQuoteQuantityRate / triggerSellNearQuoteQuantityRate
			if nearBeforeSellPriceFluctuationRate > q.NearPriceFluctuationRate && farBeforeSellPriceFluctuationRate > q.FarPriceFluctuationRate {
				isPriceFluctuationTriggerCheckPass = true
			}
		} else {
			q.Action = ActionSell
			imbalanceTriggerRate = triggerSellNearQuoteQuantityRate / triggerBuyNearQuoteQuantityRate
			if -nearBeforeBuyPriceFluctuationRate > q.NearPriceFluctuationRate && -farBeforeBuyPriceFluctuationRate > q.FarPriceFluctuationRate {
				isPriceFluctuationTriggerCheckPass = true
			}
		}
		imbalanceTriggerRateCheckPass := false
		if imbalanceTriggerRate > q.ImbalanceThresholdTriggerRate {
			imbalanceTriggerRateCheckPass = true
		}
		if triggerRatio > q.AdaptTriggerNearQuoteQuantityRateRatio &&
			isPriceFluctuationTriggerCheckPass &&
			imbalanceTriggerRateCheckPass {
			q.Trigger(bookTicker)
			q.RecordTicker(bookTicker)
			q.Logger.Infof("[Trigger][%s][%s][TND:%s][KND:%s][%f][%f][IB:%f][NFT:%f,%f][FFT:%f,%f]ratio: %f, buy: %f, sell: %f, tick: %+v",
				q.Symbol,
				q.Action,
				q.TriggerNearDuration,
				q.KeepNearDuration,
				q.AdaptTriggerNearQuoteQuantityRateRatio,
				q.AdaptKeepNearQuoteQuantityRateRatio,
				imbalanceTriggerRate,
				nearBeforeBuyPriceFluctuationRate,
				nearBeforeSellPriceFluctuationRate,
				farBeforeBuyPriceFluctuationRate,
				farBeforeSellPriceFluctuationRate,
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
		(triggerNearWindow.GetTradeCount() <= q.MinTriggerWindowTradeCount ||
			triggerNearWindow.GetTradeCount() > q.MaxTriggerWindowTradeCount) {
		searchCount++
		if triggerNearWindow.GetTradeCount() <= q.MinTriggerWindowTradeCount {
			upperTriggerNearDuration := q.TriggerNearDuration * 5 / 4
			if upperTriggerNearDuration > q.MaxTriggerNearDuration {
				break
			}
			q.TriggerNearDuration = upperTriggerNearDuration
		}
		if triggerNearWindow.GetTradeCount() > q.MaxTriggerWindowTradeCount {
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

func (q *QuoteQuantityExceedTrigger) ScaleImbalanceThresholdTriggerRate() {
	if q.ImbalanceThresholdTriggerRate > q.MaxImbalanceThresholdTriggerRate && q.ImbalanceThresholdTriggerRate < q.MinImbalanceThresholdTriggerRate {
		largerImbalanceThresholdTriggerRate := q.ImbalanceThresholdTriggerRate * 2
		if largerImbalanceThresholdTriggerRate < q.MaxImbalanceThresholdTriggerRate {
			q.ImbalanceThresholdTriggerRate = largerImbalanceThresholdTriggerRate
		} else {
			q.ImbalanceThresholdTriggerRate = q.MaxImbalanceThresholdTriggerRate
		}
	}
}

func (q *QuoteQuantityExceedTrigger) ReduceImbalanceThresholdTriggerRate() {
	if q.ImbalanceThresholdTriggerRate < q.MaxImbalanceThresholdTriggerRate && q.ImbalanceThresholdTriggerRate > q.MinImbalanceThresholdTriggerRate {
		smallerImbalanceThresholdTriggerRate := q.ImbalanceThresholdTriggerRate * 0.7
		if smallerImbalanceThresholdTriggerRate > q.MinImbalanceThresholdTriggerRate {
			q.ImbalanceThresholdTriggerRate = smallerImbalanceThresholdTriggerRate
		} else {
			q.ImbalanceThresholdTriggerRate = q.MinImbalanceThresholdTriggerRate
		}
	}
}

func (q *QuoteQuantityExceedTrigger) ReduceOngoingProfitThresholdRate() {
	if q.OngoingProfitThresholdRate < q.MaxProfitThresholdRate && q.OngoingProfitThresholdRate > q.MinProfitThresholdRate {
		smallerProfitThresholdRate := q.OngoingProfitThresholdRate * 9 / 10
		if smallerProfitThresholdRate > q.MinProfitThresholdRate {
			q.OngoingProfitThresholdRate = smallerProfitThresholdRate
		} else {
			q.OngoingProfitThresholdRate = q.MinProfitThresholdRate
		}
	}
}

func (q *QuoteQuantityExceedTrigger) ScaleTriggerPrice() {
	if q.StopLossRate > q.MaxStopLossRate && q.StopLossRate < q.MinStopLossRate {
		largerStopLossRate := q.StopLossRate * 5 / 4
		if largerStopLossRate < q.MaxStopLossRate {
			q.StopLossRate = largerStopLossRate
		} else {
			q.StopLossRate = q.MaxStopLossRate
		}
	}
	if q.NearPriceFluctuationRate > q.MaxNearPriceFluctuationRate && q.NearPriceFluctuationRate < q.MinNearPriceFluctuationRate {
		largerNearPriceFluctuationRate := q.NearPriceFluctuationRate * 5 / 4
		if largerNearPriceFluctuationRate < q.MaxNearPriceFluctuationRate {
			q.NearPriceFluctuationRate = largerNearPriceFluctuationRate
		} else {
			q.NearPriceFluctuationRate = q.MaxNearPriceFluctuationRate
		}
	}
	if q.FarPriceFluctuationRate > q.MaxFarPriceFluctuationRate && q.FarPriceFluctuationRate < q.MinFarPriceFluctuationRate {
		largerFarPriceFluctuationRate := q.FarPriceFluctuationRate * 5 / 4
		if largerFarPriceFluctuationRate < q.MaxFarPriceFluctuationRate {
			q.FarPriceFluctuationRate = largerFarPriceFluctuationRate
		} else {
			q.FarPriceFluctuationRate = q.MaxFarPriceFluctuationRate
		}
	}
	if q.ProfitThresholdRate > q.MaxProfitThresholdRate && q.ProfitThresholdRate < q.MinProfitThresholdRate {
		largerProfitThresholdRate := q.ProfitThresholdRate * 5 / 4
		if largerProfitThresholdRate < q.MaxProfitThresholdRate {
			q.ProfitThresholdRate = largerProfitThresholdRate
		} else {
			q.ProfitThresholdRate = q.MaxProfitThresholdRate
		}
	}
}

func (q *QuoteQuantityExceedTrigger) ReduceTriggerPrice() {
	if q.StopLossRate < q.MaxStopLossRate && q.StopLossRate > q.MinStopLossRate {
		smallerStopLossRate := q.StopLossRate * 9 / 10
		if smallerStopLossRate > q.MinStopLossRate {
			q.StopLossRate = smallerStopLossRate
		} else {
			q.StopLossRate = q.MinStopLossRate
		}
	}
	if q.NearPriceFluctuationRate < q.MaxNearPriceFluctuationRate && q.NearPriceFluctuationRate > q.MinNearPriceFluctuationRate {
		smallerNearPriceFluctuationRate := q.NearPriceFluctuationRate * 9 / 10
		if smallerNearPriceFluctuationRate > q.MinNearPriceFluctuationRate {
			q.NearPriceFluctuationRate = smallerNearPriceFluctuationRate
		} else {
			q.NearPriceFluctuationRate = q.MinNearPriceFluctuationRate
		}
	}
	if q.FarPriceFluctuationRate < q.MaxFarPriceFluctuationRate && q.FarPriceFluctuationRate > q.MinFarPriceFluctuationRate {
		smallerFarPriceFluctuationRate := q.FarPriceFluctuationRate * 9 / 10
		if smallerFarPriceFluctuationRate > q.MinFarPriceFluctuationRate {
			q.FarPriceFluctuationRate = smallerFarPriceFluctuationRate
		} else {
			q.FarPriceFluctuationRate = q.MinFarPriceFluctuationRate
		}
	}
	if q.ProfitThresholdRate < q.MaxProfitThresholdRate && q.ProfitThresholdRate > q.MinProfitThresholdRate {
		smallerProfitThresholdRate := q.ProfitThresholdRate * 9 / 10
		if smallerProfitThresholdRate > q.MinProfitThresholdRate {
			q.ProfitThresholdRate = smallerProfitThresholdRate
		} else {
			q.ProfitThresholdRate = q.MinProfitThresholdRate
		}
	}
}

func (q *QuoteQuantityExceedTrigger) GetKeepNearWindow(bookTicker types.BookTicker) *aggtrade.WindowBase {
	keepNearWindow := q.AggKline.GetWindow(bookTicker.TransactionTime.Add(-q.KeepNearDuration), bookTicker.TransactionTime)
	searchCount := 0
	for searchCount < maxSearchCount &&
		(keepNearWindow.GetTradeCount() <= q.MinKeepWindowTradeCount ||
			keepNearWindow.GetTradeCount() > q.MaxKeepWindowTradeCount) {
		searchCount++
		if keepNearWindow.GetTradeCount() <= q.MinKeepWindowTradeCount {
			upperKeepNearDuration := q.KeepNearDuration * 5 / 4
			if upperKeepNearDuration > q.MaxKeepNearDuration {
				break
			}
			q.KeepNearDuration = upperKeepNearDuration
		}
		if keepNearWindow.GetTradeCount() > q.MaxKeepWindowTradeCount {
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
