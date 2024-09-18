package trigger

import (
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/history"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/tick"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/sirupsen/logrus"
	"sort"
	"time"
)

type BookTickerTriggerFn func(bookTicker *types.BookTicker, profit float64)

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
	OngoingImbalanceThresholdRate          float64

	//Capture   *imbalance.Capture
	History   *history.MarketHistory
	TickKline *tick.Kline
	AggKline  *aggtrade.Kline
	Fee       types.ExchangeFee
	Session   *bbgo.ExchangeSession

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
	MinImbalanceThresholdKeepRate    float64
	MaxImbalanceThresholdKeepRate    float64

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

	IsGlobalTriggered bool

	Top5ImbalanceRate []ImbalanceRate
}

type ImbalanceRate struct {
	TickRate   float64
	BookRate   float64
	RecordTime time.Time
}

func (q *QuoteQuantityExceedTrigger) GetTotalValue() float64 {
	if q.TotalTriggerCount < 10 {
		return 0
	}
	pureProfit := q.TotalProfitRate - float64(q.TotalTriggerCount)*q.Fee.TakerFeeRate.Float64()
	if pureProfit < 0 {
		return 0
	}
	return pureProfit
}

func (q *QuoteQuantityExceedTrigger) IsTrigger() bool {
	return q.FirstTriggerTime != nil
}

func (q *QuoteQuantityExceedTrigger) Trigger(bookTicker *types.BookTicker) {
	q.OngoingProfitThresholdRate = q.ProfitThresholdRate
	q.OngoingStopLossRate = q.StopLossRate
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
	q.OnTrigger(bookTicker, 0)
}

func (q *QuoteQuantityExceedTrigger) TriggerDuration() time.Duration {
	if q.FirstTriggerTime == nil {
		return 0
	}
	return q.FinalTriggerTime.Sub(*q.FirstTriggerTime)
}

func (q *QuoteQuantityExceedTrigger) KeepTrigger(bookTicker *types.BookTicker) {
	q.FinalTriggerTime = &bookTicker.TransactionTime
	q.OnKeepTrigger(bookTicker, 0)
}

func (q *QuoteQuantityExceedTrigger) UnTrigger(bookTicker *types.BookTicker, profit float64) {
	if bookTicker.TransactionTime.Sub(*q.FirstTriggerTime) > q.MaxKeepDuration {
		q.AdaptKeepNearQuoteQuantityRateRatio = q.AdaptKeepNearQuoteQuantityRateRatio * 1.2
	}
	if bookTicker.TransactionTime.Sub(*q.FirstTriggerTime) < q.MinKeepDuration {
		q.AdaptKeepNearQuoteQuantityRateRatio = q.AdaptKeepNearQuoteQuantityRateRatio * 0.7
	}
	q.FirstTriggerTime = nil
	q.FinalTriggerTime = nil
	q.LastTriggerTime = &bookTicker.TransactionTime
	q.FirstTriggerTicker = nil
	q.FinalTriggerTicker = nil
	q.OngoingHighTicker = nil
	q.OngoingLowTicker = nil
	q.OnUnTrigger(bookTicker, profit)
}

func (q *QuoteQuantityExceedTrigger) RecordTicker(bookTicker *types.BookTicker) {
	if q.FirstTriggerTicker == nil {
		q.FirstTriggerTicker = bookTicker
	}
	q.FinalTriggerTicker = bookTicker
	if q.OngoingHighTicker == nil {
		q.OngoingHighTicker = bookTicker
	}
	if q.OngoingLowTicker == nil {
		q.OngoingLowTicker = bookTicker
	}
	if bookTicker.Buy.Float64() > q.OngoingHighTicker.Buy.Float64() {
		q.OngoingHighTicker = bookTicker
	}
	if bookTicker.Sell.Float64() < q.OngoingLowTicker.Sell.Float64() {
		q.OngoingLowTicker = bookTicker
	}
}

func (q *QuoteQuantityExceedTrigger) BookTickerPush(bookTicker *types.BookTicker) {
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
		keepDirection := ""
		var imbalanceTriggerRate float64
		if keepBuyNearQuoteQuantityRate > keepSellNearQuoteQuantityRate {
			keepDirection = "buy"
			imbalanceTriggerRate = keepBuyNearQuoteQuantityRate / keepSellNearQuoteQuantityRate
		} else {
			keepDirection = "sell"
			imbalanceTriggerRate = keepSellNearQuoteQuantityRate / keepBuyNearQuoteQuantityRate
		}
		imbalanceKeepRateCheckPass := false
		newOngoingImbalanceThresholdRate := (q.ImbalanceThresholdTriggerRate-1)/2 + 1
		if newOngoingImbalanceThresholdRate > q.MinImbalanceThresholdKeepRate && newOngoingImbalanceThresholdRate < q.MaxImbalanceThresholdKeepRate {
			q.OngoingImbalanceThresholdRate = newOngoingImbalanceThresholdRate
		} else if newOngoingImbalanceThresholdRate <= q.MinImbalanceThresholdKeepRate {
			q.OngoingImbalanceThresholdRate = q.MinImbalanceThresholdKeepRate
		} else if newOngoingImbalanceThresholdRate >= q.MaxImbalanceThresholdKeepRate {
			q.OngoingImbalanceThresholdRate = q.MaxImbalanceThresholdKeepRate
		}
		if keepDirection == q.Action && imbalanceTriggerRate > q.OngoingImbalanceThresholdRate {
			imbalanceKeepRateCheckPass = true
		}
		imbalanceKeepRateCheckReversePass := false
		if keepDirection != q.Action && imbalanceTriggerRate > q.ImbalanceThresholdTriggerRate*2 {
			imbalanceKeepRateCheckReversePass = true
		}
		q.RecordTicker(bookTicker)
		isKeep := true
		unTriggerReason := ""
		highProfit := 0.0
		highProfitRate := 0.0
		if q.Action == ActionBuy {
			buyPriceFluctuationRate := (bookTicker.Buy.Float64() - q.FirstTriggerTicker.Buy.Float64()) / q.FirstTriggerTicker.Buy.Float64()
			if imbalanceKeepRateCheckReversePass {
				if -buyPriceFluctuationRate > q.OngoingStopLossRate*0.75 {
					isKeep = false
					unTriggerReason = "pre_stop_loss"
				}
			}
			if !imbalanceKeepRateCheckPass {
				if -buyPriceFluctuationRate > q.OngoingStopLossRate {
					isKeep = false
					unTriggerReason = "stop_loss"
				}
			} else {
				if -buyPriceFluctuationRate > q.OngoingStopLossRate*1.33 {
					isKeep = false
					unTriggerReason = "later_stop_loss"
				}
			}
			highProfit = q.OngoingHighTicker.Sell.Float64() - q.FirstTriggerTicker.Sell.Float64()
			highProfitRate = highProfit / q.FirstTriggerTicker.Sell.Float64()
			if highProfit > 0 && highProfitRate > q.OngoingProfitThresholdRate {
				sellTakeProfitRate := (q.OngoingHighTicker.Sell.Float64() - bookTicker.Sell.Float64()) / highProfit
				newKeepAdaptQuoteQuantityRateRatio := (q.AdaptKeepNearQuoteQuantityRateRatio-1)/3 + 1
				if keepRatio < newKeepAdaptQuoteQuantityRateRatio &&
					sellTakeProfitRate > q.TakeProfitRate*0.3 {
					isKeep = false
					unTriggerReason = "direct_take_profit"
					q.WinCount++
				} else {
					if !imbalanceKeepRateCheckPass {
						if sellTakeProfitRate > q.TakeProfitRate*0.75 {
							isKeep = false
							unTriggerReason = "pre_take_profit"
							q.WinCount++
						}
					} else {
						if sellTakeProfitRate > q.TakeProfitRate {
							isKeep = false
							unTriggerReason = "take_profit"
							q.WinCount++
						}
					}
				}
			}

		}
		if q.Action == ActionSell {
			sellPriceFluctuationRate := (bookTicker.Sell.Float64() - q.FirstTriggerTicker.Sell.Float64()) / q.FirstTriggerTicker.Sell.Float64()
			if imbalanceKeepRateCheckReversePass {
				if sellPriceFluctuationRate > q.OngoingStopLossRate*0.75 {
					isKeep = false
					unTriggerReason = "pre_stop_loss"
				}
			}
			if !imbalanceKeepRateCheckPass {
				if sellPriceFluctuationRate > q.OngoingStopLossRate {
					isKeep = false
					unTriggerReason = "stop_loss"
				}
			} else {
				if sellPriceFluctuationRate > q.OngoingStopLossRate*1.33 {
					isKeep = false
					unTriggerReason = "later_stop_loss"
				}
			}
			highProfit = q.FirstTriggerTicker.Buy.Float64() - q.OngoingLowTicker.Buy.Float64()
			highProfitRate = highProfit / q.FirstTriggerTicker.Buy.Float64()
			if highProfit > 0 && highProfitRate > q.OngoingProfitThresholdRate {
				buyTakeProfitRate := (bookTicker.Buy.Float64() - q.OngoingLowTicker.Buy.Float64()) / highProfit
				newKeepAdaptQuoteQuantityRateRatio := (q.AdaptKeepNearQuoteQuantityRateRatio-1)/3 + 1
				if keepRatio < newKeepAdaptQuoteQuantityRateRatio &&
					buyTakeProfitRate > q.TakeProfitRate*0.3 {
					isKeep = false
					unTriggerReason = "direct_take_profit"
					q.WinCount++
				} else {
					if !imbalanceKeepRateCheckPass {
						if buyTakeProfitRate > q.TakeProfitRate*0.75 {
							isKeep = false
							unTriggerReason = "pre_take_profit"
							q.WinCount++
						}
					} else {
						if buyTakeProfitRate > q.TakeProfitRate {
							isKeep = false
							unTriggerReason = "take_profit"
							q.WinCount++
						}
					}
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
			if (unTriggerReason == "stop_loss" || unTriggerReason == "later_stop_loss" || unTriggerReason == "pre_stop_loss") &&
				q.WinRate < 0.65 {
				if unTriggerReason == "pre_stop_loss" {
					q.ImbalanceThresholdTriggerRate = q.ImbalanceThresholdTriggerRate * 1.2
				} else if unTriggerReason == "stop_loss" {
					q.AdaptTriggerNearQuoteQuantityRateRatio = q.AdaptTriggerNearQuoteQuantityRateRatio * 1.2
				} else {
					if highProfitRate <= q.MinProfitThresholdRate {
						q.ScaleTriggerPrice()
					}
				}
			}
			q.Logger.Infof("[UnTrigger][%s][%s][%s][%s][TND:%s][KND:%s][%f][%f][PF:%f][TC:%d][WR:%f][TP:%f][H:%+v][L:%+v]ratio: %f, buy: %f, sell: %f, tick: %+v",
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
				q.OngoingHighTicker,
				q.OngoingLowTicker,
				keepRatio,
				keepBuyNearQuoteQuantityRate,
				keepSellNearQuoteQuantityRate,
				bookTicker)
			q.UnTrigger(bookTicker, profitRate)
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
			if nearBeforeSellPriceFluctuationRate > q.NearPriceFluctuationRate &&
				farBeforeSellPriceFluctuationRate > q.FarPriceFluctuationRate &&
				farBeforeSellPriceFluctuationRate > nearBeforeSellPriceFluctuationRate {
				isPriceFluctuationTriggerCheckPass = true
			}
		} else {
			q.Action = ActionSell
			imbalanceTriggerRate = triggerSellNearQuoteQuantityRate / triggerBuyNearQuoteQuantityRate
			if -nearBeforeBuyPriceFluctuationRate > q.NearPriceFluctuationRate &&
				-farBeforeBuyPriceFluctuationRate > q.FarPriceFluctuationRate &&
				-farBeforeBuyPriceFluctuationRate > -nearBeforeBuyPriceFluctuationRate {
				isPriceFluctuationTriggerCheckPass = true
			}
		}
		imbalanceTriggerRateCheckPass := false
		if imbalanceTriggerRate > q.ImbalanceThresholdTriggerRate {
			imbalanceTriggerRateCheckPass = true
		}
		var tickerImbalanceTriggerRate float64
		tickerImbalanceDirection := ""
		if bookTicker.BuySize > bookTicker.SellSize {
			tickerImbalanceDirection = "sell"
			tickerImbalanceTriggerRate = bookTicker.BuySize.Float64() / bookTicker.SellSize.Float64()
		} else {
			tickerImbalanceDirection = "buy"
			tickerImbalanceTriggerRate = bookTicker.SellSize.Float64() / bookTicker.BuySize.Float64()
		}
		tickerCheckPass := false
		tickImbalanceThresholdTriggerRate := q.ImbalanceThresholdTriggerRate / 2
		if tickImbalanceThresholdTriggerRate > 5 {
			tickImbalanceThresholdTriggerRate = 5
		}
		if tickerImbalanceDirection == q.Action && tickerImbalanceTriggerRate > tickImbalanceThresholdTriggerRate {
			tickerCheckPass = true
		}
		if triggerRatio > q.AdaptTriggerNearQuoteQuantityRateRatio &&
			isPriceFluctuationTriggerCheckPass &&
			imbalanceTriggerRateCheckPass &&
			tickerCheckPass {
			q.Trigger(bookTicker)
			q.RecordTicker(bookTicker)
			q.Logger.Infof("[Trigger][%s][%s][TND:%s][KND:%s][%f][%f][IB:%f][NFT:%f,%f][FFT:%f,%f]ratio: %f, buy: %f, sell: %f, tick: %+v",
				q.Symbol,
				q.Action,
				//isReverse,
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

func (q *QuoteQuantityExceedTrigger) BookTickerPushV2(bookTicker *types.BookTicker) {
	//##通用数据准备
	//1.24h币种成交量速率
	//2.当前敞口
	//3.24h平均价格历史均价
	//对于时间窗口：当前时间前的近期窗口和远期窗口
	//时间窗口内数据：
	//1.买价波动率
	//2.卖价波动率
	//3.单位时间成交量推动价格波动率（买卖分别计算）
	//4.平均敞口大小及其方向
	//5.平均买卖量计算出的不平衡率及其方向
	//
	//关于针的信息：
	//
	if q.Top5ImbalanceRate == nil {
		q.Top5ImbalanceRate = make([]ImbalanceRate, 0)
	}
	//sort
	sort.Slice(q.Top5ImbalanceRate, func(i, j int) bool {
		return q.Top5ImbalanceRate[i].BookRate < q.Top5ImbalanceRate[j].BookRate
	})
	//只保留1小时内的
	updatedTop5ImbalanceRate := make([]ImbalanceRate, 0)
	for _, r := range q.Top5ImbalanceRate {
		if r.RecordTime.After(time.Now().Add(-1 * time.Hour)) {
			updatedTop5ImbalanceRate = append(updatedTop5ImbalanceRate, r)
		}
	}
	q.Top5ImbalanceRate = updatedTop5ImbalanceRate

	stat := q.History.GetStat()
	//历史币种成交量速率
	historyQuoteQuantityRate := stat.GetQuoteQuantityRate()
	if historyQuoteQuantityRate == 0 {
		return
	}

	//获取当前时间前的近期窗口
	nearBeforeTime := bookTicker.TransactionTime.Add(-q.NearPriceFluctuationDuration)
	//获取当前时间前的近期窗口的第一个和最后一个
	nearBeforeTicker, _ := q.TickKline.GetFirstAndEnd(nearBeforeTime, bookTicker.TransactionTime)
	if nearBeforeTicker == nil {
		return
	}
	//计算当前时间前的近期窗口的买价波动率
	//nearBeforeBuyPriceFluctuationRate := (bookTicker.Buy.Float64() - nearBeforeTicker.Buy.Float64()) / nearBeforeTicker.Buy.Float64()
	//计算当前时间前的近期窗口的卖价波动率
	nearBeforeSellPriceFluctuationRate := (bookTicker.Sell.Float64() - nearBeforeTicker.Sell.Float64()) / nearBeforeTicker.Sell.Float64()

	//近期窗口Agg trade
	nearBeforeAggTradeWindow := q.AggKline.GetWindow(nearBeforeTime, bookTicker.TransactionTime)
	//近期窗口Sell agg trade count 占比
	nearBeforeSellAggTradeCountRate := float64(nearBeforeAggTradeWindow.GetSellTradeCount()) / float64(nearBeforeAggTradeWindow.GetTradeCount())
	nearBeforeTickWindow := q.TickKline.GetWindow(nearBeforeTime, bookTicker.TransactionTime)
	nearBeforeAvgSpread := nearBeforeTickWindow.AvgSpread().Float64()

	//获取当前时间前的远期窗口
	farBeforeTime := bookTicker.TransactionTime.Add(-q.FarPriceFluctuationDuration)
	//获取当前时间前的远期窗口的第一个和最后一个
	farBeforeTicker, _ := q.TickKline.GetFirstAndEnd(farBeforeTime, bookTicker.TransactionTime)
	if farBeforeTicker == nil {
		return
	}
	//计算当前时间前的远期窗口的买价波动率
	//farBeforeBuyPriceFluctuationRate := (bookTicker.Buy.Float64() - farBeforeTicker.Buy.Float64()) / farBeforeTicker.Buy.Float64()
	//计算当前时间前的远期窗口的卖价波动率
	farBeforeSellPriceFluctuationRate := (bookTicker.Sell.Float64() - farBeforeTicker.Sell.Float64()) / farBeforeTicker.Sell.Float64()

	//远期窗口Agg trade
	farBeforeAggTradeWindow := q.AggKline.GetWindow(farBeforeTime, bookTicker.TransactionTime)
	//远期窗口Sell agg trade count 占比
	farBeforeSellAggTradeCountRate := float64(farBeforeAggTradeWindow.GetSellTradeCount()) / float64(farBeforeAggTradeWindow.GetTradeCount())
	farBeforeTickWindow := q.TickKline.GetWindow(farBeforeTime, bookTicker.TransactionTime)
	farBeforeAvgSpread := farBeforeTickWindow.AvgSpread().Float64()

	keepNearWindow := q.GetKeepNearWindow(bookTicker)
	keepNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionAll, q.KeepNearDuration)
	if keepNearQuoteQuantityRate == 0 {
		return
	}
	keepBuyNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionBuy, q.KeepNearDuration)
	keepSellNearQuoteQuantityRate := keepNearWindow.GetQuoteQuantityRate(aggtrade.TradeDirectionSell, q.KeepNearDuration)
	keepRatio := keepNearQuoteQuantityRate / historyQuoteQuantityRate
	keepDirection := ""
	var imbalanceTriggerRate float64
	if keepBuyNearQuoteQuantityRate > keepSellNearQuoteQuantityRate {
		keepDirection = "buy"
		imbalanceTriggerRate = keepBuyNearQuoteQuantityRate / keepSellNearQuoteQuantityRate
	} else {
		keepDirection = "sell"
		imbalanceTriggerRate = keepSellNearQuoteQuantityRate / keepBuyNearQuoteQuantityRate
	}

	keepNearDurationTickWindow := q.TickKline.GetWindow(bookTicker.TransactionTime.Add(-q.KeepNearDuration), bookTicker.TransactionTime)
	keepNearDurationAvgSpread := keepNearDurationTickWindow.AvgSpread().Float64()

	book, ok := q.Session.OrderBook(q.Symbol)
	if !ok {
		return
	}
	//计算+- 2%内的book总价，去除best bid和best ask
	bestAsk, _ := book.BestAsk()
	bookPriceMax := bestAsk.Price.Float64() * 1.01
	bestBid, _ := book.BestBid()
	bookPriceMin := bestBid.Price.Float64() * 0.99
	bookVolumeSell := 0.0
	bookVolumeBuy := 0.0
	bookSell := book.SideBook(types.SideTypeSell)
	bookBuy := book.SideBook(types.SideTypeBuy)
	book.Lock()
	for i, b := range bookSell {
		if i == 0 {
			continue
		}
		if b.Price.Float64() > bookPriceMax || b.Price.Float64() < bookPriceMin {
			break
		}
		bookVolumeSell += b.Volume.Float64()
	}
	for i, b := range bookBuy {
		if i == 0 {
			continue
		}
		if b.Price.Float64() > bookPriceMax || b.Price.Float64() < bookPriceMin {
			break
		}
		bookVolumeBuy += b.Volume.Float64()
	}
	book.Unlock()
	bookImbalanceDirection := ""
	bookImbalanceRate := 0.0
	if bookVolumeBuy > bookVolumeSell {
		bookImbalanceDirection = "buy"
		bookImbalanceRate = bookVolumeBuy / bookVolumeSell
	} else {
		bookImbalanceDirection = "sell"
		bookImbalanceRate = bookVolumeSell / bookVolumeBuy
	}
	untrigger := false
	//if bookImbalanceDirection != keepDirection {
	//	untrigger = true
	//}

	imbalanceKeepRateCheckPass := false
	imbalanceRate := ImbalanceRate{
		TickRate:   imbalanceTriggerRate,
		BookRate:   bookImbalanceRate,
		RecordTime: bookTicker.TransactionTime,
	}
	if len(q.Top5ImbalanceRate) > 1 {
		if bookImbalanceRate >= q.Top5ImbalanceRate[0].BookRate && imbalanceTriggerRate >= q.Top5ImbalanceRate[0].TickRate {
			imbalanceKeepRateCheckPass = true
			if len(q.Top5ImbalanceRate) >= 5 {
				q.Top5ImbalanceRate[0] = imbalanceRate
			} else {
				q.Top5ImbalanceRate = append(q.Top5ImbalanceRate, imbalanceRate)
			}
		}
	} else {
		if bookImbalanceRate > 1.05 && imbalanceTriggerRate >= q.ImbalanceThresholdTriggerRate {
			imbalanceKeepRateCheckPass = true
			q.Top5ImbalanceRate = append(q.Top5ImbalanceRate, imbalanceRate)
		}
	}
	if !imbalanceKeepRateCheckPass {
		untrigger = true
	}

	if imbalanceTriggerRate < q.ImbalanceThresholdTriggerRate {
		untrigger = true
	}

	//触发状态
	if q.IsTrigger() {
		if untrigger {
			q.UnTrigger(bookTicker, 0)
			q.Logger.Infof("[Record-OFF][%s][D:%s][FT:%s][NT:%s][KT:%s][FTF:%f][NTF:%f][KTIB:%f][KR:%f][FTSCR:%f][NTSCR:%f][KTAS:%f][FTAS:%f][NTAS:%f][BIB:%f][BID:%s][BVS:%f][BVB:%f]",
				q.Symbol,
				keepDirection,
				q.FarPriceFluctuationDuration,
				q.NearPriceFluctuationDuration,
				q.KeepNearDuration,
				farBeforeSellPriceFluctuationRate,
				nearBeforeSellPriceFluctuationRate,
				imbalanceTriggerRate,
				keepRatio,
				farBeforeSellAggTradeCountRate,
				nearBeforeSellAggTradeCountRate,
				keepNearDurationAvgSpread,
				farBeforeAvgSpread,
				nearBeforeAvgSpread,
				bookImbalanceRate,
				bookImbalanceDirection,
				bookVolumeSell,
				bookVolumeBuy)
			return
		}
		return
	}
	//非触发状态
	if !untrigger && imbalanceKeepRateCheckPass {
		//print all info
		//D : Direction
		//FT: far time, NT: near time, KT: keep time
		//FTF: far time fluctuation, NTF: near time fluctuation
		//KTIB: imbalance
		//KR: keep ratio
		//FTSCR: far time sell count rate
		//NTSCR: near time sell count rate
		//KTAS: keep time avg spread
		//FTAS: far time avg spread
		//NTAS: near time avg spread
		//BIB: book imbalance
		//BVS: book volume sell
		//BVB: book volume buy
		//print: D, FT, NT, KT, FTF, NTF, KTIB, FTSCR, NTSCR, KTAS, FTAS, NTAS, BIB, BVS, BVB
		q.Logger.Infof("[Record-ON][%s][D:%s][FT:%s][NT:%s][KT:%s][FTF:%f][NTF:%f][KTIB:%f][KR:%f][FTSCR:%f][NTSCR:%f][KTAS:%f][FTAS:%f][NTAS:%f][BIB:%f][BID:%s][BVS:%f][BVB:%f]",
			q.Symbol,
			keepDirection,
			q.FarPriceFluctuationDuration,
			q.NearPriceFluctuationDuration,
			q.KeepNearDuration,
			farBeforeSellPriceFluctuationRate,
			nearBeforeSellPriceFluctuationRate,
			imbalanceTriggerRate,
			keepRatio,
			farBeforeSellAggTradeCountRate,
			nearBeforeSellAggTradeCountRate,
			keepNearDurationAvgSpread,
			farBeforeAvgSpread,
			nearBeforeAvgSpread,
			bookImbalanceRate,
			bookImbalanceDirection,
			bookVolumeSell,
			bookVolumeBuy)
		q.Trigger(bookTicker)

	}
	return
}

var maxSearchCount = 8

func (q *QuoteQuantityExceedTrigger) GetTriggerNearWindow(bookTicker *types.BookTicker) *aggtrade.WindowBase {
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
		largerImbalanceThresholdTriggerRate := q.ImbalanceThresholdTriggerRate * 1.2
		if largerImbalanceThresholdTriggerRate < q.MaxImbalanceThresholdTriggerRate {
			q.ImbalanceThresholdTriggerRate = largerImbalanceThresholdTriggerRate
		} else {
			q.ImbalanceThresholdTriggerRate = q.MaxImbalanceThresholdTriggerRate
		}
	}
}

func (q *QuoteQuantityExceedTrigger) ReduceImbalanceThresholdTriggerRate() {
	if q.ImbalanceThresholdTriggerRate < q.MaxImbalanceThresholdTriggerRate && q.ImbalanceThresholdTriggerRate > q.MinImbalanceThresholdTriggerRate {
		smallerImbalanceThresholdTriggerRate := q.ImbalanceThresholdTriggerRate * 0.5
		if smallerImbalanceThresholdTriggerRate > q.MinImbalanceThresholdTriggerRate {
			q.ImbalanceThresholdTriggerRate = smallerImbalanceThresholdTriggerRate
		} else {
			q.ImbalanceThresholdTriggerRate = q.MinImbalanceThresholdTriggerRate
		}
	}
}

func (q *QuoteQuantityExceedTrigger) ReduceOngoingProfitThresholdRate() {
	//if q.OngoingProfitThresholdRate < q.MaxProfitThresholdRate && q.OngoingProfitThresholdRate > q.MinProfitThresholdRate {
	//	smallerProfitThresholdRate := q.OngoingProfitThresholdRate * 9 / 10
	//	if smallerProfitThresholdRate > q.MinProfitThresholdRate {
	//		q.OngoingProfitThresholdRate = smallerProfitThresholdRate
	//	} else {
	//		q.OngoingProfitThresholdRate = q.MinProfitThresholdRate
	//	}
	//}
	q.OngoingProfitThresholdRate = q.OngoingProfitThresholdRate * 9 / 10
}

func (q *QuoteQuantityExceedTrigger) ScaleOngoingProfitThresholdRate() {
	//if q.OngoingProfitThresholdRate > q.MaxProfitThresholdRate && q.OngoingProfitThresholdRate < q.MinProfitThresholdRate {
	//	largerProfitThresholdRate := q.OngoingProfitThresholdRate * 5 / 4
	//	if largerProfitThresholdRate < q.MaxProfitThresholdRate {
	//		q.OngoingProfitThresholdRate = largerProfitThresholdRate
	//	} else {
	//		q.OngoingProfitThresholdRate = q.MaxProfitThresholdRate
	//	}
	//}
	q.OngoingProfitThresholdRate = q.OngoingProfitThresholdRate * 5 / 4
}

func (q *QuoteQuantityExceedTrigger) ReduceOngoingStopLossRate() {
	//if q.OngoingStopLossRate < q.MaxStopLossRate && q.OngoingStopLossRate > q.MinStopLossRate {
	//	smallerStopLossRate := q.OngoingStopLossRate * 9 / 10
	//	if smallerStopLossRate > q.MinStopLossRate {
	//		q.OngoingStopLossRate = smallerStopLossRate
	//	} else {
	//		q.OngoingStopLossRate = q.MinStopLossRate
	//	}
	//}
	q.OngoingStopLossRate = q.OngoingStopLossRate * 9 / 10
}

func (q *QuoteQuantityExceedTrigger) ScaleOngoingStopLossRate() {
	//if q.OngoingStopLossRate > q.MaxStopLossRate && q.OngoingStopLossRate < q.MinStopLossRate {
	//	largerStopLossRate := q.OngoingStopLossRate * 5 / 4
	//	if largerStopLossRate < q.MaxStopLossRate {
	//		q.OngoingStopLossRate = largerStopLossRate
	//	} else {
	//		q.OngoingStopLossRate = q.MaxStopLossRate
	//	}
	//}
	q.OngoingStopLossRate = q.OngoingStopLossRate * 5 / 4
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

func (q *QuoteQuantityExceedTrigger) GetKeepNearWindow(bookTicker *types.BookTicker) *aggtrade.WindowBase {
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
