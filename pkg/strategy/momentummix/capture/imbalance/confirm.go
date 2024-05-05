package imbalance

import (
	"github.com/c9s/bbgo/pkg/strategy/momentummix/window"
	"github.com/c9s/bbgo/pkg/types"
	"time"
)

func (c *Capture) IsQuantityRateExcessive(deepTimeWindow, nearTimeWindow time.Duration, direct string, excessRatio float64) bool {
	return c.GetNearDeepQuantityRateRatio(deepTimeWindow, nearTimeWindow, direct) > excessRatio
}

const (
	DirectAll  = "all"
	DirectSell = "sell"
	DirectBuy  = "buy"
)

type NearDeepQuantityRateRatioWindow struct {
	All  *window.Window
	Sell *window.Window
	Buy  *window.Window
}

func NewNearDeepQuantityRateRatioWindow(size int) *NearDeepQuantityRateRatioWindow {
	return &NearDeepQuantityRateRatioWindow{
		All:  window.NewWindow(size),
		Sell: window.NewWindow(size),
		Buy:  window.NewWindow(size),
	}
}

type NearDeepQuantityRateRatioWindowItem struct {
	Value float64
	Time  time.Time
}

func (n *NearDeepQuantityRateRatioWindowItem) GetFloat64() float64 {
	return n.Value
}

func (n *NearDeepQuantityRateRatioWindowItem) GetTime() time.Time {
	return n.Time
}

func (c *Capture) PushNearDeepQuantityRateRatioWindowItem(deepTimeWindow, nearTimeWindow time.Duration) {
	c.NearDeepQuantityRateRatioWindow.All.Push(&NearDeepQuantityRateRatioWindowItem{
		Value: c.GetNearDeepQuantityRateRatio(deepTimeWindow, nearTimeWindow, DirectAll),
		Time:  time.Now(),
	})
	c.NearDeepQuantityRateRatioWindow.Sell.Push(&NearDeepQuantityRateRatioWindowItem{
		Value: c.GetNearDeepQuantityRateRatio(deepTimeWindow, nearTimeWindow, DirectSell),
		Time:  time.Now(),
	})
	c.NearDeepQuantityRateRatioWindow.Buy.Push(&NearDeepQuantityRateRatioWindowItem{
		Value: c.GetNearDeepQuantityRateRatio(deepTimeWindow, nearTimeWindow, DirectBuy),
		Time:  time.Now(),
	})
}

func (c *Capture) GetNearDeepQuantityRateRatio(deepTimeWindow, nearTimeWindow time.Duration, direct string) float64 {
	now := time.Now()
	w := c.aggKline.GetWindow(now.Add(-deepTimeWindow), now)
	if w == nil || w.IsEmpty() {
		//log.Errorf("[capture] GetNearDeepQuantityRateRatio: window is empty")
		return 1
	}
	var q float64
	if direct == "all" {
		q = w.Quantity.Float64()
	}
	if direct == "sell" {
		q = w.SellQuantity.Float64()
	}
	if direct == "buy" {
		q = w.BuyQuantity.Float64()
	}
	fixedDeepTimeSecond := w.EndTime.Sub(w.StartTime).Seconds()
	avgQuantityRate := q / fixedDeepTimeSecond

	nearW := c.aggKline.GetWindow(now.Add(-nearTimeWindow), now)
	if nearW == nil || nearW.IsEmpty() {
		//log.Errorf("[capture] GetNearDeepQuantityRateRatio: near window is empty")
		return 1
	}
	var nearQ float64
	if direct == "all" {
		nearQ = nearW.Quantity.Float64()
	}
	if direct == "sell" {
		nearQ = nearW.SellQuantity.Float64()
	}
	if direct == "buy" {
		nearQ = nearW.BuyQuantity.Float64()
	}
	fixedNearTimeSecond := nearW.EndTime.Sub(nearW.StartTime).Seconds()
	nearAvgQuantityRate := nearQ / fixedNearTimeSecond

	return nearAvgQuantityRate / avgQuantityRate
}

type NearQuantityRateWindow struct {
	All          *window.Window
	Sell         *window.Window
	Buy          *window.Window
	NearDuration time.Duration
}

type NearQuantityRateWindowItem struct {
	Value float64
	Time  time.Time
}

func (n *NearQuantityRateWindowItem) GetFloat64() float64 {
	return n.Value
}

func (n *NearQuantityRateWindowItem) GetTime() time.Time {
	return n.Time
}

func NewNearQuantityRateWindow(size int, nearDuration time.Duration) *NearQuantityRateWindow {
	return &NearQuantityRateWindow{
		All:          window.NewWindow(size),
		Sell:         window.NewWindow(size),
		Buy:          window.NewWindow(size),
		NearDuration: nearDuration,
	}
}

//func (c *Capture) PushNearQuantityRateWindowItem() {
//	c.NearQuantityRateWindow.All.Push(&NearQuantityRateWindowItem{
//		Value: c.GetNearQuantityRate(DirectAll, c.NearQuantityRateWindow.NearDuration),
//		Time:  time.Now(),
//	})
//	c.NearQuantityRateWindow.Sell.Push(&NearQuantityRateWindowItem{
//		Value: c.GetNearQuantityRate(DirectSell, c.NearQuantityRateWindow.NearDuration),
//		Time:  time.Now(),
//	})
//	c.NearQuantityRateWindow.Buy.Push(&NearQuantityRateWindowItem{
//		Value: c.GetNearQuantityRate(DirectBuy, c.NearQuantityRateWindow.NearDuration),
//		Time:  time.Now(),
//	})
//}

func (c *Capture) GetNearQuantityRate(bookTicker types.BookTicker, direct string, nearDuration time.Duration) float64 {
	now := bookTicker.TransactionTime
	w := c.aggKline.GetWindow(now.Add(-nearDuration), now)
	if w == nil || w.IsEmpty() {
		//log.Errorf("[capture] GetNearQuantity: window is empty")
		return 0
	}
	var q float64
	if direct == "all" {
		q = w.Quantity.Float64()
	}
	if direct == "sell" {
		q = w.SellQuantity.Float64()
	}
	if direct == "buy" {
		q = w.BuyQuantity.Float64()
	}
	fixedTimeSecond := nearDuration.Seconds()
	return q / fixedTimeSecond
}