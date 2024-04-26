package aggtrade

import (
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/config"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/aggtrade/influxpersist"
	"github.com/c9s/bbgo/pkg/types"
	"math"
	"time"
)

type WindowBase struct {
	StartTime         time.Time        `json:"startTime" db:"start_time"`
	EndTime           time.Time        `json:"endTime" db:"end_time"`
	Quantity          fixedpoint.Value `json:"quantity" db:"quantity"`
	QuoteQuantity     fixedpoint.Value `json:"quoteQuantity" db:"quote_quantity"`
	SellQuantity      fixedpoint.Value `json:"sellQuantity" db:"sell_quantity"`
	SellQuoteQuantity fixedpoint.Value `json:"sellQuoteQuantity" db:"sell_quote_quantity"`
	BuyQuantity       fixedpoint.Value `json:"buyQuantity" db:"buy_quantity"`
	BuyQuoteQuantity  fixedpoint.Value `json:"buyQuoteQuantity" db:"buy_quote_quantity"`
	Open              fixedpoint.Value `json:"open" db:"open"`
	Close             fixedpoint.Value `json:"close" db:"close"`
	High              fixedpoint.Value `json:"high" db:"high"`
	Low               fixedpoint.Value `json:"low" db:"low"`
	IsClosed          bool             `json:"isClosed" db:"is_closed"`
	TradeCount        int              `json:"tradeCount" db:"trade_count"`
}

var TradeDirectionAll = "all"
var TradeDirectionSell = "sell"
var TradeDirectionBuy = "buy"

func (w *WindowBase) GetQuoteQuantityRate(direction string, duration time.Duration) float64 {
	if w == nil || w.IsEmpty() {
		return 0
	}
	var qu float64
	if direction == TradeDirectionAll {
		qu = w.QuoteQuantity.Float64()
	}
	if direction == TradeDirectionSell {
		qu = w.SellQuoteQuantity.Float64()
	}
	if direction == TradeDirectionBuy {
		qu = w.BuyQuoteQuantity.Float64()
	}
	fixedTimeSecond := duration.Seconds()
	return qu / fixedTimeSecond
}

func (w *WindowBase) GetQuantityRate(direction string, duration time.Duration) float64 {
	if w == nil || w.IsEmpty() {
		return 0
	}
	var qu float64
	if direction == TradeDirectionAll {
		qu = w.Quantity.Float64()
	}
	if direction == TradeDirectionSell {
		qu = w.SellQuantity.Float64()
	}
	if direction == TradeDirectionBuy {
		qu = w.BuyQuantity.Float64()
	}
	fixedTimeSecond := duration.Seconds()
	return qu / fixedTimeSecond
}

func (w *WindowBase) GetTradeCount() int {
	if w == nil {
		return 0
	}
	return w.TradeCount
}

func (w *WindowBase) GetFluctuation() float64 {
	if w.TradeCount == 0 {
		return 0
	}
	if w.Open.IsZero() {
		return 0
	}
	return w.Close.Sub(w.Open).Div(w.Open).Float64()
}

func (w *WindowBase) GetAmplitude() float64 {
	if w.TradeCount == 0 {
		return 0
	}
	if w.Open.IsZero() {
		return 0
	}
	return w.High.Sub(w.Low).Div(w.Open).Float64()
}

func (w *WindowBase) GetLowerAmplitude() float64 {
	if w.TradeCount == 0 {
		return 0
	}
	if w.Open.IsZero() {
		return 0
	}
	return w.Open.Sub(w.Low).Div(w.Open).Float64()
}

func (w *WindowBase) GetUpperAmplitude() float64 {
	if w.TradeCount == 0 {
		return 0
	}
	if w.Open.IsZero() {
		return 0
	}
	return w.High.Sub(w.Open).Div(w.Open).Float64()
}

func (w *WindowBase) Update(trade *types.Trade) {
	if w.Open == 0 {
		w.Open = trade.Price
	}
	if w.StartTime.IsZero() {
		w.StartTime = time.Time(trade.Time)
	}

	w.Close = trade.Price
	w.EndTime = time.Time(trade.Time)
	w.High = fixedpoint.Max(w.High, trade.Price)
	w.Low = fixedpoint.Min(w.Low, trade.Price)
	w.Quantity += trade.Quantity
	w.QuoteQuantity += trade.QuoteQuantity
	if trade.IsMaker {
		w.SellQuantity += trade.Quantity
		w.SellQuoteQuantity += trade.QuoteQuantity
	} else {
		w.BuyQuantity += trade.Quantity
		w.BuyQuoteQuantity += trade.QuoteQuantity
	}
	w.TradeCount++
}

func (w *WindowBase) AppendAfter(afterWindow *WindowBase) *WindowBase {
	if w.IsEmpty() {
		return afterWindow
	}
	if afterWindow.IsEmpty() {
		return w
	}
	appendedWindow := NewWindowBase()
	appendedWindow.StartTime = w.StartTime
	appendedWindow.EndTime = afterWindow.EndTime
	appendedWindow.Quantity = w.Quantity + afterWindow.Quantity
	appendedWindow.QuoteQuantity = w.QuoteQuantity + afterWindow.QuoteQuantity
	appendedWindow.SellQuantity = w.SellQuantity + afterWindow.SellQuantity
	appendedWindow.SellQuoteQuantity = w.SellQuoteQuantity + afterWindow.SellQuoteQuantity
	appendedWindow.BuyQuantity = w.BuyQuantity + afterWindow.BuyQuantity
	appendedWindow.BuyQuoteQuantity = w.BuyQuoteQuantity + afterWindow.BuyQuoteQuantity
	appendedWindow.Open = w.Open
	appendedWindow.Close = afterWindow.Close
	appendedWindow.High = fixedpoint.Max(w.High, afterWindow.High)
	appendedWindow.Low = fixedpoint.Min(w.Low, afterWindow.Low)
	appendedWindow.TradeCount = w.TradeCount + afterWindow.TradeCount
	appendedWindow.IsClosed = afterWindow.IsClosed
	return appendedWindow
}

func (w *WindowBase) AppendBefore(beforeWindow *WindowBase) *WindowBase {
	if beforeWindow.IsEmpty() {
		return w
	}
	if w.IsEmpty() {
		return beforeWindow
	}
	appendedWindow := NewWindowBase()
	appendedWindow.StartTime = beforeWindow.StartTime
	appendedWindow.EndTime = w.EndTime
	appendedWindow.Quantity = w.Quantity + beforeWindow.Quantity
	appendedWindow.QuoteQuantity = w.QuoteQuantity + beforeWindow.QuoteQuantity
	appendedWindow.SellQuantity = w.SellQuantity + beforeWindow.SellQuantity
	appendedWindow.SellQuoteQuantity = w.SellQuoteQuantity + beforeWindow.SellQuoteQuantity
	appendedWindow.BuyQuantity = w.BuyQuantity + beforeWindow.BuyQuantity
	appendedWindow.BuyQuoteQuantity = w.BuyQuoteQuantity + beforeWindow.BuyQuoteQuantity
	appendedWindow.Open = beforeWindow.Open
	appendedWindow.Close = w.Close
	appendedWindow.High = fixedpoint.Max(w.High, beforeWindow.High)
	appendedWindow.Low = fixedpoint.Min(w.Low, beforeWindow.Low)
	appendedWindow.TradeCount = w.TradeCount + beforeWindow.TradeCount
	appendedWindow.IsClosed = w.IsClosed
	return appendedWindow
}

func (w *WindowBase) IsEmpty() bool {
	return w == nil || w.TradeCount == 0
}

func NewWindowBase() *WindowBase {
	return &WindowBase{
		Low: fixedpoint.NewFromFloat(math.Inf(1)),
	}
}

type Second struct {
	*WindowBase
	Trades   []*types.Trade
	Previous *Second
}

type Minute struct {
	*WindowBase
	Seconds  []*Second
	Previous *Minute
}

type Hour struct {
	*WindowBase
	Minutes  []*Minute
	Previous *Hour
}

type Day struct {
	*WindowBase
	Hours    []*Hour
	Previous *Day
}

type Kline struct {
	Days            map[string]*Day
	MaxHours        int
	StartTime       time.Time
	EndTime         time.Time
	TradeCount      int
	influxClient    *influxpersist.Client
	Symbol          string
	Exchange        types.ExchangeName
	IsEnablePersist bool
}

func NewSecond(previous *Second) *Second {
	return &Second{
		Trades:     make([]*types.Trade, 0, 10),
		WindowBase: NewWindowBase(),
		Previous:   previous,
	}
}

func (s *Second) RemoveBefore(t time.Time) (allRemove bool) {
	if s == nil {
		return false
	}
	if s.StartTime.After(t) {
		return false
	}
	if s.EndTime.Before(t) {
		s.Trades = nil
		s.WindowBase = nil
		s.Previous = nil
		return true
	}
	return false
}

func (s *Second) GetWindow(from, to time.Time) *WindowBase {
	if s == nil || s.IsEmpty() {
		return nil
	}
	if s.StartTime.After(to) || s.EndTime.Before(from) {
		return nil
	}
	if s.StartTime.After(from) && s.EndTime.Before(to) {
		return s.WindowBase
	}
	w := NewWindowBase()
	for _, trade := range s.Trades {
		if trade.Time.After(to) {
			continue
		}
		if trade.Time.Before(from) {
			continue
		}
		w.Update(trade)
	}
	return w
}

func (s *Second) AppendTrade(trade *types.Trade) {
	s.Trades = append(s.Trades, trade)
	s.Update(trade)
}

func (s *Second) CloseWindow() {
	if s == nil {
		return
	}
	s.IsClosed = true
}

func NewMinute(previous *Minute) *Minute {
	return &Minute{
		Seconds:    make([]*Second, 60),
		WindowBase: NewWindowBase(),
		Previous:   previous,
	}
}

func (m *Minute) RemoveBefore(t time.Time) (allRemove bool) {
	if m == nil {
		return false
	}
	if m.StartTime.After(t) {
		return false
	}
	if m.EndTime.Before(t) {
		m.Seconds = nil
		m.WindowBase = nil
		m.Previous = nil
		return true
	}
	emptyCount := 0
	isLastAllRemove := false
	for i, sec := range m.Seconds {
		if sec == nil {
			emptyCount++
			continue
		}
		isAllRemove := sec.RemoveBefore(t)
		if isAllRemove {
			m.Seconds[i] = nil
			emptyCount++
			isLastAllRemove = true
			continue
		}
		if isLastAllRemove {
			m.StartTime = sec.StartTime
			isLastAllRemove = false
		}
	}
	if emptyCount == len(m.Seconds) {
		m.Seconds = nil
		m.WindowBase = nil
		m.Previous = nil
		return true
	}
	return false
}

func (m *Minute) GetWindow(from, to time.Time) *WindowBase {
	if m == nil || m.IsEmpty() {
		return nil
	}
	if m.StartTime.After(to) || m.EndTime.Before(from) {
		return nil
	}
	if m.StartTime.After(from) && m.EndTime.Before(to) {
		return m.WindowBase
	}
	w := NewWindowBase()
	for _, sec := range m.Seconds {
		if sec == nil || sec.IsEmpty() {
			continue
		}
		if sec.StartTime.After(to) {
			continue
		}
		if sec.EndTime.Before(from) {
			continue
		}
		w = w.AppendAfter(sec.GetWindow(from, to))
	}
	return w
}

func (m *Minute) GetSecond(second int) *Second {
	if second < 0 || second > 59 {
		return nil
	}
	return m.Seconds[second]
}

func (m *Minute) AppendTrade(trade *types.Trade) {
	s := time.Time(trade.Time).Second()
	second := m.Seconds[s]
	if second == nil {
		var previousSecond *Second
		if s > 0 {
			m.Seconds[s-1].CloseWindow()
			previousSecond = m.Seconds[s-1]
		} else {
			if m.Previous != nil {
				m.Previous.CloseWindow()
				previousSecond = m.Previous.Seconds[59]
			}
		}
		second = NewSecond(previousSecond)
		m.Seconds[s] = second
	}
	second.AppendTrade(trade)
	m.Update(trade)
}

func (m *Minute) CloseWindow() {
	if m == nil {
		return
	}
	m.IsClosed = true
	m.Seconds[59].CloseWindow()
}

func NewHour(previous *Hour) *Hour {
	return &Hour{
		Minutes:    make([]*Minute, 60),
		WindowBase: NewWindowBase(),
		Previous:   previous,
	}
}

func (h *Hour) RemoveBefore(t time.Time) (allRemove bool) {
	if h == nil {
		return false
	}
	if h.StartTime.After(t) {
		return false
	}
	if h.EndTime.Before(t) {
		h.Minutes = nil
		h.WindowBase = nil
		h.Previous = nil
		return true
	}
	emptyCount := 0
	isLastAllRemove := false
	for i, minute := range h.Minutes {
		if minute == nil {
			emptyCount++
			continue
		}
		isAllRemove := minute.RemoveBefore(t)
		if isAllRemove {
			h.Minutes[i] = nil
			emptyCount++
			isLastAllRemove = true
			continue
		}
		if isLastAllRemove {
			h.StartTime = minute.StartTime
			isLastAllRemove = false
		}
	}
	if emptyCount == len(h.Minutes) {
		h.Minutes = nil
		h.WindowBase = nil
		h.Previous = nil
		return true
	}
	return false
}

func (h *Hour) GetWindow(from, to time.Time) *WindowBase {
	if h == nil || h.IsEmpty() {
		return nil
	}
	if h.StartTime.After(to) || h.EndTime.Before(from) {
		return nil
	}
	if h.StartTime.After(from) && h.EndTime.Before(to) {
		return h.WindowBase
	}
	w := NewWindowBase()
	for _, minute := range h.Minutes {
		if minute == nil || minute.IsEmpty() {
			continue
		}
		if minute.StartTime.After(to) {
			continue
		}
		if minute.EndTime.Before(from) {
			continue
		}
		w = w.AppendAfter(minute.GetWindow(from, to))
	}
	return w
}

func (h *Hour) GetMinute(minute int) *Minute {
	if minute < 0 || minute > 59 {
		return nil
	}
	return h.Minutes[minute]
}

func (h *Hour) AppendTrade(trade *types.Trade) {
	m := time.Time(trade.Time).Minute()
	minute := h.Minutes[m]
	if minute == nil {
		var previousMinute *Minute
		if m > 0 {
			h.Minutes[m-1].CloseWindow()
			previousMinute = h.Minutes[m-1]
		} else {
			if h.Previous != nil {
				h.Previous.CloseWindow()
				previousMinute = h.Previous.Minutes[59]
			}
		}
		minute = NewMinute(previousMinute)
		h.Minutes[m] = minute
	}
	minute.AppendTrade(trade)
	h.Update(trade)
}

func (h *Hour) CloseWindow() {
	if h == nil {
		return
	}
	h.IsClosed = true
	h.Minutes[59].CloseWindow()
}

func NewDay(previous *Day) *Day {
	return &Day{
		Hours:      make([]*Hour, 24),
		WindowBase: NewWindowBase(),
		Previous:   previous,
	}
}

func (d *Day) RemoveBefore(t time.Time) (allRemove bool) {
	if d == nil {
		return false
	}
	if d.StartTime.After(t) {
		return false
	}
	if d.EndTime.Before(t) {
		d.Hours = nil
		d.WindowBase = nil
		d.Previous = nil
		return true
	}
	emptyCount := 0
	isLastAllRemove := false
	for i, hour := range d.Hours {
		if hour == nil {
			emptyCount++
			continue
		}
		isAllRemove := hour.RemoveBefore(t)
		if isAllRemove {
			d.Hours[i] = nil
			emptyCount++
			isLastAllRemove = true
			continue
		}
		if isLastAllRemove {
			d.StartTime = hour.StartTime
			isLastAllRemove = false
		}
	}
	if emptyCount == len(d.Hours) {
		d.Hours = nil
		d.WindowBase = nil
		d.Previous = nil
		return true
	}
	return false
}

func (d *Day) GetWindow(from, to time.Time) *WindowBase {
	if d == nil || d.IsEmpty() {
		return nil
	}
	if d.StartTime.After(to) || d.EndTime.Before(from) {
		return nil

	}
	if d.StartTime.After(from) && d.EndTime.Before(to) {
		return d.WindowBase
	}
	w := NewWindowBase()
	for _, hour := range d.Hours {
		if hour == nil || hour.IsEmpty() {
			continue
		}
		if hour.StartTime.After(to) {
			continue
		}
		if hour.EndTime.Before(from) {
			continue
		}
		w = w.AppendAfter(hour.GetWindow(from, to))
	}
	return w
}

func (d *Day) GetHour(hour int) *Hour {
	if hour < 0 || hour > 23 {
		return nil
	}
	return d.Hours[hour]
}

func (d *Day) AppendTrade(trade *types.Trade) {
	h := time.Time(trade.Time).Hour()
	hour := d.Hours[h]
	if hour == nil {
		var previousHour *Hour
		if h > 0 {
			d.Hours[h-1].CloseWindow()
			previousHour = d.Hours[h-1]
		} else {
			if d.Previous != nil {
				d.Previous.CloseWindow()
				previousHour = d.Previous.Hours[23]
			}
		}
		hour = NewHour(previousHour)
		d.Hours[h] = hour
	}
	hour.AppendTrade(trade)
	d.Update(trade)
}

func (d *Day) CloseWindow() {
	if d == nil {
		return
	}
	d.IsClosed = true
	d.Hours[23].CloseWindow()
}

func NewKline(maxHours int, influxConfig *config.InfluxDB, symbol string, exchange types.ExchangeName) (*Kline, error) {
	var influxClient *influxpersist.Client
	var err error
	if influxConfig != nil {
		influxClient, err = influxpersist.NewClient(
			influxConfig.URL,
			influxConfig.Token,
			influxConfig.Org,
			influxpersist.Bucket,
			symbol,
			exchange)
		if err != nil {
			return nil, err
		}
	}
	return &Kline{
		Days:         make(map[string]*Day),
		MaxHours:     maxHours,
		influxClient: influxClient,
		Symbol:       symbol,
		Exchange:     exchange,
	}, nil
}

func (k *Kline) RemoveBefore(t time.Time) {
	for date, day := range k.Days {
		if day == nil {
			continue
		}
		if day.RemoveBefore(t) {
			delete(k.Days, date)
		}
	}
	var startTime time.Time
	for _, day := range k.Days {
		if day == nil {
			continue
		}
		if startTime.IsZero() || day.StartTime.Before(startTime) {
			startTime = day.StartTime
		}
	}
	k.StartTime = startTime
}

func (k *Kline) GetDay(date string) *Day {
	return k.Days[date]
}

func (k *Kline) GetPersist(from time.Time, to time.Time) ([]*types.Trade, *WindowBase, error) {
	if !k.IsEnablePersist {
		trades, window := k.Get(from, to)
		return trades, window, nil
	}
	k.influxClient.Flush()
	trades, err := k.influxClient.Get(from, to)
	if err != nil {
		return nil, nil, err
	}
	window := NewWindowBase()
	for _, trade := range trades {
		window.Update(trade)
	}
	return trades, window, nil
}

func (k *Kline) GetWindow(from time.Time, to time.Time) *WindowBase {
	if k == nil {
		return nil
	}
	if k.StartTime.After(to) || k.EndTime.Before(from) {
		return nil
	}
	w := NewWindowBase()
	for _, day := range k.Days {
		if day == nil || day.IsEmpty() {
			continue
		}
		if day.StartTime.After(to) {
			continue
		}
		if day.EndTime.Before(from) {
			continue
		}
		w = w.AppendAfter(day.GetWindow(from, to))
	}
	return w
}

func (k *Kline) Get(from time.Time, to time.Time) ([]*types.Trade, *WindowBase) {
	var trades []*types.Trade
	window := NewWindowBase()
	for _, day := range k.Days {
		if day.StartTime.After(to) {
			continue
		}
		if day.EndTime.Before(from) {
			continue
		}

		for _, hour := range day.Hours {
			if hour == nil {
				continue
			}
			if hour.StartTime.After(to) {
				continue
			}
			if hour.EndTime.Before(from) {
				continue
			}

			for _, minute := range hour.Minutes {
				if minute == nil {
					continue
				}
				if minute.StartTime.After(to) {
					continue
				}
				if minute.EndTime.Before(from) {
					continue
				}

				for _, second := range minute.Seconds {
					if second == nil {
						continue
					}
					if second.StartTime.After(to) {
						continue
					}
					if second.EndTime.Before(from) {
						continue
					}
					for _, trade := range second.Trades {
						if trade.Time.After(to) {
							continue
						}
						if trade.Time.Before(from) {
							continue
						}
						trades = append(trades, trade)

						window.Update(trade)
					}
				}
			}
		}
	}
	return trades, window
}

func (k *Kline) AppendTrade(trade *types.Trade) {
	date := (time.Time)(trade.Time).Format("2006-01-02")
	day := k.Days[date]
	if day == nil {
		previousDate := time.Time(trade.Time).AddDate(0, 0, -1).Format("2006-01-02")
		if previousDay := k.Days[previousDate]; previousDay != nil {
			previousDay.CloseWindow()
		}
		day = NewDay(k.Days[date])
		k.Days[date] = day
	}
	day.AppendTrade(trade)
	k.Update(trade)

	hoursCount := trade.Time.Time().Sub(k.StartTime).Hours()
	if k.MaxHours != 0 && int(hoursCount) > k.MaxHours {
		k.RemoveBefore(trade.Time.Time().Add(-time.Duration(k.MaxHours) * time.Hour))
	}

	if k.IsEnablePersist {
		k.influxClient.AppendTrade(trade)
	}
}

func (k *Kline) Update(trade *types.Trade) {
	if k.StartTime.IsZero() {
		k.StartTime = time.Time(trade.Time)
	}
	k.EndTime = time.Time(trade.Time)
	k.TradeCount++
}

func (k *Kline) CloseKline() {
	if k == nil {
		return
	}
	if k.IsEnablePersist {
		k.influxClient.Flush()
		k.influxClient.Close()
	}
}
