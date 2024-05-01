package tick

import (
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/config"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/tick/influxpersist"
	"github.com/c9s/bbgo/pkg/types"
	"math"
	"sort"
	"time"
)

type WindowBase struct {
	StartTime time.Time         `json:"startTime" db:"start_time"`
	EndTime   time.Time         `json:"endTime" db:"end_time"`
	Open      *types.BookTicker `json:"open" db:"open"`
	Close     *types.BookTicker `json:"close" db:"close"`
	High      fixedpoint.Value  `json:"high" db:"high"`
	Low       fixedpoint.Value  `json:"low" db:"low"`
	IsClosed  bool              `json:"isClosed" db:"is_closed"`
	TickCount int               `json:"tickCount" db:"tick_count"`
}

func (w *WindowBase) Update(ticker *types.BookTicker) {
	if w.Open == nil {
		w.Open = ticker
	}
	if w.StartTime.IsZero() {
		w.StartTime = ticker.TransactionTime
	}

	w.Close = ticker
	w.EndTime = ticker.TransactionTime
	w.High = fixedpoint.Max(w.High, ticker.Buy)
	w.Low = fixedpoint.Min(w.Low, ticker.Sell)
	w.TickCount++
}

func NewWindowBase() *WindowBase {
	return &WindowBase{
		Low: fixedpoint.NewFromFloat(math.Inf(1)),
	}
}

type Second struct {
	*WindowBase
	Ticks    []*types.BookTicker
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
	TickCount       int
	influxClient    *influxpersist.Client
	Symbol          string
	Exchange        types.ExchangeName
	IsEnablePersist bool
}

func NewSecond(previous *Second) *Second {
	return &Second{
		Ticks:      make([]*types.BookTicker, 0, 20),
		WindowBase: NewWindowBase(),
		Previous:   previous,
	}
}

func (s *Second) GetFirstAndEnd(from time.Time, to time.Time) (start, end *types.BookTicker, isRealStart, isRealEnd bool) {
	if s == nil {
		return nil, nil, false, false
	}
	if s.StartTime.After(to) {
		return nil, nil, false, false
	}
	if s.EndTime.Before(from) {
		return nil, nil, false, false
	}
	if s.StartTime.After(from) {
		start = s.Open
	} else {
		for _, tick := range s.Ticks {
			if tick.TransactionTime.After(from) {
				start = tick
				isRealStart = true
				break
			}
		}
	}
	if s.EndTime.Before(to) {
		end = s.Close
	} else {
		for i := len(s.Ticks) - 1; i >= 0; i-- {
			if s.Ticks[i].Time.Before(to) {
				end = s.Ticks[i]
				isRealEnd = true
				break
			}
		}
	}
	return start, end, isRealStart, isRealEnd
}

func (s *Second) RemoveBefore(t time.Time) (allRemove bool) {
	if s == nil {
		return false
	}
	if s.StartTime.After(t) {
		return false
	}
	if s.EndTime.Before(t) {
		s.Ticks = nil
		s.WindowBase = nil
		s.Previous = nil
		return true
	}
	return false
}

func (s *Second) GetPrevious() *Second {
	return s.Previous
}

func (s *Second) AppendTick(ticker *types.BookTicker) {
	s.Ticks = append(s.Ticks, ticker)
	s.Update(ticker)
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

func (m *Minute) GetFirstAndEnd(from time.Time, to time.Time) (start, end *types.BookTicker, isRealStart, isRealEnd bool) {
	if m == nil {
		return nil, nil, false, false
	}
	if m.StartTime.After(to) {
		return nil, nil, false, false
	}
	if m.EndTime.Before(from) {
		return nil, nil, false, false
	}
	if m.StartTime.After(from) {
		start = m.Open
	} else {
		for _, second := range m.Seconds {
			if second == nil {
				continue
			}
			secStart, _, secIsRealStart, _ := second.GetFirstAndEnd(from, to)
			if secStart != nil && secIsRealStart {
				start = secStart
				isRealStart = true
				break
			}
		}
	}
	if m.EndTime.Before(to) {
		end = m.Close
	} else {
		for i := 59; i >= 0; i-- {
			if m.Seconds[i] == nil {
				continue
			}
			_, secEnd, _, secIsRealEnd := m.Seconds[i].GetFirstAndEnd(from, to)
			if secEnd != nil && secIsRealEnd {
				end = secEnd
				isRealEnd = true
				break
			}
		}
	}
	return start, end, isRealStart, isRealEnd
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

func (m *Minute) GetSecond(second int) *Second {
	if second < 0 || second > 59 {
		return nil
	}
	return m.Seconds[second]
}

func (m *Minute) AppendTick(ticker *types.BookTicker) {
	s := ticker.TransactionTime.Second()
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
	second.AppendTick(ticker)
	m.Update(ticker)
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

func (h *Hour) GetFirstAndEnd(from time.Time, to time.Time) (start, end *types.BookTicker, isRealStart, isRealEnd bool) {
	if h == nil {
		return nil, nil, false, false
	}
	if h.StartTime.After(to) {
		return nil, nil, false, false
	}
	if h.EndTime.Before(from) {
		return nil, nil, false, false
	}
	if h.StartTime.After(from) {
		start = h.Open
	} else {
		for _, minute := range h.Minutes {
			if minute == nil {
				continue
			}
			minStart, _, minIsRealStart, _ := minute.GetFirstAndEnd(from, to)
			if minStart != nil && minIsRealStart {
				start = minStart
				isRealStart = true
				break
			}
		}
	}
	if h.EndTime.Before(to) {
		end = h.Close
	} else {
		for i := 59; i >= 0; i-- {
			if h.Minutes[i] == nil {
				continue
			}
			_, minEnd, _, minIsRealEnd := h.Minutes[i].GetFirstAndEnd(from, to)
			if minEnd != nil && minIsRealEnd {
				end = minEnd
				isRealEnd = true
				break
			}
		}
	}
	return start, end, isRealStart, isRealEnd
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

func (h *Hour) AppendTick(ticker *types.BookTicker) {
	m := ticker.TransactionTime.Minute()
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
	minute.AppendTick(ticker)
	h.Update(ticker)
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

func (d *Day) GetFirstAndEnd(from time.Time, to time.Time) (start, end *types.BookTicker, isRealStart, isRealEnd bool) {
	if d == nil {
		return nil, nil, false, false
	}
	if d.StartTime.After(to) {
		return nil, nil, false, false
	}
	if d.EndTime.Before(from) {
		return nil, nil, false, false
	}
	if d.StartTime.After(from) {
		start = d.Open
	} else {
		for _, hour := range d.Hours {
			if hour == nil {
				continue
			}
			hourStart, _, hourIsRealStart, _ := hour.GetFirstAndEnd(from, to)
			if hourStart != nil && hourIsRealStart {
				start = hourStart
				isRealStart = true
				break
			}
		}
	}
	if d.EndTime.Before(to) {
		end = d.Close
	} else {
		for i := 23; i >= 0; i-- {
			if d.Hours[i] == nil {
				continue
			}
			_, hourEnd, _, hourIsRealEnd := d.Hours[i].GetFirstAndEnd(from, to)
			if hourEnd != nil && hourIsRealEnd {
				end = hourEnd
				isRealEnd = true
				break
			}
		}
	}
	return start, end, isRealStart, isRealEnd
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

func (d *Day) GetHour(hour int) *Hour {
	if hour < 0 || hour > 23 {
		return nil
	}
	return d.Hours[hour]
}

func (d *Day) AppendTick(ticker *types.BookTicker) {
	h := ticker.TransactionTime.Hour()
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
	hour.AppendTick(ticker)
	d.Update(ticker)
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

func (k *Kline) GetPersist(from time.Time, to time.Time) ([]*types.BookTicker, *WindowBase, error) {
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

func (k *Kline) GetFirstAndEnd(from time.Time, to time.Time) (start, end *types.BookTicker) {
	if k == nil {
		return nil, nil
	}
	if k.StartTime.After(to) {
		return nil, nil
	}
	if k.EndTime.Before(from) {
		return nil, nil
	}
	sortedDays := make([]time.Time, 0, len(k.Days))
	for date := range k.Days {
		day, err := time.Parse("2006-01-02", date)
		if err != nil {
			continue
		}
		sortedDays = append(sortedDays, day)
	}
	sort.Slice(sortedDays, func(i, j int) bool {
		return sortedDays[i].Before(sortedDays[j])
	})
	if k.StartTime.After(from) {
		key := sortedDays[0].Format("2006-01-02")
		start = k.Days[key].Open
	} else {
		for _, day := range sortedDays {
			if day.Before(from) {
				continue
			}
			dayStart, _, dayIsRealStart, _ := k.Days[day.Format("2006-01-02")].GetFirstAndEnd(from, to)
			if dayStart != nil && dayIsRealStart {
				start = dayStart
				break
			}
		}
	}
	if k.EndTime.Before(to) {
		key := sortedDays[len(sortedDays)-1].Format("2006-01-02")
		end = k.Days[key].Close
	} else {
		for i := len(sortedDays) - 1; i >= 0; i-- {
			if sortedDays[i].After(to) {
				continue
			}
			dayEnd, _, _, dayIsRealEnd := k.Days[sortedDays[i].Format("2006-01-02")].GetFirstAndEnd(from, to)
			if dayEnd != nil && dayIsRealEnd {
				end = dayEnd
				break
			}
		}
	}
	return start, end
}

func (k *Kline) Get(from time.Time, to time.Time) ([]*types.BookTicker, *WindowBase) {
	var tickers []*types.BookTicker
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
					for _, tick := range second.Ticks {
						if tick.Time.After(to) {
							continue
						}
						if tick.Time.Before(from) {
							continue
						}
						tickers = append(tickers, tick)

						window.Update(tick)
					}
				}
			}
		}
	}
	return tickers, window
}

func (k *Kline) AppendTick(ticker *types.BookTicker) {
	date := ticker.TransactionTime.Format("2006-01-02")
	day := k.Days[date]
	if day == nil {
		previousDate := ticker.TransactionTime.AddDate(0, 0, -1).Format("2006-01-02")
		if previousDay := k.Days[previousDate]; previousDay != nil {
			previousDay.CloseWindow()
		}
		day = NewDay(k.Days[date])
		k.Days[date] = day
	}
	day.AppendTick(ticker)
	k.Update(ticker)

	hoursCount := ticker.TransactionTime.Sub(k.StartTime).Hours()
	if k.MaxHours != 0 && int(hoursCount) > k.MaxHours {
		k.RemoveBefore(ticker.TransactionTime.Add(-time.Duration(k.MaxHours) * time.Hour))
	}

	if k.IsEnablePersist {
		k.influxClient.AppendTick(ticker)
	}
}

func (k *Kline) Update(ticker *types.BookTicker) {
	if k.StartTime.IsZero() {
		k.StartTime = ticker.TransactionTime
	}
	k.EndTime = ticker.TransactionTime
	k.TickCount++
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
