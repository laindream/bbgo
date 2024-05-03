package book

import (
	"fmt"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/book/fullbook"
	"github.com/c9s/bbgo/pkg/types"
	"sort"
	"time"
)

type WindowBase struct {
	StartTime       time.Time             `json:"startTime" db:"start_time"`
	EndTime         time.Time             `json:"endTime" db:"end_time"`
	AggUpdate       *types.SliceOrderBook `json:"aggUpdate" db:"agg_update"`
	Open            *types.SliceOrderBook `json:"open" db:"open"`
	Close           *types.SliceOrderBook `json:"close" db:"close"`
	IsClosed        bool                  `json:"isClosed" db:"is_closed"`
	BookUpdateCount int                   `json:"bookUpdateCount" db:"book_update_count"`
}

func AppendBookUpdates(src *types.SliceOrderBook, bookUpdates []*types.SliceOrderBook) {
	if len(bookUpdates) == 0 {
		return
	}
	for i := range bookUpdates {
		src.UpdateBids(bookUpdates[i].Bids)
		src.UpdateAsks(bookUpdates[i].Asks)
		src.LastUpdateId = bookUpdates[i].LastUpdateId
		src.TransactionTime = bookUpdates[i].TransactionTime
		src.FinalUpdateID = bookUpdates[i].FinalUpdateID
	}
	return
}

func CopyBook(book *types.SliceOrderBook) *types.SliceOrderBook {
	if book == nil {
		return nil
	}
	newBids := make(types.PriceVolumeSlice, 0, len(book.Bids))
	newAsks := make(types.PriceVolumeSlice, 0, len(book.Asks))
	copy(newBids, book.Bids)
	copy(newAsks, book.Asks)
	return &types.SliceOrderBook{
		Symbol:                book.Symbol,
		Bids:                  newBids,
		Asks:                  newAsks,
		Time:                  book.Time,
		LastUpdateId:          book.LastUpdateId,
		TransactionTime:       book.TransactionTime,
		FirstUpdateID:         book.FirstUpdateID,
		FinalUpdateID:         book.FinalUpdateID,
		PreviousFinalUpdateID: book.PreviousFinalUpdateID,
	}
}

func (w *WindowBase) Update(book *types.SliceOrderBook) {
	if w.Open == nil {
		w.Open = book
	}
	if w.AggUpdate == nil {
		w.AggUpdate = CopyBook(book)
	} else {
		AppendBookUpdates(w.AggUpdate, append([]*types.SliceOrderBook{}, book))
	}
	if w.StartTime.IsZero() {
		w.StartTime = book.TransactionTime
	}

	w.Close = book
	w.EndTime = book.TransactionTime
	w.BookUpdateCount++
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
	appendedWindow.AggUpdate = CopyBook(w.AggUpdate)
	AppendBookUpdates(appendedWindow.AggUpdate, []*types.SliceOrderBook{afterWindow.AggUpdate})
	appendedWindow.Open = w.Open
	appendedWindow.Close = afterWindow.Close
	appendedWindow.IsClosed = afterWindow.IsClosed
	appendedWindow.BookUpdateCount = w.BookUpdateCount + afterWindow.BookUpdateCount
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
	appendedWindow.AggUpdate = CopyBook(beforeWindow.AggUpdate)
	AppendBookUpdates(appendedWindow.AggUpdate, []*types.SliceOrderBook{w.AggUpdate})
	appendedWindow.Open = beforeWindow.Open
	appendedWindow.Close = w.Close
	appendedWindow.IsClosed = w.IsClosed
	appendedWindow.BookUpdateCount = beforeWindow.BookUpdateCount + w.BookUpdateCount
	return appendedWindow
}

func (w *WindowBase) IsEmpty() bool {
	return w == nil || w.BookUpdateCount == 0
}

func NewWindowBase() *WindowBase {
	return &WindowBase{}
}

type Second struct {
	*WindowBase
	BookUpdates []*types.SliceOrderBook
	Previous    *Second
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
	Days                 map[string]*Day
	MaxHours             int
	StartTime            time.Time
	EndTime              time.Time
	BookUpdateCount      int
	Symbol               string
	Exchange             types.ExchangeName
	BookSnapshot         *types.SliceOrderBook
	BookSnapshotWatchDog *fullbook.WatchDog
	SetupTime            time.Time
}

func NewSecond(previous *Second) *Second {
	return &Second{
		BookUpdates: make([]*types.SliceOrderBook, 0),
		WindowBase:  NewWindowBase(),
		Previous:    previous,
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
		s.BookUpdates = nil
		s.WindowBase = nil
		s.Previous = nil
		return true
	}
	return false
}

func (s *Second) GetWindowByUpdateID(from, to int64) *WindowBase {
	if s == nil || s.IsEmpty() {
		return nil
	}
	if s.AggUpdate.FinalUpdateID < from {
		return nil
	}
	if to != 0 && s.AggUpdate.PreviousFinalUpdateID > to {
		return nil
	}
	if s.AggUpdate.PreviousFinalUpdateID >= from && (to == 0 || s.AggUpdate.FinalUpdateID <= to) {
		return s.WindowBase
	}
	w := NewWindowBase()
	for _, b := range s.BookUpdates {
		if b.FinalUpdateID < from {
			continue
		}
		if to != 0 && b.PreviousFinalUpdateID > to {
			continue
		}
		w.Update(b)
	}
	return s.WindowBase
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
	for _, b := range s.BookUpdates {
		if b.TransactionTime.After(to) {
			continue
		}
		if b.TransactionTime.Before(from) {
			continue
		}
		w.Update(b)
	}
	return w
}

func (s *Second) AppendBook(book *types.SliceOrderBook) {
	s.BookUpdates = append(s.BookUpdates, book)
	s.Update(book)
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

func (m *Minute) GetWindowByUpdateID(from, to int64) *WindowBase {
	if m == nil || m.IsEmpty() {
		return nil
	}
	if m.AggUpdate.FinalUpdateID < from {
		return nil
	}
	if to != 0 && m.AggUpdate.PreviousFinalUpdateID > to {
		return nil
	}
	if m.AggUpdate.PreviousFinalUpdateID >= from && (to == 0 || m.AggUpdate.FinalUpdateID <= to) {
		return m.WindowBase
	}
	w := NewWindowBase()
	for _, sec := range m.Seconds {
		if sec == nil {
			continue
		}
		w = w.AppendAfter(sec.GetWindowByUpdateID(from, to))
	}
	return w
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

func (m *Minute) AppendBook(book *types.SliceOrderBook) {
	s := book.TransactionTime.Second()
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
	second.AppendBook(book)
	m.Update(book)
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

func (h *Hour) GetWindowByUpdateID(from, to int64) *WindowBase {
	if h == nil || h.IsEmpty() {
		return nil
	}
	if h.AggUpdate.FinalUpdateID < from {
		return nil
	}
	if to != 0 && h.AggUpdate.PreviousFinalUpdateID > to {
		return nil
	}
	if h.AggUpdate.PreviousFinalUpdateID >= from && (to == 0 || h.AggUpdate.FinalUpdateID <= to) {
		return h.WindowBase
	}
	w := NewWindowBase()
	for _, minute := range h.Minutes {
		if minute == nil {
			continue
		}
		w = w.AppendAfter(minute.GetWindowByUpdateID(from, to))
	}
	return w
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

func (h *Hour) AppendBook(book *types.SliceOrderBook) {
	m := book.TransactionTime.Minute()
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
	minute.AppendBook(book)
	h.Update(book)
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

func (d *Day) GetWindowByUpdateID(from, to int64) *WindowBase {
	if d == nil || d.IsEmpty() {
		return nil
	}
	if d.AggUpdate.FinalUpdateID < from {
		return nil
	}
	if to != 0 && d.AggUpdate.PreviousFinalUpdateID > to {
		return nil
	}
	if d.AggUpdate.PreviousFinalUpdateID >= from && (to == 0 || d.AggUpdate.FinalUpdateID <= to) {
		return d.WindowBase
	}
	w := NewWindowBase()
	for _, hour := range d.Hours {
		if hour == nil {
			continue
		}
		w = w.AppendAfter(hour.GetWindowByUpdateID(from, to))
	}
	return w
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

func (d *Day) AppendBook(book *types.SliceOrderBook) {
	h := book.TransactionTime.Hour()
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
	hour.AppendBook(book)
	d.Update(book)
}

func (d *Day) CloseWindow() {
	if d == nil {
		return
	}
	d.IsClosed = true
	d.Hours[23].CloseWindow()
}

func NewKline(maxHours int, symbol string, session *bbgo.ExchangeSession) (*Kline, error) {
	return &Kline{
		SetupTime:            time.Now(),
		Days:                 make(map[string]*Day),
		MaxHours:             maxHours,
		Symbol:               symbol,
		Exchange:             session.ExchangeName,
		BookSnapshotWatchDog: fullbook.NewWatchDog(symbol, session),
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

func (k *Kline) GetWindowByUpdateID(from, to int64) *WindowBase {
	if k == nil {
		return nil
	}
	if len(k.Days) == 0 {
		return nil
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
	w := NewWindowBase()
	for _, day := range sortedDays {
		d := k.Days[day.Format("2006-01-02")]
		if d == nil {
			continue
		}
		w = w.AppendAfter(d.GetWindowByUpdateID(from, to))
	}
	return w
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

func (k *Kline) Get(from time.Time, to time.Time) ([]*types.SliceOrderBook, *WindowBase) {
	var books []*types.SliceOrderBook
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
					for _, b := range second.BookUpdates {
						if b.TransactionTime.After(to) {
							continue
						}
						if b.TransactionTime.Before(from) {
							continue
						}
						books = append(books, b)

						window.Update(b)
					}
				}
			}
		}
	}
	return books, window
}

func (k *Kline) TryUpdateSnapshot() error {
	if k.BookSnapshotWatchDog == nil {
		return fmt.Errorf("book snapshot watch dog is not initialized")
	}
	bs := k.BookSnapshotWatchDog.GetLatestBookSnapshot()
	if bs == nil {
		return fmt.Errorf("book snapshot is not available")
	}
	if k.BookSnapshot == nil {
		//cbs := CopyBook(bs)
		//bookUpdates := k.GetWindowByUpdateID(cbs.LastUpdateId, 0)
		return nil
	} else {

	}
	return nil
}

func (k *Kline) AppendBook(book *types.SliceOrderBook) {
	date := book.TransactionTime.Format("2006-01-02")
	day := k.Days[date]
	if day == nil {
		previousDate := book.TransactionTime.AddDate(0, 0, -1).Format("2006-01-02")
		if previousDay := k.Days[previousDate]; previousDay != nil {
			previousDay.CloseWindow()
		}
		day = NewDay(k.Days[date])
		k.Days[date] = day
	}
	day.AppendBook(book)
	k.Update(book)

	minutesCount := book.TransactionTime.Sub(k.StartTime).Minutes()
	if k.MaxHours != 0 && int(minutesCount) > k.MaxHours*60 {
		k.RemoveBefore(book.TransactionTime.Add(-time.Duration(k.MaxHours) * time.Hour))
	}
}

func (k *Kline) Update(book *types.SliceOrderBook) {
	if k.StartTime.IsZero() {
		k.StartTime = book.TransactionTime
	}
	k.EndTime = book.TransactionTime
	k.BookUpdateCount++
}

func (k *Kline) CloseKline() {
	if k == nil {
		return
	}
}
