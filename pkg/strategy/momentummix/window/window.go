package window

import (
	log "github.com/sirupsen/logrus"
	"math"
	"time"
)

type Item interface {
	//Get() interface{}
	GetFloat64() float64
	GetTime() time.Time
	//IsLargerThan(Item) bool
	//IsSmallerThan(Item) bool
	//IsEqual(Item) bool
}

type Second struct {
	StartTime time.Time
	EndTime   time.Time
	Items     []Item
	Max       float64
	Min       float64
	Quantity  float64
}

type Statistic struct {
	Max      float64
	Min      float64
	Quantity float64
	Count    int64
	Start    time.Time
	End      time.Time
}

func (s *Statistic) IsEmpty() bool {
	return s == nil || s.Count == 0
}

func (s *Statistic) GetTimeAvg() float64 {
	if s.IsEmpty() {
		return 0
	}
	duration := s.End.Sub(s.Start)
	return s.Quantity / duration.Seconds()
}

func (s *Statistic) GetCountAvg() float64 {
	if s.IsEmpty() {
		return 0
	}
	duration := s.End.Sub(s.Start)
	return float64(s.Count) / duration.Seconds()
}

func NewSecond() *Second {
	return &Second{
		Min: math.Inf(1),
	}
}

func (s *Second) IsEmpty() bool {
	return s == nil || len(s.Items) == 0
}

func (s *Second) GetStatistic(from time.Time, to time.Time) *Statistic {
	if s == nil || len(s.Items) == 0 {
		return nil
	}
	if s.StartTime.After(to) || s.EndTime.Before(from) {
		return nil
	}
	if s.StartTime.After(from) && s.EndTime.Before(to) {
		return &Statistic{
			Max:      s.Max,
			Min:      s.Min,
			Quantity: s.Quantity,
			Count:    int64(len(s.Items)),
			Start:    s.StartTime,
			End:      s.EndTime,
		}
	}
	var quantity float64
	var count int64
	var mx float64
	mn := math.Inf(1)
	var start, end time.Time
	for i := range s.Items {
		if s.Items[i].GetTime().After(to) {
			continue
		}
		if s.Items[i].GetTime().Before(from) {
			continue
		}
		quantity += s.Items[i].GetFloat64()
		count++
		if s.Items[i].GetFloat64() > mx {
			mx = s.Items[i].GetFloat64()
		}
		if s.Items[i].GetFloat64() < mn {
			mn = s.Items[i].GetFloat64()
		}
		if start.IsZero() {
			start = s.Items[i].GetTime()
		}
		end = s.Items[i].GetTime()
	}
	return &Statistic{
		Max:      mx,
		Min:      mn,
		Quantity: quantity,
		Count:    count,
		Start:    start,
		End:      end,
	}
}

func (s *Second) Get(from time.Time, to time.Time) []Item {
	if s == nil || len(s.Items) == 0 {
		return nil
	}
	if s.StartTime.After(to) || s.EndTime.Before(from) {
		return nil
	}
	if s.StartTime.After(from) && s.EndTime.Before(to) {
		return s.Items
	}
	items := make([]Item, 0, 20)
	for i := range s.Items {
		if s.Items[i].GetTime().After(to) {
			continue
		}
		if s.Items[i].GetTime().Before(from) {
			continue
		}
		items = append(items, s.Items[i])
	}
	return items
}

func (s *Second) Push(item Item) {
	if s == nil {
		return
	}
	if s.StartTime.IsZero() {
		s.StartTime = item.GetTime()
	}
	s.EndTime = item.GetTime()
	s.Items = append(s.Items, item)
	if item.GetFloat64() > s.Max {
		s.Max = item.GetFloat64()
	}
	if item.GetFloat64() < s.Min {
		s.Min = item.GetFloat64()
	}
	s.Quantity += item.GetFloat64()
}

type Window struct {
	Seconds   []*Second
	MaxSecond int
	StartTime time.Time
	EndTime   time.Time
}

func NewWindow(maxSecond int) *Window {
	return &Window{
		Seconds:   make([]*Second, 0, maxSecond),
		MaxSecond: maxSecond,
	}
}

func (w *Window) Get(from time.Time, to time.Time) []Item {
	if w == nil {
		return nil
	}
	if w.StartTime.After(to) || w.EndTime.Before(from) {
		return nil
	}
	items := make([]Item, 0, 20)
	for _, second := range w.Seconds {
		if second.IsEmpty() {
			continue
		}
		if second.EndTime.Before(from) {
			continue
		}
		if second.StartTime.After(to) {
			continue
		}
		items = append(items, second.Get(from, to)...)
	}
	return items
}
func (w *Window) GetStatistic(from time.Time, to time.Time) *Statistic {
	if w == nil {
		return nil
	}
	if w.StartTime.After(to) || w.EndTime.Before(from) {
		return nil
	}
	var quantity float64
	var count int64
	var mx float64
	mn := math.Inf(1)
	var start, end time.Time
	for _, second := range w.Seconds {
		if second.IsEmpty() {
			continue
		}
		if second.EndTime.Before(from) {
			continue
		}
		if second.StartTime.After(to) {
			continue
		}
		statistic := second.GetStatistic(from, to)
		quantity += statistic.Quantity
		count += statistic.Count
		if statistic.Max > mx {
			mx = statistic.Max
		}
		if statistic.Min < mn {
			mn = statistic.Min
		}
		if start.IsZero() {
			start = statistic.Start
		}
		end = statistic.End
	}
	return &Statistic{
		Max:      mx,
		Min:      mn,
		Quantity: quantity,
		Count:    count,
		Start:    start,
		End:      end,
	}
}

func (w *Window) Push(item Item) {
	if w == nil {
		return
	}
	if w.StartTime.IsZero() {
		w.StartTime = item.GetTime()
	}
	w.EndTime = item.GetTime()
	if len(w.Seconds) == 0 {
		second := NewSecond()
		w.Seconds = append(w.Seconds, second)
		second.Push(item)
		return
	}
	lastSecond := w.Seconds[len(w.Seconds)-1]
	lastTimeSec := lastSecond.StartTime.Truncate(time.Second)
	nowTimeSec := item.GetTime().Truncate(time.Second)
	if lastTimeSec == nowTimeSec {
		lastSecond.Push(item)
		return
	}
	if nowTimeSec.Sub(lastTimeSec) > 0 {
		second := NewSecond()
		w.Seconds = append(w.Seconds, second)
		second.Push(item)
	}
	if nowTimeSec.Sub(lastTimeSec) < 0 {
		log.Errorf("time is not in order, lastTime: %s, nowTime: %s", lastTimeSec, nowTimeSec)
	}
	if len(w.Seconds) >= w.MaxSecond {
		w.Seconds = w.Seconds[1:]
		w.StartTime = w.Seconds[0].StartTime
	}
}
