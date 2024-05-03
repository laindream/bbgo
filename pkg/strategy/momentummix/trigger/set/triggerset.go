package set

import (
	"github.com/c9s/bbgo/pkg/strategy/momentummix/trigger/trigger"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/sirupsen/logrus"
	"sort"
	"time"
)

type TriggerSet struct {
	QuoteQuantityExceedTriggers map[string]*trigger.QuoteQuantityExceedTrigger
	TotalValueRank              []string
	Logger                      *logrus.Logger
	TotalTriggerCount           int
	TotalProfit                 float64
	WinCount                    int
	WinRate                     float64
}

var printKey = make(map[string]bool)

func (s *TriggerSet) OnUpdateRank() {
	QuoteQuantityExceedTriggerList := make([]*trigger.QuoteQuantityExceedTrigger, 0)
	for _, t := range s.QuoteQuantityExceedTriggers {
		if t.GetTotalValue() == 0 {
			continue
		}
		QuoteQuantityExceedTriggerList = append(QuoteQuantityExceedTriggerList, t)
	}
	sort.Slice(QuoteQuantityExceedTriggerList, func(i, j int) bool {
		return QuoteQuantityExceedTriggerList[i].GetTotalValue() > QuoteQuantityExceedTriggerList[j].GetTotalValue()
	})
	s.TotalValueRank = make([]string, 0)
	for _, t := range QuoteQuantityExceedTriggerList {
		s.TotalValueRank = append(s.TotalValueRank, t.Symbol)
	}
	iPrintKey := time.Now().Truncate(10 * time.Minute)
	if _, ok := printKey[iPrintKey.String()]; !ok {
		printKey[iPrintKey.String()] = true
		s.Logger.Infof("TotalValueRank: %+v", s.TotalValueRank)
	}
}

func (s *TriggerSet) OnTrigger(book *types.BookTicker) {
	topK := len(s.TotalValueRank) * 6 / 10
	if topK == 0 {
		return
	}
	for i := 0; i < topK; i++ {
		if s.TotalValueRank[i] == book.Symbol {
			s.Logger.Infof("[%s]GlobalTrigger: %s", book.Symbol, book)
			s.QuoteQuantityExceedTriggers[book.Symbol].IsGlobalTriggered = true
			break
		}
	}
}

func (s *TriggerSet) OnUnTrigger(book *types.BookTicker, profit float64) {
	if t, ok := s.QuoteQuantityExceedTriggers[book.Symbol]; ok && t.IsGlobalTriggered {
		if profit > 0 {
			s.WinCount++
		}
		s.TotalProfit += profit
		s.TotalTriggerCount++
		s.WinRate = float64(s.WinCount) / float64(s.TotalTriggerCount)
		s.Logger.Infof("[%s]GlobalUnTrigger[PF:%f][TC:%d][TP:%f][WR:%f]: %s", book.Symbol, profit, s.TotalTriggerCount, s.TotalProfit, s.WinRate, book)
		t.IsGlobalTriggered = false
	}
}
