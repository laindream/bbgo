package history

import (
	"context"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/types"
	log "github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
)

type Statistic struct {
	QuantityRate      float64
	QuoteQuantityRate float64
	mu                sync.Mutex
}

func (s *Statistic) GetQuantityRate() float64 {
	if s == nil {
		return 0
	}
	return s.QuantityRate
}

func (s *Statistic) GetQuoteQuantityRate() float64 {
	if s == nil {
		return 0
	}
	return s.QuoteQuantityRate
}

type MarketHistory struct {
	Symbol         string
	UpdateInterval time.Duration
	historyStat    *types.MarketStats
	session        *bbgo.ExchangeSession
	mu             sync.Mutex
	stat           *Statistic
}

func NewMarketHistory(symbol string, updateInterval time.Duration, session *bbgo.ExchangeSession) *MarketHistory {
	m := &MarketHistory{
		Symbol:         symbol,
		UpdateInterval: updateInterval,
		session:        session,
	}
	go m.watchdog()
	return m
}

func (s *MarketHistory) Get() *types.MarketStats {
	if s.historyStat == nil {
		s.update()
	}
	return s.historyStat
}

func (s *MarketHistory) GetStat() *Statistic {
	if s.stat == nil {
		s.update()
	}
	return s.stat
}

func (s *MarketHistory) updateStat() {
	if s.stat == nil {
		s.stat = &Statistic{}
	}
	s.stat.mu.Lock()
	s.stat.QuantityRate = s.getQuantityRate()
	s.stat.QuoteQuantityRate = s.getQuoteQuantityRate()
	s.stat.mu.Unlock()
}

func (s *MarketHistory) getQuantityRate() float64 {
	stat := s.Get()
	quantityStr := stat.Volume
	quantity, err := strconv.ParseFloat(quantityStr, 64)
	if err != nil {
		log.WithError(err).Errorf("[marketstats] failed to parse quantity: %s", quantityStr)
		return 0
	}
	timeDiff := stat.CloseTime.Sub(stat.OpenTime)
	if timeDiff == 0 {
		log.Errorf("[marketstats] time diff is zero")
		return 0
	}
	return quantity / timeDiff.Seconds()
}

func (s *MarketHistory) getQuoteQuantityRate() float64 {
	stat := s.Get()
	quantityStr := stat.QuoteVolume
	quantity, err := strconv.ParseFloat(quantityStr, 64)
	if err != nil {
		log.WithError(err).Errorf("[marketstats] failed to parse quote quantity: %s", quantityStr)
		return 0
	}
	timeDiff := stat.CloseTime.Sub(stat.OpenTime)
	if timeDiff == 0 {
		log.Errorf("[marketstats] time diff is zero")
		return 0
	}
	return quantity / timeDiff.Seconds()
}

func (s *MarketHistory) watchdog() {
	ticker := time.NewTicker(s.UpdateInterval)
	for range ticker.C {
		s.update()
	}
}

func (s *MarketHistory) update() {
	var err error
	s.mu.Lock()
	s.historyStat, err = s.session.Exchange.(types.ExchangeMarketDataService).Query24hrMarketStat(context.Background(), s.Symbol)
	if err != nil {
		log.WithError(err).Errorf("failed to query 24hr market stat")
	}
	s.mu.Unlock()
	s.updateStat()
}
