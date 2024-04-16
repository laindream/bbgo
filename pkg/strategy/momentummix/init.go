package momentummix

import (
	"context"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/tick"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"time"
)

var log = logrus.New()

func (s *Strategy) Init(ctx context.Context, session *bbgo.ExchangeSession) error {
	s.InitLogger()
	if err := s.InitFeeRates(ctx, session); err != nil {
		return err
	}
	s.InitSearchDepth()
	s.InitLastTriggerTime()
	if err := s.InitAggKline(session); err != nil {
		return err
	}
	s.InitTickKline()
	return nil
}

func (s *Strategy) InitLogger() {
	logFilePath := filepath.Join("log", "momentum_mix.log")

	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			log.Infof("Log directory does not exist, creating: %s", filepath.Dir(logFilePath))
			if err := os.MkdirAll(filepath.Dir(logFilePath), 0755); err != nil {
				log.Fatalf("Failed to create log directory: %s, error: %v", filepath.Dir(logFilePath), err)
			}
			file, err = os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				log.Fatalf("Failed to open log file: %s, error: %v", logFilePath, err)
			}
		}
	}
	log.Out = file
}

func (s *Strategy) InitFeeRates(ctx context.Context, session *bbgo.ExchangeSession) error {
	s.FeeRates = make(map[string]types.ExchangeFee)
	for _, symbol := range s.Symbols {
		rates, err := session.Exchange.(types.ExchangeDefaultFeeRates).GetFeeRates(ctx, symbol)
		if err != nil {
			return err
		}
		s.FeeRates[symbol] = rates
		log.Infof("[%s]FeeRate: %s", symbol, s.FeeRates[symbol])
		time.Sleep(2 * time.Second)
	}
	return nil
}

func (s *Strategy) InitTickKline() {
	s.TickKline = make(map[string]*tick.Kline)
	for _, symbol := range s.Symbols {
		s.TickKline[symbol] = tick.NewKline(10)
	}
}

func (s *Strategy) InitAggKline(session *bbgo.ExchangeSession) error {
	s.AggKline = make(map[string]*aggtrade.Kline)
	for _, symbol := range s.Symbols {
		aggKline, err := aggtrade.NewKline(10, *s.InfluxDB, symbol, session.Exchange.Name())
		if err != nil {
			return err
		}
		s.AggKline[symbol] = aggKline
	}
	return nil
}

func (s *Strategy) InitSearchDepth() {
	s.SearchDepth = make(map[string]int)
	for _, symbol := range s.Symbols {
		s.SearchDepth[symbol] = DefaultSearchDepth
	}
}

func (s *Strategy) InitLastTriggerTime() {
	s.LastTriggerTime = make(map[string]time.Time)
}
