package momentummix

import (
	"context"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/tick"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"time"
)

var log = logrus.New()

func (s *Strategy) Init(ctx context.Context, session *bbgo.ExchangeSession) error {
	s.InitLogger()

	accountStatus, err := s.GetAccountStatus(ctx, session)
	if err != nil {
		log.Warnf("Failed to get account status: %v", err)
	}
	log.Infof("Account Status: %s", accountStatus)

	if err := s.InitFeeRates(ctx, session); err != nil {
		return err
	}
	s.InitSearchDepth()
	s.InitLastTriggerTime()
	if err := s.InitAggKline(session); err != nil {
		return err
	}
	if err := s.InitTickKline(session); err != nil {
		return err
	}
	return nil
}

func (s *Strategy) GetAccountStatus(ctx context.Context, session *bbgo.ExchangeSession) (string, error) {
	accountStatus, err := session.Exchange.GetAccountStatus(ctx)
	if err != nil {
		return "", err
	}
	return accountStatus, nil
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
	log.Out = io.MultiWriter(os.Stdout, file)
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

func (s *Strategy) InitTickKline(session *bbgo.ExchangeSession) error {
	s.TickKline = make(map[string]*tick.Kline)
	for _, symbol := range s.Symbols {
		tickKline, err := tick.NewKline(10, *s.InfluxDB, symbol, session.Exchange.Name())
		if err != nil {
			return errors.Wrapf(err, "failed to create tick kline for symbol: %s,", symbol)
		}
		tickKline.IsEnablePersist = true
		s.TickKline[symbol] = tickKline
	}
	return nil
}

func (s *Strategy) InitAggKline(session *bbgo.ExchangeSession) error {
	s.AggKline = make(map[string]*aggtrade.Kline)
	for _, symbol := range s.Symbols {
		aggKline, err := aggtrade.NewKline(10, *s.InfluxDB, symbol, session.Exchange.Name())
		if err != nil {
			return errors.Wrapf(err, "failed to create aggtrade kline for symbol: %s,", symbol)
		}
		aggKline.IsEnablePersist = true
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
