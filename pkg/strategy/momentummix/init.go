package momentummix

import (
	"context"
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/capture/imbalance"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/history"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/tick"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/trigger/set"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/trigger/trigger"
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
	s.StartTime = time.Now()
	s.InitLogger()

	accountStatus, err := s.GetAccountStatus(ctx, session)
	if err != nil {
		log.Warnf("Failed to get account status: %v", err)
	}
	log.Infof("Account Status: %s", accountStatus)

	if err := s.InitFeeRates(ctx, session); err != nil {
		return err
	}
	s.InitHistoryMarketStats(ctx, session)
	s.InitSearchDepth()
	s.InitLastTriggerTime()
	if err := s.InitAggKline(session); err != nil {
		return err
	}
	if err := s.InitTickKline(session); err != nil {
		return err
	}
	s.InitCapture(session)
	s.InitQuoteQuantityExceedTrigger()
	return nil
}

func (s *Strategy) GetAccountStatus(ctx context.Context, session *bbgo.ExchangeSession) (string, error) {
	accountStatus, err := session.Exchange.GetAccountStatus(ctx)
	if err != nil {
		return "", err
	}
	return accountStatus, nil
}

var DefaultAdaptNearQuantityRate = 5.0

func (s *Strategy) InitQuoteQuantityExceedTrigger() {
	s.QuoteQuantityExceedTriggers = make(map[string]*trigger.QuoteQuantityExceedTrigger)
	for _, symbol := range s.Symbols {
		trig := &trigger.QuoteQuantityExceedTrigger{
			Symbol:                                 symbol,
			AdaptTriggerNearQuoteQuantityRateRatio: 5,
			AdaptKeepNearQuoteQuantityRateRatio:    3,
			FirstTriggerTime:                       nil,
			FinalTriggerTime:                       nil,
			LastTriggerTime:                        nil,
			//Capture:                                s.Captures[symbol],
			Fee:                        s.FeeRates[symbol],
			History:                    s.HistoryMarketStats[symbol],
			AggKline:                   s.AggKline[symbol],
			TickKline:                  s.TickKline[symbol],
			MinTriggerWindowTradeCount: 15,
			MaxTriggerWindowTradeCount: 75,
			MinKeepWindowTradeCount:    20,
			MaxKeepWindowTradeCount:    100,
			MinTriggerNearDuration:     250 * time.Millisecond,
			TriggerNearDuration:        500 * time.Millisecond,
			MaxTriggerNearDuration:     8000 * time.Millisecond,
			MinKeepNearDuration:        1000 * time.Millisecond,
			KeepNearDuration:           1000 * time.Millisecond,
			MaxKeepNearDuration:        5000 * time.Millisecond,
			MinKeepDuration:            10000 * time.Millisecond,
			MaxKeepDuration:            100000 * time.Millisecond,
			MinTriggerInterval:         20 * time.Minute,
			MaxTriggerInterval:         60 * time.Minute,
			//OnTrigger:                  func(book types.BookTicker) {},
			//OnUnTrigger:                func(book types.BookTicker) {},
			//OnKeepTrigger:              func(book types.BookTicker) {},
			Logger: log,

			MinImbalanceThresholdKeepRate:    1.5,
			MaxImbalanceThresholdKeepRate:    750,
			MinImbalanceThresholdTriggerRate: 2.0,
			ImbalanceThresholdTriggerRate:    6.0,
			MaxImbalanceThresholdTriggerRate: 1000.0,
			MinStopLossRate:                  0.0035,
			StopLossRate:                     0.0035,
			MaxStopLossRate:                  0.028,
			MinNearPriceFluctuationRate:      0.0016,
			NearPriceFluctuationRate:         0.0016,
			MaxNearPriceFluctuationRate:      0.0128,
			NearPriceFluctuationDuration:     10 * time.Second,
			MinFarPriceFluctuationRate:       0.0025,
			FarPriceFluctuationRate:          0.0025,
			MaxFarPriceFluctuationRate:       0.020,
			FarPriceFluctuationDuration:      30 * time.Second,
			TakeProfitRate:                   0.3,
			MinProfitThresholdRate:           0.006,
			ProfitThresholdRate:              0.006,
			MaxProfitThresholdRate:           0.048,
			//ReverseImbalanceThresholdTriggerRate: 0.5,
		}
		trig.OnTrigger = func(book types.BookTicker, profit float64) {
			s.TriggerSet.OnTrigger(book)
		}
		trig.OnUnTrigger = func(book types.BookTicker, profit float64) {
			s.TriggerSet.OnUnTrigger(book, profit)
		}
		trig.OnKeepTrigger = func(book types.BookTicker, profit float64) {
		}
		s.QuoteQuantityExceedTriggers[symbol] = trig
	}
	s.TriggerSet = &set.TriggerSet{
		QuoteQuantityExceedTriggers: s.QuoteQuantityExceedTriggers,
		TotalValueRank:              []string{},
		Logger:                      log,
	}
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
	log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.00000",
		FullTimestamp:   true,
	})
	log.Out = io.MultiWriter(os.Stdout, file)
}

func (s *Strategy) InitHistoryMarketStats(ctx context.Context, session *bbgo.ExchangeSession) {
	s.HistoryMarketStats = make(map[string]*history.MarketHistory)
	for _, symbol := range s.Symbols {
		marketStats := history.NewMarketHistory(symbol, 1*time.Hour, session)
		marketStats.Symbol = symbol
		s.HistoryMarketStats[symbol] = marketStats
	}
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
		tickKline, err := tick.NewKline(2, s.InfluxDB, symbol, session.Exchange.Name())
		if err != nil {
			return errors.Wrapf(err, "failed to create tick kline for symbol: %s,", symbol)
		}
		//tickKline.IsEnablePersist = true
		s.TickKline[symbol] = tickKline
	}
	return nil
}

func (s *Strategy) InitAggKline(session *bbgo.ExchangeSession) error {
	s.AggKline = make(map[string]*aggtrade.Kline)
	for _, symbol := range s.Symbols {
		aggKline, err := aggtrade.NewKline(2, s.InfluxDB, symbol, session.Exchange.Name())
		if err != nil {
			return errors.Wrapf(err, "failed to create aggtrade kline for symbol: %s,", symbol)
		}
		//aggKline.IsEnablePersist = true
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

func (s *Strategy) InitCapture(session *bbgo.ExchangeSession) {
	s.Captures = make(map[string]*imbalance.Capture)
	for _, symbol := range s.Symbols {
		capture := imbalance.NewCapture(s.CaptureConfig, symbol, session.Exchange.Name(), s.AggKline[symbol], s.TickKline[symbol])
		s.Captures[symbol] = capture
	}
}
