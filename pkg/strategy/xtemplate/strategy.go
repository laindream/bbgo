package xtemplate

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/types"
)

const ID = "xtemplate"

var log = logrus.WithField("strategy", ID)

func init() {
	bbgo.RegisterStrategy(ID, &Strategy{})
}

type Strategy struct {
	Environment *bbgo.Environment
	Market      types.Market

	Symbol   string         `json:"symbol"`
	Interval types.Interval `json:"interval"`
}

func (s *Strategy) ID() string {
	return ID
}

func (s *Strategy) CrossSubscribe(sessions map[string]*bbgo.ExchangeSession) {
	for _, session := range sessions {
		session.Subscribe(types.KLineChannel, s.Symbol, types.SubscribeOptions{Interval: s.Interval})
	}
}

func (s *Strategy) CrossRun(ctx context.Context, _ bbgo.OrderExecutionRouter, sessions map[string]*bbgo.ExchangeSession) error {
	bbgo.OnShutdown(ctx, func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		bbgo.Sync(ctx, s)
	})

	for _, session := range sessions {
		session.MarketDataStream.OnKLineClosed(func(kline types.KLine) {
			log.Infof("session: %s, kline: %s", session.Exchange.Name().String(), kline.String())
		})
	}
	return nil
}
