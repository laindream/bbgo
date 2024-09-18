package context

import (
	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/history"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/aggtrade"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/kline/tick"
	"github.com/c9s/bbgo/pkg/types"
)

type Context struct {
	Symbol    string
	History   *history.MarketHistory
	TickKline *tick.Kline
	AggKline  *aggtrade.Kline
	Fee       types.ExchangeFee
	Session   *bbgo.ExchangeSession
}
