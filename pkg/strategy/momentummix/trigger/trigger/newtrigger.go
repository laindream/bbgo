package trigger

import (
	"github.com/c9s/bbgo/pkg/strategy/momentummix/trigger/trigger/context"
	"github.com/c9s/bbgo/pkg/strategy/momentummix/trigger/trigger/factor"
)

type GeneralizedTrigger struct {
	Symbol  string
	Ctx     *context.Context
	Factors []factor.Factor
}
