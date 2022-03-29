package pvd

import (
	"context"
	"fmt"
	"time"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

var zeroTime time.Time

type PVDotSet struct {
	types.IntervalWindow
	session         *bbgo.ExchangeSession
	BaseCurrency    string
	QuoteCurrencies []string

	Indicators map[string]*PVDot
}

func (i *PVDotSet) InitIndicators(ctx context.Context) error {
	i.Indicators = make(map[string]*PVDot)
	for _, quoteCurrency := range i.QuoteCurrencies {
		symbol := quoteCurrency + i.BaseCurrency
		i.Indicators[quoteCurrency] = &PVDot{Symbol: symbol, IntervalWindow: i.IntervalWindow}
	}

	fmt.Println(i.QuoteCurrencies)

	for _, indicator := range i.Indicators {
		endTime := time.Now()
		options := types.KLineQueryOptions{Limit: i.Window, EndTime: &endTime}
		klines, err := i.session.Exchange.QueryKLines(ctx, indicator.Symbol, i.Interval, options)
		if err != nil {
			return err
		}
		indicator.UpdateFromKLines(klines)
	}
	return nil
}
func (i *PVDotSet) getIndicator(symbol string) (*PVDot, error) {
	for _, indicator := range i.Indicators {
		if symbol == indicator.Symbol {
			return indicator, nil
		}
	}
	return nil, fmt.Errorf("indicator with symbol: %s not found", symbol)
}

func (i *PVDotSet) Update(kline types.KLine) error {
	inc, err := i.getIndicator(kline.Symbol)
	if err != nil {
		return err
	}
	klines := []types.KLine{kline}
	inc.UpdateFromKLines(klines)
	return nil
}

func (i *PVDotSet) TargetWeights() map[string]fixedpoint.Value {
	targetWeights := make(map[string]fixedpoint.Value)
	for quoteCurrency, indicator := range i.Indicators {
		targetWeights[quoteCurrency] = fixedpoint.NewFromFloat(indicator.Last())
	}
	return Normalize(targetWeights)
}

// price volume avg
type PVDot struct {
	Symbol string

	types.IntervalWindow
	Values  types.Float64Slice
	Prices  types.Float64Slice
	Volumes types.Float64Slice

	EndTime         time.Time
	UpdateCallbacks []func(value float64)
}

func (inc *PVDot) Last() float64 {
	if len(inc.Values) == 0 {
		return 0
	}
	return inc.Values[len(inc.Values)-1]
}

func (inc *PVDot) Update(price, volume float64) {
	inc.Prices.Push(price)
	inc.Volumes.Push(volume)

	pva := inc.Prices.Tail(inc.Window).Dot(inc.Volumes.Tail(inc.Window)) / float64(inc.Window)
	inc.Values.Push(pva)
}

func (inc *PVDot) UpdateFromKLines(klines []types.KLine) {

	for _, k := range klines {
		if inc.EndTime != zeroTime && k.EndTime.Before(inc.EndTime) {
			continue
		}
		inc.Update(k.Close.Float64(), k.Volume.Float64())
	}
	inc.EndTime = klines[len(klines)-1].EndTime.Time()
}
