package pvd

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

const ID = "pvdot"

var log = logrus.WithField("strategy", ID)

func init() {
	bbgo.RegisterStrategy(ID, &Strategy{})
}

func Sum(m map[string]fixedpoint.Value) fixedpoint.Value {
	sum := fixedpoint.NewFromFloat(0.0)
	for _, v := range m {
		sum = sum.Add(v)
	}
	return sum
}

func Normalize(m map[string]fixedpoint.Value) map[string]fixedpoint.Value {
	sum := Sum(m)
	if sum.Float64() == 1.0 {
		return m
	}

	normalized := make(map[string]fixedpoint.Value)
	for k, v := range m {
		normalized[k] = v.Div(sum)
	}
	return normalized
}

func ElementwiseProduct(m1, m2 map[string]fixedpoint.Value) map[string]fixedpoint.Value {
	m := make(map[string]fixedpoint.Value)
	for k, v := range m1 {
		m[k] = v.Mul(m2[k])
	}
	return m
}

type Strategy struct {
	Notifiability *bbgo.Notifiability

	Interval        types.Interval   `json:"interval"`
	Window          int              `json:"window"`
	BaseCurrency    string           `json:"baseCurrency"`
	QuoteCurrencies []string         `json:"quoteCurrencies"`
	Threshold       fixedpoint.Value `json:"threshold"`
	IgnoreLocked    bool             `json:"ignoreLocked"`
	Verbose         bool             `json:"verbose"`
	DryRun          bool             `json:"dryRun"`

	// max amount to buy or sell per order
	MaxAmount fixedpoint.Value `json:"maxAmount"`

	set PVDotSet
}

func (s *Strategy) ID() string {
	return ID
}

func (s *Strategy) Validate() error {
	if s.Threshold.Sign() < 0 {
		return fmt.Errorf("threshold should not less than 0")
	}

	if s.MaxAmount.Sign() < 0 {
		return fmt.Errorf("maxAmount shoud not less than 0")
	}

	return nil
}

func (s *Strategy) Subscribe(session *bbgo.ExchangeSession) {
	for _, symbol := range s.getSymbols() {
		session.Subscribe(types.KLineChannel, symbol, types.SubscribeOptions{Interval: s.Interval})
	}
}

func (s *Strategy) Run(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) error {
	iw := types.IntervalWindow{Interval: s.Interval, Window: s.Window}
	s.set = PVDotSet{IntervalWindow: iw, session: session, BaseCurrency: s.BaseCurrency, QuoteCurrencies: s.QuoteCurrencies}
	err := s.set.InitIndicators(ctx)
	if err != nil {
		return err
	}

	session.MarketDataStream.OnKLineClosed(func(kline types.KLine) {
		s.set.Update(kline)
		s.rebalance(ctx, orderExecutor, session)
	})
	return nil
}

func (s *Strategy) rebalance(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) {
	targetWeights := s.set.TargetWeights()

	prices, err := s.getPrices(ctx, session, targetWeights)
	if err != nil {
		return
	}

	balances := session.Account.Balances()
	quantities := s.getQuantities(balances, targetWeights)
	marketValues := ElementwiseProduct(prices, quantities)

	orders := s.generateSubmitOrders(prices, marketValues, targetWeights)
	for _, order := range orders {
		log.Infof("generated submit order: %s", order.String())
	}

	if s.DryRun {
		return
	}

	_, err = orderExecutor.SubmitOrders(ctx, orders...)
	if err != nil {
		log.WithError(err).Error("submit order error")
		return
	}
}

func (s *Strategy) getPrices(ctx context.Context, session *bbgo.ExchangeSession, targetWeights map[string]fixedpoint.Value) (map[string]fixedpoint.Value, error) {
	prices := make(map[string]fixedpoint.Value)

	for currency := range targetWeights {
		if currency == s.BaseCurrency {
			prices[currency] = fixedpoint.One
			continue
		}

		symbol := currency + s.BaseCurrency
		ticker, err := session.Exchange.QueryTicker(ctx, symbol)
		if err != nil {
			s.Notifiability.Notify("query ticker error: %s", err.Error())
			log.WithError(err).Error("query ticker error")
			return prices, err
		}

		prices[currency] = ticker.Last
	}
	return prices, nil
}

func (s *Strategy) getQuantities(balances types.BalanceMap, targetWeights map[string]fixedpoint.Value) map[string]fixedpoint.Value {
	quantities := make(map[string]fixedpoint.Value)
	for currency := range targetWeights {
		if s.IgnoreLocked {
			quantities[currency] = balances[currency].Total()
		} else {
			quantities[currency] = balances[currency].Available
		}
	}
	return quantities
}

func (s *Strategy) generateSubmitOrders(prices, marketValues map[string]fixedpoint.Value, targetWeights map[string]fixedpoint.Value) []types.SubmitOrder {
	var submitOrders []types.SubmitOrder

	currentWeights := Normalize(marketValues)
	totalValue := Sum(marketValues)

	log.Infof("total value: %f", totalValue.Float64())

	for currency, targetWeight := range targetWeights {
		if currency == s.BaseCurrency {
			continue
		}
		symbol := currency + s.BaseCurrency
		currentWeight := currentWeights[currency]
		currentPrice := prices[currency]
		log.Infof("%s price: %v, current weight: %v, target weight: %v",
			symbol,
			currentPrice,
			currentWeight,
			targetWeight)

		// calculate the difference between current weight and target weight
		// if the difference is less than threshold, then we will not create the order
		weightDifference := targetWeight.Sub(currentWeight)
		if weightDifference.Abs().Compare(s.Threshold) < 0 {
			log.Infof("%s weight distance |%v - %v| = |%v| less than the threshold: %v",
				symbol,
				currentWeight,
				targetWeight,
				weightDifference,
				s.Threshold)
			continue
		}

		quantity := weightDifference.Mul(totalValue).Div(currentPrice)

		side := types.SideTypeBuy
		if quantity.Sign() < 0 {
			side = types.SideTypeSell
			quantity = quantity.Abs()
		}

		if s.MaxAmount.Sign() > 0 {
			quantity = bbgo.AdjustQuantityByMaxAmount(quantity, currentPrice, s.MaxAmount)
			log.Infof("adjust the quantity %v (%s %s @ %v) by max amount %v",
				quantity,
				symbol,
				side.String(),
				currentPrice,
				s.MaxAmount)
		}

		order := types.SubmitOrder{
			Symbol:   symbol,
			Side:     side,
			Type:     types.OrderTypeMarket,
			Quantity: quantity}

		submitOrders = append(submitOrders, order)
	}
	return submitOrders
}

func (s *Strategy) getSymbols() []string {
	var symbols []string
	for _, c := range s.QuoteCurrencies {
		symbols = append(symbols, c+s.BaseCurrency)
	}
	return symbols
}
